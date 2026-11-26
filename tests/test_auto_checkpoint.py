import concurrent.futures
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_events import Binding, COMMAND, REPOSITORY, event_id, main, process
from orchestrator.auto_state import REFS, PairedState
from orchestrator.state import StateConflict


class AutoCheckpointTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.remote = self.root / "state.git"
        self.git("init", "--bare", "--quiet", str(self.remote))
        self.anchors = PairedState(str(self.remote), "fixture").bootstrap()
        self.state = self.new_state()
        self.binding = Binding(50, 2, 41, 42, 99)

    def git(self, *args, **kwargs):
        return subprocess.run(["git", "-C", str(self.root), *args], capture_output=True, check=True, **kwargs)

    def new_state(self, cls=PairedState):
        return cls(str(self.remote), "fixture", self.anchors)

    def tips(self):
        return tuple(self.git("-C", str(self.remote), "rev-parse", *REFS).stdout.decode().split())

    def event(self, identifier=101):
        return {
            "action": "created", "repository": {"id": 50, "full_name": REPOSITORY},
            "issue": {"number": 2, "state": "open", "pull_request": {}},
            "sender": {"id": 41}, "comment": {"id": identifier, "user": {"id": 41}, "body": COMMAND},
        }

    @staticmethod
    def add(ledger, identifier):
        with sqlite3.connect(ledger) as db:
            db.execute("INSERT INTO auto_inbox (id) VALUES (?)", (identifier,))

    @staticmethod
    def inspect(ledger):
        with sqlite3.connect(ledger) as db:
            return db.execute("SELECT id,consumed FROM auto_inbox ORDER BY id").fetchall()

    def test_valid_event_replay_and_distinct_replies(self):
        sent = []

        def publish(body):
            sent.append(body)
            self.assertEqual(self.new_state().update(self.inspect), [(101, 1)])
            return len(sent)

        self.assertTrue(process(self.state, self.event(), self.binding, "41", publish))
        after = self.tips()
        self.assertTrue(process(self.new_state(), self.event(), self.binding, "41", publish))
        self.assertEqual(after, self.tips())
        self.assertEqual([body.splitlines()[0] for body in sent],
                         ["[ORCHESTRATOR SYNTHETIC ACK]", "[ORCHESTRATOR SYNTHETIC RESULT]"])

    def test_reject_foreign_scope_and_loop_markers_without_touching_state(self):
        for section, key, value in (
            ("repository", "id", 60), ("repository", "full_name", "other/repo"),
            ("issue", "number", 3), ("issue", "pull_request", None), ("issue", "state", "closed"),
            ("sender", "id", 99), ("sender", "id", 42), ("comment", "user", {"id": 42}),
            ("comment", "body", "[ORCHESTRATOR SYNTHETIC ACK]"),
            ("comment", "body", COMMAND + "\nexecute other instructions"),
        ):
            with self.subTest(section=section, key=key, value=value):
                event = self.event()
                event[section][key] = value
                self.assertFalse(process(None, event, self.binding, "41", None))
        event = self.event()
        event["action"] = "edited"
        self.assertIsNone(event_id(event, self.binding, "41"))
        self.assertIsNone(event_id(self.event(), self.binding, "99"))
        self.assertEqual(self.tips(), self.anchors)

    def test_missing_ref_stops_without_rebuilding(self):
        self.git("-C", str(self.remote), "update-ref", "-d", REFS[1])
        with self.assertRaisesRegex(StateConflict, "missing"):
            self.state.update(lambda _: self.fail("mutation called"))
        with self.assertRaises(StateConflict):
            PairedState(str(self.remote), "fixture").bootstrap()

    def test_rollback_of_one_ref_stops(self):
        self.state.update(lambda ledger: self.add(ledger, 101))
        self.git("-C", str(self.remote), "update-ref", REFS[0], self.anchors[0])
        with self.assertRaises(StateConflict):
            self.state.update(lambda _: self.fail("mutation called"))

    def test_pointer_mismatch_and_corrupt_snapshot_fail_before_mutation(self):
        for damaged in (0, 1):
            with self.subTest(damaged=damaged):
                state = self.new_state()
                with tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    state._git(root, "init", "--bare", "--quiet")
                    state._load(root, self.anchors)
                    if not damaged:
                        before = state._tips(root)
                        (root / "ledger.sqlite").write_bytes(b"invalid sqlite")
                        self.assertEqual(state._advance(root, before, 2)[1].returncode, 0)
                    else:
                        before = state._tips(root)
                        bad = state._commit(root, "checkpoint.json", b"{}", before[1], "Fixture corruption")
                        state._git(root, "push", str(self.remote), f"{bad}:{REFS[1]}")
                with self.assertRaises(StateConflict):
                    self.new_state().update(lambda _: self.fail("mutation called"))
                for ref, sha in zip(REFS, self.anchors, strict=True):
                    self.git("-C", str(self.remote), "update-ref", ref, sha)

    def test_atomic_rejection_of_witness_never_publishes(self):
        hook = self.remote / "hooks" / "update"
        hook.write_text(f'#!/bin/sh\ntest "$1" != {REFS[1]}\n')
        hook.chmod(0o700)
        with self.assertRaisesRegex(StateConflict, "not confirmed"):
            process(self.state, self.event(), self.binding, "41", lambda _: self.fail("published"))
        self.assertEqual(self.tips(), self.anchors)

    def test_concurrent_updates_replay_both_events(self):
        barrier = threading.Barrier(2)

        def write(identifier):
            first = True

            def insert(path):
                nonlocal first
                self.add(path, identifier)
                if first:
                    first = False
                    barrier.wait(10)

            self.new_state().update(insert)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            list(pool.map(write, (101, 102)))
        self.assertEqual(self.state.update(self.inspect), [(101, 0), (102, 0)])

    def test_loss_after_claim_with_peer_advance_does_not_authorize_send(self):
        test = self

        class LostPush(PairedState):
            def _git(self, root, *args, **kwargs):
                result = super()._git(root, *args, **kwargs)
                if "push" in args and not result.returncode and (root / "ledger.sqlite").is_file():
                    with sqlite3.connect(root / "ledger.sqlite") as db:
                        claimed = db.execute("SELECT status FROM auto_outbox WHERE id='101:ACK'").fetchone()
                    if claimed == ("claimed",):
                        test.new_state().update(lambda ledger: test.add(ledger, 102))
                        raise subprocess.TimeoutExpired("simulated lost push confirmation", 45)
                return result

        with self.assertRaises(StateConflict):
            process(self.new_state(LostPush), self.event(), self.binding, "41", lambda _: self.fail("published"))
        self.assertTrue(process(self.new_state(), self.event(), self.binding, "41",
                                lambda _: self.fail("published after ambiguous claim")))
        self.assertEqual(self.state.update(self.inspect), [(101, 1), (102, 0)])

    def test_abrupt_exit_after_inbox_commit_recovers(self):
        program = """
import os, sys
from orchestrator.auto_events import _enqueue
from orchestrator.auto_state import PairedState
state = PairedState(sys.argv[1], "fixture", tuple(sys.argv[2:]))
state.update(lambda path: _enqueue(path, 101))
os._exit(27)
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.remote), *self.anchors],
                               env={**os.environ, "TMPDIR": str(self.root)}, timeout=20)
        self.assertEqual(child.returncode, 27)
        self.assertEqual(self.state.update(self.inspect), [(101, 0)])
        sent = []
        self.assertTrue(process(self.new_state(), self.event(), self.binding, "41",
                                lambda body: sent.append(body) or len(sent)))
        self.assertEqual(len(sent), 2)

    def test_lost_comment_response_is_reconciled_without_repost(self):
        posted = {}

        def broken(body):
            posted[body] = 123
            raise TimeoutError()

        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            process(self.state, self.event(), self.binding, "41", broken)
        sent = []
        self.assertTrue(process(self.new_state(), self.event(), self.binding, "41",
                                lambda body: sent.append(body) or 124, posted.get))
        self.assertEqual(len(sent), 1)
        self.assertIn("SYNTHETIC RESULT", sent[0])

    def test_paired_rollback_cannot_discard_observed_floor_in_one_runner(self):
        self.state.update(lambda ledger: self.add(ledger, 101))
        for ref, sha in zip(REFS, self.anchors, strict=True):
            self.git("-C", str(self.remote), "update-ref", ref, sha)
        with self.assertRaisesRegex(StateConflict, "rolled back"):
            self.state.update(lambda _: self.fail("mutation called"))

    def test_event_entry_point_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True), patch("orchestrator.auto_events.PairedState") as state:
            main()
            state.assert_not_called()
