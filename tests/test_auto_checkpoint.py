import concurrent.futures
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_events import (
    ACTIONS_BOT_ID, ACTIONS_BOT_LOGIN, AUTHOR_ACCOUNT, COMMAND, REPOSITORY, SHARED_MODE,
    Binding, ReplyUnconfirmed, event_id, github_publisher, main, process,
)
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

        with self.assertRaises(ReplyUnconfirmed):
            process(self.new_state(LostPush), self.event(), self.binding, "41", lambda _: self.fail("published"))
        with self.assertRaisesRegex(ReplyUnconfirmed, "BLOCKED/UNCONFIRMED"):
            process(self.new_state(), self.event(), self.binding, "41",
                    lambda _: self.fail("published after ambiguous claim"))
        self.assertEqual(self.state.update(self.inspect), [(101, 1), (102, 0)])

    def test_abrupt_exit_after_inbox_commit_recovers(self):
        program = """
import os, sys
from orchestrator.auto_events import Binding, _enqueue
from orchestrator.auto_state import PairedState
state = PairedState(sys.argv[1], "fixture", tuple(sys.argv[2:]))
state.update(lambda path: _enqueue(path, 101, Binding(50, 2, 41, 42, 99)))
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

        with self.assertRaisesRegex(ReplyUnconfirmed, "BLOCKED/UNCONFIRMED"):
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

    def test_same_event_concurrent_replay_blocks_until_first_reply_is_confirmed(self):
        entered = threading.Event()
        release = threading.Event()
        posted = []

        def publish(body):
            posted.append(body)
            if len(posted) == 1:
                entered.set()
                self.assertTrue(release.wait(10))
            return len(posted)

        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            first = pool.submit(process, self.new_state(), self.event(), self.binding, "41", publish)
            try:
                self.assertTrue(entered.wait(10))
                with self.assertRaisesRegex(ReplyUnconfirmed, "BLOCKED/UNCONFIRMED"):
                    process(self.new_state(), self.event(), self.binding, "41",
                            lambda _: self.fail("same event published concurrently"))
            finally:
                release.set()
            self.assertTrue(first.result(timeout=10))
        self.assertTrue(process(self.new_state(), self.event(), self.binding, "41", publish))
        self.assertEqual(len(posted), 2)

    def test_binding_changes_reject_replay_before_lookup_or_send(self):
        with self.assertRaises(ReplyUnconfirmed):
            process(self.state, self.event(), self.binding, "41",
                    lambda _: (_ for _ in ()).throw(TimeoutError()))
        before = self.tips()
        cases = (
            Binding(51, 2, 41, 42, 99), Binding(50, 3, 41, 42, 99),
            Binding(50, 2, 43, 42, 99), Binding(50, 2, 41, 44, 99),
        )
        for changed in cases:
            with self.subTest(binding=changed):
                event = self.event()
                event["repository"]["id"] = changed.repository_id
                event["issue"]["number"] = changed.pr
                event["sender"]["id"] = event["comment"]["user"]["id"] = changed.sender
                with self.assertRaisesRegex(StateConflict, "persisted synthetic binding"):
                    process(self.new_state(), event, changed, str(changed.sender),
                            lambda _: self.fail("replayed to different binding"),
                            lambda _: self.fail("looked up reply in a different binding"))
                self.assertEqual(self.tips(), before)

    def test_interrupted_after_claim_blocks_replay_without_reposting(self):
        program = """
import os, sys
from orchestrator.auto_events import Binding, _claim, _consume, _enqueue
from orchestrator.auto_state import PairedState
state = PairedState(sys.argv[1], "fixture", tuple(sys.argv[2:]))
state.update(lambda path: _enqueue(path, 101, Binding(50, 2, 41, 42, 99)))
state.update(lambda path: _consume(path, 101))
assert state.update(lambda path: _claim(path, "101:ACK"))
os._exit(28)
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.remote), *self.anchors],
                               env={**os.environ, "TMPDIR": str(self.root)}, timeout=20)
        self.assertEqual(child.returncode, 28)
        before = self.tips()
        with self.assertRaisesRegex(ReplyUnconfirmed, "BLOCKED/UNCONFIRMED"):
            process(self.new_state(), self.event(), self.binding, "41",
                    lambda _: self.fail("interrupted claim silently reposted"), lambda _: None)
        self.assertEqual(self.tips(), before)

    def test_ambiguous_recovery_returns_blocked(self):
        body = "[ORCHESTRATOR SYNTHETIC ACK]\n\nSynthetic test only. Reply ID: `101:ACK`."
        with patch("orchestrator.auto_events.urllib.request.build_opener") as build:
            build.return_value.open.return_value = io.BytesIO(json.dumps([
                {"id": 7, "user": {"id": 42}, "body": body},
                {"id": 8, "user": {"id": 42}, "body": body},
            ]).encode())
            _, find = github_publisher(self.binding, "fixture")
            with self.assertRaises(ReplyUnconfirmed):
                process(self.state, self.event(), self.binding, "41",
                        lambda _: (_ for _ in ()).throw(TimeoutError()))
            with self.assertRaisesRegex(ReplyUnconfirmed, "no unique authenticated"):
                process(self.new_state(), self.event(), self.binding, "41",
                        lambda _: self.fail("ambiguous reply was reposted"), find)
            self.assertEqual(build.return_value.open.call_count, 1)

    def test_publish_checks_identity_open_pr_and_bound_repository_before_post(self):
        target = {"state": "open", "number": 2, "base": {"repo": {"id": 50, "full_name": REPOSITORY}}}
        for owner, metadata in ((99, target), (42, {**target, "state": "closed"}),
                                (42, {**target, "number": 3}),
                                (42, {**target, "base": {"repo": {"id": 51, "full_name": REPOSITORY}}})):
            with self.subTest(owner=owner, metadata=metadata), patch(
                "orchestrator.auto_events.urllib.request.build_opener",
            ) as build:
                opener = build.return_value
                opener.open.side_effect = [io.BytesIO(json.dumps(value).encode())
                                           for value in [{"id": owner}, metadata]]
                publish, _ = github_publisher(self.binding, "fixture")
                with self.assertRaisesRegex(ReplyUnconfirmed, "BLOCKED/UNCONFIRMED"):
                    publish("synthetic")
                self.assertTrue(all(call.args[0].get_method() == "GET" for call in opener.open.call_args_list))
        with patch("orchestrator.auto_events.urllib.request.build_opener") as build:
            opener = build.return_value
            opener.open.side_effect = [io.BytesIO(json.dumps(value).encode()) for value in
                                       [{"id": 42}, target, {"id": 7, "user": {"id": 42}}]]
            publish, _ = github_publisher(self.binding, "fixture")
            self.assertEqual(publish("synthetic"), 7)
            self.assertEqual([call.args[0].get_method() for call in opener.open.call_args_list],
                             ["GET", "GET", "POST"])

    def shared_fixture(self):
        binding = Binding(50, 2, AUTHOR_ACCOUNT, ACTIONS_BOT_ID, AUTHOR_ACCOUNT, SHARED_MODE)
        event = self.event()
        event["sender"]["id"] = event["comment"]["user"]["id"] = AUTHOR_ACCOUNT
        return binding, event

    def test_shared_account_accepts_only_synthetic_command_and_keeps_existing_anchors(self):
        binding, event = self.shared_fixture()
        sent = []
        publish = lambda body: sent.append(body) or len(sent)
        self.assertTrue(process(self.state, event, binding, str(AUTHOR_ACCOUNT), publish))
        self.assertTrue(process(self.new_state(), event, binding, str(AUTHOR_ACCOUNT), publish))
        self.assertEqual(len(sent), 2)
        self.assertTrue(all("Shared-account test with no AI attribution." in body for body in sent))
        self.assertEqual(self.state.anchors, self.anchors)
        self.assertEqual(self.state.update(self.inspect), [(101, 1)])
        with self.assertRaisesRegex(ValueError, "distinct"):
            Binding(50, 2, AUTHOR_ACCOUNT, ACTIONS_BOT_ID, AUTHOR_ACCOUNT)
        with self.assertRaisesRegex(StateConflict, "persisted synthetic binding"):
            process(self.new_state(), self.event(), self.binding, "41", publish)
        event["sender"]["id"] = event["comment"]["user"]["id"] = ACTIONS_BOT_ID
        event["comment"]["body"] = sent[0]
        self.assertFalse(process(None, event, binding, str(ACTIONS_BOT_ID), None))
        event["comment"]["body"] = COMMAND
        self.assertFalse(process(None, event, binding, str(ACTIONS_BOT_ID), None))

    def test_actions_reply_authors_and_recovery_use_system_identity(self):
        binding, _ = self.shared_fixture()
        target = {"state": "open", "number": 2, "base": {"repo": {"id": 50, "full_name": REPOSITORY}}}
        bot = {"id": ACTIONS_BOT_ID, "login": ACTIONS_BOT_LOGIN, "type": "Bot"}
        with patch("orchestrator.auto_events.urllib.request.build_opener") as build:
            opener = build.return_value
            values = [target, {"id": 7, "user": bot}, [
                {"id": 8, "body": "synthetic", "user": {**bot, "login": "someone-else"}},
                {"id": 7, "body": "synthetic", "user": bot},
            ]]
            opener.open.side_effect = [io.BytesIO(json.dumps(value).encode()) for value in values]
            publish, find = github_publisher(binding, "fixture-token")
            self.assertEqual(publish("synthetic"), 7)
            self.assertEqual(find("synthetic"), 7)
            requests = [call.args[0] for call in opener.open.call_args_list]
            self.assertEqual([req.get_method() for req in requests], ["GET", "POST", "GET"])
            self.assertTrue(all("/repos/" in req.full_url for req in requests))
        with patch("orchestrator.auto_events.urllib.request.build_opener") as build:
            build.return_value.open.side_effect = [
                io.BytesIO(json.dumps(target).encode()),
                io.BytesIO(json.dumps({"id": 8, "user": {**bot, "type": "User"}}).encode()),
            ]
            publish, _ = github_publisher(binding, "fixture-token")
            with self.assertRaisesRegex(RuntimeError, "unexpected reply author"):
                publish("synthetic")

    def test_shared_entrypoint_uses_only_the_builtin_reply_token(self):
        binding, event = self.shared_fixture()
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event))
        env = {
            "ORCHESTRATOR_AUTO_SYNTHETIC_ENABLED": "1", "ORCHESTRATOR_AUTO_IDENTITY_MODE": SHARED_MODE,
            "GITHUB_EVENT_NAME": "issue_comment", "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_PATH": str(event_path),
            "GITHUB_ACTOR_ID": str(AUTHOR_ACCOUNT), "ORCHESTRATOR_AUTO_REPOSITORY_ID": "50",
            "ORCHESTRATOR_AUTO_PR": "2", "ORCHESTRATOR_AUTO_ENVIRONMENT": "fixture",
            "ORCHESTRATOR_AUTO_STATE_ANCHOR": self.anchors[0],
            "ORCHESTRATOR_AUTO_CHECKPOINT_ANCHOR": self.anchors[1],
            "ORCHESTRATOR_STATE_TOKEN": "fixture-state", "ORCHESTRATOR_AUTO_REPLY_TOKEN": "wrong-channel",
        }
        with patch.dict(os.environ, env, clear=True), patch(
            "orchestrator.auto_events.github_publisher",
        ) as publisher, patch("orchestrator.auto_events.PairedState"), patch("orchestrator.auto_events.process"):
            with self.assertRaisesRegex(RuntimeError, "credentials"):
                main()
            publisher.assert_not_called()
            os.environ["GITHUB_TOKEN"] = "fixture-actions"
            publisher.return_value = (None, None)
            main()
            publisher.assert_called_once_with(binding, "fixture-actions")
