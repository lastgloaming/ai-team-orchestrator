import concurrent.futures
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_events import Binding, COMMAND, REPOSITORY, ReplyUnconfirmed, _consume, _enqueue, process
from orchestrator.auto_state import PairedState
from orchestrator.deepseek import Limits
from orchestrator.mock_scheduling import MockScheduler
from orchestrator.state import StateConflict

BINDING = Binding(50, 2, 41, 42, 99)
LIMITS = Limits("mock", 2000, 8000, 10, 4, 58_000, 100_000, 5, 10, retries=0, currency="CNY")


def response(suggestion="inspect_status"):
    return {
        "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        "choices": [{"finish_reason": "stop", "message": {"content": '{"suggestion":"' + suggestion + '"}'}}],
    }


class MockSchedulingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.remote = str(Path(temp.name) / "remote.git")
        subprocess.run(["git", "init", "--bare", "--quiet", self.remote], check=True)
        self.anchors = PairedState(self.remote, "fixture").bootstrap()

    def state(self, cls=PairedState):
        return cls(self.remote, "fixture", self.anchors)

    def event(self, identifier):
        return {
            "action": "created", "repository": {"id": 50, "full_name": REPOSITORY},
            "issue": {"number": 2, "state": "open", "pull_request": {}},
            "sender": {"id": 41}, "comment": {"id": identifier, "user": {"id": 41}, "body": COMMAND},
        }

    def consume(self, identifier):
        state = self.state()
        state.update(lambda path: _enqueue(path, identifier, BINDING))
        state.update(lambda path: _consume(path, identifier))

    def rows(self):
        def read(path):
            with closing(sqlite3.connect(path)) as db:
                return db.execute("""
                    SELECT event_id,status,suggestion,amount,currency,usage.state
                    FROM auto_model_attempt JOIN usage ON usage.id=reservation_id
                    ORDER BY event_id
                """).fetchall()

        return self.state().update(read)

    def test_legacy_communication_survives_linkage_and_replay(self):
        sent = []
        self.assertTrue(process(self.state(), self.event(100), BINDING, "41",
                                lambda body: sent.append(body) or len(sent)))

        def mock(request, timeout):
            self.assertEqual(self.rows(), [(100, "RESERVED", None, 52_000, "CNY", "active")])
            self.assertEqual(request["messages"][-1]["content"], "ACCEPTED")
            return response()

        scheduler = MockScheduler(self.state(), LIMITS, mock)
        self.assertTrue(process(self.state(), self.event(100), BINDING, "41",
                                lambda _: self.fail("duplicate reply"), schedule=scheduler))
        self.assertEqual(len(sent), 2)
        self.assertEqual(self.rows(), [(100, "ADVISED", "inspect_status", 10_000, "CNY", "counted")])
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: self.fail("duplicate model"))(100),
                         "ADVISED")

    def test_budget_persists_across_clients_and_blocks_before_transport(self):
        for identifier in (101, 102):
            self.consume(identifier)
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: response())(101), "ADVISED")
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: self.fail("budget exceeded"))(102),
                         "BLOCKED")
        self.assertEqual(len(self.rows()), 1)

    def test_malicious_output_is_rejected_without_routing(self):
        published = []
        scheduler = MockScheduler(self.state(), LIMITS, lambda *_: response("publish_private_material"))
        self.assertTrue(process(self.state(), self.event(101), BINDING, "41",
                                lambda body: published.append(body) or len(published), schedule=scheduler))
        self.assertEqual(self.rows(), [(101, "REJECTED", None, 10_000, "CNY", "counted")])
        self.assertEqual(len(published), 2)
        self.assertTrue(all("publish_private_material" not in reply for reply in published))

    def test_concurrent_reservation_conflict_does_not_duplicate_transport(self):
        self.consume(101)
        barrier = threading.Barrier(2)
        calls = []

        class RacingState(PairedState):
            first = True

            def _advance(self, *args):
                if self.first:
                    self.first = False
                    barrier.wait(10)
                return super()._advance(*args)

        def mock(*_):
            calls.append(1)
            return response()

        def invoke(_):
            try:
                return MockScheduler(self.state(RacingState), LIMITS, mock)(101)
            except StateConflict as error:
                # Identical proposals may have the same commit IDs. A worker
                # without a successful push must still refuse to send.
                self.assertIn("confirmation lost", str(error))
                return "UNCONFIRMED"

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(invoke, range(2)))
        self.assertIn("ADVISED", outcomes)
        self.assertTrue(all(status in {"ADVISED", "RESERVED", "UNCONFIRMED"} for status in outcomes))
        self.assertEqual(calls, [1])

    def test_crash_in_transport_leaves_reservation_and_blocks_replay_in_new_process(self):
        self.consume(101)
        program = """
import os, sys
from orchestrator.auto_state import PairedState
from orchestrator.deepseek import Limits
from orchestrator.mock_scheduling import MockScheduler
limits = Limits("mock", 2000, 8000, 10, 4, 58000, 100000, 5, 10, retries=0, currency="CNY")
MockScheduler(PairedState(sys.argv[1], "fixture", tuple(sys.argv[2:])), limits, lambda *_: os._exit(23))(101)
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, self.remote, *self.anchors], timeout=20)
        self.assertEqual(child.returncode, 23)
        self.assertEqual(self.rows(), [(101, "RESERVED", None, 52_000, "CNY", "active")])
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: self.fail("crash retried"))(101),
                         "RESERVED")

    def test_uncertain_response_retains_budget_and_disables_retry(self):
        self.consume(101)
        calls = []

        def timeout(*_):
            calls.append(1)
            raise TimeoutError()

        scheduler = MockScheduler(self.state(), LIMITS, timeout)
        self.assertEqual(scheduler(101), "UNCERTAIN")
        self.assertEqual(scheduler(101), "UNCERTAIN")
        self.assertEqual(calls, [1])
        self.assertEqual(self.rows(), [(101, "UNCERTAIN", None, 52_000, "CNY", "uncertain")])

    def test_stops_are_independent(self):
        published = []
        with patch.dict(os.environ, {"ORCHESTRATOR_MODEL_STOP": "1"}):
            scheduler = MockScheduler(self.state(), LIMITS, lambda *_: self.fail("model stop failed"))
            self.assertTrue(process(self.state(), self.event(101), BINDING, "41",
                                    lambda body: published.append(body) or len(published), schedule=scheduler))
        self.assertEqual(len(published), 2)
        with patch.dict(os.environ, {"ORCHESTRATOR_AUTO_COMMUNICATION_STOP": "1"}):
            scheduler = MockScheduler(self.state(), LIMITS, lambda *_: response())
            with self.assertRaisesRegex(ReplyUnconfirmed, "communication stopped"):
                process(self.state(), self.event(102), BINDING, "41",
                        lambda _: self.fail("communication stop failed"), schedule=scheduler)
        self.assertEqual(self.rows(), [(102, "ADVISED", "inspect_status", 10_000, "CNY", "counted")])

    def test_requires_committed_consumed_event_and_zero_retries(self):
        self.state().update(lambda path: _enqueue(path, 101, BINDING))
        with self.assertRaisesRegex(StateConflict, "committed, consumed"):
            MockScheduler(self.state(), LIMITS, lambda *_: self.fail("unconsumed event"))(101)
        with self.assertRaisesRegex(ValueError, "no retries"):
            MockScheduler(self.state(), replace(LIMITS, retries=1), lambda *_: None)

    def test_settlement_conflict_replays_without_calling_model_again(self):
        self.consume(101)
        test = self

        class SettlementRace(PairedState):
            raced = False

            def _advance(self, root, before, sequence):
                with closing(sqlite3.connect(root / "ledger.sqlite")) as db:
                    status = db.execute("SELECT status FROM auto_model_attempt WHERE event_id=101").fetchone()
                if status == ("ADVISED",) and not self.raced:
                    self.raced = True
                    test.state().update(lambda path: _enqueue(path, 102, BINDING))
                return super()._advance(root, before, sequence)

        state = self.state(SettlementRace)
        calls = []

        def transport(*_):
            calls.append(1)
            return response()

        self.assertEqual(MockScheduler(state, LIMITS, transport)(101), "ADVISED")
        self.assertTrue(state.raced)
        self.assertEqual(calls, [1])

    def test_model_never_calls_transport_when_atomic_reservation_is_rejected(self):
        self.consume(101)
        hook = Path(self.remote) / "hooks" / "update"
        hook.write_text('#!/bin/sh\ntest "$1" != refs/heads/p4-auto-test-checkpoints\n')
        hook.chmod(0o700)
        with self.assertRaisesRegex(StateConflict, "not confirmed"):
            MockScheduler(self.state(), LIMITS, lambda *_: self.fail("uncommitted reservation called model"))(101)
