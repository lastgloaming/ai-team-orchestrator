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
from unittest.mock import Mock, patch

from orchestrator.auto_events import Binding, COMMAND, REPOSITORY, SCHEDULING_COMMAND, ReplyUnconfirmed, _consume, _enqueue, process
from orchestrator.auto_state import PairedState
from orchestrator.deepseek import Limits
from orchestrator.mock_scheduling import MockScheduler
from orchestrator.scheduling_ledger import ModelStatus
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
        self.forbidden_transport = Mock(return_value=response())
        self.addCleanup(self.forbidden_transport.assert_not_called)
        self.enterContext(patch.dict(os.environ, {"ORCHESTRATOR_MODEL_SCHEDULING_ENABLED": "1"}))
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.remote = str(Path(temp.name) / "remote.git")
        subprocess.run(["git", "init", "--bare", "--quiet", self.remote], check=True)
        self.anchors = PairedState(self.remote, "fixture").bootstrap()

    def state(self, cls=PairedState):
        return cls(self.remote, "fixture", self.anchors)

    def event(self, identifier, body=SCHEDULING_COMMAND):
        return {
            "action": "created", "repository": {"id": 50, "full_name": REPOSITORY},
            "issue": {"number": 2, "state": "open", "pull_request": {}},
            "sender": {"id": 41}, "comment": {"id": identifier, "user": {"id": 41}, "body": body},
        }

    def consume(self, identifier):
        state = self.state()
        state.update(lambda path: _enqueue(path, identifier, BINDING, eligible=True))
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
        self.assertTrue(process(self.state(), self.event(100, COMMAND), BINDING, "41",
                                lambda body: sent.append(body) or len(sent)))
        scheduler = MockScheduler(self.state(), LIMITS, self.forbidden_transport)
        result = process(self.state(), self.event(100, COMMAND), BINDING, "41",
                         lambda _: self.fail("duplicate reply"), schedule=scheduler)
        self.assertTrue(result.communication_completed)
        self.assertEqual(result.scheduling.status, ModelStatus.NOT_ELIGIBLE)
        self.assertEqual(len(sent), 2)

        def mock(request, timeout):
            self.assertEqual(self.rows(), [(101, "RESERVED", None, 52_000, "CNY", "active")])
            self.assertEqual(request["messages"][-1]["content"], "ACCEPTED")
            return response()

        result = process(self.state(), self.event(101), BINDING, "41",
                         lambda body: sent.append(body) or len(sent),
                         schedule=MockScheduler(self.state(), LIMITS, mock))
        self.assertTrue(result.communication_completed)
        self.assertEqual(result.scheduling.status, ModelStatus.ADVISED)
        self.assertEqual(result.scheduling.suggestion, "inspect_status")
        self.assertEqual(self.rows(), [(101, "ADVISED", "inspect_status", 10_000, "CNY", "counted")])
        self.assertEqual(MockScheduler(self.state(), LIMITS, self.forbidden_transport)(101).status,
                         ModelStatus.ADVISED)

    def test_budget_persists_across_clients_and_blocks_before_transport(self):
        for identifier in (101, 102):
            self.consume(identifier)
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: response())(101).status, "ADVISED")
        scheduler = MockScheduler(self.state(), LIMITS, self.forbidden_transport)
        result = process(self.state(), self.event(102), BINDING, "41", lambda _: 7, schedule=scheduler)
        self.assertTrue(result.communication_completed)
        self.assertEqual((result.scheduling.status, result.scheduling.reason),
                         (ModelStatus.BLOCKED, "budget_exhausted"))
        raised_budget = replace(LIMITS, daily_budget=90_000)
        again = MockScheduler(self.state(), raised_budget, self.forbidden_transport)(102)
        self.assertEqual(again, result.scheduling)
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
                return MockScheduler(self.state(RacingState), LIMITS, mock)(101).status
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
        self.assertEqual(MockScheduler(self.state(), LIMITS, self.forbidden_transport)(101).status,
                         "RESERVED")

    def test_uncertain_response_retains_budget_and_disables_retry(self):
        self.consume(101)
        calls = []

        def timeout(*_):
            calls.append(1)
            raise TimeoutError()

        scheduler = MockScheduler(self.state(), LIMITS, timeout)
        self.assertEqual(scheduler(101).status, "UNCERTAIN")
        self.assertEqual(scheduler(101).status, "UNCERTAIN")
        self.assertEqual(calls, [1])
        self.assertEqual(self.rows(), [(101, "UNCERTAIN", None, 52_000, "CNY", "uncertain")])

    def test_stops_are_independent(self):
        published = []
        with patch.dict(os.environ, {"ORCHESTRATOR_MODEL_STOP": "1"}):
            scheduler = MockScheduler(self.state(), LIMITS, self.forbidden_transport)
            result = process(self.state(), self.event(101), BINDING, "41",
                             lambda body: published.append(body) or len(published), schedule=scheduler)
            self.assertTrue(result.communication_completed)
            self.assertEqual(result.scheduling.status, ModelStatus.STOPPED)
        self.assertEqual(len(published), 2)
        with patch.dict(os.environ, {"ORCHESTRATOR_AUTO_COMMUNICATION_STOP": "1"}):
            scheduler = MockScheduler(self.state(), LIMITS, lambda *_: response())
            with self.assertRaisesRegex(ReplyUnconfirmed, "communication stopped"):
                process(self.state(), self.event(102), BINDING, "41",
                        lambda _: self.fail("communication stop failed"), schedule=scheduler)
        self.assertEqual(self.rows(), [(102, "ADVISED", "inspect_status", 10_000, "CNY", "counted")])

    def test_requires_committed_consumed_event_and_zero_retries(self):
        self.state().update(lambda path: _enqueue(path, 101, BINDING))
        self.assertEqual(MockScheduler(self.state(), LIMITS, self.forbidden_transport)(101).status,
                         ModelStatus.NOT_ELIGIBLE)
        for invalid in ({"retries": 1}, {"currency": "USD"}, {"concurrent": 2},
                        {"monthly_budget": 2_200_000_001}, {"max_output": 65}):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "requires CNY"):
                MockScheduler(self.state(), replace(LIMITS, **invalid), lambda *_: None)

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

        self.assertEqual(MockScheduler(state, LIMITS, transport)(101).status, "ADVISED")
        self.assertTrue(state.raced)
        self.assertEqual(calls, [1])

    def test_model_never_calls_transport_when_atomic_reservation_is_rejected(self):
        self.consume(101)
        hook = Path(self.remote) / "hooks" / "update"
        hook.write_text('#!/bin/sh\ntest "$1" != refs/heads/p4-auto-test-checkpoints\n')
        hook.chmod(0o700)
        with self.assertRaisesRegex(StateConflict, "not confirmed"):
            MockScheduler(self.state(), LIMITS, self.forbidden_transport)(101)

    def test_default_off_and_later_enable_cannot_retroactively_admit_event(self):
        scheduler = MockScheduler(self.state(), LIMITS, self.forbidden_transport)
        with patch.dict(os.environ, {"ORCHESTRATOR_MODEL_SCHEDULING_ENABLED": ""}):
            result = process(self.state(), self.event(101), BINDING, "41", lambda _: 7, schedule=scheduler)
            self.assertTrue(result.communication_completed)
            self.assertEqual(result.scheduling.status, ModelStatus.DISABLED)
        again = process(self.state(), self.event(101), BINDING, "41",
                        lambda _: self.fail("duplicate reply"), schedule=scheduler)
        self.assertEqual(again.scheduling.status, ModelStatus.NOT_ELIGIBLE)

    def test_integrity_rejects_removal_of_attempt_and_usage_history(self):
        self.consume(101)
        self.assertEqual(MockScheduler(self.state(), LIMITS, lambda *_: response())(101).status,
                         ModelStatus.ADVISED)

        def erase(path):
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("DELETE FROM auto_model_attempt")
                db.execute("DELETE FROM usage")

        with self.assertRaisesRegex(StateConflict, "attempt was removed"):
            self.state().update(erase)

    def test_integrity_rejects_tampered_remote_missing_scheduling_table(self):
        self.consume(101)
        state = self.state()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state._git(root, "init", "--quiet", "--bare")
            tips, sequence = state._load(root, self.anchors)
            with closing(sqlite3.connect(root / "ledger.sqlite")) as db, db:
                db.execute("DROP TABLE auto_model_attempt")
            _, pushed = state._advance(root, tips, sequence + 1)
            self.assertEqual(pushed.returncode, 0)
        with self.assertRaisesRegex(StateConflict, "incomplete synthetic scheduling schema"):
            MockScheduler(self.state(), LIMITS, self.forbidden_transport)(101)
