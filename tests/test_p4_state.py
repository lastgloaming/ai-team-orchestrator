import concurrent.futures
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

from orchestrator.core import transaction
from orchestrator.deepseek import Limits
from orchestrator.p4 import prepare_probe, probe_binding, send_probe, single_call, verify_probe
from orchestrator.state import STATE_REF, GitState, StateConflict, consume, enqueue


class P4StateTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.remote = Path(scratch.name) / "remote.git"
        subprocess.run(["git", "init", "--bare", "--quiet", str(self.remote)], check=True)
        self.state = GitState(str(self.remote))
        self.genesis = self.state.initialize()
        self.payload = probe_binding(2, 42, 99)

    def current(self, cls=GitState):
        tip = subprocess.check_output(["git", "-C", str(self.remote), "rev-parse", STATE_REF], text=True).strip()
        return cls(str(self.remote), tip)

    def test_concurrent_writes_and_replay(self):
        start = threading.Barrier(2)

        def submit(event_id):
            first = True

            def mutate(path):
                nonlocal first
                added = enqueue(path, event_id, self.payload)
                if first:
                    first = False
                    start.wait(10)
                return added

            return GitState(str(self.remote), self.genesis).update(mutate)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            self.assertEqual(list(pool.map(submit, ("TEST-1", "TEST-2"))), [True, True])
        self.state = self.current()
        self.assertFalse(self.state.update(lambda path: enqueue(path, "TEST-1", self.payload)))
        with self.assertRaises(ValueError):
            self.state.update(lambda path: enqueue(path, "TEST-1", {"kind": "different"}))
        restarted = self.current()
        found = [restarted.update(lambda path: consume(path, prepare_probe)) for _ in range(2)]
        self.assertEqual({event_id for event_id, _ in found}, {"TEST-1", "TEST-2"})
        self.assertIsNone(restarted.update(lambda path: consume(path, prepare_probe)))

    def test_consumption_rolls_back_on_failure(self):
        self.state.update(lambda path: enqueue(path, "TEST-1", self.payload))

        def fail(*_):
            raise RuntimeError("runner stopped before committing")

        with self.assertRaisesRegex(RuntimeError, "runner stopped"):
            self.state.update(lambda path: consume(path, fail))
        restarted = self.current()
        event_id, command = restarted.update(lambda path: consume(path, prepare_probe))
        self.assertEqual(event_id, "TEST-1")
        self.assertIn("target: integration", command["command"])

    def test_single_call_persists_reservation_before_transport(self):
        limits = Limits("mock", 300, 1200, 1024, 64, 384000, 384000, 1, 1, retries=0)
        seen = []

        def transport(request, timeout):
            def inspect(path):
                with transaction(path) as db:
                    return db.execute("SELECT state FROM usage").fetchone()[0]

            seen.append(self.current().update(inspect))
            raise TimeoutError()

        with self.assertRaisesRegex(RuntimeError, "reservation retained"):
            single_call(self.state, limits, transport)
        self.assertEqual(single_call(self.current(), limits, transport),
                         "already reserved; no request sent")
        self.assertEqual(seen, ["active"])

    def test_authentication_and_probe_correlation(self):
        ack = "AI-INTEGRATION-ACK\nid: TEST-1\nstatus: ACCEPTED\nreason: synthetic"
        result = "AI-INTEGRATION-RESULT\nid: TEST-1\nstatus: DONE\nevidence: synthetic\nblockers: none"
        origin = ("lastgloaming/ai-team-orchestrator", 2, 42)
        self.assertTrue(verify_probe("TEST-1", self.payload, ack, result, ack_origin=origin, result_origin=origin))
        for wrong in (("lastgloaming/ai-team-orchestrator", 2, 99),
                      ("lastgloaming/ai-team-orchestrator", 3, 42),
                      ("lastgloaming/ai-writer", 2, 42)):
            self.assertFalse(verify_probe("TEST-1", self.payload, ack, result,
                                          ack_origin=origin, result_origin=wrong))
        with self.assertRaises(ValueError):
            probe_binding(2, 99, 99)

    def test_probe_commit_precedes_send_and_lost_response_does_not_repost(self):
        self.state.update(lambda path: enqueue(path, "TEST-1", self.payload))
        self.state.update(lambda path: consume(path, prepare_probe))
        with self.assertRaisesRegex(ValueError, "prepared for this PR"):
            send_probe(self.state, "TEST-1", probe_binding(3, 42, 99),
                       lambda *_: self.fail("wrong PR sent"))
        posted = []

        def send(pr_number, command):
            self.assertEqual(pr_number, 2)
            posted.append(command)
            raise TimeoutError()

        with self.assertRaisesRegex(RuntimeError, "delivery unconfirmed"):
            send_probe(self.state, "TEST-1", self.payload, send)
        self.assertEqual(send_probe(self.current(), "TEST-1", self.payload, send),
                         "probe already claimed; no comment sent")
        self.assertEqual(len(posted), 1)

    def test_missing_branch_and_reinitialization_fail_closed(self):
        with self.assertRaisesRegex(StateConflict, "already exists"):
            GitState(str(self.remote)).initialize()
        subprocess.run(["git", "-C", str(self.remote), "update-ref", "-d", STATE_REF], check=True)
        with self.assertRaisesRegex(StateConflict, "missing"):
            self.state.update(lambda _: self.fail("mutated missing state"))
        with self.assertRaisesRegex(StateConflict, "checkpoint"):
            self.state.initialize()
        with self.assertRaisesRegex(StateConflict, "checkpoint"):
            GitState(str(self.remote)).update(lambda _: self.fail("ran without checkpoint"))

    def test_rollback_to_valid_snapshot_refused(self):
        self.state.update(lambda path: enqueue(path, "TEST-1", self.payload))
        subprocess.run(["git", "-C", str(self.remote), "update-ref", STATE_REF, self.genesis], check=True)
        with self.assertRaisesRegex(StateConflict, "checkpoint mismatch"):
            self.state.update(lambda _: self.fail("used a rolled-back ledger"))

    def test_corrupt_snapshot_refused_even_at_expected_version(self):
        self.state.update(lambda path: path.write_bytes(b"corrupt SQLite payload"))
        with self.assertRaisesRegex(StateConflict, "invalid state snapshot"):
            self.state.update(lambda _: self.fail("used corrupt ledger"))

    def test_concurrent_single_call(self):
        barrier = threading.Barrier(2)
        calls = []
        limits = Limits("mock", 300, 1200, 1024, 64, 384000, 384000, 1, 1, retries=0)

        class RacingState(GitState):
            first = True

            def _push_snapshot(self, *args):
                if self.first:
                    self.first = False
                    barrier.wait(10)
                return super()._push_snapshot(*args)

        def model(*_):
            calls.append(1)
            return {
                "usage": {"prompt_tokens": 5, "completion_tokens": 5},
                "choices": [{"finish_reason": "stop", "message": {"content": '{"suggestion":"inspect_status"}'}}],
            }

        def invoke(_):
            return single_call(RacingState(str(self.remote), self.genesis), limits, model)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(invoke, range(2)))
        self.assertCountEqual(results, ["one model request settled", "already reserved; no request sent"])
        self.assertEqual(calls, [1])

    def test_accepted_remote_push_with_lost_confirmation(self):
        class LostConfirmation(GitState):
            def _git(self, root, *args, **kwargs):
                outcome = super()._git(root, *args, **kwargs)
                if "push" in args and not outcome.returncode:
                    raise subprocess.TimeoutExpired("simulated lost push confirmation", 45)
                return outcome

        state = LostConfirmation(str(self.remote), self.genesis)
        self.assertTrue(state.update(lambda path: enqueue(path, "TEST-1", self.payload)))
        restarted = self.current()
        self.assertEqual(state.checkpoint, restarted.checkpoint)
        self.assertFalse(restarted.update(lambda path: enqueue(path, "TEST-1", self.payload)))
