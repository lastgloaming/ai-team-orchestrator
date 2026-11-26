import concurrent.futures
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from orchestrator.core import transaction
from orchestrator.deepseek import Advisor, BudgetExceeded, Limits, live_transport
from orchestrator.p4 import prepare_probe, probe_binding, single_call
from orchestrator.state import GitState, StateConflict, consume, enqueue

LIMITS = Limits("deepseek-flash", 300, 1200, 33077, 64, 10_000_000, 10_000_000, 1, 1, retries=0)
BINDING = probe_binding(2, 42, 99)


class FaultChecks(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="orch-p4-fault-")
        self.addCleanup(tmp.cleanup)
        self.remote = Path(tmp.name) / "state.git"
        subprocess.run(["git", "init", "-q", "--bare", str(self.remote)], check=True)
        self.state = GitState(str(self.remote))
        self.base = self.state.initialize()

    def version(self):
        return subprocess.check_output(
            ["git", "-C", str(self.remote), "rev-parse", "refs/heads/p4-state"], text=True
        ).strip()

    def child(self, code, checkpoint, expected=0):
        # Keep crash leftovers in the test's private scratch directory.
        result = subprocess.run([sys.executable, "-B", "-c", code, str(self.remote), checkpoint],
                                env={**os.environ, "TMPDIR": str(self.remote.parent), "ORCHESTRATOR_LIVE_APPROVED": "0"},
                                check=False, timeout=20)
        self.assertEqual(result.returncode, expected)

    def test_stale_checkpoint(self):
        self.state.update(lambda p: enqueue(p, "event-1", BINDING))
        tip = self.version()
        with self.assertRaisesRegex(StateConflict, "checkpoint mismatch"):
            GitState(str(self.remote), self.base).update(lambda _: self.fail("mutation started"))
        self.assertEqual(self.version(), tip)

    def test_concurrent_initial_checkpoint_conflict(self):
        barrier = threading.Barrier(2)

        def writer(event):
            first = True

            def mutate(path):
                nonlocal first
                value = enqueue(path, event, BINDING)
                if first:
                    first = False
                    barrier.wait(10)
                return value

            return GitState(str(self.remote), self.base).update(mutate)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            self.assertEqual(list(pool.map(writer, ("event-A", "event-B"))), [True, True])
        fresh = GitState(str(self.remote), self.version())
        with self.assertRaisesRegex(ValueError, "different payload"):
            fresh.update(lambda p: enqueue(p, "event-A", probe_binding(3, 42, 99)))
        results = [fresh.update(lambda p: consume(p, prepare_probe)) for _ in range(2)]
        self.assertEqual({row[0] for row in results}, {"event-A", "event-B"})

    def test_lost_ack_then_separate_process_advances(self):
        test = self

        class LostThenAdvanced(GitState):
            simulated = False

            def _git(self, root, *args, **kwargs):
                outcome = super()._git(root, *args, **kwargs)
                if "push" in args and outcome.returncode == 0 and not self.simulated:
                    self.simulated = True
                    test.child("""
import sys
from orchestrator.state import GitState, enqueue
from orchestrator.p4 import probe_binding
state = GitState(*sys.argv[1:])
assert state.update(lambda p: enqueue(p, "peer", probe_binding(2, 42, 99)))
""", test.version())
                    raise subprocess.TimeoutExpired("simulated accepted push with no acknowledgement", 45)
                return outcome

        state = LostThenAdvanced(str(self.remote), self.base)
        message = single_call(state, LIMITS, lambda *_: self.fail("ambiguous claimant sent"))
        self.assertTrue(state.simulated)
        self.assertEqual(message, "already reserved; no request sent")
        reload = GitState(str(self.remote), self.version())

        def inspect(path):
            with transaction(path) as db:
                return (db.execute("SELECT count(*) FROM p4_call").fetchone()[0],
                        db.execute("SELECT state, amount FROM usage").fetchall(),
                        db.execute("SELECT id FROM inbox").fetchall())

        self.assertEqual(reload.update(inspect), (1, [("active", 9_999_900)], [("peer",)]))
        self.assertEqual(single_call(reload, LIMITS, lambda *_: self.fail("duplicate request sent")),
                         "already reserved; no request sent")

    def test_process_dies_before_consumption_commit(self):
        self.state.update(lambda p: enqueue(p, "recover", BINDING))
        tip = self.version()
        self.child("""
import os, sys
from orchestrator.state import GitState, consume
GitState(*sys.argv[1:]).update(lambda p: consume(p, lambda *_: os._exit(27)))
""", tip, expected=27)
        self.assertEqual(self.version(), tip)
        fresh = GitState(str(self.remote), tip)
        self.assertEqual(fresh.update(lambda p: consume(p, prepare_probe))[0], "recover")
        self.assertIsNone(fresh.update(lambda p: consume(p, prepare_probe)))

    def test_process_dies_after_reservation_and_rollback_fails_closed(self):
        self.child("""
import os, sys
from orchestrator.state import GitState
from orchestrator.deepseek import Limits
from orchestrator.p4 import single_call
limits = Limits("deepseek-flash", 300, 1200, 33077, 64, 10000000, 10000000, 1, 1, retries=0)
single_call(GitState(*sys.argv[1:]), limits, lambda *_: os._exit(28))
""", self.base, expected=28)
        tip = self.version()
        fresh = GitState(str(self.remote), tip)
        self.assertEqual(single_call(fresh, LIMITS, lambda *_: self.fail("crashed attempt resent")),
                         "already reserved; no request sent")
        subprocess.run(["git", "-C", str(self.remote), "update-ref", "refs/heads/p4-state", self.base],
                       check=True)
        with self.assertRaisesRegex(StateConflict, "checkpoint mismatch"):
            single_call(fresh, LIMITS, lambda *_: self.fail("rolled-back attempt sent"))

    def test_emergency_stop_before_reservation(self):
        with patch.dict(os.environ, {"ORCHESTRATOR_STOP": "1"}):
            with self.assertRaisesRegex(BudgetExceeded, "emergency stop"):
                single_call(self.state, LIMITS, lambda *_: self.fail("stop violated"))
        self.assertEqual(self.version(), self.base)
        request = Advisor(None, LIMITS, None).request("ACCEPTED")
        self.assertEqual(request["max_tokens"], 64)
        self.assertEqual(request["thinking"], {"type": "disabled"})
        self.assertEqual(request["response_format"], {"type": "json_object"})

    def test_budget_boundary_rejects_before_transport(self):
        over_limit = replace(LIMITS, input_ceiling=33078)
        with self.assertRaisesRegex(BudgetExceeded, "period budget"):
            single_call(self.state, over_limit, lambda *_: self.fail("over budget"))
        self.assertEqual(self.version(), self.base)

    def test_emergency_stop_after_reservation_before_http(self):
        def verify_stop(request, timeout):
            with patch.dict(os.environ, {"ORCHESTRATOR_LIVE_APPROVED": "1", "ORCHESTRATOR_STOP": "1"}):
                return live_transport(request, timeout)

        with patch("orchestrator.deepseek.urllib.request.build_opener") as http:
            with self.assertRaisesRegex(RuntimeError, "reservation retained"):
                single_call(self.state, LIMITS, verify_stop)
            http.assert_not_called()
        self.assertEqual(single_call(self.state, LIMITS, lambda *_: self.fail("stopped attempt resent")),
                         "already reserved; no request sent")


if __name__ == "__main__":
    unittest.main(verbosity=2)
