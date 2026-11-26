import concurrent.futures
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

from orchestrator.core import transaction
from orchestrator.deepseek import Limits
from orchestrator.p4 import prepare_probe, send_probe, single_call, verify_probe
from orchestrator.state import GitState, consume, enqueue


class P4StateTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.remote = Path(scratch.name) / "remote.git"
        subprocess.run(["git", "init", "--bare", "--quiet", str(self.remote)], check=True)
        self.state = GitState(str(self.remote))
        self.payload = {"kind": "integration_probe"}

    def test_concurrent_initialization_and_replay(self):
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

            return GitState(str(self.remote)).update(mutate)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            self.assertEqual(list(pool.map(submit, ("TEST-1", "TEST-2"))), [True, True])
        self.assertFalse(self.state.update(lambda path: enqueue(path, "TEST-1", self.payload)))
        with self.assertRaises(ValueError):
            self.state.update(lambda path: enqueue(path, "TEST-1", {"kind": "different"}))
        restarted = GitState(str(self.remote))
        found = [restarted.update(lambda path: consume(path, prepare_probe)) for _ in range(2)]
        self.assertEqual({event_id for event_id, _ in found}, {"TEST-1", "TEST-2"})
        self.assertIsNone(restarted.update(lambda path: consume(path, prepare_probe)))

    def test_consumption_rolls_back_on_failure(self):
        self.state.update(lambda path: enqueue(path, "TEST-1", self.payload))

        def fail(*_):
            raise RuntimeError("runner stopped before committing")

        with self.assertRaisesRegex(RuntimeError, "runner stopped"):
            self.state.update(lambda path: consume(path, fail))
        restarted = GitState(str(self.remote))
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

            seen.append(GitState(str(self.remote)).update(inspect))
            raise TimeoutError()

        with self.assertRaisesRegex(RuntimeError, "reservation retained"):
            single_call(self.state, limits, transport)
        self.assertEqual(single_call(GitState(str(self.remote)), limits, transport),
                         "already reserved; no request sent")
        self.assertEqual(seen, ["active"])

    def test_authentication_and_probe_correlation(self):
        ack = "AI-INTEGRATION-ACK\nid: TEST-1\nstatus: ACCEPTED\nreason: synthetic"
        result = "AI-INTEGRATION-RESULT\nid: TEST-1\nstatus: DONE\nevidence: synthetic\nblockers: none"
        self.assertTrue(verify_probe("TEST-1", ack, result, ack_actor=42, result_actor=42, expected_actor=42, shared_actor=99))
        self.assertFalse(verify_probe("TEST-1", ack, result, ack_actor=42, result_actor=99, expected_actor=42, shared_actor=99))
        self.assertFalse(verify_probe("TEST-2", ack, result, ack_actor=42, result_actor=42, expected_actor=42, shared_actor=99))

    def test_probe_commit_precedes_send_and_lost_response_does_not_repost(self):
        self.state.update(lambda path: enqueue(path, "TEST-1", self.payload))
        self.state.update(lambda path: consume(path, prepare_probe))
        posted = []

        def send(command):
            posted.append(command)
            raise TimeoutError()

        with self.assertRaisesRegex(RuntimeError, "delivery unconfirmed"):
            send_probe(self.state, "TEST-1", send)
        self.assertEqual(send_probe(GitState(str(self.remote)), "TEST-1", send),
                         "probe already claimed; no comment sent")
        self.assertEqual(len(posted), 1)
