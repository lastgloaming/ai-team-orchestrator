import concurrent.futures
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from orchestrator.v1 import Queue, Task

HEAD, BASE = "a" * 40, "b" * 40
RUN = "https://github.com/example/alpha/actions/runs/123"


class QueueTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "queue.sqlite"
        self.now = 100
        self.queue = self.open()
        self.review = Task("r1", "example/alpha", "review", "approval-1", 50, 1,
                           pr=1, head=HEAD, base=BASE, important=True)
        self.dev = Task("d1", "example/beta", "development", "approval-2", 30, 1, scope="fixture")
        self.config = {
            "stop": False, "authorizations": {"r1": "approval-1", "d1": "approval-2"},
            "budget": {"remaining": 5, "reserve": 1, "window": "fixture-window"},
            "observations": {"example/alpha#1": {"state": "OPEN", "head": HEAD, "base": BASE,
                                                  "checked": 100, "tested_sha": HEAD, "run": RUN}},
        }
        self.evidence = {"conclusion": "PASS", "run": RUN, "passed": 1, "failed": 0,
                         "risk": "fixture", "decision": "author_merge", "tested_sha": HEAD}

    def open(self):
        return Queue(self.path, now=lambda: self.now)

    def test_exact_review_dedupe_priorities_and_independent_handoff(self):
        self.queue.add([self.dev, self.review, replace(self.review, id="duplicate", authorization="other")])
        work = self.queue.claim(self.config)
        self.assertEqual(work["task"]["id"], "r1")
        result = self.queue.complete("r1", work["lease"], self.config, self.evidence)
        self.assertEqual(result["status"], "WAITING_FINAL")
        request, = self.open().final_requests(self.config)
        self.assertEqual((request["task"]["head"], request["delivery"]), (HEAD, "BLOCKED_NO_CONNECTOR"))
        self.assertEqual(self.queue.claim(self.config)["task"]["repository"], "example/beta")
        self.assertNotIn("duplicate", self.queue.status())

    def test_expiry_and_stale_head_do_not_replay(self):
        self.queue.add([self.review])
        lease = self.queue.claim(self.config)["lease"]
        self.config["observations"]["example/alpha#1"]["head"] = "c" * 40
        self.assertEqual(self.queue.complete("r1", lease, self.config, self.evidence)["status"], "STALE")
        self.queue.add([self.dev])
        self.assertEqual(self.queue.claim(self.config)["status"], "CLAIMED")
        self.now += 901
        self.assertEqual(self.open().claim(self.config)["status"], "NOOP")
        self.assertEqual(self.queue.status()["d1"], "UNCONFIRMED")
        self.queue.reconcile("d1", RUN)
        self.assertEqual(self.open().claim(self.config)["status"], "NOOP")

    def test_blocked_dependencies_budget_authority_and_external_role(self):
        tasks = [
            Task("m1", "example/alpha", "memory", "auth", 100, 0),
            Task("x1", "example/beta", "merge", "auth", 100, 0),
            replace(self.review, dependencies=("d1",)),
            self.dev,
        ]
        self.queue.add(tasks)
        self.config["authorizations"]["m1"] = "auth"
        self.config["authorizations"]["d1"] = "different"
        result = self.queue.claim(self.config)
        self.assertEqual(result["blocked"], ["authorization", "dependencies", "external_role_transport"])
        self.config["authorizations"]["d1"] = "approval-2"
        self.config["budget"]["remaining"] = 2
        self.assertIn("budget", self.queue.claim(self.config)["blocked"])
        self.assertEqual(self.queue.status()["x1"], "WAITING_AUTHOR")

    def test_concurrent_claimants_and_wrong_ci_evidence(self):
        self.queue.add([self.review])
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(lambda _: self.open().claim(self.config), range(2)))
        self.assertCountEqual([item["status"] for item in outcomes], ["CLAIMED", "NOOP"])
        lease = next(item["lease"] for item in outcomes if item["status"] == "CLAIMED")
        self.evidence["tested_sha"] = BASE
        self.assertEqual(self.queue.complete("r1", lease, self.config, self.evidence)["status"], "BLOCKED")
        self.assertEqual(self.queue.final_requests(self.config), [])
