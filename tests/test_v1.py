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
        work = self.queue.claim(self.config)
        self.assertEqual(work["status"], "CLAIMED")
        self.now += 901
        self.assertEqual(self.open().claim(self.config)["status"], "NOOP")
        self.assertEqual(self.queue.status()["d1"], "UNCONFIRMED")
        with self.assertRaisesRegex(ValueError, "verified recovery"):
            self.queue.reconcile("d1", RUN, self.config)
        run = "https://github.com/example/beta/actions/runs/123"
        self.config["recovery"] = {"d1": {"lease": work["lease"], "run": run,
                                           "no_active_or_external_effect": True}}
        self.queue.reconcile("d1", run, self.config)
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

    def test_alias_dependency_unblocks_only_after_exact_independent_review(self):
        alias = replace(self.review, id="alias-r1")
        self.queue.add([self.review, alias, replace(self.dev, dependencies=("alias-r1",))])
        work = self.queue.claim(self.config)
        self.queue.complete("r1", work["lease"], self.config, self.evidence)
        self.assertEqual(self.queue.claim(self.config)["status"], "NOOP")
        self.config["final_capacity_confirmed"] = True
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            outcomes = list(pool.map(lambda _: self.open().final_claim(self.config, "chatgpt"), range(2)))
        self.assertCountEqual([result["status"] for result in outcomes], ["CLAIMED", "NOOP"])
        lease = next(item["lease"] for item in outcomes if item["status"] == "CLAIMED")
        decision = {"task_id": "r1", "lease": lease, "head": HEAD, "base": BASE, "authorization": "approval-1",
                    "outcome": "PASS", "evidence": "https://github.com/example/alpha/pull/1#pullrequestreview-15"}
        self.assertEqual(self.open().final_decide(self.config, "chatgpt", {**decision, "head": BASE}),
                         {"status": "STALE"})
        self.assertEqual(self.open().final_decide(self.config, "chatgpt", decision)["status"], "BLOCKED")
        self.config["final_evidence"] = {"r1": {
            **{k: decision[k] for k in ("head", "base", "authorization", "outcome", "evidence")}, "reviewer": "chatgpt",
        }}
        outcome = self.open().final_decide(self.config, "chatgpt", decision)
        self.assertEqual(outcome, {"status": "DONE", "merge": "AUTHOR_REQUIRED"})
        self.assertTrue(self.open().final_decide(self.config, "chatgpt", decision)["duplicate"])
        self.assertEqual(self.open().claim(self.config)["task"]["id"], "d1")

    def test_final_claim_requires_proof_before_recovery(self):
        self.queue.add([self.review])
        work = self.queue.claim(self.config)
        self.queue.complete("r1", work["lease"], self.config, self.evidence)
        self.config["final_capacity_confirmed"] = True
        claim = self.queue.final_claim(self.config, "chatgpt")
        self.now += 901
        self.config["observations"]["example/alpha#1"]["checked"] = self.now
        self.assertEqual(self.open().final_claim(self.config, "chatgpt")["status"], "NOOP")
        with self.assertRaises(ValueError):
            self.queue.final_reconcile(self.config, "r1", RUN)
        self.config["final_recovery"] = {"r1": {"lease": claim["lease"], "run": RUN,
                                                "no_active_or_external_effect": True}}
        self.queue.final_reconcile(self.config, "r1", RUN)
        self.assertNotEqual(self.open().final_claim(self.config, "chatgpt")["lease"], claim["lease"])

    def test_authorization_update_can_restore_unclaimed_work_without_duplicate(self):
        self.queue.add([self.dev])
        self.config["authorizations"] = {"renewed": "approval-3"}
        self.queue.add([replace(self.dev, id="renewed", authorization="approval-3")], self.config)
        work = self.queue.claim(self.config)
        self.assertEqual((work["task"]["id"], work["task"]["authorization"]), ("d1", "approval-3"))
        self.assertEqual(self.queue.status(), {"d1": "CLAIMED"})

    def test_handoff_observation_is_durable_and_never_claims_execution(self):
        memory = Task("m1", "example/alpha", "memory", "auth", 30, 1)
        self.queue.add([memory])
        packet = {"version": "control-v1.0", "task_id": "m1", "authorization": "auth",
                  "repository": "lastgloaming/ai-writer", "pr": 3, "control_id": "fixture-1",
                  "publisher_app_id": 12, "publication": 20, "receipt": 21}
        self.config["authorizations"]["m1"] = "auth"
        with self.assertRaises(ValueError):
            self.queue.associate_handoff(self.config, packet)
        self.config["verified_handoffs"] = {"m1": packet}
        self.assertEqual(self.queue.associate_handoff(self.config, packet)["execution"], "UNVERIFIED")
        self.assertEqual(self.open().associate_handoff(self.config, packet)["association"], "RECORDED")
        alternate = {**packet, "receipt": 22}
        self.config["verified_handoffs"] = {"m1": alternate}
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.open().associate_handoff(self.config, alternate)
        self.assertEqual(self.open().status()["m1"], "READY")
