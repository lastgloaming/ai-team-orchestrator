import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from orchestrator.v1 import Queue, Task
from orchestrator.v1_bridge import handler

HEAD, BASE = "a" * 40, "b" * 40
RUN = "https://github.com/example/repo/actions/runs/1"


class Workers(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "queue"
        self.now = 100
        self.authors = {"one"}
        self.q = Queue(self.path, now=lambda: self.now, verified_authors=lambda task: self.authors)
        self.cfg = {"workers": ["one", "two"], "stop": False, "authorizations": {},
                    "budget": {"remaining": 6, "reserve": 1, "window": "plan"}}
        self.t = Task("a", "example/repo", "development", "auth", 1, 0, scope="file:src/a")
        self.ev = dict(conclusion="PASS", run=RUN, passed=1, failed=0, risk="low", decision="ok", tested_sha=HEAD)

    def add(self, *tasks):
        self.cfg["authorizations"].update({t.id: t.authorization for t in tasks})
        self.q.add(tasks)

    def race(self):
        barrier = threading.Barrier(2)

        def run(worker):
            barrier.wait(5)
            return self.q.claim(self.cfg, worker=worker)

        with ThreadPoolExecutor(2) as pool:
            return list(pool.map(run, self.cfg["workers"]))

    def test_distinct_atomic_and_single_worker_limit(self):
        self.add(self.t, replace(self.t, id="b", scope="file:src/b"))
        a, b = self.race()
        self.assertEqual({a["task"]["id"], b["task"]["id"]}, {"a", "b"})
        self.add(replace(self.t, id="c", scope="file:src/c"))
        self.assertEqual(self.q.claim(self.cfg, worker="one")["status"], "NOOP")

    def test_one_task_competition(self):
        self.add(self.t)
        self.assertCountEqual([r["status"] for r in self.race()], ["CLAIMED", "NOOP"])

    def test_budget_window_and_zero_units(self):
        self.cfg["budget"]["remaining"] = 3
        self.add(self.t, replace(self.t, id="b", scope="file:b"))
        self.assertCountEqual([r["status"] for r in self.race()], ["CLAIMED", "NOOP"])
        self.cfg["budget"]["window"] = "new"
        self.assertFalse(any(r["status"] == "CLAIMED" for r in self.race()))

    def test_renew_expire_overlap_and_wrong_worker(self):
        self.add(self.t)
        lease = self.q.claim(self.cfg, worker="one")["lease"]
        for worker in (None, "two", "unconfigured"):
            with self.assertRaises(ValueError):
                self.q.renew("a", lease, self.cfg, worker=worker)
            with self.assertRaises(ValueError):
                self.q.complete("a", lease, self.cfg, self.ev, worker=worker)
        self.now = 999
        self.assertEqual(self.q.renew("a", lease, self.cfg, worker="one")["deadline"], 1899)
        self.now = 1899
        with self.assertRaises(ValueError):
            self.q.renew("a", lease, self.cfg, worker="one")
        self.add(replace(self.t, id="b", scope="file:./src//a/child"),
                 replace(self.t, id="c", scope="repo"), replace(self.t, id="d", scope="unknown"),
                 replace(self.t, id="e", scope="branch:main"))
        self.assertEqual(self.q.claim(self.cfg, worker="two")["status"], "NOOP")
        self.add(replace(self.t, id="f", scope="file:tests/b"))
        self.assertEqual(self.q.claim(self.cfg, worker="two")["task"]["id"], "f")
        self.assertEqual(self.q.status()["a"], "UNCONFIRMED")

    def test_exact_author_proof_and_current_version(self):
        t = replace(self.t, kind="review", pr=2, head=HEAD, base=BASE, authored_here=False)
        self.cfg["observations"] = {"example/repo#2": dict(state="OPEN", checked=100, head=HEAD,
                                                            base=BASE, tested_sha=HEAD, run=RUN)}
        self.add(t)
        self.assertEqual(self.q.claim(self.cfg, worker="one")["status"], "NOOP")
        self.authors = set()
        self.assertEqual(self.q.claim(self.cfg, worker="two")["status"], "NOOP")
        self.authors = {"one"}
        claim = self.q.claim(self.cfg, worker="two")
        self.cfg["observations"]["example/repo#2"]["base"] = HEAD
        self.assertEqual(self.q.complete("a", claim["lease"], self.cfg, self.ev, worker="two")["status"], "STALE")

    def test_independent_review_can_settle(self):
        t = replace(self.t, kind="review", pr=2, head=HEAD, base=BASE)
        self.add(t)
        self.cfg["observations"] = {"example/repo#2": dict(state="OPEN", checked=100, head=HEAD,
                                                            base=BASE, tested_sha=HEAD, run=RUN)}
        lease = self.q.claim(self.cfg, worker="two")["lease"]
        self.assertEqual(self.q.complete("a", lease, self.cfg, self.ev, worker="two")["status"], "DONE")

    def test_bound_http_workers_ignore_payload_identity(self):
        self.add(self.t)
        policy = self.path.parent / "policy.json"
        policy.write_text(json.dumps(self.cfg))
        tokens = {"codex": {"one": "1"*32, "two": "2"*32}, "observer": "3"*32,
                  "operator": "4"*32, "chatgpt": "5"*32}
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler(self.q, policy, tokens))
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            def post(route, worker, **payload):
                req = Request(f"http://127.0.0.1:{server.server_port}/v1/{route}",
                              data=json.dumps(payload).encode(),
                              headers={"Authorization": "Bearer " + tokens["codex"][worker]})
                with urlopen(req, timeout=5) as response:
                    return json.load(response)
            lease = post("next", "one", worker_id="two")["lease"]
            with self.assertRaises(HTTPError) as error:
                post("renew", "two", task_id="a", lease=lease, worker_id="one")
            self.assertEqual(error.exception.code, 400)
            self.assertEqual(post("renew", "one", task_id="a", lease=lease)["status"], "RENEWED")
            self.assertEqual(post("result", "one", task_id="a", lease=lease, evidence=self.ev)["status"], "DONE")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_unknown_and_self_final_cannot_release_merge(self):
        t = replace(self.t, kind="review", pr=2, head=HEAD, base=BASE, important=True)
        self.add(t, replace(self.t, id="merge", kind="merge", dependencies=("a",)))
        self.cfg["observations"] = {"example/repo#2": dict(state="OPEN", checked=100, head=HEAD,
                                                            base=BASE, tested_sha=HEAD, run=RUN)}
        self.cfg["final_capacity_confirmed"] = True
        lease = self.q.claim(self.cfg, worker="two")["lease"]
        self.assertEqual(self.q.complete("a", lease, self.cfg, self.ev, worker="two")["status"], "WAITING_FINAL")
        for authors in (None, set(), {"chatgpt"}):
            self.authors = authors
            self.assertEqual(self.q.final_claim(self.cfg, "chatgpt")["status"], "NOOP")
        self.authors = {"one"}
        lease = self.q.final_claim(self.cfg, "chatgpt")["lease"]
        proof = dict(head=HEAD, base=BASE, authorization="auth", outcome="PASS",
                     evidence="https://github.com/example/repo/pull/2#pullrequestreview-3")
        self.cfg["final_evidence"] = {"a": {**proof, "reviewer": "chatgpt"}}
        decision = dict(task_id="a", lease=lease, **proof)
        for authors in (set(), {"chatgpt"}):
            self.authors = authors
            for outcome in ("PASS", "REQUEST_CHANGES"):
                self.cfg["final_evidence"]["a"]["outcome"] = outcome
                self.assertEqual(self.q.final_decide(self.cfg, "chatgpt", dict(decision, outcome=outcome))["status"],
                                 "BLOCKED")
        self.cfg["final_evidence"]["a"]["outcome"] = "PASS"
        self.authors = {"one"}
        self.assertEqual(self.q.final_decide(self.cfg, "chatgpt", decision),
                         {"status": "DONE", "merge": "AUTHOR_REQUIRED"})
        self.assertEqual(self.q.status()["merge"], "WAITING_AUTHOR")

    def test_unknown_legacy_lease_and_invalid_scope(self):
        self.add(replace(self.t, scope="default"))
        self.assertEqual(self.q.claim(self.cfg, worker="one")["status"], "NOOP")
        serial = dict(self.cfg)
        del serial["workers"]
        self.assertEqual(self.q.claim(serial)["status"], "CLAIMED")
        self.add(replace(self.t, id="other", repository="example/other"))
        self.assertEqual(self.q.claim(self.cfg, worker="two")["status"], "NOOP")
        for scope in ("file:../../escape", "file:/absolute", "branch:../main"):
            with self.assertRaises(ValueError):
                replace(self.t, scope=scope)

    def test_independent_final_request_changes(self):
        t = replace(self.t, kind="review", pr=2, head=HEAD, base=BASE, important=True)
        self.add(t)
        self.cfg["observations"] = {"example/repo#2": dict(state="OPEN", checked=100, head=HEAD,
                                                            base=BASE, tested_sha=HEAD, run=RUN)}
        self.cfg["final_capacity_confirmed"] = True
        work = self.q.claim(self.cfg, worker="two")
        self.q.complete("a", work["lease"], self.cfg, self.ev, worker="two")
        final = self.q.final_claim(self.cfg, "chatgpt")
        proof = dict(head=HEAD, base=BASE, authorization="auth", outcome="REQUEST_CHANGES",
                     evidence="https://github.com/example/repo/pull/2#pullrequestreview-3")
        self.cfg["final_evidence"] = {"a": {**proof, "reviewer": "chatgpt"}}
        decision = dict(task_id="a", lease=final["lease"], **proof)
        self.assertEqual(self.q.final_decide(self.cfg, "chatgpt", decision)["status"], "REQUEST_CHANGES")

    def leased_chain(self):
        root = replace(self.t, pr=2, head=HEAD, base=BASE)
        self.cfg["observations"] = {"example/repo#2": dict(state="OPEN", checked=100, head=HEAD, base=BASE)}
        self.cfg["budget"]["remaining"] = 10
        self.add(root, replace(self.t, id="b", scope="file:b", dependencies=("a",)),
                 replace(self.t, id="c", scope="file:c", dependencies=("b",)),
                 replace(self.t, id="d", scope="file:d", dependencies=("c",)))
        for task_id in ("a", "b"):
            claim = self.q.claim(self.cfg, worker="one")
            self.assertEqual(claim["task"]["id"], task_id)
            self.assertEqual(self.q.complete(task_id, claim["lease"], self.cfg, self.ev, worker="one")["status"], "DONE")
        return self.q.claim(self.cfg, worker="one")["lease"]

    def test_current_chain_completes_and_releases_descendant(self):
        lease = self.leased_chain()
        self.assertEqual(self.q.complete("c", lease, self.cfg, self.ev, worker="one")["status"], "DONE")
        self.assertEqual(self.q.claim(self.cfg, worker="two")["task"]["id"], "d")

    def test_revoked_grandparent_blocks_completion_and_descendant(self):
        lease = self.leased_chain()
        del self.cfg["authorizations"]["a"]
        self.assertEqual(self.q.complete("c", lease, self.cfg, self.ev, worker="one")["status"], "STALE")
        self.assertEqual(self.q.claim(self.cfg, worker="two")["status"], "NOOP")

    def test_changed_grandparent_version_blocks_completion(self):
        lease = self.leased_chain()
        self.cfg["observations"]["example/repo#2"]["base"] = HEAD
        self.assertEqual(self.q.complete("c", lease, self.cfg, self.ev, worker="one")["status"], "STALE")
        self.assertEqual(self.q.claim(self.cfg, worker="two")["status"], "NOOP")

    def test_cycle_in_stored_completed_prerequisites_blocks_completion(self):
        lease = self.leased_chain()
        with self.q._db() as db:
            spec = json.loads(db.execute("SELECT spec FROM v1_task WHERE id='a'").fetchone()[0])
            spec["dependencies"] = ["b"]
            db.execute("UPDATE v1_task SET spec=? WHERE id='a'", (json.dumps(spec),))
        self.assertEqual(self.q.complete("c", lease, self.cfg, self.ev, worker="one")["status"], "STALE")
