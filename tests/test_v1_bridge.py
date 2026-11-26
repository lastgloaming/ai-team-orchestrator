import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import asdict
from http.server import ThreadingHTTPServer
from pathlib import Path

from orchestrator.v1 import Queue, Task
from orchestrator.v1_bridge import handler


class BridgeTests(unittest.TestCase):
    def test_http_event_claim_result_final_and_next_decision(self):
        head, base = "a" * 40, "b" * 40
        run = "https://github.com/example/alpha/actions/runs/19"
        url = "https://github.com/example/alpha/pull/1#pullrequestreview-20"
        review = Task("review", "example/alpha", "review", "v1", 100, 1, 1, head, base, important=True)
        next_task = Task("next", "example/beta", "research", "v2", 10, 1, dependencies=("review",))
        config = {
            "tasks": [asdict(review), asdict(next_task)], "stop": False,
            "authorizations": {"review": "v1", "next": "v2"}, "final_capacity_confirmed": True,
            "budget": {"remaining": 8, "reserve": 2, "window": "fixture"},
            "observations": {"example/alpha#1": {"state": "OPEN", "head": head, "base": base,
                                                  "checked": 100, "tested_sha": head, "run": run}},
            "final_evidence": {"review": {"head": head, "base": base, "authorization": "v1",
                                         "outcome": "PASS", "evidence": url, "reviewer": "chatgpt"}},
        }
        tokens = {role: str(i) * 32 for i, role in enumerate(("observer", "codex", "chatgpt", "operator"), 1)}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = root / "policy.json"
            policy.write_text(json.dumps(config))
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler(
                Queue(root / "queue.sqlite", now=lambda: 100), policy, tokens,
            ))
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                def post(route, role, payload):
                    request = urllib.request.Request(
                        f"http://127.0.0.1:{server.server_port}{route}", data=json.dumps(payload).encode(),
                        headers={"Authorization": f"Bearer {tokens[role]}"}, method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=5) as response:
                        return json.load(response)

                self.assertEqual(post("/v1/event", "observer", {"task_id": "review"})["status"], "INGESTED")
                post("/v1/event", "observer", {"task_id": "next"})
                work = post("/v1/next", "codex", {})
                evidence = {"conclusion": "PASS", "run": run, "passed": 1, "failed": 0,
                            "risk": "fixture", "decision": "author_merge", "tested_sha": head}
                self.assertEqual(post("/v1/result", "codex", {"task_id": "review", "lease": work["lease"],
                                                             "evidence": evidence})["status"], "WAITING_FINAL")
                with self.assertRaises(urllib.error.HTTPError) as blocked:
                    post("/v1/final/claim", "codex", {})
                self.assertEqual(blocked.exception.code, 403)
                self.assertEqual(post("/v1/next", "codex", {})["status"], "NOOP")
                final = post("/v1/final/claim", "chatgpt", {})
                decision = {"task_id": "review", "lease": final["lease"], "head": head, "base": base,
                            "authorization": "v1", "outcome": "PASS", "evidence": url}
                self.assertEqual(post("/v1/final/result", "chatgpt", decision)["merge"], "AUTHOR_REQUIRED")
                self.assertTrue(post("/v1/final/result", "chatgpt", decision)["duplicate"])
                self.assertEqual(post("/v1/next", "codex", {})["task"]["id"], "next")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
