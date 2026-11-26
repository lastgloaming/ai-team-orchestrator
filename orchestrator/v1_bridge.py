"""Private operational JSON bridge. Deploy behind the existing trusted TLS boundary."""

import argparse
import hmac
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .v1 import Queue, Task

ROUTES = {
    "/v1/event": "observer", "/v1/next": "codex", "/v1/result": "codex",
    "/v1/final/claim": "chatgpt", "/v1/final/result": "chatgpt",
    "/v1/reconcile": "operator", "/v1/final/reconcile": "operator",
    "/v1/handoffs": "operator", "/v1/handoffs/observe": "operator",
}


def operate(queue, config, route, payload):
    if route == "/v1/event":
        # Body supplies only a pointer. The trusted config defines all task data.
        tasks = [Task(**item) for item in config["tasks"] if item["id"] == payload["task_id"]]
        if len(tasks) != 1:
            raise ValueError("event must name one trusted task")
        queue.add(tasks, config)
        return {"status": "INGESTED", "decision": queue.status()}
    if route == "/v1/next":
        return queue.claim(config)
    if route == "/v1/result":
        return queue.complete(payload["task_id"], payload["lease"], config, payload["evidence"])
    if route == "/v1/final/claim":
        return queue.final_claim(config, "chatgpt")
    if route == "/v1/final/result":
        return queue.final_decide(config, "chatgpt", payload)
    if route == "/v1/reconcile":
        return queue.reconcile(payload["task_id"], payload["run"], config)
    if route == "/v1/final/reconcile":
        return queue.final_reconcile(config, payload["task_id"], payload["run"])
    if route == "/v1/handoffs/observe":
        return queue.associate_handoff(config, payload)
    return {"handoffs": queue.external_handoffs(config)}


def handler(queue, config_path, secrets):
    if set(secrets) != set(ROUTES.values()) or len(set(secrets.values())) != len(secrets) or any(
        not isinstance(value, str) or not value.isascii() or len(value) < 32 for value in secrets.values()
    ):
        raise ValueError("distinct private bridge credentials required")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, code, payload):
            encoded = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self):
            role = ROUTES.get(self.path)
            supplied = self.headers.get("Authorization", "").encode()
            if not role or not hmac.compare_digest(supplied, f"Bearer {secrets[role]}".encode()):
                self.reply(403, {"status": "BLOCKED"})
                return
            self.connection.settimeout(10)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16384:
                    raise ValueError("invalid payload")
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError("invalid payload")
                result = operate(queue, json.loads(config_path.read_bytes()), self.path, payload)
            except (ValueError, TypeError, KeyError, OSError, sqlite3.Error):
                self.reply(400, {"status": "BLOCKED"})
                return
            self.reply(200, result)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8790)
    args = parser.parse_args()
    os.umask(0o077)
    secrets = json.loads(os.environ["ORCHESTRATOR_V1_BRIDGE_TOKENS"])
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler(Queue(args.db), args.config, secrets)) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
