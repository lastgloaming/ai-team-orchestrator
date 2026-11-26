"""Synthetic P4 exercise. Fixture credentials and endpoint events have no live authority."""

import argparse
import hmac
import json
import sqlite3
import tempfile
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.core import Orchestrator, transaction
from orchestrator.deepseek import Advisor, BudgetExceeded, Limits

ACTORS = {"chatgpt": 101, "editor": 102, "shared": 199}
KEYS = {role: f"public-fixture-key:{role}".encode() for role in ACTORS}
COMMAND = """AI-INTEGRATION-COMMAND
id: CMD-P4-1
from: ChatGPT
target: editor
scope: coordination
task: summarize_status"""
RESULT = """AI-INTEGRATION-RESULT
id: CMD-P4-1
status: DONE
evidence: synthetic observatory shift summary
blockers: none"""


def envelope(role, body, event_id, *, sender=None):
    actor = ACTORS[role] if sender is None else sender
    raw = json.dumps({
        "action": "created",
        "sender": {"id": actor},
        "repository": {"full_name": "lastgloaming/ai-writer"},
        "issue": {"number": 3, "pull_request": {}},
        "comment": {"id": event_id, "user": {"id": actor}, "body": body},
    }).encode()
    return role, raw, hmac.digest(KEYS[role], raw, "sha256")


def receive(core, role, raw, signature):
    if role not in {"chatgpt", "editor"}:
        return "UNAUTHORIZED"
    if not hmac.compare_digest(hmac.digest(KEYS[role], raw, "sha256"), signature):
        return "UNAUTHORIZED"
    event = json.loads(raw)
    if event.get("sender") != {"id": ACTORS[role]}:
        return "UNAUTHORIZED"
    return core.process(event)


def new_core(path, now):
    principals = {ACTORS[role]: role for role in ("chatgpt", "editor")}
    return Orchestrator(path, principals, {ACTORS["shared"]}, now=lambda: now)


def advisor(path, transport):
    limits = Limits("mock-flash", 300, 1200, 1024, 64, 384000, 384000, 1, 1, retries=0)
    return Advisor(path, limits, transport, now=lambda: datetime(2026, 11, 26, tzinfo=UTC))


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def prepare(snapshot):
    require(not snapshot.exists(), "snapshot already exists")
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "ledger.sqlite"
        core = new_core(path, 100)
        require(receive(core, *envelope("shared", COMMAND, 1)) == "UNAUTHORIZED",
                "shared identity was accepted")
        require(receive(core, *envelope("editor", COMMAND, 1, sender=ACTORS["chatgpt"])) == "UNAUTHORIZED",
                "editor key accepted a ChatGPT sender")
        role, raw, signature = envelope("chatgpt", COMMAND, 1)
        require(receive(core, role, raw + b" ", signature) == "UNAUTHORIZED", "tampering was accepted")
        require(receive(core, role, raw, signature) == "ACCEPTED", "command was not accepted")

        # Simulate an interrupted dispatcher after the durable claim and before transport.
        first = core.claim_next()
        require(first is not None and first[0]["kind"] == "COMMAND", "command was not claimed")
        calls = []

        def mock_call(request, timeout):
            calls.append(request)
            return {
                "usage": {"prompt_tokens": 32, "completion_tokens": 8},
                "choices": [{"finish_reason": "stop", "message": {"content": '{"suggestion":"inspect_status"}'}}],
            }

        require(advisor(path, mock_call).suggest("ACCEPTED") == "inspect_status", "unexpected advice")
        require(len(calls) == 1 and calls[0]["thinking"] == {"type": "disabled"}
                and calls[0]["max_tokens"] == 64, "single-call request shape changed")
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as source, closing(sqlite3.connect(snapshot)) as destination:
            source.backup(destination)
    print("Authenticated synthetic command, preserved interrupted outbox claim, one mock model call.")


def restore(snapshot):
    require(snapshot.is_file(), "missing snapshot")
    core = new_core(snapshot, 161)
    with transaction(snapshot) as db:
        require(db.execute("PRAGMA integrity_check").fetchone() == ("ok",), "invalid SQLite snapshot")
        require(db.execute("SELECT state,input_tokens,output_tokens FROM usage").fetchall()
                == [("counted", 32, 8)], "budget ledger did not survive")
    require(receive(core, *envelope("chatgpt", COMMAND, 1)) == "DUPLICATE", "deduplication did not survive")

    def unexpected_call(*_):
        raise RuntimeError("budget failed to block another call")

    try:
        advisor(snapshot, unexpected_call).suggest("ACCEPTED")
    except BudgetExceeded:
        pass
    else:
        raise RuntimeError("expected persisted call cap to stop the advisor")

    delivered = []
    while claim := core.claim_next():
        message, token = claim
        delivered.append(message["id"])
        if message["kind"] == "COMMAND":
            require(message["target"] == "editor", "unexpected executor")
            require(receive(core, *envelope("editor", RESULT, 2)) == "DONE", "executor result was rejected")
        require(core.mark_delivered(message["id"], token), "delivery confirmation failed")
    require(delivered == ["COMMAND:CMD-P4-1", "ACK:CMD-P4-1", "RESULT:CMD-P4-1"], "unexpected deliveries")
    require(not core.pending(), "undelivered items remain")
    print("Restored task, outbox, deduplication, and model usage. Mock COMMAND → ACK → RESULT completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "restore"))
    parser.add_argument("snapshot", type=Path)
    args = parser.parse_args()
    (prepare if args.phase == "prepare" else restore)(args.snapshot)
