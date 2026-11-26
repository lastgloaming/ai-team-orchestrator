"""Explicitly gated P4 state and one-shot model entry points."""

import argparse
import os
import json
import urllib.request

from .core import IDENTIFIER, read_historical_receipt, transaction
from .deepseek import Advisor, InvalidResponse, Limits, NoRedirect, live_transport
from .state import LIVE_REMOTE, GitState, consume, enqueue


def probe_command(event_id):
    if not IDENTIFIER.fullmatch(event_id):
        raise ValueError("invalid probe ID")
    return (
        f"AI-INTEGRATION-COMMAND\nid: {event_id}\nfrom: ChatGPT\ntarget: integration\n"
        "scope: synthetic handshake only\ntask: HANDSHAKE"
    )


def probe_binding(pr_number, expected_actor, shared_actor):
    if any(type(value) is not int or value <= 0 for value in (pr_number, expected_actor, shared_actor)):
        raise ValueError("invalid Integration binding")
    if pr_number <= 1 or expected_actor == shared_actor:
        raise ValueError("require an isolated PR and an independent Integration identity")
    return {"kind": "integration_probe", "pr": pr_number, "actor": expected_actor, "shared": shared_actor}


def prepare_probe(event_id, payload):
    if payload != probe_binding(payload["pr"], payload["actor"], payload["shared"]):
        raise ValueError("unsupported P4 event")
    return {"command": probe_command(event_id), "binding": payload}


def verify_probe(event_id, binding, ack, result, *, ack_origin, result_origin):
    """Origins must be authenticated (repository, PR number, numeric actor ID) tuples."""
    validated = probe_binding(binding["pr"], binding["actor"], binding["shared"])
    if binding != validated:
        return False
    origin = ("lastgloaming/ai-team-orchestrator", binding["pr"], binding["actor"])
    if ack_origin != origin or result_origin != origin:
        return False
    for body, kind, status in ((ack, "ACK", "ACCEPTED"), (result, "RESULT", "DONE")):
        try:
            receipt = read_historical_receipt(body)
        except ValueError:
            return False
        if not receipt or (receipt.id, receipt.kind, receipt.fields["status"]) != (event_id, kind, status):
            return False
    return True


def send_probe(state, event_id, binding, transport):
    if not IDENTIFIER.fullmatch(event_id):
        raise ValueError("invalid probe ID")

    def claim(path):
        with transaction(path) as db:
            ready = db.execute("SELECT result FROM inbox WHERE id=?", (event_id,)).fetchone()
            if not ready or ready[0] is None or json.loads(ready[0]) != prepare_probe(event_id, binding):
                raise ValueError("probe must be prepared for this PR and identity")
            return bool(db.execute("INSERT OR IGNORE INTO probe_sends VALUES (?)", (event_id,)).rowcount)

    if not state.update(claim):
        return "probe already claimed; no comment sent"
    try:
        comment_id = transport(binding["pr"], probe_command(event_id))
    except Exception:
        raise RuntimeError("probe delivery unconfirmed; reconcile before further action") from None
    return f"probe comment ID: {comment_id}"


def github_probe_transport():
    token = os.environ["ORCHESTRATOR_PROBE_TOKEN"]
    opener = urllib.request.build_opener(NoRedirect())

    def send(pr_number, command):
        base = "https://api.github.com/repos/lastgloaming/ai-team-orchestrator"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                   "Content-Type": "application/json"}
        with opener.open(urllib.request.Request(f"{base}/pulls/{pr_number}", headers=headers), timeout=10) as response:
            if json.load(response)["state"] != "open":
                raise ValueError("probe target must be an open test PR")
        request = urllib.request.Request(
            f"{base}/issues/{pr_number}/comments", headers=headers, method="POST",
            data=json.dumps({"body": command}).encode(),
        )
        with opener.open(request, timeout=10) as response:
            return json.load(response)["id"]

    return send


def single_call(state, limits, transport):
    if (limits.retries, limits.concurrent, limits.daily_calls, limits.monthly_calls) != (0, 1, 1, 1):
        raise ValueError("P4 requires one call, with zero retries")

    def reserve(path):
        advisor = Advisor(path, limits, transport)
        with transaction(path) as db:
            if db.execute("SELECT 1 FROM p4_call").fetchone():
                advisor._require_currency(db)
                return None
        reservation = advisor._reserve()
        with transaction(path) as db:
            db.execute("INSERT INTO p4_call VALUES (1, ?)", (reservation,))
        return reservation

    reservation = state.update(reserve)
    if reservation is None:
        return "already reserved; no request sent"
    request = Advisor(None, limits, transport).request("ACCEPTED")
    try:
        response = transport(request, 10)
    except Exception:
        state.update(lambda path: Advisor(path, limits, transport).reconcile_terminated_attempt(reservation))
        raise RuntimeError("model attempt failed; reservation retained") from None

    def settle(path):
        advisor = Advisor(path, limits, transport)
        try:
            advisor._account_and_validate(reservation, response)
        except (InvalidResponse, TypeError, ValueError, KeyError, IndexError):
            advisor.reconcile_terminated_attempt(reservation)
            return False
        return True

    if not state.update(settle):
        raise RuntimeError("model response failed validation; inspect state ledger")
    return "one model request settled"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("initialize-state", "enqueue-probe", "prepare-probe", "send-probe", "deepseek-once"))
    parser.add_argument("event_id", nargs="?")
    args = parser.parse_args()
    if os.environ.get("ORCHESTRATOR_P4_STATE_APPROVED") != "1":
        parser.error("P4 state writes are disabled")
    if not os.environ.get("ORCHESTRATOR_STATE_TOKEN"):
        parser.error("approved state credential is required")
    checkpoint = os.environ.get("ORCHESTRATOR_P4_STATE_CHECKPOINT") or None
    state = GitState(LIVE_REMOTE, checkpoint)
    if args.operation == "initialize-state":
        if os.environ.get("ORCHESTRATOR_P4_BOOTSTRAP_APPROVED") != "1" or checkpoint:
            parser.error("state bootstrap is disabled or a checkpoint already exists")
        print(f"state checkpoint: {state.initialize()}")
        return
    if not checkpoint:
        parser.error("trusted state checkpoint is required")
    try:
        run(state, args, parser)
    finally:
        print(f"last confirmed state checkpoint: {state.checkpoint}")


def configured_probe():
    return probe_binding(int(os.environ["ORCHESTRATOR_P4_TEST_PR"]),
                         int(os.environ["ORCHESTRATOR_P4_INTEGRATION_ACTOR"]),
                         int(os.environ["ORCHESTRATOR_P4_SHARED_ACTOR"]))


CNY_TEST_BUDGET = 2_200_000_000


def configured_model_limits():
    currency = os.environ.get("ORCHESTRATOR_P4_BILLING_CURRENCY") or "USD"
    rates = {"USD": (300, 1200, "NANODOLLARS"), "CNY": (2000, 8000, "NANOYUAN")}
    if currency not in rates:
        raise ValueError("billing currency must be USD or CNY")
    input_rate, output_rate, denomination = rates[currency]
    budget = int(os.environ[f"ORCHESTRATOR_P4_BUDGET_{denomination}"])
    if currency == "CNY" and budget > CNY_TEST_BUDGET:
        raise ValueError("CNY test budget exceeds 2.20 CNY")
    input_bound = int(os.environ["ORCHESTRATOR_P4_INPUT_TOKEN_BOUND"])
    return Limits("deepseek-flash", input_rate, output_rate, input_bound, 64,
                  budget, budget, 1, 1, concurrent=1, retries=0, currency=currency)


def run(state, args, parser):

    if args.operation == "enqueue-probe":
        if not args.event_id:
            parser.error("event_id is required")
        binding = configured_probe()
        added = state.update(lambda path: enqueue(path, args.event_id, binding))
        print("event committed" if added else "event already committed")
    elif args.operation == "prepare-probe":
        print(state.update(lambda path: consume(path, prepare_probe)))
    elif args.operation == "send-probe":
        if (os.environ.get("ORCHESTRATOR_P4_PROBE_APPROVED") != "1"
                or not os.environ.get("ORCHESTRATOR_PROBE_TOKEN") or not args.event_id):
            parser.error("Integration probe is disabled or missing configuration")
        print(send_probe(state, args.event_id, configured_probe(), github_probe_transport()))
    else:
        if os.environ.get("ORCHESTRATOR_LIVE_APPROVED") != "1" or not os.environ.get("DEEPSEEK_API_KEY"):
            parser.error("live model request is disabled or missing credentials")
        print(single_call(state, configured_model_limits(), live_transport))


if __name__ == "__main__":
    main()
