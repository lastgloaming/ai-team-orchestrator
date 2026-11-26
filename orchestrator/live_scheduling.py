"""Explicitly gated one-shot advice on a dedicated private paired ledger."""

import json
import os
import re
import sqlite3
import subprocess
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from .auto_events import AUTHOR_ACCOUNT, REPOSITORY, positive_id
from .auto_state import SCHEMA as COMMUNICATION_SCHEMA, PairedState
from .core import transaction
from .deepseek import Advisor, NoRedirect, live_transport
from .live_approval import Execution, binding_bytes, decode_snapshot, execution_from_payload, request_digest
from .mock_scheduling import Scheduler, model_transaction
from .scheduling_ledger import ModelStatus, check_transition, inspect_snapshot
from .state import CHECKPOINT, StateConflict

ENVIRONMENT = "p5-private-live"
COMMAND = "P5-SYNTHETIC-LIVE-ADVICE"
REFS = ("refs/heads/p5-live-charge-state", "refs/heads/p5-live-charge-checkpoints")
LEDGER_NAME = re.compile(r"lastgloaming/[A-Za-z0-9][A-Za-z0-9_.-]{0,98}")
AUTH_ID = re.compile(r"[0-9a-f]{32}")
# Deployment is blocked until an independently durable private trust design is
# reviewed. These are code-reviewed trust settings, never repository variables.
PRIVATE_DURABILITY_APPROVED = False
REVIEWED_APPROVAL_KEY_SHA256 = None

AUTH_SCHEMA = """
CREATE TABLE live_authorization (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    approval TEXT NOT NULL,
    event_id INTEGER NOT NULL UNIQUE REFERENCES auto_inbox(id),
    repository_id INTEGER NOT NULL,
    pr INTEGER NOT NULL,
    author INTEGER NOT NULL CHECK(author = 338499555),
    binding TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class Authorization:
    approval: str
    event_id: int
    repository_id: int
    pr: int
    execution: Execution

    def __post_init__(self):
        if (not AUTH_ID.fullmatch(self.approval) or not all(positive_id(value) for value in
                (self.event_id, self.repository_id, self.pr)) or self.pr <= 1):
            raise ValueError("exact one-shot approval and isolated PR required")
        execution = self.execution
        if (not CHECKPOINT.fullmatch(execution.code_sha) or not LEDGER_NAME.fullmatch(execution.ledger_repo)
                or execution.ledger_repo.lower() == REPOSITORY
                or not positive_id(execution.ledger_id) or execution.ledger_id == self.repository_id
                or len(execution.anchors) != 2 or execution.anchors[0] == execution.anchors[1]
                or any(not CHECKPOINT.fullmatch(anchor) for anchor in execution.anchors)
                or type(execution.request_version) is not int or execution.request_version != 1
                or not re.fullmatch(r"[0-9a-f]{64}", execution.request_sha256)):
            raise ValueError("invalid reviewed execution binding")


class PrivateLedger(PairedState):
    refs = REFS
    schema = COMMUNICATION_SCHEMA + AUTH_SCHEMA

    def __init__(self, remote, anchors=None, *, token=None, ledger_repo=None, ledger_id=None):
        super().__init__(remote, ENVIRONMENT, anchors)
        self.git_token = token
        self.ledger_identity = (ledger_repo, ledger_id)

    @staticmethod
    def inspect_snapshot(path):
        model = inspect_snapshot(path)
        inbox, eligible, attempts, _ = model
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
            bindings = db.execute(
                "SELECT singleton,approval,event_id,repository_id,pr,author,binding FROM live_authorization"
            ).fetchall()
            if any(db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                   for table in ("auto_binding", "auto_outbox")):
                raise StateConflict("private billing ledger contains communication records")
        if not bindings:
            if inbox or eligible is not None:
                raise StateConflict("unbound private billing history")
            return model, None
        if len(bindings) != 1:
            raise StateConflict("invalid private one-shot binding")
        singleton, approval, event_id, repo, pr, author, encoded = bindings[0]
        payload = json.loads(encoded)
        bound = parse_approval(payload)
        if (binding_bytes(bound).decode() != encoded
                or (bound.approval, bound.event_id, bound.repository_id, bound.pr) != (approval, event_id, repo, pr)):
            raise StateConflict("approval snapshot does not match private ledger columns")
        if (singleton != 1 or author != AUTHOR_ACCOUNT or inbox != {event_id: 1}
                or eligible != {event_id} or set(attempts) != {event_id}):
            raise StateConflict("private billing reservation and approval do not match")
        return model, bindings[0]

    @staticmethod
    def check_transition(previous, current):
        check_transition(previous[0], current[0])
        if previous[1] is not None and previous[1] != current[1]:
            raise StateConflict("one-shot approval changed")


def authorize(path, approved):
    with transaction(path, schema="") as db:
        row = db.execute("SELECT approval,event_id,repository_id,pr,author,binding FROM live_authorization").fetchone()
        values = (approved.approval, approved.event_id, approved.repository_id, approved.pr,
                  AUTHOR_ACCOUNT, binding_bytes(approved).decode())
        if row is not None:
            if row != values:
                raise StateConflict("different approval cannot reuse this private ledger")
            return
        db.execute("INSERT INTO auto_inbox (id,consumed) VALUES (?,1)", (approved.event_id,))
        db.execute("INSERT INTO live_authorization VALUES (1,?,?,?,?,?,?)", values)
    with model_transaction(path) as db:
        db.execute("INSERT INTO auto_model_eligibility VALUES (?)", (approved.event_id,))


def event_matches(event, actor, approved):
    try:
        repo, issue, comment = event["repository"], event["issue"], event["comment"]
        return (
            event["action"] == "created" and actor == str(AUTHOR_ACCOUNT)
            and event["sender"]["id"] == comment["user"]["id"] == AUTHOR_ACCOUNT
            and repo["full_name"] == REPOSITORY and repo["id"] == approved.repository_id
            and issue["number"] == approved.pr and issue["state"] == "open"
            and isinstance(issue["pull_request"], dict)
            and comment["id"] == approved.event_id and comment["body"] == COMMAND
        )
    except (KeyError, TypeError):
        return False


def parse_approval(payload):
    data = dict(payload)
    data["execution"] = execution_from_payload(data["execution"])
    return Authorization(**data)


def verify_execution(approved, limits, code_sha, ledger_identity, anchors):
    execution = approved.execution
    if (code_sha != execution.code_sha or ledger_identity != (execution.ledger_repo, execution.ledger_id)
            or anchors != execution.anchors or limits != execution.limits
            or limits.model != "deepseek-flash" or limits.max_output != 64
            or limits.daily_calls != 1 or limits.monthly_calls != 1
            or request_digest(Advisor(None, limits, None).request("ACCEPTED")) != execution.request_sha256):
        raise ValueError("runtime configuration differs from signed execution")


def schedule_event(state, approved, event, actor, limits, transport, *, scheduling, live, token, code_sha):
    if not scheduling() or not live():
        return {"communication": "DISABLED", "model_call": "NOT_SENT", "scheduling": "DISABLED"}
    if not event_matches(event, actor, approved):
        return {"communication": "REJECTED", "model_call": "NOT_SENT", "scheduling": "NOT_ELIGIBLE"}
    if not isinstance(state, PrivateLedger):
        raise ValueError("private ledger required")
    verify_execution(approved, limits, code_sha, state.ledger_identity, state.anchors)
    expected_remote = f"https://github.com/{approved.execution.ledger_repo}.git"
    if state.remote.startswith(("https://", "http://")) and state.remote != expected_remote:
        raise ValueError("billing remote differs from signed execution")
    if not token:
        raise ValueError("configured model credential required")
    attempted = False

    def invoke(request, timeout):
        nonlocal attempted
        attempted = True
        return transport(request, timeout)

    try:
        outcome = Scheduler(state, limits, invoke, gate=lambda: scheduling() and live(),
                            authorize=lambda path: authorize(path, approved))(approved.event_id)
    except StateConflict:
        return {"communication": "UNCONFIRMED", "model_call": "UNCONFIRMED" if attempted else "NOT_SENT",
                "scheduling": "BLOCKED", "event_id": approved.event_id, "approval_id": approved.approval}
    model_call = {
        ModelStatus.ADVISED: "SUCCEEDED", ModelStatus.REJECTED: "INVALID_RESPONSE",
        ModelStatus.UNCERTAIN: "UNCONFIRMED", ModelStatus.RESERVED: "UNCONFIRMED",
    }.get(outcome.status, "NOT_SENT")
    if not attempted and model_call in {"SUCCEEDED", "INVALID_RESPONSE"}:
        model_call = f"RECORDED_{model_call}"
    return {"communication": "ACCEPTED", "model_call": model_call,
            "scheduling": outcome.status.value, "event_id": approved.event_id,
            "approval_id": approved.approval}


def checked_remote(repo, repository_id, token):
    if (not token or not LEDGER_NAME.fullmatch(repo) or repo.lower() == REPOSITORY
            or not positive_id(repository_id)):
        raise ValueError("separate private billing repository and credential required")
    opener = urllib.request.build_opener(NoRedirect())
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}",
                                 headers={"Authorization": f"Bearer {token}",
                                          "Accept": "application/vnd.github+json"})
    with opener.open(req, timeout=10) as response:
        metadata = json.load(response)
    if (metadata["id"] != repository_id or metadata["full_name"] != repo
            or metadata["visibility"] != "private" or metadata["private"] is not True
            or metadata["fork"] is not False):
        raise ValueError("billing repository failed privacy and identity checks")
    return f"https://github.com/{repo}.git"


def check_source(approved, token, *, marker=COMMAND):
    """A delayed or retried run must not rely only on the original issue payload."""
    if not token:
        raise ValueError("source metadata credential missing")
    opener = urllib.request.build_opener(NoRedirect())
    base = f"https://api.github.com/repos/{REPOSITORY}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    with opener.open(urllib.request.Request(f"{base}/pulls/{approved.pr}", headers=headers), timeout=10) as response:
        pr = json.load(response)
    repo = pr["base"]["repo"]
    if (pr["state"] != "open" or pr["number"] != approved.pr or repo["id"] != approved.repository_id
            or repo["full_name"] != REPOSITORY):
        raise ValueError("isolated PR no longer matches approval")
    with opener.open(urllib.request.Request(f"{base}/issues/comments/{approved.event_id}", headers=headers),
                     timeout=10) as response:
        comment = json.load(response)
    if (comment["id"] != approved.event_id or comment["user"]["id"] != AUTHOR_ACCOUNT
            or comment["issue_url"] != f"{base}/issues/{approved.pr}" or comment["body"] != marker):
        raise ValueError("approved comment no longer matches")


def main():
    env = os.environ
    if env.get("GITHUB_REPOSITORY") != REPOSITORY or env.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("trusted main workflow required")

    def scheduling():
        return env.get("ORCHESTRATOR_P5_SCHEDULING_ENABLED") == "1"

    def live():
        return env.get("ORCHESTRATOR_P5_LIVE_APPROVED") == "1"

    if not PRIVATE_DURABILITY_APPROVED or not REVIEWED_APPROVAL_KEY_SHA256:
        raise ValueError("BLOCKED: private rollback protection and approval trust root are not verified")
    bootstrap = env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    if bootstrap:
        expected = env.get("ORCHESTRATOR_P5_BOOTSTRAP_MAIN", "")
        if (env.get("ORCHESTRATOR_P5_BOOTSTRAP_APPROVED") != "1" or scheduling() or live()
                or not CHECKPOINT.fullmatch(expected) or expected != env.get("GITHUB_SHA")
                or expected != subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
                or env.get("ORCHESTRATOR_P5_STATE_ANCHOR") or env.get("ORCHESTRATOR_P5_WITNESS_ANCHOR")):
            raise ValueError("bootstrap requires separate approval, pinned main, no anchors and disabled gates")
    elif env.get("GITHUB_EVENT_NAME") != "issue_comment" or not scheduling() or not live():
        return

    if bootstrap:
        ledger_token = env.get("ORCHESTRATOR_P5_LEDGER_TOKEN")
        remote = checked_remote(env["ORCHESTRATOR_P5_LEDGER_REPO"], int(env["ORCHESTRATOR_P5_LEDGER_ID"]), ledger_token)
        PrivateLedger(remote, token=ledger_token).bootstrap()
        print("Private genesis pair verified. Read and pin both refs through the private repository.")
        return

    approved = parse_approval(decode_snapshot(env["ORCHESTRATOR_P5_SIGNED_APPROVAL"],
                                             REVIEWED_APPROVAL_KEY_SHA256))
    execution = approved.execution
    code_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if env.get("GITHUB_SHA") != code_sha:
        raise ValueError("checkout differs from Actions commit")
    verify_execution(approved, execution.limits, code_sha, (execution.ledger_repo, execution.ledger_id),
                     execution.anchors)
    event = json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes())
    actor = env.get("GITHUB_ACTOR_ID")
    if not event_matches(event, actor, approved):
        raise ValueError("comment not approved")
    ledger_token = env.get("ORCHESTRATOR_P5_LEDGER_TOKEN")
    model_token = env.get("DEEPSEEK_API_KEY")
    if not model_token:
        raise ValueError("model credential missing")
    check_source(approved, env.get("GITHUB_TOKEN"))
    remote = checked_remote(execution.ledger_repo, execution.ledger_id, ledger_token)
    transport = partial(live_transport, approval_env="ORCHESTRATOR_P5_LIVE_APPROVED")
    state = PrivateLedger(remote, execution.anchors, token=ledger_token,
                          ledger_repo=execution.ledger_repo, ledger_id=execution.ledger_id)
    result = schedule_event(state, approved, event, actor, execution.limits, transport,
                            scheduling=scheduling, live=live, token=model_token, code_sha=code_sha)
    print(json.dumps(result))
    if result["scheduling"] != "ADVISED":
        raise SystemExit("P5 scheduling did not complete")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        if not PRIVATE_DURABILITY_APPROVED:
            raise SystemExit("BLOCKED: private rollback protection is unverified.") from None
        raise SystemExit("P5 stopped; inspect the private checkpoint before any further action.") from None
