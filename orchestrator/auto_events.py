"""Default-off GitHub entry point for synthetic comments on one isolated PR."""

import json
import os
import sqlite3
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .auto_state import PairedState
from .deepseek import NoRedirect
from .state import LIVE_REMOTE, StateConflict

REPOSITORY = "lastgloaming/ai-team-orchestrator"
COMMAND = "P4-SYNTHETIC-COMMAND"
KINDS = ("ACK", "RESULT")
SHARED_MODE = "shared-synthetic"
INDEPENDENT_MODE = "independent"
AUTHOR_ACCOUNT = 338499555
ACTIONS_BOT_ID = 41898282
ACTIONS_BOT_LOGIN = "github-actions[bot]"


class ReplyUnconfirmed(RuntimeError):
    """An incomplete synthetic task must produce a failing workflow result."""

    def __init__(self, reason):
        super().__init__(f"BLOCKED/UNCONFIRMED: {reason}")


def positive_id(value):
    return type(value) is int and 0 < value < 2**63


@dataclass(frozen=True)
class Binding:
    repository_id: int
    pr: int
    sender: int
    publisher: int
    shared: int
    mode: str = INDEPENDENT_MODE

    def __post_init__(self):
        identities = (self.sender, self.publisher, self.shared)
        if not all(positive_id(value) for value in (self.repository_id, self.pr, *identities)) or self.pr <= 1:
            raise ValueError("require an isolated PR and authenticated identities")
        if self.mode == SHARED_MODE:
            if identities != (AUTHOR_ACCOUNT, ACTIONS_BOT_ID, AUTHOR_ACCOUNT):
                raise ValueError("shared synthetic mode requires the approved author and Actions bot")
        elif self.mode != INDEPENDENT_MODE or len(set(identities)) != 3:
            raise ValueError("independent mode requires distinct authenticated identities")


def event_id(event, binding, actor):
    """Only consume the trusted Actions event file, not an arbitrary HTTP payload."""
    try:
        repository, issue, comment = event["repository"], event["issue"], event["comment"]
        if (event["action"] != "created" or repository["full_name"] != REPOSITORY
                or not positive_id(repository["id"]) or repository["id"] != binding.repository_id
                or not positive_id(issue["number"]) or issue["number"] != binding.pr
                or issue["state"] != "open" or not isinstance(issue["pull_request"], dict)
                or actor != str(binding.sender)
                or any(not positive_id(value) or value != binding.sender
                       for value in (event["sender"]["id"], comment["user"]["id"]))
                or comment["body"] != COMMAND or not positive_id(comment["id"])):
            return None
        return comment["id"]
    except (KeyError, TypeError):
        return None


def _enqueue(ledger, event, binding):
    with closing(sqlite3.connect(ledger)) as db, db:
        values = (REPOSITORY, binding.repository_id, binding.pr, binding.sender, binding.publisher, binding.shared)
        db.execute("INSERT OR IGNORE INTO auto_binding VALUES (1, ?, ?, ?, ?, ?, ?)", values)
        stored = db.execute(
            "SELECT repository, repository_id, pr, sender, publisher, shared FROM auto_binding WHERE singleton=1"
        ).fetchone()
        if stored != values:
            raise StateConflict("configured identities or PR do not match the persisted synthetic binding")
        db.execute("INSERT OR IGNORE INTO auto_inbox (id) VALUES (?)", (event,))


def _consume(ledger, event):
    with closing(sqlite3.connect(ledger)) as db, db:
        changed = db.execute("UPDATE auto_inbox SET consumed=1 WHERE id=? AND consumed=0", (event,)).rowcount
        if changed:
            db.executemany("INSERT INTO auto_outbox (id,event_id,kind) VALUES (?,?,?)",
                           [(f"{event}:{kind}", event, kind) for kind in KINDS])


def _claim(ledger, message_id):
    with closing(sqlite3.connect(ledger)) as db, db:
        return bool(db.execute("UPDATE auto_outbox SET status='claimed' WHERE id=? AND status='pending'",
                               (message_id,)).rowcount)


def _settle(ledger, message_id, comment_id):
    with closing(sqlite3.connect(ledger)) as db, db:
        changed = db.execute("UPDATE auto_outbox SET status='sent',comment_id=? WHERE id=? AND status='claimed'",
                             (comment_id, message_id)).rowcount
        if not changed and db.execute(
            "SELECT status,comment_id FROM auto_outbox WHERE id=?", (message_id,),
        ).fetchone() != ("sent", comment_id):
            raise RuntimeError("claimed synthetic reply missing or conflicting")


def process(state, event, binding, actor, publish, find_reply=None):
    identifier = event_id(event, binding, actor)
    if identifier is None:
        return False
    state.update(lambda ledger: _enqueue(ledger, identifier, binding))
    state.update(lambda ledger: _consume(ledger, identifier))
    for kind in KINDS:
        message_id = f"{identifier}:{kind}"
        context = (" Shared-account test with no AI attribution." if binding.mode == SHARED_MODE else "")
        body = f"[ORCHESTRATOR SYNTHETIC {kind}]\n\nSynthetic test only.{context} Reply ID: `{message_id}`."
        try:
            claimed = state.update(lambda ledger: _claim(ledger, message_id))
            if not claimed and state.update(lambda ledger: _status(ledger, message_id)) == "sent":
                continue
        except StateConflict:
            raise ReplyUnconfirmed("synthetic reply claim or status could not be confirmed") from None
        try:
            sent = publish(body) if claimed else (find_reply(body) if find_reply else None)
        except ReplyUnconfirmed:
            raise
        except Exception:
            raise ReplyUnconfirmed("synthetic reply could not be verified") from None
        if not positive_id(sent):
            # Missing or ambiguous observations cannot prove the comment was never sent.
            raise ReplyUnconfirmed("no unique authenticated synthetic reply was confirmed")
        try:
            state.update(lambda ledger: _settle(ledger, message_id, sent))
        except RuntimeError:
            raise ReplyUnconfirmed("synthetic reply settlement could not be confirmed") from None
    return True


def _status(ledger, identifier):
    with closing(sqlite3.connect(ledger)) as db, db:
        return db.execute("SELECT status FROM auto_outbox WHERE id=?", (identifier,)).fetchone()[0]


def github_publisher(binding, token):
    # Actions event metadata authenticates inbound comments. Authenticate the
    # configured outbound credential too before it can create a comment.
    opener = urllib.request.build_opener(NoRedirect())
    base = "https://api.github.com"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
               "Content-Type": "application/json"}

    comments = f"{base}/repos/{REPOSITORY}/issues/{binding.pr}/comments"

    def is_publisher(user):
        return (positive_id(user["id"]) and user["id"] == binding.publisher
                and (binding.mode != SHARED_MODE or
                     (user.get("login") == ACTIONS_BOT_LOGIN and user.get("type") == "Bot")))

    def send(body):
        if binding.mode == INDEPENDENT_MODE:
            with opener.open(urllib.request.Request(f"{base}/user", headers=headers), timeout=10) as response:
                if not is_publisher(json.load(response)):
                    raise ReplyUnconfirmed("reply credential identity does not match binding")
        # The trusted workflow injects github.token in shared mode. Installation
        # tokens cannot use GET /user, so confirm its bot author on the POST result.
        pr_url = f"{base}/repos/{REPOSITORY}/pulls/{binding.pr}"
        with opener.open(urllib.request.Request(pr_url, headers=headers), timeout=10) as response:
            target = json.load(response)
        repository = target["base"]["repo"]
        if (target["state"] != "open" or target["number"] != binding.pr
                or repository["full_name"] != REPOSITORY or not positive_id(repository["id"])
                or repository["id"] != binding.repository_id):
            raise ReplyUnconfirmed("target PR is closed or does not match binding")
        request = urllib.request.Request(
            comments, headers=headers,
            data=json.dumps({"body": body}).encode(), method="POST",
        )
        with opener.open(request, timeout=10) as response:
            result = json.load(response)
        if not is_publisher(result["user"]):
            raise RuntimeError("unexpected reply author")
        return result["id"]

    def find_reply(body):
        matches = []
        for page in range(1, 4):
            request = urllib.request.Request(f"{comments}?per_page=100&page={page}", headers=headers)
            with opener.open(request, timeout=10) as response:
                entries = json.load(response)
            matches.extend(entry["id"] for entry in entries
                           if is_publisher(entry["user"]) and entry["body"] == body)
            if len(entries) < 100:
                return matches[0] if len(matches) == 1 else None
        return None

    return send, find_reply


def main():
    if (os.environ.get("ORCHESTRATOR_AUTO_SYNTHETIC_ENABLED") != "1"
            or os.environ.get("GITHUB_EVENT_NAME") != "issue_comment"
            or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/main"):
        return
    mode = os.environ.get("ORCHESTRATOR_AUTO_IDENTITY_MODE") or INDEPENDENT_MODE
    repository_id, pr = (int(os.environ[f"ORCHESTRATOR_AUTO_{key}"]) for key in ("REPOSITORY_ID", "PR"))
    if mode == SHARED_MODE:
        binding = Binding(repository_id, pr, AUTHOR_ACCOUNT, ACTIONS_BOT_ID, AUTHOR_ACCOUNT, mode)
        reply_token = os.environ.get("GITHUB_TOKEN")
    else:
        identities = (int(os.environ[f"ORCHESTRATOR_AUTO_{key}"])
                      for key in ("SENDER_ID", "PUBLISHER_ID", "SHARED_ID"))
        binding = Binding(repository_id, pr, *identities, mode)
        reply_token = os.environ.get("ORCHESTRATOR_AUTO_REPLY_TOKEN")
    with Path(os.environ["GITHUB_EVENT_PATH"]).open() as stream:
        event = json.load(stream)
    actor = os.environ.get("GITHUB_ACTOR_ID")
    if event_id(event, binding, actor) is None:
        return
    if not os.environ.get("ORCHESTRATOR_STATE_TOKEN") or not reply_token:
        raise RuntimeError("existing isolated state and reply credentials are required")
    state = PairedState(LIVE_REMOTE, os.environ["ORCHESTRATOR_AUTO_ENVIRONMENT"],
                        (os.environ["ORCHESTRATOR_AUTO_STATE_ANCHOR"], os.environ["ORCHESTRATOR_AUTO_CHECKPOINT_ANCHOR"]))
    send, find_reply = github_publisher(binding, reply_token)
    process(state, event, binding, actor, send, find_reply)


if __name__ == "__main__":
    try:
        main()
    except ReplyUnconfirmed as error:
        raise SystemExit(str(error)) from None
