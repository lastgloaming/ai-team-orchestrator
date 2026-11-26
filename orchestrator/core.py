"""Offline event processor. Adapters supply authenticated event metadata."""

import json
import re
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass

ROLES = frozenset({"author", "chatgpt", "integration", "memory", "editor", "writer", "codex"})
TARGETS = ROLES - {"chatgpt", "author"}
SCOPES = {"coordination": {"integration", "memory", "editor"}, "status": TARGETS}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}")
SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (event_id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY, requester TEXT NOT NULL, target TEXT NOT NULL,
  scope TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL,
  delivered INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS usage (
  id INTEGER PRIMARY KEY, day TEXT NOT NULL, model TEXT NOT NULL,
  amount INTEGER NOT NULL, state TEXT NOT NULL,
  input_tokens INTEGER, output_tokens INTEGER, attempts INTEGER NOT NULL DEFAULT 0
);
"""


@contextmanager
def transaction(path):
    with closing(sqlite3.connect(path, timeout=10)) as db, db:
        db.executescript(SCHEMA)
        db.execute("BEGIN IMMEDIATE")
        yield db


@dataclass(frozen=True)
class Message:
    kind: str
    id: str
    fields: dict[str, str]


def parse(body):
    if not isinstance(body, str) or len(body) > 8192:
        raise ValueError("invalid body")
    lines = body.splitlines()
    marker = lines[0] if lines else ""
    formats = {
        "COMMAND": {"id", "from", "target", "scope", "task"},
        "REVIEW": {"id", "from", "target", "scope", "task", "ref"},
        "RESULT": {"id", "status", "evidence", "blockers"},
    }
    kind = marker.removeprefix("AI-INTEGRATION-")
    if marker != "AI-INTEGRATION-" + kind or kind not in formats:
        return None
    fields = {}
    for line in lines[1:]:
        key, separator, value = line.partition(":")
        if not separator or key not in formats[kind] or key in fields or not value.strip():
            raise ValueError("invalid field")
        fields[key] = value.strip()
    if fields.keys() != formats[kind]:
        raise ValueError("missing field")
    for key in {"id", "ref"} & fields.keys():
        if not IDENTIFIER.fullmatch(fields[key]):
            raise ValueError("invalid identifier")
    return Message(kind, fields["id"], fields)


def event_metadata(event):
    try:
        comment = event["comment"]
        actor = event["sender"]["id"]
        delivery = comment["id"]
        issue = event["issue"]
        if (event["action"] != "created" or
                event["repository"]["full_name"] != "lastgloaming/ai-writer" or
                issue["number"] != 3 or not isinstance(issue["pull_request"], dict) or
                type(actor) is not int or actor <= 0 or actor != comment["user"]["id"] or
                type(delivery) is not int or delivery <= 0):
            return None
        return actor, delivery, comment["body"]
    except (KeyError, TypeError):
        return None


class Orchestrator:
    def __init__(self, path, principals, shared_ids):
        if not shared_ids or principals.keys() & set(shared_ids):
            raise ValueError("identify and exclude shared accounts")
        if not set(principals.values()) <= ROLES or len(set(principals.values())) != len(principals):
            raise ValueError("require distinct known roles")
        self.path = path
        self.principals = dict(principals)

    def process(self, event):
        """Accept only payloads from an already authenticated event adapter."""
        metadata = event_metadata(event)
        if metadata is None:
            return "IGNORED"
        actor, delivery, body = metadata
        role = self.principals.get(actor)
        if role is None:
            return "UNAUTHORIZED"
        try:
            message = parse(body)
        except ValueError:
            return "REJECTED"
        if message is None:
            return "IGNORED"
        with transaction(self.path) as db:
            if not db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (delivery,)).rowcount:
                return "DUPLICATE"
            if message.kind == "COMMAND":
                return self._command(db, message, role)
            if message.kind == "RESULT":
                return self._result(db, message, role)
            return self._review(db, message, role)

    def _command(self, db, message, role):
        fields = message.fields
        target, scope = fields["target"], fields["scope"]
        if role not in {"author", "chatgpt"} or fields["from"].lower() != role or target not in TARGETS:
            return "UNAUTHORIZED"
        # Free-form instructions never enter the execution queue.
        allowed_tasks = {"coordination": {"summarize_status", "list_blockers"}, "status": {"report_status"}}
        allowed = target in SCOPES.get(scope, ()) and fields["task"] in allowed_tasks.get(scope, ())
        state = "ACCEPTED" if allowed else "WAITING_AUTHOR"
        scope = scope if scope in SCOPES else "unclassified"
        if not db.execute("INSERT OR IGNORE INTO decisions VALUES (?)", (message.id,)).rowcount:
            return "DUPLICATE"
        db.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?)", (message.id, role, target, scope, state))
        if allowed:
            self._emit(db, "COMMAND", message.id, role, target, scope, state, action=fields["task"])
            self._emit(db, "ACK", message.id, "orchestrator", role, scope, state)
        else:
            self._emit(db, state, message.id, "orchestrator", "author", scope, state)
        return state

    def _result(self, db, message, role):
        row = db.execute("SELECT requester,target,scope,status FROM tasks WHERE id=?", (message.id,)).fetchone()
        if not row or role != row[1]:
            return "UNAUTHORIZED"
        status = message.fields["status"]
        if status not in {"DONE", "BLOCKED"}:
            return "REJECTED"
        if row[3] != "ACCEPTED":
            return "IGNORED"
        db.execute("UPDATE tasks SET status=? WHERE id=?", (status, message.id))
        kind = "BLOCKED" if status == "BLOCKED" else "RESULT"
        # Evidence stays on the source PR. Its contents are never routed or sent to a model.
        self._emit(db, kind, message.id, role, row[0], row[2], status)
        return status

    def _review(self, db, message, role):
        fields = message.fields
        ref = fields["ref"]
        row = db.execute("SELECT target,scope,status FROM tasks WHERE id=?", (ref,)).fetchone()
        if role != "chatgpt" or fields["from"].lower() != role or not row or row[:2] != (
            fields["target"], fields["scope"]
        ):
            return "UNAUTHORIZED"
        if row[2] != "DONE" or fields["task"] != "record_review":
            return "REJECTED"
        if not db.execute("INSERT OR IGNORE INTO decisions VALUES (?)", (message.id,)).rowcount:
            return "DUPLICATE"
        db.execute("UPDATE tasks SET status='REVIEWED' WHERE id=?", (ref,))
        self._emit(db, "REVIEW", message.id, role, "author", row[1], "REVIEWED", task_id=ref)
        return "REVIEWED"

    @staticmethod
    def _emit(db, kind, ref, source, target, scope, status, task_id=None, **details):
        message_id = f"{kind}:{ref}"
        payload = dict(id=message_id, kind=kind, source=source, target=target,
                       scope=scope, status=status, task_id=task_id or ref, **details)
        db.execute("INSERT INTO outbox (id,payload) VALUES (?,?)", (message_id, json.dumps(payload)))

    def pending(self):
        with transaction(self.path) as db:
            return [json.loads(payload) for (payload,) in db.execute(
                "SELECT payload FROM outbox WHERE delivered=0 ORDER BY rowid"
            )]

    def mark_delivered(self, message_id):
        with transaction(self.path) as db:
            db.execute("UPDATE outbox SET delivered=1 WHERE id=?", (message_id,))
