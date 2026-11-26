"""Offline event processor. Adapters supply authenticated event metadata."""

import json
import re
import sqlite3
import time
import uuid
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
CREATE TABLE IF NOT EXISTS outbox_leases (
  id TEXT PRIMARY KEY REFERENCES outbox(id), token TEXT NOT NULL, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS usage (
  id INTEGER PRIMARY KEY, day TEXT NOT NULL, model TEXT NOT NULL,
  amount INTEGER NOT NULL, state TEXT NOT NULL,
  input_tokens INTEGER, output_tokens INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
  currency TEXT NOT NULL DEFAULT 'USD' CHECK(currency IN ('USD', 'CNY'))
);
"""


@contextmanager
def transaction(path):
    with closing(sqlite3.connect(path, timeout=10)) as db, db:
        db.execute("PRAGMA foreign_keys = ON")
        db.executescript(SCHEMA)
        db.execute("BEGIN IMMEDIATE")
        yield db


@dataclass(frozen=True)
class Message:
    kind: str
    id: str
    fields: dict[str, str]


EXECUTABLE_FIELDS = {
    "COMMAND": {"id", "from", "target", "scope", "task"},
    "REVIEW": {"id", "from", "target", "scope", "task", "ref"},
    "RESULT": {"id", "status", "evidence", "blockers"},
}
RECEIPT_FIELDS = {
    "ACK": {"id", "status", "reason"},
    "RESULT": EXECUTABLE_FIELDS["RESULT"],
}
HISTORICAL_FIELDS = {
    "ACK": {"protocol", "from", "to", "command_id", "status", "reason"},
    "RESULT": {"protocol", "from", "to", "command_id", "status", "evidence"},
}
RECEIPT_STATUS = {
    "ACK": {"ACCEPTED", "REJECTED", "DUPLICATE", "BLOCKED"},
    "RESULT": {"DONE", "BLOCKED"},
}


def _message_lines(body):
    if not isinstance(body, str) or len(body) > 8192:
        raise ValueError("invalid body")
    body = body.replace("\r\n", "\n")
    if any(ord(char) < 32 and char not in "\n\t" for char in body):
        raise ValueError("invalid control character")
    marker, *lines = body.rstrip("\n").split("\n")
    prefix = "AI-INTEGRATION-"
    return marker[len(prefix):] if marker.startswith(prefix) else "", lines


def _read_fields(lines, *, multiline=False):
    fields = {}
    key = None
    for line in lines:
        if multiline and line.startswith((" ", "\t")) and key in {"reason", "evidence", "blockers"}:
            fields[key] += "\n" + line.strip()
            continue
        key, separator, value = line.partition(":")
        if not separator or key in fields or not value.strip():
            raise ValueError("invalid field")
        fields[key] = value.strip()
    return fields


def _validate_fields(kind, fields, expected, id_key="id"):
    if fields.keys() != expected:
        raise ValueError("unexpected or missing fields")
    for key in {id_key, "ref"} & fields.keys():
        if not IDENTIFIER.fullmatch(fields[key]):
            raise ValueError("invalid identifier")
    if kind in RECEIPT_STATUS and fields["status"] not in RECEIPT_STATUS[kind]:
        raise ValueError("invalid status")


def parse(body):
    """Read the strict protocol used for authenticated state transitions."""
    kind, lines = _message_lines(body)
    if kind not in EXECUTABLE_FIELDS:
        return None
    fields = _read_fields(lines)
    # Legacy receipts have their own read-only parser and cannot update state.
    if kind == "RESULT" and fields.keys() == HISTORICAL_FIELDS["RESULT"]:
        return None
    _validate_fields(kind, fields, EXECUTABLE_FIELDS[kind])
    return Message(kind, fields["id"], fields)


def read_historical_receipt(body):
    """Read receipt text for audit. Claimed identities are untrusted metadata."""
    kind, lines = _message_lines(body)
    if kind not in RECEIPT_FIELDS:
        return None
    fields = _read_fields(lines, multiline=True)
    if "protocol" in fields:
        if fields["protocol"] != "1.0":
            raise ValueError("unknown receipt protocol")
        _validate_fields(kind, fields, HISTORICAL_FIELDS[kind], "command_id")
        return Message(kind, fields["command_id"], fields)
    _validate_fields(kind, fields, RECEIPT_FIELDS[kind])
    return Message(kind, fields["id"], fields)


def event_metadata(event):
    try:
        comment = event["comment"]
        actor = event["sender"]["id"]
        delivery = comment["id"]
        comment_actor = comment["user"]["id"]
        issue = event["issue"]
        if (event["action"] != "created" or
                event["repository"]["full_name"] != "lastgloaming/ai-writer" or
                issue["number"] != 3 or not isinstance(issue["pull_request"], dict) or
                type(actor) is not int or not 0 < actor < 2**63 or
                type(comment_actor) is not int or actor != comment_actor or
                type(delivery) is not int or not 0 < delivery < 2**63):
            return None
        return actor, delivery, comment["body"]
    except (KeyError, TypeError):
        return None


class Orchestrator:
    def __init__(self, path, principals, shared_ids, *, now=time.time):
        shared_ids = set(shared_ids)
        if any(type(identity) is not int or not 0 < identity < 2**63
               for identity in principals.keys() | shared_ids):
            raise ValueError("require valid numeric GitHub IDs")
        if not shared_ids or principals.keys() & shared_ids:
            raise ValueError("identify and exclude shared accounts")
        if not set(principals.values()) <= ROLES or len(set(principals.values())) != len(principals):
            raise ValueError("require distinct known roles")
        self.path = path
        self.principals = dict(principals)
        self.now = now

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
        if db.execute("SELECT 1 FROM decisions WHERE id=?", (message.id,)).fetchone():
            return "DUPLICATE"
        if row[2] != "DONE" or fields["task"] != "record_review":
            return "REJECTED"
        db.execute("INSERT INTO decisions VALUES (?)", (message.id,))
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

    def claim_next(self, lease_seconds=60):
        """Claim one message. Use payload["id"] as the stable destination idempotency key."""
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds out of range")
        with transaction(self.path) as db:
            now = int(self.now() * 1000)
            row = db.execute("""
                SELECT o.id, o.payload FROM outbox o
                LEFT JOIN outbox_leases l ON o.id = l.id
                WHERE o.delivered = 0 AND (l.id IS NULL OR l.expires_at <= ?)
                ORDER BY o.rowid LIMIT 1
                """, (now,)).fetchone()
            if row is None:
                return None
            message_id, payload = row
            token = uuid.uuid4().hex
            db.execute("""
                INSERT INTO outbox_leases VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET token=excluded.token, expires_at=excluded.expires_at
                """, (message_id, token, now + lease_seconds * 1000))
            return json.loads(payload), token

    def mark_delivered(self, message_id, token):
        """Acknowledge a successful delivery while its lease is still valid."""
        with transaction(self.path) as db:
            removed = db.execute("""
                DELETE FROM outbox_leases WHERE id=? AND token=? AND expires_at>?
                """, (message_id, token, int(self.now() * 1000))).rowcount
            if not removed:
                return False
            db.execute("UPDATE outbox SET delivered=1 WHERE id=?", (message_id,))
            return True
