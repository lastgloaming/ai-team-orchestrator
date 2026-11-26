"""Deterministic outbound adapter for Control v1.0, with Mock-only execution."""

import re
from dataclasses import dataclass
from datetime import datetime

from .core import IDENTIFIER, Message, _message_lines, _read_fields, read_historical_receipt, transaction

REPOSITORY = "lastgloaming/ai-writer"
CONTROL_PR = 3
OWNER_ID = 338499555
OWNER_LOGIN = "lastgloaming"
REAL_SEND_REVIEWED = False
TASK_ID = re.compile(r"CMD-(\d{8})-(?!000)\d{3}")
PROGRESS = {"REVIEW", "WAITING_AUTHOR", "PARTIAL"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS integration_tasks (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL,
    target TEXT,
    ref TEXT UNIQUE REFERENCES integration_tasks(id),
    state TEXT NOT NULL,
    comment_id INTEGER UNIQUE
);
CREATE TABLE IF NOT EXISTS integration_reserved_ids (
    id TEXT PRIMARY KEY,
    claimed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS integration_delivery_floor (
    task_id TEXT PRIMARY KEY REFERENCES integration_tasks(id),
    comment_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS integration_publication_seen (
    task_id TEXT NOT NULL REFERENCES integration_tasks(id), comment_id INTEGER NOT NULL,
    PRIMARY KEY(task_id,comment_id)
);
CREATE TABLE IF NOT EXISTS integration_receipts (
    comment_id INTEGER PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES integration_tasks(id),
    kind TEXT NOT NULL,
    status TEXT NOT NULL
);
"""

# Costs are conservative test units, not model prices or measured provider usage.
ROUTES = {
    "report_status": ("integration", 1, "Report synthetic orchestration status."),
    "check_memory_index": ("memory", 1, "Check the synthetic fixture index via Integration."),
    "record_review": (None, 1, "Record the technical review of the referenced synthetic result."),
}


@dataclass(frozen=True)
class Resources:
    remaining: int
    reserve: int

    def allows(self, outstanding, cost):
        return (type(self.remaining) is int and type(self.reserve) is int
                and self.remaining >= 0 and self.reserve >= 0
                and self.remaining - outstanding - cost > self.reserve)


@dataclass(frozen=True)
class Comment:
    repository: str
    pr: int
    id: int
    actor_id: int
    actor_login: str
    body: str

    def scoped(self):
        return (self.repository == REPOSITORY and self.pr == CONTROL_PR
                and type(self.id) is int and 0 < self.id < 2**63)


def render(task_id, action, target, ref):
    marker = "REVIEW" if ref else "COMMAND"
    task = ROUTES[action][2]
    body = (f"AI-INTEGRATION-{marker}\nid: {task_id}\nfrom: author\ntarget: {target}\n"
            f"scope: coordination\ntask: {task}")
    return body + (f"\nref: {ref}" if ref else "")


class IntegrationAdapter:
    def __init__(self, path):
        self.path = path

    def provision_mock_ids(self, ids):
        """Simulate an exclusive grant by the Control channel's authoritative allocator."""
        ids = tuple(ids)
        for task_id in ids:
            match = TASK_ID.fullmatch(task_id)
            if not match:
                raise ValueError("invalid reserved v1.0 ID")
            datetime.strptime(match[1], "%Y%m%d")
        with transaction(self.path, schema=SCHEMA) as db:
            db.executemany("INSERT OR IGNORE INTO integration_reserved_ids (id) VALUES (?)",
                           [(task_id,) for task_id in ids])

    def enqueue(self, source, action, *, ref=None, author_delegated=False):
        """Ingest a trusted upstream intent. GitHub comments never call this entry."""
        if author_delegated is not True:
            raise PermissionError("verified author delegation required")
        if not isinstance(source, str) or not IDENTIFIER.fullmatch(source):
            raise ValueError("stable upstream event ID required")
        with transaction(self.path, schema=SCHEMA) as db:
            existing = db.execute("SELECT id,action,ref FROM integration_tasks WHERE source=?", (source,)).fetchone()
            if existing:
                if existing[1:] != (action, ref):
                    raise ValueError("event reused with a different intent")
                return existing[0]
            if not isinstance(action, str) or not IDENTIFIER.fullmatch(action):
                raise ValueError("structured action required")
            state = "QUEUED" if action in ROUTES else "WAITING_AUTHOR"
            target = ROUTES[action][0] if action in ROUTES else None
            if action == "record_review":
                previous = db.execute(
                    "SELECT action,target,state FROM integration_tasks WHERE id=?", (ref,)
                ).fetchone()
                if not previous or previous[0] == "record_review" or previous[2] != "DONE":
                    raise ValueError("review requires a completed command")
                prior = db.execute("SELECT id FROM integration_tasks WHERE ref=?", (ref,)).fetchone()
                if prior:
                    raise ValueError("review for this result already exists")
                target = previous[1]
            elif ref is not None:
                raise ValueError("only REVIEW accepts a reference")
            reserved = db.execute("""
                SELECT r.id FROM integration_reserved_ids r
                WHERE NOT EXISTS (SELECT 1 FROM integration_tasks t WHERE t.id=r.id)
                ORDER BY r.id LIMIT 1
            """).fetchone()
            if reserved is None:
                raise RuntimeError("BLOCKED: no verified Control ID allocation")
            task_id = reserved[0]
            db.execute("UPDATE integration_reserved_ids SET claimed=1 WHERE id=?", (task_id,))
            db.execute("INSERT INTO integration_tasks VALUES (?,?,?,?,?,?,NULL)",
                       (task_id, source, action, target, ref, state))
            return task_id

    def status(self, task_id):
        with transaction(self.path, schema=SCHEMA) as db:
            row = db.execute("SELECT state FROM integration_tasks WHERE id=?", (task_id,)).fetchone()
            return row[0] if row else None

    def dispatch_real(self, task_id, *, enabled=False):
        if not enabled or not REAL_SEND_REVIEWED:
            return "BLOCKED"
        raise RuntimeError("no approved live sender is installed")

    def dispatch_mock(self, task_id, publish, *, sender_id, sender_login, resources=None, after_comment_id=None):
        """Sender identity is supplied by a trusted fixture, never by a message body."""
        if (sender_id != OWNER_ID or sender_login != OWNER_LOGIN or type(after_comment_id) is not int
                or not 0 <= after_comment_id < 2**63):
            return "BLOCKED"
        with transaction(self.path, schema=SCHEMA) as db:
            row = db.execute("SELECT action,target,ref,state FROM integration_tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise ValueError("unknown task")
            action, target, ref, state = row
            if state != "QUEUED":
                return state
            if not db.execute("SELECT 1 FROM integration_reserved_ids WHERE id=? AND claimed=1",
                              (task_id,)).fetchone():
                return "BLOCKED"
            cost = ROUTES[action][1]
            attempted = db.execute("""
                SELECT COUNT(*) FROM integration_tasks
                WHERE comment_id IS NOT NULL OR state IN ('SENDING','UNCONFIRMED','AMBIGUOUS')
            """).fetchone()[0]
            if not isinstance(resources, Resources) or not resources.allows(attempted, cost):
                return "BLOCKED"
            body = render(task_id, action, target, ref)
            db.execute("INSERT INTO integration_delivery_floor VALUES (?,?)", (task_id, after_comment_id))
            db.execute("UPDATE integration_tasks SET state='SENDING' WHERE id=?", (task_id,))
        try:
            comment = publish(body)
            if not self._outbound_matches(comment, body, after_comment_id):
                raise ValueError("send confirmation does not match control channel")
            self._record_publications(task_id, [comment], direct=True)
        except Exception:
            with transaction(self.path, schema=SCHEMA) as db:
                db.execute("UPDATE integration_tasks SET state='UNCONFIRMED' WHERE id=? AND state='SENDING'",
                           (task_id,))
        return self.status(task_id)

    @staticmethod
    def _outbound_matches(comment, body, floor):
        return (isinstance(comment, Comment) and comment.scoped() and comment.actor_id == OWNER_ID
                and comment.actor_login == OWNER_LOGIN and comment.id > floor and comment.body == body)

    def reconcile_mock(self, task_id, comments, *, verified_complete=False):
        """Only the trusted mock reader attests full pagination for this task's channel."""
        return self._record_publications(task_id, comments, complete=verified_complete is True)

    def _record_publications(self, task_id, comments, *, direct=False, complete=False):
        with transaction(self.path, schema=SCHEMA) as db:
            row = db.execute("""
                SELECT action,target,ref,state,f.comment_id,t.comment_id FROM integration_tasks t
                LEFT JOIN integration_delivery_floor f ON f.task_id=t.id WHERE t.id=?
            """, (task_id,)).fetchone()
            if not row or row[4] is None or row[3] == "AMBIGUOUS":
                return row[3] if row else None
            body = render(task_id, *row[:3])
            matches = {c.id for c in comments if self._outbound_matches(c, body, row[4])}
            observed = matches.copy()
            if row[5] is not None:
                observed.add(row[5])
            db.executemany("INSERT OR IGNORE INTO integration_publication_seen VALUES (?,?)",
                           ((task_id, comment) for comment in observed))
            count, = db.execute("SELECT count(*) FROM integration_publication_seen WHERE task_id=?",
                                (task_id,)).fetchone()
            state = row[3]
            if count > 1:
                state = "AMBIGUOUS"
            elif (direct or complete) and len(matches) == 1 and state in {"SENDING", "UNCONFIRMED"}:
                db.execute("UPDATE integration_tasks SET comment_id=? WHERE id=?", (matches.pop(), task_id))
                state = "SENT"
            elif state == "SENDING" or (complete and not matches and state == "SENT"):
                state = "UNCONFIRMED"
            db.execute("UPDATE integration_tasks SET state=? WHERE id=?", (state, task_id))
            return state

    def receive_mock(self, comment, *, integration_id):
        # The configured principal must come from authenticated transport metadata.
        # The currently shared owner identity is deliberately insufficient.
        if (not isinstance(comment, Comment) or not comment.scoped() or type(integration_id) is not int or integration_id <= 0
                or integration_id == OWNER_ID or comment.actor_id != integration_id):
            return "UNAUTHENTICATED"
        try:
            receipt = parse_receipt(comment.body)
        except ValueError:
            return "REJECTED"
        if receipt is None:
            return "IGNORED"
        with transaction(self.path, schema=SCHEMA) as db:
            if db.execute("SELECT 1 FROM integration_receipts WHERE comment_id=?", (comment.id,)).fetchone():
                return "DUPLICATE"
            row = db.execute("""
                SELECT t.state,t.comment_id FROM integration_tasks t
                JOIN integration_delivery_floor f ON f.task_id=t.id WHERE t.id=?
            """, (receipt.id,)).fetchone()
            if not row or row[0] not in ({"SENT", "ACKED"} | PROGRESS) or comment.id <= row[1]:
                return "IGNORED"
            latest = db.execute("SELECT MAX(comment_id) FROM integration_receipts WHERE task_id=?",
                                (receipt.id,)).fetchone()[0]
            if latest is not None and comment.id <= latest:
                return "IGNORED"
            status = receipt.fields["status"]
            if receipt.kind == "ACK":
                next_state = {"ACCEPTED": "ACKED", "DUPLICATE": "DUPLICATE_HOLD",
                              "REJECTED": "REJECTED", "BLOCKED": "BLOCKED"}[status]
                if row[0] != "SENT" and status != "DUPLICATE":
                    return "IGNORED"
            else:
                next_state = status
            db.execute("INSERT INTO integration_receipts VALUES (?,?,?,?)",
                       (comment.id, receipt.id, receipt.kind, status))
            db.execute("UPDATE integration_tasks SET state=? WHERE id=?", (next_state, receipt.id))
            return next_state


def parse_receipt(body):
    """Accept narrowly defined progress only; never derive completion from prose."""
    kind, lines = _message_lines(body)
    if any(re.match(r"^\s*(?:id|command_id)\s*:\s*CMD-ORCH-", line, re.I) for line in lines):
        raise ValueError("reserved namespace requires verified rc1 observation")
    if kind not in {"RESULT", "REVIEW"}:
        return read_historical_receipt(body)
    if any(re.match(r"^\s*(?:part|parts|section|continuation)\s*:", line, re.IGNORECASE)
           for line in lines):
        raise ValueError("multipart receipt requires reconciliation")
    fields = _read_fields(lines, multiline=True)
    if fields.get("status") not in PROGRESS:
        return read_historical_receipt(body)
    if fields.keys() == {"id", "status", "evidence", "blockers"}:
        identifier = fields["id"]
    elif (fields.keys() == {"protocol", "from", "to", "command_id", "status", "evidence"}
          and fields["protocol"] == "1.0"):
        identifier = fields["command_id"]
    else:
        raise ValueError("unsupported progress receipt")
    if not TASK_ID.fullmatch(identifier):
        raise ValueError("invalid progress receipt ID")
    return Message(kind, identifier, fields)
