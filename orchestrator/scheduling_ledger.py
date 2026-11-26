"""Schema and integrity checks for synthetic scheduling snapshots."""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum

from .core import USAGE_SCHEMA
from .state import StateConflict


class ModelStatus(StrEnum):
    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    DISABLED = "DISABLED"
    STOPPED = "STOPPED"
    BLOCKED = "BLOCKED"
    RESERVED = "RESERVED"
    UNCERTAIN = "UNCERTAIN"
    REJECTED = "REJECTED"
    ADVISED = "ADVISED"


@dataclass(frozen=True)
class ModelOutcome:
    status: ModelStatus
    reason: str | None = None
    suggestion: str | None = None


SCHEMA = USAGE_SCHEMA + """
CREATE TABLE IF NOT EXISTS auto_model_eligibility (
    event_id INTEGER PRIMARY KEY REFERENCES auto_inbox(id)
);
CREATE TABLE IF NOT EXISTS auto_model_attempt (
    event_id INTEGER PRIMARY KEY REFERENCES auto_model_eligibility(event_id),
    reservation_id INTEGER UNIQUE REFERENCES usage(id),
    status TEXT NOT NULL,
    reason TEXT,
    suggestion TEXT
);
"""
TABLES = {"usage", "auto_model_eligibility", "auto_model_attempt"}
DENIALS = {"budget_exhausted", "concurrency_limit", "reconciliation_required", "model_stopped"}
ATTEMPT_STATES = {"RESERVED": {"active"}, "UNCERTAIN": {"uncertain"},
                  "REJECTED": {"overrun", "counted"}, "ADVISED": {"counted"}}


def inspect_snapshot(path):
    """Read legacy or scheduling state without creating tables or repairing links."""
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
        inbox = dict(db.execute("SELECT id,consumed FROM auto_inbox"))
        tables = TABLES & {name for (name,) in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
        if not tables:
            return inbox, None, {}, {}
        if tables != TABLES:
            raise StateConflict("incomplete synthetic scheduling schema")
        eligible = {event_id for (event_id,) in db.execute("SELECT event_id FROM auto_model_eligibility")}
        attempts = {event_id: (reservation, status, reason, suggestion)
                    for event_id, reservation, status, reason, suggestion in db.execute(
                        "SELECT event_id,reservation_id,status,reason,suggestion FROM auto_model_attempt")}
        usage = {row[0]: row[1:] for row in db.execute(
            "SELECT id,currency,state,amount,day,model,input_tokens,output_tokens,attempts FROM usage")}
        linked = set()
        if not eligible <= inbox.keys() or not attempts.keys() <= eligible:
            raise StateConflict("invalid synthetic scheduling eligibility")
        for event_id, (reservation, status, reason, suggestion) in attempts.items():
            if inbox[event_id] != 1:
                raise StateConflict("scheduling attempt precedes event consumption")
            if status in {"BLOCKED", "STOPPED"}:
                if reservation is not None or reason not in DENIALS or suggestion is not None:
                    raise StateConflict("invalid synthetic scheduling denial")
                continue
            if reservation in linked or reservation not in usage or status not in ATTEMPT_STATES:
                raise StateConflict("missing or invalid synthetic usage linkage")
            linked.add(reservation)
            currency, accounting, amount, _, _, incoming, outgoing, count = usage[reservation]
            if (currency != "CNY" or accounting not in ATTEMPT_STATES[status]
                    or type(amount) is not int or amount < 0 or count != 1 or reason is not None
                    or (status == "ADVISED" and suggestion not in {"inspect_status", "check_blockers", "escalate"})
                    or (status != "ADVISED" and suggestion is not None)):
                raise StateConflict("invalid synthetic scheduling outcome")
            if accounting == "counted":
                if (type(incoming) is not int or type(outgoing) is not int or incoming < 0
                        or not 0 <= outgoing <= 64 or amount != 2000 * incoming + 8000 * outgoing):
                    raise StateConflict("invalid synthetic accounted amount")
            elif incoming is not None or outgoing is not None:
                raise StateConflict("unexpected token usage on unsettled attempt")
        if linked != usage.keys():
            raise StateConflict("unlinked synthetic usage row")
        return inbox, eligible, attempts, usage


def check_transition(previous, current):
    old_inbox, old_eligible, old_attempts, old_usage = previous
    _, eligible, attempts, usage = current
    if old_eligible is not None and (eligible is None or not old_eligible <= eligible):
        raise StateConflict("synthetic scheduling eligibility was removed")
    if eligible is not None and (eligible - (old_eligible or set())) & old_inbox.keys():
        raise StateConflict("historical communication cannot gain model eligibility")
    for event, old in old_attempts.items():
        new = attempts.get(event)
        if new is None or old[0] != new[0] or (old[1] != "RESERVED" and old != new):
            raise StateConflict("synthetic scheduling attempt was removed or rewritten")
    if any(attempts[event][1] not in {"RESERVED", "BLOCKED", "STOPPED"}
           for event in attempts.keys() - old_attempts.keys()):
        raise StateConflict("model outcome did not follow a persisted reservation")
    for reservation, old in old_usage.items():
        new = usage.get(reservation)
        if (new is None or new[0] != old[0] or new[3:5] != old[3:5]
                or (old[1] != "active" and old != new) or new[2] > old[2]
                or (new[1] != "counted" and new[2] != old[2])):
            raise StateConflict("synthetic budget history was removed or rewritten")
