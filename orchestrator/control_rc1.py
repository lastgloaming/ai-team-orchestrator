"""Candidate Control v1.0-rc1 contract. No network or live authorization."""

import hashlib
import re
import secrets

from .core import _message_lines

VERSION = "control-v1.0-rc1"
SENDER = (1144995, "chatgpt-codex-connector")
RECEIVER = (1210556, "cursor")
OWNER = 338499555
ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
COMMAND = r"CMD-ORCH-[0-9A-HJKMNP-TV-Z]{26}"
PROGRESS = {"ACCEPTED", "IN_PROGRESS", "PARTIAL", "REVIEW", "WAITING_AUTHOR"}
EXCEPTIONS = {"REJECTED", "DUPLICATE", "AMBIGUOUS", "UNCONFIRMED"}
SCHEMA = """
CREATE TABLE IF NOT EXISTS rc1_delivery (
 task_id TEXT PRIMARY KEY REFERENCES v1_task(id),
 command_id TEXT UNIQUE NOT NULL, repository_id INTEGER NOT NULL, pr INTEGER NOT NULL,
 authorization TEXT NOT NULL, digest TEXT NOT NULL, state TEXT NOT NULL, publication INTEGER UNIQUE, latest INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS rc1_group (
 task_id TEXT REFERENCES rc1_delivery(task_id), receipt_id TEXT NOT NULL,
 kind TEXT NOT NULL, status TEXT NOT NULL, total INTEGER NOT NULL,
 PRIMARY KEY(task_id,receipt_id)
);
CREATE TABLE IF NOT EXISTS rc1_part (
 task_id TEXT NOT NULL, receipt_id TEXT NOT NULL, part INTEGER NOT NULL,
 digest TEXT NOT NULL, comment INTEGER NOT NULL UNIQUE,
 PRIMARY KEY(task_id,receipt_id,part),
 FOREIGN KEY(task_id,receipt_id) REFERENCES rc1_group(task_id,receipt_id)
);
"""


def digest(body):
    return hashlib.sha256(body.encode()).hexdigest()


def allocate(now):
    value = (int(now * 1000) << 80) | secrets.randbits(80)
    return "CMD-ORCH-" + "".join(ALPHABET[value >> shift & 31] for shift in range(125, -1, -5))


def stage(db, task, repository_id, now):
    existing = db.execute("""SELECT command_id,repository_id,authorization,digest,state
                             FROM rc1_delivery WHERE task_id=?""", (task.id,)).fetchone()
    command = existing[0] if existing else allocate(now)
    target = "memory" if task.kind == "memory" else "integration"
    body = (f"AI-INTEGRATION-COMMAND\nid: {command}\nfrom: ChatGPT\ntarget: {target}\n"
            "scope: coordination\ntask: Report synthetic status for the authorized task.")
    if existing:
        if existing[1:4] != (repository_id, task.authorization, digest(body)):
            raise ValueError("existing Control command binding cannot change")
    else:
        db.execute("INSERT INTO rc1_delivery VALUES (?,?,?,3,?,?, 'PENDING_SEND', NULL, 0)",
                   (task.id, command, repository_id, task.authorization, digest(body)))
    # Candidate metadata only. No transport is available, even with a staged command.
    return {"version": VERSION, "status": "BLOCKED", "command_id": command,
            "delivery": existing[4] if existing else "PENDING_SEND", "digest": digest(body)}


def parse(body):
    kind, lines = _message_lines(body)
    if kind not in ("ACK", "RESULT"):
        raise ValueError("unsupported receipt marker")
    allowed = {"id", "command_id", "protocol", "from", "to", "status", "receipt_id",
               "in_reply_to", "part", "reason", "evidence", "blockers"}
    fields = {}
    key = None
    for line in lines:
        candidate, separator, value = line.partition(":")
        if separator and candidate.strip() in allowed:
            if candidate != candidate.strip() or candidate in fields or not value.strip():
                raise ValueError("duplicate or conflicting receipt field")
            key = candidate
            fields[key] = value.strip()
        elif key in {"evidence", "reason", "blockers"} and (
                line.startswith((" ", "\t", "[I]", "[S]", "[C]"))):
            # Source text is opaque and never persisted.
            continue
        else:
            raise ValueError("unsupported receipt line")
    command = fields.get("id", fields.get("command_id", ""))
    if (not re.fullmatch(COMMAND, command) or fields.get("command_id", command) != command
            or fields.get("protocol", "1.0") != "1.0"
            or not re.fullmatch(rf"{kind}[-_:]{command}[-_:][1-9][0-9]*", fields.get("receipt_id", ""))
            or not re.fullmatch(r"[1-9][0-9]{0,18}", fields.get("in_reply_to", ""))):
        raise ValueError("invalid receipt binding")
    status = fields.get("status")
    if status not in PROGRESS | EXCEPTIONS | ({"DONE", "BLOCKED"} if kind == "RESULT" else set()):
        raise ValueError("invalid receipt state")
    match = re.fullmatch(r"([1-9][0-9]?)/([1-9][0-9]?)", fields.get("part", "1/1"))
    if not match or int(match[1]) > int(match[2]):
        raise ValueError("invalid receipt part")
    return (kind, command, fields["receipt_id"], int(fields["in_reply_to"]), status,
            int(match[1]), int(match[2]))


def observe(db, packet):
    """Associate independently verified GitHub metadata; never dispatch."""
    task_id = packet["task_id"]
    row = db.execute("""SELECT command_id,repository_id,pr,authorization,digest,state,publication,latest
                        FROM rc1_delivery WHERE task_id=?""", (task_id,)).fetchone()
    comment = packet["comment"]
    if (not row or (packet["repository_id"], packet["pr"], packet["authorization"]) != row[1:4]
            or packet["actor_id"] != OWNER or type(comment) is not int or not 0 < comment < 2**63):
        raise ValueError("unbound Control observation")
    if row[5] == "AMBIGUOUS":
        return {"status": "AMBIGUOUS"}
    identity = (packet["app_id"], packet["app_slug"])
    if packet["kind"] == "publication":
        if identity != SENDER or digest(packet["body"]) != row[4]:
            raise ValueError("unverified publication")
        if row[6] is not None and row[6] != comment:
            db.execute("UPDATE rc1_delivery SET state='AMBIGUOUS' WHERE task_id=?", (task_id,))
            return {"status": "AMBIGUOUS"}
        if row[6] is None:
            db.execute("UPDATE rc1_delivery SET state='SENT',publication=? WHERE task_id=?", (comment, task_id))
        return {"status": "SENT"}
    if packet["kind"] != "receipt" or identity != RECEIVER or row[6] is None or comment <= row[6]:
        raise ValueError("unverified receipt source")
    kind, command, receipt, publication, status, part, total = parse(packet["body"])
    if (command, publication) != (row[0], row[6]):
        raise ValueError("receipt belongs to another command or publication")
    group = db.execute("SELECT kind,status,total FROM rc1_group WHERE task_id=? AND receipt_id=?",
                       (task_id, receipt)).fetchone()
    previous = db.execute("SELECT digest FROM rc1_part WHERE task_id=? AND receipt_id=? AND part=?",
                          (task_id, receipt, part)).fetchone()
    body_digest = digest(packet["body"])
    if (group and group != (kind, status, total)) or (previous and previous[0] != body_digest):
        db.execute("UPDATE rc1_delivery SET state='AMBIGUOUS' WHERE task_id=?", (task_id,))
        return {"status": "AMBIGUOUS"}
    if previous:
        return {"status": "DUPLICATE_PART", "delivery": row[5]}
    if not group:
        db.execute("INSERT INTO rc1_group VALUES (?,?,?,?,?)", (task_id, receipt, kind, status, total))
    db.execute("INSERT INTO rc1_part VALUES (?,?,?,?,?)", (task_id, receipt, part, body_digest, comment))
    count, latest = db.execute("SELECT count(*),max(comment) FROM rc1_part WHERE task_id=? AND receipt_id=?",
                               (task_id, receipt)).fetchone()
    if count != total:
        return {"status": "INCOMPLETE"}
    if latest > row[7] and row[5] not in {"DONE", "BLOCKED", "DUPLICATE", "REJECTED", "UNCONFIRMED"}:
        db.execute("UPDATE rc1_delivery SET state=?,latest=? WHERE task_id=?", (status, latest, task_id))
    # Candidate receipt status is transport evidence only; cannot unlock v1 work.
    return {"status": "RECORDED", "receipt_status": status, "execution": "UNVERIFIED"}
