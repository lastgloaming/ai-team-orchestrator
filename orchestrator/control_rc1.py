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
CREATE TABLE IF NOT EXISTS rc1_seen (
 comment INTEGER PRIMARY KEY, task_id TEXT NOT NULL REFERENCES rc1_delivery(task_id),
 kind TEXT NOT NULL, digest TEXT NOT NULL, created_at INTEGER
);
CREATE TABLE IF NOT EXISTS rc1_unedited (
 comment INTEGER PRIMARY KEY REFERENCES rc1_seen(comment), updated_at INTEGER NOT NULL
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
-- Legacy part rows bind an original GitHub comment even when its times were never observed.
-- Keep those deliveries in reconciliation across reopen, including after an identical repost.
UPDATE rc1_delivery SET state='AMBIGUOUS'
WHERE state<>'AMBIGUOUS' AND task_id IN (
 SELECT p.task_id FROM rc1_part p WHERE NOT EXISTS (
  SELECT 1 FROM rc1_seen s JOIN rc1_unedited u ON u.comment=s.comment
  WHERE s.comment=p.comment AND s.task_id=p.task_id AND s.kind='receipt'
    AND s.digest=p.digest AND s.created_at=u.updated_at AND s.created_at>0
 )
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
            if candidate != candidate.strip() or candidate in fields or (not value.strip() and candidate != "evidence"):
                raise ValueError("duplicate or conflicting receipt field")
            key = candidate
            fields[key] = value.strip()
        elif key == "evidence" and re.fullmatch(r"- \[[ISC]\] \S.*", line):
            fields[key] = fields[key] or "present"
        elif key in {"evidence", "reason", "blockers"} and (
                line.startswith((" ", "\t", "[I]", "[S]", "[C]"))):
            if re.match(r"^\s+[a-zA-Z_][a-zA-Z_0-9-]*\s*:", line):
                raise ValueError("indented receipt field")
            if key == "evidence" and line.strip():
                fields[key] = fields[key] or "present"
            continue
        else:
            raise ValueError("unsupported receipt line")
    command = fields.get("id", fields.get("command_id", ""))
    if (fields.get("evidence") == "" or not re.fullmatch(COMMAND, command) or fields.get("command_id", command) != command
            or fields.get("protocol", "1.0") != "1.0"
            or fields.get("from", "Integration") != "Integration"
            or fields.get("to", "ChatGPT") != "ChatGPT"
            or not re.fullmatch(rf"{kind}-{command}-[1-9][0-9]*", fields.get("receipt_id", ""))
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
            or any(type(packet[key]) is not int for key in ("repository_id", "pr", "actor_id", "app_id"))
            or packet["actor_id"] != OWNER or type(comment) is not int or not 0 < comment < 2**63):
        raise ValueError("unbound Control observation")
    if row[5] == "AMBIGUOUS":
        return {"status": "AMBIGUOUS"}
    identity = (packet["app_id"], packet["app_slug"])
    publication = packet["kind"] == "publication"
    if publication and identity != SENDER:
        raise ValueError("unverified publication")
    if not publication and (packet["kind"] != "receipt" or identity != RECEIVER or row[6] is None):
        raise ValueError("unverified receipt source")
    stamp = packet.get("created_at")
    updated = packet.get("updated_at")
    body_digest = digest(packet["body"])
    binding = (task_id, packet["kind"], body_digest, stamp)
    seen = db.execute("SELECT task_id,kind,digest,created_at FROM rc1_seen WHERE comment=?",
                      (comment,)).fetchone()
    # Include observations written by the previous schema before the fingerprint table existed.
    prior = db.execute("""
        SELECT task_id,'publication',digest FROM rc1_delivery WHERE publication=?
        UNION ALL SELECT task_id,'receipt',digest FROM rc1_part WHERE comment=?
    """, (comment, comment)).fetchall()
    if (seen and seen != binding) or any(old != binding[:3] for old in prior):
        affected = {task_id, *(old[0] for old in prior)}
        if seen:
            affected.add(seen[0])
        return _ambiguous(db, *affected)
    if type(stamp) is not int or not 0 < stamp < 2**63 or type(updated) is not int or updated != stamp:
        return _ambiguous(db, task_id)
    if publication:
        if body_digest != row[4]:
            raise ValueError("unverified publication")
        if row[6] is not None and row[6] != comment:
            return _ambiguous(db, task_id)
        if row[6] is None:
            db.execute("UPDATE rc1_delivery SET state='SENT',publication=? WHERE task_id=?", (comment, task_id))
        db.execute("INSERT OR IGNORE INTO rc1_seen VALUES (?,?,?,?,?)", (comment, *binding))
        db.execute("INSERT OR IGNORE INTO rc1_unedited VALUES (?,?)", (comment, updated))
        return {"status": "SENT"}
    kind, command, receipt, reply, status, part, total = parse(packet["body"])
    if (command, reply) != (row[0], row[6]):
        raise ValueError("receipt belongs to another command or publication")
    db.execute("INSERT OR IGNORE INTO rc1_seen VALUES (?,?,?,?,?)", (comment, *binding))
    db.execute("INSERT OR IGNORE INTO rc1_unedited VALUES (?,?)", (comment, updated))
    published = db.execute("SELECT updated_at FROM rc1_unedited WHERE comment=?", (reply,)).fetchone()
    if stamp is None or not published or published[0] is None or stamp <= published[0]:
        return _ambiguous(db, task_id)
    group = db.execute("SELECT kind,status,total FROM rc1_group WHERE task_id=? AND receipt_id=?",
                       (task_id, receipt)).fetchone()
    previous = db.execute("SELECT digest FROM rc1_part WHERE task_id=? AND receipt_id=? AND part=?",
                          (task_id, receipt, part)).fetchone()
    if (group and group != (kind, status, total)) or (previous and previous[0] != body_digest):
        return _ambiguous(db, task_id)
    if not group:
        db.execute("INSERT INTO rc1_group VALUES (?,?,?,?,?)", (task_id, receipt, kind, status, total))
    if not previous:
        db.execute("INSERT INTO rc1_part VALUES (?,?,?,?,?)", (task_id, receipt, part, body_digest, comment))
    complete = db.execute("""
        SELECT g.receipt_id,g.status,min(u.updated_at),max(u.updated_at),count(u.comment),count(*)
        FROM rc1_group g JOIN rc1_part p USING(task_id,receipt_id)
        LEFT JOIN rc1_seen s ON s.task_id=p.task_id AND s.digest=p.digest AND s.kind='receipt'
        LEFT JOIN rc1_unedited u ON u.comment=s.comment
        WHERE g.task_id=? GROUP BY g.receipt_id HAVING count(DISTINCT p.part)=g.total
        ORDER BY max(u.updated_at),g.receipt_id
    """, (task_id,)).fetchall()
    if not any(entry[0] == receipt for entry in complete):
        return {"status": "DUPLICATE_PART", "delivery": row[5]} if previous else {"status": "INCOMPLETE"}
    for index, (_, state, _, end, timed, size) in enumerate(complete):
        if timed != size:
            return _ambiguous(db, task_id)
        for _, later, next_start, _, _, _ in complete[index + 1:]:
            if state != later and (state not in PROGRESS or end >= next_start):
                return _ambiguous(db, task_id)
    db.execute("UPDATE rc1_delivery SET state=? WHERE task_id=?", (complete[-1][1], task_id))
    if previous:
        return {"status": "DUPLICATE_PART", "delivery": complete[-1][1]}
    return {"status": "RECORDED", "receipt_status": status, "execution": "UNVERIFIED"}


def _ambiguous(db, *tasks):
    db.executemany("UPDATE rc1_delivery SET state='AMBIGUOUS' WHERE task_id=?",
                   ((task,) for task in set(tasks)))
    return {"status": "AMBIGUOUS"}
