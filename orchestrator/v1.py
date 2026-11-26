"""Private task queue and operator entry point for Orchestrator v1."""

import argparse
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from .core import IDENTIFIER, transaction

SHA = re.compile(r"[0-9a-f]{40}")
REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
RUN = re.compile(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(?:/job/[0-9]+)?")
ROLES = {
    "review": "codex", "development": "codex", "remediation": "codex", "research": "codex",
    "writer_update": "writer", "integration": "integration", "memory": "integration",
}
SCHEMA = """
CREATE TABLE IF NOT EXISTS v1_task (
 id TEXT PRIMARY KEY, logical_key TEXT NOT NULL UNIQUE, spec TEXT NOT NULL,
 state TEXT NOT NULL, lease TEXT, deadline INTEGER, evidence TEXT
);
CREATE TABLE IF NOT EXISTS v1_alias (
 alias TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES v1_task(id)
);
CREATE TABLE IF NOT EXISTS v1_safe_recovery (
 task_id TEXT PRIMARY KEY REFERENCES v1_task(id)
);
CREATE TABLE IF NOT EXISTS v1_final_claim (
 task_id TEXT PRIMARY KEY REFERENCES v1_final(task_id),
 reviewer TEXT NOT NULL, lease TEXT NOT NULL, deadline INTEGER NOT NULL, decision TEXT
);
CREATE TABLE IF NOT EXISTS v1_connector_receipt (
 task_id TEXT PRIMARY KEY REFERENCES v1_task(id),
 contract TEXT NOT NULL, publication INTEGER NOT NULL UNIQUE, receipt INTEGER NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS v1_budget (
 task_id TEXT PRIMARY KEY REFERENCES v1_task(id), window TEXT NOT NULL, units INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS v1_final (
 task_id TEXT PRIMARY KEY REFERENCES v1_task(id), request TEXT NOT NULL
);
"""


def identifier(value):
    return isinstance(value, str) and bool(IDENTIFIER.fullmatch(value))


def sha(value):
    return isinstance(value, str) and bool(SHA.fullmatch(value))


@dataclass(frozen=True)
class Task:
    id: str
    repository: str
    kind: str
    authorization: str
    priority: int
    units: int
    pr: int = 0
    head: str = ""
    base: str = ""
    scope: str = "default"
    important: bool = False
    authored_here: bool | None = None
    dependencies: tuple = ()

    def __post_init__(self):
        if (not all(identifier(x) for x in (self.id, self.authorization, self.scope, *self.dependencies))
                or self.id in self.dependencies or not REPO.fullmatch(self.repository)
                or type(self.important) is not bool
                or (self.authored_here is not None and type(self.authored_here) is not bool)
                or type(self.priority) is not int or not 0 <= self.priority <= 100
                or type(self.units) is not int or self.units < 0 or not identifier(self.kind)
                or type(self.pr) is not int or self.pr < 0
                or (self.kind == "review" and not self.pr)
                or (self.pr and not (sha(self.head) and sha(self.base)))
                or (not self.pr and (self.head or self.base or self.important))):
            raise ValueError("invalid structured task")

    def logical_key(self):
        parts = [self.repository.lower(), self.kind, self.scope, self.pr, self.head, self.base]
        return hashlib.sha256(json.dumps(parts).encode()).hexdigest()


class Queue:
    def __init__(self, path, *, now=time.time):
        self.path = path
        self.now = now

    def _db(self):
        return transaction(self.path, schema=SCHEMA)

    @staticmethod
    def _task(spec):
        data = json.loads(spec)
        data["dependencies"] = tuple(data["dependencies"])
        return Task(**data)

    def add(self, tasks, config=None):
        approvals = (config or {}).get("authorizations", {})
        with self._db() as db:
            for task in tasks:
                spec = json.dumps(asdict(task), sort_keys=True)
                alias = db.execute("SELECT task_id FROM v1_alias WHERE alias=?", (task.id,)).fetchone()
                prior = db.execute(
                    "SELECT id,spec,state,logical_key FROM v1_task WHERE id=? OR logical_key=?",
                    (alias[0] if alias else task.id, task.logical_key()),
                ).fetchall()
                if prior:
                    if len(prior) != 1 or prior[0][3] != task.logical_key():
                        raise ValueError("task ID already names a different intent")
                    canonical, old_spec, state, _ = prior[0]
                    old = self._task(old_spec)
                    if replace(task, id=canonical, authorization=old.authorization,
                               dependencies=tuple(task.dependencies)) != old:
                        raise ValueError("task alias changes the approved work")
                    if task.id != canonical:
                        db.execute("INSERT OR IGNORE INTO v1_alias VALUES (?,?)", (task.id, canonical))
                    if old.authorization != task.authorization and approvals.get(task.id) == task.authorization:
                        recoverable = db.execute("SELECT 1 FROM v1_safe_recovery WHERE task_id=?",
                                                 (canonical,)).fetchone()
                        replacement = json.dumps(asdict(replace(old, authorization=task.authorization)), sort_keys=True)
                        if state == "WAITING_FINAL" and not db.execute(
                            "SELECT 1 FROM v1_final_claim WHERE task_id=?", (canonical,)
                        ).fetchone():
                            request = json.loads(db.execute("SELECT request FROM v1_final WHERE task_id=?",
                                                            (canonical,)).fetchone()[0])
                            request["task"] = json.loads(replacement)
                            db.execute("UPDATE v1_task SET spec=? WHERE id=?", (replacement, canonical))
                            db.execute("UPDATE v1_final SET request=? WHERE task_id=?", (json.dumps(request), canonical))
                        elif (state == "READY" or recoverable or (state == "STALE" and old.kind == "review"
                                and not db.execute("SELECT 1 FROM v1_final WHERE task_id=?",
                                                   (canonical,)).fetchone())):
                            db.execute("UPDATE v1_task SET spec=?,state='READY' WHERE id=?",
                                       (replacement, canonical))
                            db.execute("DELETE FROM v1_safe_recovery WHERE task_id=?", (canonical,))
                    continue
                state = "READY" if task.kind in ROLES else "WAITING_AUTHOR"
                db.execute("INSERT INTO v1_task (id,logical_key,spec,state) VALUES (?,?,?,?)",
                           (task.id, task.logical_key(), spec, state))

    @staticmethod
    def _states(db):
        states = dict(db.execute("SELECT id,state FROM v1_task"))
        states.update({alias: states[task_id] for alias, task_id in db.execute("SELECT alias,task_id FROM v1_alias")})
        return states

    @staticmethod
    def _authorized(db, task, config):
        approvals = config.get("authorizations", {})
        if task.id in approvals:
            return approvals[task.id] == task.authorization
        aliases = {approvals[alias] for (alias,) in db.execute("SELECT alias FROM v1_alias WHERE task_id=?", (task.id,))
                   if alias in approvals}
        return aliases == {task.authorization}

    @staticmethod
    def _expire(db, now):
        db.execute("UPDATE v1_task SET state='UNCONFIRMED' WHERE state='CLAIMED' AND deadline<=?", (now,))

    def _current(self, task, observations):
        if not task.pr:
            return True
        observed = observations.get(f"{task.repository}#{task.pr}", {})
        checked = observed.get("checked")
        return (type(checked) is int and 0 <= int(self.now()) - checked <= 300
                and observed.get("state") == "OPEN") and (observed.get("head"), observed.get("base")) == (
            task.head, task.base
        )

    def _dependencies_ready(self, db, task, states, config):
        for dependency in task.dependencies:
            if states.get(dependency) != "DONE":
                return False
            row = db.execute("""SELECT spec FROM v1_task WHERE id=COALESCE(
                (SELECT task_id FROM v1_alias WHERE alias=?), ?)""", (dependency, dependency)).fetchone()
            parent = self._task(row[0])
            if not self._authorized(db, parent, config) or not self._current(parent, config.get("observations", {})):
                return False
        return True

    def claim(self, config):
        with self._db() as db:
            now = int(self.now())
            self._expire(db, now)
            if config.get("stop") is not False or os.environ.get("ORCHESTRATOR_V1_STOP") == "1":
                return {"status": "BLOCKED", "reason": "stopped"}
            if db.execute("SELECT 1 FROM v1_task WHERE state IN ('CLAIMED','UNCONFIRMED') LIMIT 1").fetchone():
                return {"status": "NOOP", "reason": "in_flight_or_unconfirmed"}
            states = self._states(db)
            ready = [self._task(spec) for (spec,) in db.execute("SELECT spec FROM v1_task WHERE state='READY'")]
            ready.sort(key=lambda task: (-task.priority, task.kind != "review", task.id))
            blockers = set()
            for task in ready:
                if not self._authorized(db, task, config):
                    blockers.add("authorization")
                elif not self._dependencies_ready(db, task, states, config):
                    blockers.add("dependencies")
                elif not self._current(task, config.get("observations", {})):
                    blockers.add("project_version_unconfirmed")
                elif ROLES[task.kind] != "codex":
                    blockers.add("external_role_transport")
                elif not self._reserve(db, task, config):
                    blockers.add("budget")
                else:
                    lease = secrets.token_hex(16)
                    db.execute("UPDATE v1_task SET state='CLAIMED',lease=?,deadline=? WHERE id=?",
                               (lease, now + 900, task.id))
                    return {"status": "CLAIMED", "task": asdict(task), "role": "codex", "lease": lease}
            return {"status": "NOOP", "blocked": sorted(blockers)}

    @staticmethod
    def _reserve(db, task, config):
        budget = config.get("budget", {})
        remaining, reserve, window = (budget.get(k) for k in ("remaining", "reserve", "window"))
        if (type(remaining) is not int or type(reserve) is not int or reserve < 0
                or not identifier(window)):
            return False
        used = db.execute("SELECT COALESCE(SUM(units),0) FROM v1_budget WHERE window=?", (window,)).fetchone()[0]
        previous = db.execute("SELECT window FROM v1_budget WHERE task_id=?", (task.id,)).fetchone()
        if previous and previous[0] != window:
            return False
        units = max(task.units, 1)
        if remaining - used - units <= reserve:
            return False
        db.execute("INSERT INTO v1_budget VALUES (?,?,?) ON CONFLICT(task_id) DO UPDATE SET units=units+excluded.units",
                   (task.id, window, units))
        return True

    def complete(self, task_id, lease, config, evidence):
        required = {"conclusion", "run", "passed", "failed", "risk", "decision", "tested_sha"}
        if (evidence.keys() != required or evidence["conclusion"] not in {"PASS", "REQUEST_CHANGES", "BLOCKED"}
                or not RUN.fullmatch(evidence["run"]) or not sha(evidence["tested_sha"])
                or not all(identifier(evidence[k]) for k in ("risk", "decision"))
                or any(type(evidence[k]) is not int or evidence[k] < 0 for k in ("passed", "failed"))
                or (evidence["conclusion"] == "PASS" and (evidence["failed"] or not evidence["passed"]))):
            raise ValueError("structured execution evidence required")
        with self._db() as db:
            self._expire(db, int(self.now()))
            row = db.execute("SELECT spec,state,lease FROM v1_task WHERE id=?", (task_id,)).fetchone()
            if row is None or row[1:] != ("CLAIMED", lease):
                raise ValueError("active claim required")
            task = self._task(row[0])
            state = evidence["conclusion"]
            if (not self._authorized(db, task, config)
                    or not self._current(task, config.get("observations", {}))):
                state = "STALE"
            elif task.kind == "review":
                observed = config["observations"][f"{task.repository}#{task.pr}"]
                run_root = f"https://github.com/{task.repository}/actions/runs/"
                if not evidence["run"].startswith(run_root) or (
                    evidence["tested_sha"] != observed.get("tested_sha") or evidence["run"] != observed.get("run")
                ):
                    state = "BLOCKED"
            if state == "PASS":
                state = "WAITING_FINAL" if task.kind == "review" and (task.important or task.authored_here is not False) else "DONE"
            db.execute("UPDATE v1_task SET state=?,lease=NULL,deadline=NULL,evidence=? WHERE id=?",
                       (state, json.dumps(evidence), task.id))
            if state == "WAITING_FINAL":
                request = {"to": "ChatGPT", "task": asdict(task), "evidence": evidence,
                           "delivery": "BLOCKED_NO_CONNECTOR", "merge_authority": "author"}
                db.execute("INSERT INTO v1_final VALUES (?,?)", (task.id, json.dumps(request)))
            return {"status": state}

    def reconcile(self, task_id, evidence_run, config=None):
        """Only independently verified, claim-bound no-effect evidence permits reauthorization."""
        if not isinstance(evidence_run, str) or not RUN.fullmatch(evidence_run):
            raise ValueError("recovery evidence required")
        config = config or {}
        with self._db() as db:
            self._expire(db, int(self.now()))
            row = db.execute("SELECT spec,state,lease FROM v1_task WHERE id=?", (task_id,)).fetchone()
            proof = config.get("recovery", {}).get(task_id)
            if (not row or row[1] != "UNCONFIRMED" or proof != {
                "lease": row[2], "run": evidence_run, "no_active_or_external_effect": True,
            } or not evidence_run.startswith(f"https://github.com/{self._task(row[0]).repository}/actions/runs/")):
                raise ValueError("verified recovery for this repository and claim required")
            db.execute("UPDATE v1_task SET state='BLOCKED',lease=NULL,deadline=NULL,evidence=? WHERE id=?",
                       (json.dumps(proof), task_id))
            db.execute("INSERT INTO v1_safe_recovery VALUES (?)", (task_id,))
        return {"status": "BLOCKED"}

    def _final_current(self, db, request, config):
        task = Task(**request["task"])
        if (not self._authorized(db, task, config)
                or not self._current(task, config.get("observations", {}))):
            return False
        observed = config["observations"][f"{task.repository}#{task.pr}"]
        evidence = request["evidence"]
        return (observed.get("tested_sha"), observed.get("run")) == (evidence["tested_sha"], evidence["run"])

    def final_claim(self, config, reviewer):
        if reviewer != "chatgpt":
            raise PermissionError("independent ChatGPT receiver required")
        if (config.get("stop") is not False or os.environ.get("ORCHESTRATOR_V1_STOP") == "1"
                or config.get("final_capacity_confirmed") is not True):
            return {"status": "BLOCKED"}
        with self._db() as db:
            candidates = db.execute("""
                SELECT t.id,f.request FROM v1_task t JOIN v1_final f ON t.id=f.task_id
                LEFT JOIN v1_final_claim c ON c.task_id=t.id
                WHERE t.state='WAITING_FINAL' AND c.task_id IS NULL ORDER BY t.id
            """)
            for task_id, encoded in candidates:
                request = json.loads(encoded)
                if self._final_current(db, request, config):
                    lease = secrets.token_hex(16)
                    db.execute("INSERT INTO v1_final_claim VALUES (?,?,?,?,NULL)",
                               (task_id, reviewer, lease, int(self.now()) + 900))
                    request["delivery"] = "CLAIMED"
                    return {"status": "CLAIMED", "request": request, "lease": lease}
            return {"status": "NOOP"}

    def final_decide(self, config, reviewer, decision):
        keys = {"task_id", "lease", "head", "base", "authorization", "outcome", "evidence"}
        if (reviewer != "chatgpt" or decision.keys() != keys
                or decision["outcome"] not in ("PASS", "REQUEST_CHANGES")
                or not isinstance(decision["evidence"], str)):
            raise ValueError("structured independent final decision required")
        encoded = json.dumps(decision, sort_keys=True)
        with self._db() as db:
            row = db.execute("""
                SELECT f.request,c.reviewer,c.lease,c.deadline,c.decision,t.state
                FROM v1_final_claim c JOIN v1_final f ON f.task_id=c.task_id
                JOIN v1_task t ON t.id=c.task_id WHERE c.task_id=?
            """, (decision["task_id"],)).fetchone()
            if not row or (row[1], row[2]) != (reviewer, decision["lease"]):
                raise ValueError("final claim not owned by this receiver")
            request = json.loads(row[0])
            if not self._final_current(db, request, config):
                return {"status": "STALE"}
            if row[4] is not None:
                if row[4] != encoded:
                    raise ValueError("final decision already recorded")
                return {"status": row[5], "duplicate": True, "merge": "AUTHOR_REQUIRED"}
            if row[5] != "WAITING_FINAL" or row[3] <= int(self.now()):
                return {"status": "UNCONFIRMED"}
            task = request["task"]
            if (any(decision[k] != task[k] for k in ("head", "base", "authorization"))
                    or not re.fullmatch(
                        rf"https://github\.com/{re.escape(task['repository'])}/pull/{task['pr']}#pullrequestreview-[0-9]+",
                        decision["evidence"],
                    )):
                return {"status": "STALE"}
            expected = {key: decision[key] for key in ("head", "base", "authorization", "outcome", "evidence")}
            if config.get("final_evidence", {}).get(task["id"]) != {**expected, "reviewer": reviewer}:
                return {"status": "BLOCKED", "reason": "independent_review_evidence_unverified"}
            state = "DONE" if decision["outcome"] == "PASS" else "REQUEST_CHANGES"
            db.execute("UPDATE v1_final_claim SET decision=? WHERE task_id=?", (encoded, task["id"]))
            db.execute("UPDATE v1_task SET state=? WHERE id=?", (state, task["id"]))
            return {"status": state, "merge": "AUTHOR_REQUIRED"}

    def final_reconcile(self, config, task_id, run):
        with self._db() as db:
            row = db.execute("""
                SELECT f.request,c.lease,c.deadline,c.decision FROM v1_final_claim c
                JOIN v1_final f ON f.task_id=c.task_id WHERE c.task_id=?
            """, (task_id,)).fetchone()
            proof = config.get("final_recovery", {}).get(task_id)
            if (not row or row[2] > int(self.now()) or row[3] is not None
                    or proof != {"lease": row[1], "run": run, "no_active_or_external_effect": True}
                    or not isinstance(run, str) or not RUN.fullmatch(run)
                    or not run.startswith(f"https://github.com/{json.loads(row[0])['task']['repository']}/actions/runs/")):
                raise ValueError("verified expired final claim recovery required")
            db.execute("DELETE FROM v1_final_claim WHERE task_id=?", (task_id,))
        return {"status": "WAITING_FINAL"}

    def final_requests(self, config):
        with self._db() as db:
            requests = []
            for value, deadline, decision in db.execute("""
                SELECT f.request,c.deadline,c.decision FROM v1_final f
                LEFT JOIN v1_final_claim c ON c.task_id=f.task_id ORDER BY f.task_id
            """):
                request = json.loads(value)
                if not self._final_current(db, request, config):
                    request["delivery"] = "STALE"
                elif decision is not None:
                    request["delivery"] = "RECORDED"
                elif deadline is not None:
                    request["delivery"] = "UNCONFIRMED" if deadline <= int(self.now()) else "CLAIMED"
                requests.append(request)
            return requests

    def external_handoffs(self, config):
        """Private handoff envelopes; no app identity or delivery is presumed."""
        with self._db() as db:
            states = self._states(db)
            result = []
            for (spec,) in db.execute("SELECT spec FROM v1_task WHERE state='READY' ORDER BY id"):
                task = self._task(spec)
                role = ROLES[task.kind]
                if role == "codex":
                    continue
                reason = "unverified_publisher"
                if (config.get("stop") is not False or os.environ.get("ORCHESTRATOR_V1_STOP") == "1"
                        or not self._authorized(db, task, config)
                        or not self._dependencies_ready(db, task, states, config)
                        or not self._current(task, config.get("observations", {}))):
                    reason = "prerequisites"
                result.append({"task": asdict(task), "role": role, "status": "BLOCKED", "reason": reason})
            return result

    def associate_handoff(self, config, packet):
        """Persist transport evidence without claiming that an unverified receiver executed."""
        keys = {"version", "task_id", "authorization", "control_id", "repository", "pr",
                "publisher_app_id", "publication", "receipt"}
        if (packet.keys() != keys or packet["version"] != "control-v1.0"
                or packet["repository"] != "lastgloaming/ai-writer" or packet["pr"] != 3
                or config.get("verified_handoffs", {}).get(packet["task_id"]) != packet
                or not identifier(packet["control_id"])
                or any(type(packet[k]) is not int or not 0 < packet[k] < 2**63
                       for k in ("publication", "receipt", "publisher_app_id"))
                or packet["receipt"] <= packet["publication"]):
            raise ValueError("verified versioned handoff association required")
        with self._db() as db:
            row = db.execute("SELECT spec FROM v1_task WHERE id=?", (packet["task_id"],)).fetchone()
            task = self._task(row[0]) if row else None
            if (task is None or ROLES.get(task.kind) != "integration" or task.authorization != packet["authorization"]
                    or not self._authorized(db, task, config)
                    or not self._current(task, config.get("observations", {}))):
                raise ValueError("handoff does not match current authorized task")
            record = json.dumps(packet, sort_keys=True)
            existing = db.execute("SELECT contract FROM v1_connector_receipt WHERE task_id=?", (task.id,)).fetchone()
            if existing and existing[0] != record:
                raise ValueError("conflicting handoff observation")
            if not existing:
                db.execute("INSERT INTO v1_connector_receipt VALUES (?,?,?,?)",
                           (task.id, record, packet["publication"], packet["receipt"]))
            return {"status": "BLOCKED", "association": "RECORDED", "execution": "UNVERIFIED"}

    def status(self):
        with self._db() as db:
            self._expire(db, int(self.now()))
            return dict(db.execute("SELECT id,state FROM v1_task ORDER BY id"))


def observe(config):
    """Collect current GitHub PR metadata via the existing read-only gh route."""
    result = {}
    for task in (Task(**item) for item in config["tasks"]):
        key = f"{task.repository}#{task.pr}"
        if not task.pr or key in result:
            continue
        raw = subprocess.run(
            ["gh", "pr", "view", str(task.pr), "--repo", task.repository,
             "--json", "state,headRefOid,baseRefOid"],
            capture_output=True, check=True, timeout=25,
        )
        pr = json.loads(raw.stdout)
        result[key] = {"state": pr["state"], "head": pr["headRefOid"],
                       "base": pr["baseRefOid"], "checked": int(time.time())}
    return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("operation", choices=("sync", "observe", "claim", "complete", "status", "final-requests", "external-handoffs", "reconcile"))
    parser.add_argument("--task")
    parser.add_argument("--lease")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--run")
    args = parser.parse_args()
    config = json.loads(args.config.read_bytes())
    queue = Queue(args.db)
    if args.operation == "sync":
        queue.add([Task(**item) for item in config["tasks"]], config)
        result = queue.status()
    elif args.operation == "observe":
        result = observe(config)
    elif args.operation == "claim":
        result = queue.claim(config)
    elif args.operation == "complete":
        result = queue.complete(args.task, args.lease, config, json.loads(args.evidence.read_bytes()))
    elif args.operation == "reconcile":
        result = queue.reconcile(args.task, args.run, config)
    elif args.operation == "external-handoffs":
        result = queue.external_handoffs(config)
    elif args.operation == "final-requests":
        result = queue.final_requests(config)
    else:
        result = queue.status()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
