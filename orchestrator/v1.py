"""Private task queue and operator entry point for Orchestrator v1."""

import argparse
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
from dataclasses import asdict, dataclass
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

    def add(self, tasks):
        with self._db() as db:
            for task in tasks:
                spec = json.dumps(asdict(task), sort_keys=True)
                prior = db.execute("SELECT spec FROM v1_task WHERE id=?", (task.id,)).fetchone()
                if prior and prior[0] != spec:
                    raise ValueError("task ID already names a different intent or approval")
                if db.execute("SELECT 1 FROM v1_task WHERE logical_key=?", (task.logical_key(),)).fetchone():
                    continue
                state = "READY" if task.kind in ROLES else "WAITING_AUTHOR"
                db.execute("INSERT INTO v1_task (id,logical_key,spec,state) VALUES (?,?,?,?)",
                           (task.id, task.logical_key(), spec, state))

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

    def claim(self, config):
        with self._db() as db:
            now = int(self.now())
            self._expire(db, now)
            if config.get("stop") is not False or os.environ.get("ORCHESTRATOR_V1_STOP") == "1":
                return {"status": "BLOCKED", "reason": "stopped"}
            if db.execute("SELECT 1 FROM v1_task WHERE state IN ('CLAIMED','UNCONFIRMED') LIMIT 1").fetchone():
                return {"status": "NOOP", "reason": "in_flight_or_unconfirmed"}
            states = {id_: state for id_, state in db.execute("SELECT id,state FROM v1_task")}
            ready = [self._task(spec) for (spec,) in db.execute("SELECT spec FROM v1_task WHERE state='READY'")]
            ready.sort(key=lambda task: (-task.priority, task.kind != "review", task.id))
            blockers = set()
            for task in ready:
                if config.get("authorizations", {}).get(task.id) != task.authorization:
                    blockers.add("authorization")
                elif not all(states.get(dep) == "DONE" for dep in task.dependencies):
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
        if task.units == 0:
            return True
        budget = config.get("budget", {})
        remaining, reserve, window = (budget.get(k) for k in ("remaining", "reserve", "window"))
        if (type(remaining) is not int or type(reserve) is not int or reserve < 0
                or not identifier(window)):
            return False
        used = db.execute("SELECT COALESCE(SUM(units),0) FROM v1_budget WHERE window=?", (window,)).fetchone()[0]
        if remaining - used - task.units <= reserve:
            return False
        db.execute("INSERT INTO v1_budget VALUES (?,?,?)", (task.id, window, task.units))
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
            if (config.get("authorizations", {}).get(task.id) != task.authorization
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

    def reconcile(self, task_id, evidence_run):
        """Operator records an independently inspected run; never automatically replay an unknown effect."""
        if not RUN.fullmatch(evidence_run):
            raise ValueError("recovery evidence required")
        with self._db() as db:
            self._expire(db, int(self.now()))
            if not db.execute("""UPDATE v1_task SET state='BLOCKED', lease=NULL, deadline=NULL, evidence=?
                                 WHERE id=? AND state='UNCONFIRMED'""",
                              (json.dumps({"recovery_run": evidence_run}), task_id)).rowcount:
                raise ValueError("only an unconfirmed task can be reconciled")
        return {"status": "BLOCKED"}

    def final_requests(self, config):
        with self._db() as db:
            requests = [json.loads(value) for (value,) in db.execute("SELECT request FROM v1_final ORDER BY task_id")]
            for request in requests:
                task = Task(**request["task"])
                if (config.get("authorizations", {}).get(task.id) != task.authorization
                        or not self._current(task, config.get("observations", {}))):
                    request["delivery"] = "STALE"
                else:
                    observed = config["observations"][f"{task.repository}#{task.pr}"]
                    if (observed.get("tested_sha"), observed.get("run")) != (
                        request["evidence"]["tested_sha"], request["evidence"]["run"]
                    ):
                        request["delivery"] = "STALE"
            return requests

    def external_handoffs(self, config):
        """Private handoff envelopes; no app identity or delivery is presumed."""
        with self._db() as db:
            states = dict(db.execute("SELECT id,state FROM v1_task"))
            result = []
            for (spec,) in db.execute("SELECT spec FROM v1_task WHERE state='READY' ORDER BY id"):
                task = self._task(spec)
                role = ROLES[task.kind]
                if role == "codex":
                    continue
                reason = "unverified_publisher"
                if (config.get("stop") is not False or os.environ.get("ORCHESTRATOR_V1_STOP") == "1"
                        or config.get("authorizations", {}).get(task.id) != task.authorization
                        or not all(states.get(dep) == "DONE" for dep in task.dependencies)
                        or not self._current(task, config.get("observations", {}))):
                    reason = "prerequisites"
                result.append({"task": asdict(task), "role": role, "status": "BLOCKED", "reason": reason})
            return result

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
        queue.add([Task(**item) for item in config["tasks"]])
        result = queue.status()
    elif args.operation == "observe":
        result = observe(config)
    elif args.operation == "claim":
        result = queue.claim(config)
    elif args.operation == "complete":
        result = queue.complete(args.task, args.lease, config, json.loads(args.evidence.read_bytes()))
    elif args.operation == "reconcile":
        result = queue.reconcile(args.task, args.run)
    elif args.operation == "external-handoffs":
        result = queue.external_handoffs(config)
    elif args.operation == "final-requests":
        result = queue.final_requests(config)
    else:
        result = queue.status()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
