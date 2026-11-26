"""Git-backed SQLite snapshots. Mutations must be replayable and perform no I/O effects."""

import base64
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from contextlib import closing

from .core import IDENTIFIER, transaction

STATE_REF = "refs/heads/p4-state"
CHECKPOINT = re.compile(r"[0-9a-f]{40}")
LIVE_REMOTE = "https://github.com/lastgloaming/ai-team-orchestrator.git"


class StateConflict(RuntimeError):
    pass


class GitState:
    def __init__(self, remote, checkpoint=None):
        if checkpoint is not None and not CHECKPOINT.fullmatch(checkpoint):
            raise ValueError("invalid state checkpoint")
        self.remote = remote
        self.checkpoint = checkpoint

    def _git(self, root, *args, check=True, data=None):
        env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
        env.pop("DEEPSEEK_API_KEY", None)
        env.pop("ORCHESTRATOR_PROBE_TOKEN", None)
        env.pop("ORCHESTRATOR_AUTO_REPLY_TOKEN", None)
        token = env.pop("ORCHESTRATOR_STATE_TOKEN", None)
        if self.remote == LIVE_REMOTE and token and (args[0] in {"ls-remote", "fetch"} or "push" in args):
            auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            env.update(GIT_CONFIG_COUNT="2",
                       GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
                       GIT_CONFIG_VALUE_0=f"Authorization: Basic {auth}",
                       GIT_CONFIG_KEY_1="http.followRedirects", GIT_CONFIG_VALUE_1="false")
        return subprocess.run(
            ["git", "-C", str(root), *args], input=data, capture_output=True, check=check,
            env=env, timeout=45,
        )

    def _version(self, root):
        result = self._git(root, "ls-remote", "--exit-code", self.remote, STATE_REF, check=False)
        if result.returncode == 2:
            return None
        if result.returncode:
            raise RuntimeError("could not read state ref")
        return result.stdout.decode().split()[0]

    def _push_snapshot(self, root, ledger, version):
        blob = self._git(root, "hash-object", "-w", "--stdin", data=ledger.read_bytes()).stdout.strip()
        tree = self._git(root, "mktree", data=b"100644 blob " + blob + b"\tledger.sqlite\n")
        parent = ("-p", version) if version else ()
        commit = self._git(
            root, "-c", "user.name=Orchestrator P4", "-c", "user.email=orchestrator@users.noreply.github.com",
            "commit-tree", tree.stdout.decode().strip(), *parent,
            "-m", "Update P4 ledger", "-m", f"Transaction: {os.urandom(16).hex()}",
        ).stdout.decode().strip()
        hook = root / "p4-hooks" / "pre-push"
        hook.parent.mkdir(exist_ok=True)
        expected = version or "0" * 40
        hook.write_text(
            "#!/bin/sh\n"
            "read -r local_ref local_sha remote_ref remote_sha || exit 1\n"
            f'test "$remote_ref" = {STATE_REF} && test "$remote_sha" = {expected}\n'
        )
        hook.chmod(0o700)
        try:
            pushed = self._git(root, "-c", f"core.hooksPath={hook.parent}", "push",
                               self.remote, f"{commit}:{STATE_REF}", check=False)
        except subprocess.TimeoutExpired:
            pushed = None
        current = self._version(root)
        if current == commit:
            return commit, current
        if current is None or current == version:
            raise StateConflict("state push unconfirmed; no external action authorized")
        if pushed is not None and not pushed.returncode:
            # Verify the new tip descends from the just-pushed commit before replay.
            current = self._require_descendant(root, commit)
        return None, current

    def _require_descendant(self, root, floor):
        self._git(root, "fetch", "--quiet", self.remote, STATE_REF)
        fetched = self._git(root, "rev-parse", "FETCH_HEAD").stdout.decode().strip()
        ancestor = self._git(root, "merge-base", "--is-ancestor", floor, fetched, check=False)
        if ancestor.returncode:
            raise StateConflict("state branch rolled back or diverged")
        return fetched

    def initialize(self):
        """Bootstrap an empty remote explicitly. Existing state is never replaced."""
        if self.checkpoint is not None:
            raise StateConflict("cannot initialize after a checkpoint is configured")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._git(root, "init", "--quiet", "--bare")
            if self._version(root) is not None:
                raise StateConflict("state branch already exists")
            ledger = root / "ledger.sqlite"
            with transaction(ledger) as db:
                db.execute("PRAGMA user_version=1")
                db.execute("CREATE TABLE inbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT)")
                db.execute("CREATE TABLE p4_call (once INTEGER PRIMARY KEY CHECK(once=1), usage_id INTEGER)")
                db.execute("CREATE TABLE probe_sends (id TEXT PRIMARY KEY)")
            commit, _ = self._push_snapshot(root, ledger, None)
            if commit is None:
                raise StateConflict("another initialization won; verify its checkpoint")
            self.checkpoint = commit
            return commit

    def update(self, mutate):
        """Apply a replayable mutation against the exact trusted starting checkpoint."""
        if self.checkpoint is None:
            raise StateConflict("trusted state checkpoint is required")
        version = self.checkpoint
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._git(root, "init", "--quiet", "--bare")
            current = self._version(root)
            if current is None:
                raise StateConflict("state branch missing")
            if current != version:
                raise StateConflict("state checkpoint mismatch; verify the remote history")
            for _ in range(4):
                version = self._require_descendant(root, version)
                ledger = root / "ledger.sqlite"
                try:
                    before = self._git(root, "show", f"{version}:ledger.sqlite").stdout
                    ledger.write_bytes(before)
                    with closing(sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)) as db:
                        tables = {name for (name,) in db.execute(
                            "SELECT name FROM sqlite_schema WHERE type='table'"
                        )}
                        required = {"inbox", "p4_call", "probe_sends", "usage", "seen", "tasks",
                                    "decisions", "outbox", "outbox_leases"}
                        if (db.execute("PRAGMA integrity_check").fetchone() != ("ok",)
                                or db.execute("PRAGMA user_version").fetchone() != (1,)
                                or not required <= tables):
                            raise StateConflict("invalid state snapshot")
                except (sqlite3.DatabaseError, subprocess.CalledProcessError):
                    raise StateConflict("invalid state snapshot") from None
                answer = mutate(ledger)
                if ledger.read_bytes() == before:
                    self.checkpoint = version
                    return answer
                commit, current = self._push_snapshot(root, ledger, version)
                if commit is not None:
                    self.checkpoint = commit
                    return answer
            raise StateConflict("state changed repeatedly; retry the same event ID")


def enqueue(ledger, event_id, payload):
    if not IDENTIFIER.fullmatch(event_id):
        raise ValueError("invalid event ID")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 8192:
        raise ValueError("event too large")
    with transaction(ledger) as db:
        existing = db.execute("SELECT payload FROM inbox WHERE id=?", (event_id,)).fetchone()
        if existing:
            if existing[0] != encoded:
                raise ValueError("event ID already has a different payload")
            return False
        db.execute("INSERT INTO inbox (id,payload) VALUES (?,?)", (event_id, encoded))
        return True


def consume(ledger, handler):
    """Record a deterministic handler's result atomically with consuming the event."""
    with transaction(ledger) as db:
        row = db.execute("SELECT id,payload FROM inbox WHERE result IS NULL ORDER BY rowid LIMIT 1").fetchone()
        if row is None:
            return None
        event_id, payload = row
        result = handler(event_id, json.loads(payload))
        db.execute("UPDATE inbox SET result=? WHERE id=?", (json.dumps(result), event_id))
        return event_id, result
