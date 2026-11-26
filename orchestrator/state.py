"""Git-backed SQLite snapshots. Mutations must be replayable and perform no I/O effects."""

import base64
import json
import os
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from contextlib import closing

from .core import IDENTIFIER, transaction

STATE_REF = "refs/heads/p4-state"
LIVE_REMOTE = "https://github.com/lastgloaming/ai-team-orchestrator.git"


class StateConflict(RuntimeError):
    pass


class GitState:
    def __init__(self, remote):
        self.remote = remote

    def _git(self, root, *args, check=True, data=None):
        env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
        env.pop("DEEPSEEK_API_KEY", None)
        env.pop("ORCHESTRATOR_PROBE_TOKEN", None)
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

    def update(self, mutate):
        """Commit a pure local mutation; return its result after push confirmation."""
        for _ in range(4):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self._git(root, "init", "--quiet", "--bare")
                version = self._version(root)
                ledger = root / "ledger.sqlite"
                if version is not None:
                    self._git(root, "fetch", "--quiet", "--depth=1", self.remote, STATE_REF)
                    fetched = self._git(root, "rev-parse", "FETCH_HEAD").stdout.decode().strip()
                    if version != fetched:
                        continue
                    ledger.write_bytes(self._git(root, "show", f"{version}:ledger.sqlite").stdout)
                    with closing(sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)) as db:
                        if db.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                            raise RuntimeError("invalid state snapshot")
                before = ledger.read_bytes() if version is not None else None
                answer = mutate(ledger)
                snapshot = ledger.read_bytes()
                if snapshot == before:
                    return answer
                blob = self._git(root, "hash-object", "-w", "--stdin", data=snapshot).stdout.strip()
                tree = self._git(root, "mktree", data=b"100644 blob " + blob + b"\tledger.sqlite\n")
                parent = ("-p", version) if version else ()
                commit = self._git(
                    root, "-c", "user.name=Orchestrator P4", "-c", "user.email=orchestrator@users.noreply.github.com",
                    "commit-tree", tree.stdout.decode().strip(), *parent,
                    "-m", "Update P4 ledger", "-m", f"Transaction: {os.urandom(16).hex()}",
                ).stdout.decode().strip()
                hook = root / "hooks" / "pre-push"
                expected = version or "0" * 40
                hook.write_text(
                    "#!/bin/sh\n"
                    "read -r local_ref local_sha remote_ref remote_sha || exit 1\n"
                    f'test "$remote_ref" = {STATE_REF} && test "$remote_sha" = {expected}\n'
                )
                hook.chmod(0o700)
                pushed = self._git(root, "-c", f"core.hooksPath={hook.parent}", "push",
                                   self.remote, f"{commit}:{STATE_REF}", check=False)
                if not pushed.returncode:
                    return answer
                current = self._version(root)
                if current == commit:
                    return answer
                if current == version or current is None:
                    raise RuntimeError("state push failed; no external action authorized")
                # A peer advanced the ref, or an earlier acceptance was superseded.
                # Replay pure operations only. A replayed reservation cannot authorize a send.
        raise StateConflict("state changed repeatedly; retry the same event ID")


def enqueue(ledger, event_id, payload):
    if not IDENTIFIER.fullmatch(event_id):
        raise ValueError("invalid event ID")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 8192:
        raise ValueError("event too large")
    with transaction(ledger) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS inbox (
            id TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT
        )""")
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
        db.execute("""CREATE TABLE IF NOT EXISTS inbox (
            id TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT
        )""")
        row = db.execute("SELECT id,payload FROM inbox WHERE result IS NULL ORDER BY rowid LIMIT 1").fetchone()
        if row is None:
            return None
        event_id, payload = row
        result = handler(event_id, json.loads(payload))
        db.execute("UPDATE inbox SET result=? WHERE id=?", (json.dumps(result), event_id))
        return event_id, result
