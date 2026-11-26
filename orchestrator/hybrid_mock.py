"""Two-repository one-shot proof using local Git and an injected Mock only."""

import re
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .auto_events import positive_id
from .live_approval import decode_snapshot
from .live_scheduling import PrivateLedger, event_matches, parse_approval, schedule_event, verify_execution
from .state import CHECKPOINT, GitState, StateConflict

REF = "refs/heads/p5-opaque-witness"
COMMITMENT = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class WitnessBinding:
    repository_id: int
    anchor: str
    commitment: str
    run_id: int

    def __post_init__(self):
        if (not positive_id(self.repository_id) or not positive_id(self.run_id)
                or not CHECKPOINT.fullmatch(self.anchor) or not COMMITMENT.fullmatch(self.commitment)):
            raise ValueError("invalid witness binding")


class LocalWitness(GitState):
    """Proof-of-concept witness. Production requires independently enforced append-only refs."""

    def __init__(self, remote, repository_id):
        path = Path(remote)
        if not path.is_absolute() or not path.is_dir() or not positive_id(repository_id):
            raise ValueError("only existing absolute local witness repositories are allowed")
        super().__init__(str(path.resolve()))
        self.repository_id = repository_id

    def tip(self, root):
        rows = self._git(root, "ls-remote", self.remote, REF).stdout.decode().split()
        return rows[0] if rows else None

    def commit(self, root, content, parent=None):
        entries = b""
        if content is not None:
            blob = self._git(root, "hash-object", "-w", "--stdin", data=content).stdout.strip()
            entries = b"100644 blob " + blob + b"\tcommitment\n"
        tree = self._git(root, "mktree", data=entries).stdout.decode().strip()
        return self._git(root, "-c", "user.name=Opaque Witness",
                         "-c", "user.email=witness@users.noreply.github.com", "commit-tree", tree,
                         *(("-p", parent) if parent else ()), "-m", "Opaque witness").stdout.decode().strip()

    def push(self, root, commit, old):
        hook = root / "hooks" / "pre-push"
        hook.write_text("#!/bin/sh\nread -r local_ref local_sha remote_ref remote_sha || exit 1\n"
                        f'test "$remote_ref" = {REF} && test "$remote_sha" = {old or ("0" * 40)}\n')
        hook.chmod(0o700)
        result = self._git(root, "-c", f"core.hooksPath={hook.parent}", "push", "--porcelain",
                           self.remote, f"{commit}:{REF}", check=False)
        # Up-to-date is not a grant, even if another runner made an identical commit.
        flag = b" " if old else b"*"
        changed = any(line.startswith(flag + b"\t") and line.split(b"\t")[1].endswith(
            b":" + REF.encode()) for line in result.stdout.splitlines())
        if result.returncode or not changed or self.tip(root) != commit:
            raise StateConflict("witness write not independently confirmed")

    def bootstrap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._git(root, "init", "--bare", "--quiet")
            if self.tip(root) is not None:
                raise StateConflict("witness already initialized")
            commit = self.commit(root, None)
            self.push(root, commit, None)
            return commit

    def consume(self, binding):
        if binding.repository_id != self.repository_id:
            raise ValueError("wrong witness repository")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._git(root, "init", "--bare", "--quiet")
            if self.tip(root) != binding.anchor:
                raise StateConflict("witness consumed, missing or changed")
            self._git(root, "fetch", "--quiet", self.remote, REF)
            fetched = self._git(root, "rev-parse", "FETCH_HEAD").stdout.decode().strip()
            parents = self._git(root, "rev-list", "--parents", "-n", "1", fetched).stdout.decode().split()
            if (fetched != binding.anchor or parents != [fetched]
                    or self._git(root, "ls-tree", "-r", fetched).stdout):
                raise StateConflict("witness genesis is not empty or did not match")
            commit = self.commit(root, binding.commitment.encode(), fetched)
            self.push(root, commit, fetched)
            return commit

    def confirm(self, commit):
        if self.tip(Path(self.remote)) != commit:
            raise StateConflict("witness no longer confirms consumption")


def schedule_hybrid_mock(witness, state, envelope, trusted_key, event, actor, code_sha, run_id, run_attempt, mock):
    """Verify a sealed snapshot, burn the public slot, then use the private scheduler."""
    payload = decode_snapshot(envelope, trusted_key)
    if payload.keys() != {"approval", "witness"}:
        raise ValueError("invalid hybrid approval snapshot")
    approved = parse_approval(payload["approval"])
    binding = WitnessBinding(**payload["witness"])
    if (not positive_id(run_id) or run_id != binding.run_id or type(run_attempt) is not int or run_attempt != 1
            or not event_matches(event, actor, approved)):
        raise ValueError("event or first run attempt did not match signed approval")
    if (not isinstance(witness, LocalWitness) or not isinstance(state, PrivateLedger)
            or not Path(state.remote).is_absolute() or not Path(state.remote).is_dir()
            or Path(state.remote).resolve() == Path(witness.remote)
            or binding.repository_id == approved.execution.ledger_id):
        raise ValueError("distinct local witness and private repositories required")
    verify_execution(approved, approved.execution.limits, code_sha, state.ledger_identity, state.anchors)

    # Refuse a preexisting private attempt (including a forked or substituted ledger).
    def private_empty(path):
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
            if db.execute("SELECT 1 FROM live_authorization").fetchone():
                raise StateConflict("private approval already exists")

    state.update(private_empty)
    grant = witness.consume(binding)

    def invoke(request, timeout):
        witness.confirm(grant)

        def verify_reservation(path):
            with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
                row = db.execute("""
                    SELECT auto_model_attempt.status,usage.state
                    FROM auto_model_attempt JOIN usage ON usage.id=reservation_id
                    WHERE event_id=?
                """, (approved.event_id,)).fetchone()
                if row != ("RESERVED", "active"):
                    raise StateConflict("private reservation no longer confirmed")

        state.update(verify_reservation)
        return mock(request, timeout)

    return schedule_event(state, approved, event, actor, approved.execution.limits, invoke,
                          scheduling=lambda: True, live=lambda: True, token="mock-only", code_sha=code_sha)
