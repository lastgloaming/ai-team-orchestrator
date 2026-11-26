"""Paired, append-only Git state for the isolated synthetic event workflow."""

import json
import sqlite3
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

from .core import IDENTIFIER
from .state import CHECKPOINT, GitState, StateConflict

STATE = "refs/heads/p4-auto-test-state"
WITNESS = "refs/heads/p4-auto-test-checkpoints"
REFS = (STATE, WITNESS)

SCHEMA = """
PRAGMA user_version = 2;
CREATE TABLE auto_binding (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    repository TEXT NOT NULL, repository_id INTEGER NOT NULL, pr INTEGER NOT NULL,
    sender INTEGER NOT NULL, publisher INTEGER NOT NULL, shared INTEGER NOT NULL
);
CREATE TABLE auto_inbox (
    id INTEGER PRIMARY KEY, consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE auto_outbox (
    id TEXT PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES auto_inbox(id),
    kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', comment_id INTEGER
);
"""


class PairedState(GitState):
    def __init__(self, remote, environment, anchors=None):
        super().__init__(remote)
        if not IDENTIFIER.fullmatch(environment):
            raise ValueError("invalid environment ID")
        if anchors is not None and (len(anchors) != 2 or any(not CHECKPOINT.fullmatch(x) for x in anchors)):
            raise ValueError("two fixed genesis SHAs are required")
        self.environment = environment
        self.anchors = tuple(anchors) if anchors is not None else None
        self._floor = self.anchors

    def _tips(self, root):
        response = self._git(root, "ls-remote", self.remote, *REFS).stdout.decode()
        entries = {ref: sha for sha, ref in (line.split() for line in response.splitlines())}
        return tuple(entries.get(ref) for ref in REFS)

    def _object(self, root, commit, name):
        return self._git(root, "show", f"{commit}:{name}").stdout

    def _record(self, root, commit):
        record = json.loads(self._object(root, commit, "checkpoint.json"))
        if (not isinstance(record, dict) or record.keys() != {"environment", "state", "sequence", "previous"}
                or record["environment"] != self.environment or type(record["sequence"]) is not int
                or record["sequence"] < 0):
            raise StateConflict("invalid checkpoint record")
        return record

    def _history(self, root, tip, anchor):
        result = self._git(root, "merge-base", "--is-ancestor", anchor, tip, check=False)
        if result.returncode:
            raise StateConflict("state pair left its trusted ancestry")
        return [anchor, *self._git(root, "rev-list", "--first-parent", "--reverse",
                                  f"{anchor}..{tip}").stdout.decode().split()]

    def _load(self, root, floor):
        # Fetch both references into an isolated Git directory and verify the pair before use.
        self._git(root, "fetch", "--quiet", "--no-tags", self.remote,
                  *(f"{ref}:refs/local/{index}" for index, ref in enumerate(REFS)))
        tips = tuple(self._git(root, "rev-parse", f"refs/local/{index}").stdout.decode().strip()
                     for index in range(2))
        if floor != self.anchors:
            for tip, seen in zip(tips, floor, strict=True):
                if self._git(root, "merge-base", "--is-ancestor", seen, tip, check=False).returncode:
                    raise StateConflict("state pair rolled back during this run")
        states, witnesses = (self._history(root, tip, anchor)
                             for tip, anchor in zip(tips, self.anchors, strict=True))
        if len(states) != len(witnesses):
            raise StateConflict("unpaired state and checkpoint histories")
        previous = None
        for sequence, (state, witness) in enumerate(zip(states, witnesses, strict=True)):
            expected = dict(environment=self.environment, state=state, sequence=sequence, previous=previous)
            if self._record(root, witness) != expected:
                raise StateConflict("checkpoint does not match state history")
            for commit, history in ((state, states), (witness, witnesses)):
                parents = self._git(root, "rev-list", "--parents", "-n", "1", commit).stdout.decode().split()[1:]
                if parents != ([history[sequence - 1]] if sequence else []):
                    raise StateConflict("state pair must have linear histories")
            previous = witness
        ledger = root / "ledger.sqlite"
        ledger.write_bytes(self._object(root, tips[0], "ledger.sqlite"))
        self._check_ledger(ledger)
        return tips, len(states) - 1

    @staticmethod
    def _check_ledger(ledger):
        with closing(sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)) as db:
            if (db.execute("PRAGMA integrity_check").fetchone() != ("ok",)
                    or db.execute("PRAGMA user_version").fetchone() != (2,)
                    or db.execute("PRAGMA foreign_key_check").fetchone() is not None):
                raise StateConflict("invalid isolated ledger")
            db.execute("SELECT repository, repository_id, pr, sender, publisher, shared FROM auto_binding LIMIT 0")
            db.execute("SELECT id, consumed FROM auto_inbox LIMIT 0")
            db.execute("SELECT id, event_id, kind, status, comment_id FROM auto_outbox LIMIT 0")

    def _commit(self, root, name, content, parent, message):
        blob = self._git(root, "hash-object", "-w", "--stdin", data=content).stdout.strip()
        tree = self._git(root, "mktree", data=b"100644 blob " + blob + b"\t" + name.encode() + b"\n")
        args = ("-p", parent) if parent else ()
        return self._git(root, "-c", "user.name=Orchestrator Test",
                         "-c", "user.email=orchestrator@users.noreply.github.com", "commit-tree",
                         tree.stdout.decode().strip(), *args, "-m", message).stdout.decode().strip()

    def _advance(self, root, before, sequence):
        state = self._commit(root, "ledger.sqlite", (root / "ledger.sqlite").read_bytes(),
                             before[0], "Update synthetic event ledger")
        record = dict(environment=self.environment, state=state, sequence=sequence, previous=before[1])
        witness = self._commit(root, "checkpoint.json", json.dumps(record, sort_keys=True).encode(),
                               before[1], "Witness synthetic event ledger")
        hook = root / "auto-hooks" / "pre-push"
        hook.parent.mkdir()
        hook.write_text(
            "#!/bin/sh\nset -eu\nseen_state=0\nseen_witness=0\n"
            "while read -r local_ref local_sha remote_ref remote_sha; do\n"
            " case \"$remote_ref\" in\n"
            f"  {STATE}) test \"$remote_sha\" = {before[0] or '0' * 40}; seen_state=1 ;;\n"
            f"  {WITNESS}) test \"$remote_sha\" = {before[1] or '0' * 40}; seen_witness=1 ;;\n"
            "  *) exit 1 ;;\n esac\ndone\n"
            'test "$seen_state$seen_witness" = 11\n'
        )
        hook.chmod(0o700)
        return (state, witness), self._git(
            root, "-c", f"core.hooksPath={hook.parent}", "push", "--atomic", self.remote,
            f"{state}:{STATE}", f"{witness}:{WITNESS}", check=False,
        )

    def bootstrap(self):
        """Explicit one-time setup. Normal runs never call this."""
        if self.anchors is not None:
            raise StateConflict("anchors already configured")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self._git(root, "init", "--quiet", "--bare")
            before = self._tips(root)
            if before != (None, None):
                raise StateConflict("isolated references already exist or partially exist")
            with closing(sqlite3.connect(root / "ledger.sqlite")) as db:
                db.executescript(SCHEMA)
            anchors, result = self._advance(root, before, 0)
            if result.returncode or self._tips(root) != anchors:
                raise StateConflict("bootstrap unconfirmed; inspect both refs independently")
            return anchors

    def update(self, mutate):
        if self.anchors is None:
            raise StateConflict("fixed bootstrap anchors are required")
        floor = self._floor
        for _ in range(4):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self._git(root, "init", "--quiet", "--bare")
                if None in self._tips(root):
                    raise StateConflict("one or both isolated refs are missing")
                try:
                    before, sequence = self._load(root, floor)
                    floor = self._floor = before
                    ledger = root / "ledger.sqlite"
                    snapshot = ledger.read_bytes()
                    answer = mutate(ledger)
                    if snapshot == ledger.read_bytes():
                        return answer
                    self._check_ledger(ledger)
                    proposed, result = self._advance(root, before, sequence + 1)
                    observed = self._tips(root)
                except (sqlite3.DatabaseError, subprocess.SubprocessError, ValueError) as error:
                    raise StateConflict("state could not be verified or committed") from error
                if not result.returncode and observed == proposed:
                    self._floor = proposed
                    return answer
                if None in observed or observed == before:
                    raise StateConflict("atomic state update was not confirmed")
                if not result.returncode:
                    # An acknowledged write was superseded. Verify ancestry before replay.
                    floor = proposed
                elif observed == proposed:
                    raise StateConflict("proposal visible but push confirmation lost")
                # A competing writer may have advanced both refs. Replaying an already
                # recorded claim yields no send grant, even if our push was accepted.
        raise StateConflict("state pair kept changing; no external action authorized")
