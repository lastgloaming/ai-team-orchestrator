"""One-shot paired-state initialization for the approved main workflow."""

import os
import sqlite3
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

from .auto_events import REPOSITORY
from .auto_state import REFS, PairedState
from .state import CHECKPOINT, LIVE_REMOTE, STATE_REF, StateConflict

ENVIRONMENT = "p4-auto-synthetic"
MAIN = "refs/heads/main"


def initialize(remote, expected_main, *, approval, auto_enabled):
    if approval != "1" or auto_enabled not in ("", "0"):
        raise StateConflict("bootstrap requires approval and automatic events must be off")
    if not CHECKPOINT.fullmatch(expected_main):
        raise StateConflict("expected main must be a complete commit SHA")

    state = PairedState(remote, ENVIRONMENT)

    def read_refs(root):
        data = state._git(root, "ls-remote", remote, MAIN, STATE_REF, *REFS).stdout.decode()
        return dict((ref, sha) for sha, ref in map(str.split, data.splitlines()))

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        state._git(root, "init", "--quiet", "--bare")
        before = read_refs(root)
        if before.get(MAIN) != expected_main or not before.get(STATE_REF):
            raise StateConflict("remote main or original p4-state failed preflight")
        if any(ref in before for ref in REFS):
            raise StateConflict("isolated state references already exist; do not retry bootstrap")

        anchors = state.bootstrap()

        # Fresh read plus independent fetch of both resulting objects. Never repair
        # or reattempt a push if the observed result is incomplete or unexpected.
        after = read_refs(root)
        if (tuple(after.get(ref) for ref in REFS) != anchors or after.get(MAIN) != expected_main
                or after.get(STATE_REF) != before[STATE_REF]):
            raise StateConflict("bootstrap result did not match independent remote verification")
        verifier = PairedState(remote, ENVIRONMENT, anchors)
        tips, sequence = verifier._load(root, anchors)
        if tips != anchors or sequence != 0:
            raise StateConflict("remote state is not the expected genesis pair")
        with closing(sqlite3.connect(f"file:{root / 'ledger.sqlite'}?mode=ro", uri=True)) as db:
            if any(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone() != (0,)
                   for table in ("auto_binding", "auto_inbox", "auto_outbox")):
                raise StateConflict("genesis ledger is not empty")
        return anchors


def main():
    expected_main = os.environ.get("ORCHESTRATOR_AUTO_EXPECTED_MAIN", "")
    if (os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != MAIN
            or os.environ.get("ORCHESTRATOR_AUTO_BOOTSTRAP_CONFIRMED") != "true"
            or not CHECKPOINT.fullmatch(expected_main)
            or os.environ.get("GITHUB_SHA") != expected_main):
        raise StateConflict("bootstrap requires explicit dispatch from the confirmed main commit")
    if not os.environ.get("ORCHESTRATOR_STATE_TOKEN"):
        raise StateConflict("existing state credential required")
    checked_out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if checked_out != expected_main:
        raise StateConflict("checked-out commit does not match confirmed main")
    anchors = initialize(LIVE_REMOTE, expected_main,
                         approval=os.environ.get("ORCHESTRATOR_AUTO_BOOTSTRAP_APPROVED", ""),
                         auto_enabled=os.environ.get("ORCHESTRATOR_AUTO_SYNTHETIC_ENABLED", ""))
    evidence = (
        "Atomic bootstrap verified; schema version=2; empty ledger; checkpoint sequence=0\n"
        f"p4-auto-test-state: {anchors[0]}\n"
        f"p4-auto-test-checkpoints: {anchors[1]}\n"
    )
    print(evidence, end="")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as stream:
            stream.write(evidence)


if __name__ == "__main__":
    try:
        main()
    except StateConflict as error:
        raise SystemExit(f"Bootstrap stopped: {error}. Independently inspect both refs before any further action.") from None
    except (subprocess.SubprocessError, sqlite3.DatabaseError, ValueError, OSError):
        raise SystemExit(
            "Bootstrap stopped. The refs may already exist: independently inspect both refs before any further action."
        ) from None
