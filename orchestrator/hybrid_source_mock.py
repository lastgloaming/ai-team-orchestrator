"""Read-only native-event probe. Does not open either ledger."""

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from .auto_events import AUTHOR_ACCOUNT, REPOSITORY, positive_id
from .live_approval import canonical
from .live_scheduling import check_source

MARKER = "P5-HYBRID-SOURCE-MOCK"


@dataclass(frozen=True)
class Source:
    repository_id: int
    pr: int
    event_id: int
    run_id: int
    code_sha: str


def probe(event, actor, test_pr, run_id, code_sha, token):
    repo, issue, comment = event["repository"], event["issue"], event["comment"]
    if (event["action"] != "created" or repo["full_name"] != REPOSITORY or issue["number"] != test_pr
            or test_pr <= 1 or issue["state"] != "open" or not isinstance(issue["pull_request"], dict)
            or actor != str(AUTHOR_ACCOUNT) or event["sender"]["id"] != AUTHOR_ACCOUNT
            or comment["user"]["id"] != AUTHOR_ACCOUNT or comment["body"] != MARKER
            or not all(positive_id(value) for value in (repo["id"], comment["id"], run_id))):
        raise ValueError("unexpected native Mock event")
    source = Source(repo["id"], test_pr, comment["id"], run_id, code_sha)
    check_source(source, token, marker=MARKER)
    report = asdict(source)
    print(json.dumps({"source_get": "PASS", "snapshot_sha256": hashlib.sha256(canonical(report)).hexdigest()}))


if __name__ == "__main__":
    env = os.environ
    if (env.get("ORCHESTRATOR_P5_HYBRID_MOCK_ENABLED") != "1"
            or env.get("GITHUB_EVENT_NAME") != "issue_comment"
            or env.get("GITHUB_REF") != "refs/heads/main" or env.get("GITHUB_REPOSITORY") != REPOSITORY):
        raise SystemExit("native source Mock is disabled")
    try:
        probe(json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes()), env["GITHUB_ACTOR_ID"],
              int(env["ORCHESTRATOR_P5_HYBRID_MOCK_PR"]), int(env["GITHUB_RUN_ID"]),
              env["GITHUB_SHA"], env.get("GITHUB_TOKEN"))
    except Exception:
        raise SystemExit("native source Mock failed; no write attempted") from None
