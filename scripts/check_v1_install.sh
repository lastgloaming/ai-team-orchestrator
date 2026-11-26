#!/usr/bin/env bash
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp -r "$project/pyproject.toml" "$project/orchestrator" "$scratch/"
python -m venv --system-site-packages "$scratch/env"
(
  cd "$scratch"
  "$scratch/env/bin/python" -m pip install --no-index --no-build-isolation .
  cat > policy.json <<'JSON'
{"stop":false,"tasks":[{"id":"fixture","repository":"example/alpha","kind":"research","authorization":"v1","priority":1,"units":1}],"authorizations":{"fixture":"v1"},"budget":{"remaining":5,"reserve":1,"window":"fixture"}}
JSON
  "$scratch/env/bin/orchestrator-v1-bridge" --help >/dev/null
  cli="$scratch/env/bin/orchestrator-v1"
  "$cli" --db queue.sqlite --config policy.json sync
  "$cli" --db queue.sqlite --config policy.json claim | python -c 'import json,sys; assert json.load(sys.stdin)["status"]=="CLAIMED"'
  "$cli" --db queue.sqlite --config policy.json claim | python -c 'import json,sys; assert json.load(sys.stdin)["status"]=="NOOP"'
)
