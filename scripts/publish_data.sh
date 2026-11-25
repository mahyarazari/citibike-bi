#!/usr/bin/env bash
# Publish pipeline outputs to the `data` branch as ONE commit without history.
#
# Why: the live job runs every 30 minutes. Committing ~40 MB of Parquet to main
# 48x per day would grow the repository by gigabytes. The data branch is
# force-pushed with a single commit, so the repo stays small and Power BI always
# reads the latest files from:
#   https://raw.githubusercontent.com/<owner>/citibike-bi/data/data/clean/<table>.parquet
#
# Usage: bash scripts/publish_data.sh "commit message"
set -euo pipefail

MSG="${1:-data refresh}"
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"; git worktree prune' EXIT

# Separate worktree on a fresh orphan branch, so the main checkout is untouched
git worktree add --detach "$TMP" >/dev/null 2>&1
cd "$TMP"
git checkout -q --orphan data-tmp
git rm -rq --cached . >/dev/null 2>&1 || true
find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +

mkdir -p data powerbi
cp -r "$ROOT/data/clean" "$ROOT/data/agg" data/
[ -d "$ROOT/data/sample" ] && cp -r "$ROOT/data/sample" data/
[ -f "$ROOT/data/manifest.json" ] && cp "$ROOT/data/manifest.json" data/
cp -r "$ROOT/powerbi/maps" powerbi/
cat > README.md <<'EOF'
# data branch (generated)
Latest outputs of the Citi Bike pipeline. Rewritten on every run - no history.
Code lives on `main`. Power BI reads `data/clean/*.parquet` from this branch.
EOF

git add -A -f
git -c user.name="${GIT_AUTHOR_NAME:-citibike-bot}" -c user.email="${GIT_AUTHOR_EMAIL:-citibike-bot@users.noreply.github.com}" \
  commit -q -m "$MSG"
git push -q -f origin HEAD:refs/heads/data
echo "Published $(git ls-files | wc -l) files to branch 'data'"
