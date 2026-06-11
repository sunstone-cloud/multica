#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-origin}"
PRIVATE_REMOTE="${PRIVATE_REMOTE:-fork}"
PRIVATE_MAIN="${PRIVATE_MAIN:-main}"
BASE_FILE="${BASE_FILE:-scripts/selfhost-upstream-base.txt}"
BRANCH_PREFIX="${BRANCH_PREFIX:-sync/upstream}"
RUN_TESTS=1
PUSH=0
DEPLOY=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/selfhost-upstream-sync.sh [--no-tests] [--push] [--deploy]

Behavior:
  1. Fetch upstream and private remotes.
  2. Create a sync branch from private main.
  3. Rebase all private commits since scripts/selfhost-upstream-base.txt
     onto latest upstream main.
  4. Record the new upstream base in scripts/selfhost-upstream-base.txt.
  5. Run server tests by default.
  6. Optionally push private main and deploy local self-host compose stack.

Flags:
  --no-tests   Skip go tests.
  --push       Push the sync branch to PRIVATE_REMOTE and fast-forward PRIVATE_MAIN.
  --deploy     Build/deploy local docker compose stack after tests. Implies --push.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-tests)
      RUN_TESTS=0
      ;;
    --push)
      PUSH=1
      ;;
    --deploy)
      PUSH=1
      DEPLOY=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

require_clean_worktree() {
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "working tree is not clean; commit or stash changes first" >&2
    git status --short >&2
    exit 1
  fi
}

read_base() {
  if [[ ! -f "$BASE_FILE" ]]; then
    echo "missing base file: $BASE_FILE" >&2
    exit 1
  fi
  tr -d '[:space:]' < "$BASE_FILE"
}

require_clean_worktree

git fetch "$UPSTREAM_REMOTE" main
git fetch "$PRIVATE_REMOTE" "$PRIVATE_MAIN"

upstream_ref="$UPSTREAM_REMOTE/main"
upstream_sha="$(git rev-parse "$upstream_ref")"
private_ref="$PRIVATE_REMOTE/$PRIVATE_MAIN"
old_base="$(read_base)"
branch="$BRANCH_PREFIX-$(date +%Y%m%d-%H%M%S)-${upstream_sha:0:8}"

if [[ "$old_base" == "$upstream_sha" ]]; then
  echo "Private main is already based on latest upstream main: $upstream_sha"
else
  if ! git merge-base --is-ancestor "$old_base" "$private_ref"; then
    echo "configured base $old_base is not an ancestor of $private_ref" >&2
    exit 1
  fi
fi

git switch -c "$branch" "$private_ref"

if [[ "$old_base" != "$upstream_sha" ]]; then
  echo "Rebasing private commits from $old_base onto $upstream_sha..."
  if ! git rebase --onto "$upstream_ref" "$old_base" "$branch"; then
    echo "rebase failed" >&2
    echo "Resolve conflicts, then run: git rebase --continue" >&2
    echo "Or abort with: git rebase --abort" >&2
    exit 1
  fi
fi

printf '%s\n' "$upstream_sha" > "$BASE_FILE"
if ! git diff --quiet -- "$BASE_FILE"; then
  git add "$BASE_FILE"
  git commit -m "chore(selfhost): record upstream sync base"
fi

if [[ "$RUN_TESTS" == "1" ]]; then
  echo "Running server tests..."
  (cd server && go test ./...)
fi

if [[ "$PUSH" == "1" ]]; then
  echo "Pushing sync branch and fast-forwarding $PRIVATE_REMOTE/$PRIVATE_MAIN..."
  git push "$PRIVATE_REMOTE" "$branch"
  git push "$PRIVATE_REMOTE" "HEAD:$PRIVATE_MAIN"
fi

if [[ "$DEPLOY" == "1" ]]; then
  backup_dir="backups"
  backup_file="$backup_dir/multica_$(date +%Y%m%d_%H%M%S).sql"
  mkdir -p "$backup_dir"
  echo "Backing up database to $backup_file..."
  docker exec multica-postgres-1 pg_dump -U multica -d multica > "$backup_file"

  echo "Building and deploying self-host images..."
  docker compose -f docker-compose.selfhost.yml -f docker-compose.selfhost.build.yml build backend frontend
  docker compose -f docker-compose.selfhost.yml -f docker-compose.selfhost.build.yml up -d

  echo "Checking health..."
  curl -fsS http://127.0.0.1:8080/health
  echo
fi

echo "Sync branch ready: $branch"
echo "HEAD: $(git rev-parse HEAD)"
