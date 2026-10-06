#!/usr/bin/env bash
# Publish ONLY the app (src/app, the trip_intel library it imports, app.yaml, requirements.txt) to the private
# Git repo BCD's UAT workspace deploys Apps from. Docs, notes, jobs and pipeline code never leave this repo.
# Usage: scripts/publish_app_repo.sh [repo-url]
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
url="${1:-https://github.com/rohit-db/bcd-trip-intel-app.git}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git clone -q "$url" "$work" 2>/dev/null || git -C "$work" init -q -b main
# Replace everything except .git with the current app sources.
for entry in "$work"/* "$work"/.[!.]*; do
  [ -e "$entry" ] && [ "$(basename "$entry")" != ".git" ] && rm -rf "$entry"
done
cp "$root/src/app.yaml" "$root/src/requirements.txt" "$work/"
rsync -a --exclude __pycache__ "$root/src/app" "$root/src/trip_intel" "$work/"
printf '%s\n' "BCD Trip Intelligence app (Databricks App). Published from the main demo repo by" \
  "scripts/publish_app_repo.sh; do not edit here." > "$work/README.md"
cd "$work"
git add -A
if git diff --cached --quiet; then echo "app repo already up to date"; exit 0; fi
git commit -q -m "app: sync from bcd-scheduled-changes-demo $(git -C "$root" rev-parse --short HEAD)"
git remote get-url origin >/dev/null 2>&1 || git remote add origin "$url"
git push -q origin main
git rev-parse HEAD
