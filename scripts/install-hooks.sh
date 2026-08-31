#!/usr/bin/env bash
# git hook 설치 — 클론 후 1회 실행.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

chmod +x .githooks/* 2>/dev/null || true
git config core.hooksPath .githooks

echo "✓ core.hooksPath = $(git config core.hooksPath)"
echo "  우회가 필요하면: SKIP_DOC_LINT=1 git commit ..."
