#!/usr/bin/env bash
# Weekly host script: regenerate the public channel lists and push them to
# the configured git remote (e.g. the public GitHub repo).
#
# Only channels that are both active AND accessible are exported — channels
# reported as deleted/banned/private by Telegram are filtered out. The
# st hourly channel_join job keeps those flags fresh via Telegram checks.
#
# Usage: ./scripts/update-channels.sh [--push]
#
# --push: git commit + push to the default remote (GitHub). Without it, the
#         lists are only written locally under data/.
#
# Set up as a cron (add via `crontab -e`):
#   0 3 * * 1 /home/user/TeleCrime/scripts/update-channels.sh --push >> /home/user/TeleCrime/data/channel-export.log 2>&1

set -euo pipefail
cd "$(dirname "$0")/.."

PUSH=0
if [ "${1:-}" = "--push" ]; then
  PUSH=1
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] channel export starting (push=$PUSH)"

mkdir -p data

# Uses TELECRIME_DATABASE_URL from the environment (set in .env or exported).
# Falls back to the default dev URL if not set — override before running in production.
if [ "$PUSH" = "1" ]; then
  # Push to the `github` remote if configured, otherwise the default remote.
  # Git credentials must be available to the cron job (e.g. a `~/.git-credentials`
  # file owned by the crontab user, or an SSH key configured in ~/.ssh).
  if git remote get-url github >/dev/null 2>&1; then
    TELECRIME_DATABASE_URL="${TELECRIME_DATABASE_URL:-postgresql://telecrime:change-me@localhost:5432/telecrime}" \
      uv run python -m telecrime channels-export --output-dir . --commit --push-github
  else
    TELECRIME_DATABASE_URL="${TELECRIME_DATABASE_URL:-postgresql://telecrime:change-me@localhost:5432/telecrime}" \
      uv run python -m telecrime channels-export --output-dir . --commit --push
  fi
else
  TELECRIME_DATABASE_URL="${TELECRIME_DATABASE_URL:-postgresql://telecrime:change-me@localhost:5432/telecrime}" \
    uv run python -m telecrime channels-export --output-dir data
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] channel export done"
