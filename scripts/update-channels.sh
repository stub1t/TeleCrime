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
#   0 3 * * 1 /path/to/TeleCrime/scripts/update-channels.sh --push >> /path/to/TeleCrime/data/channel-export.log 2>&1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

dotenv_value() {
  local key="$1"
  if [[ -n "${!key:-}" ]]; then
    printf '%s' "${!key}"
    return
  fi
  if [[ -f "$REPO_DIR/.env" ]]; then
    (
      set -a
      # shellcheck disable=SC1091
      . "$REPO_DIR/.env"
      printf '%s' "${!key:-}"
    )
  fi
}

DATA_DIR="${TELECRIME_DATA_DIR:-$(dotenv_value TELECRIME_DATA_DIR)}"
DATA_DIR="${DATA_DIR:-$REPO_DIR/data}"
EXPORT_DIR="${TELECRIME_CHANNEL_EXPORT_DIR:-$DATA_DIR}"
TELECRIME_DATABASE_URL="${TELECRIME_DATABASE_URL:-$(dotenv_value TELECRIME_DATABASE_URL)}"
cd "$REPO_DIR"

PUSH=0
if [ "${1:-}" = "--push" ]; then
  PUSH=1
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] channel export starting (push=$PUSH)"

mkdir -p "$EXPORT_DIR"

if [[ -z "${TELECRIME_DATABASE_URL:-}" ]]; then
  echo "TELECRIME_DATABASE_URL must be set (source .env or export it)" >&2
  exit 1
fi

if [ "$PUSH" = "1" ]; then
  COMMIT_DIR="${TELECRIME_CHANNEL_EXPORT_REPO_DIR:-$REPO_DIR}"
  # Push to the `github` remote if configured, otherwise the default remote.
  # Git credentials must be available to the cron job (e.g. a `~/.git-credentials`
  # file owned by the crontab user, or an SSH key configured in ~/.ssh).
  if git remote get-url github >/dev/null 2>&1; then
    uv run python -m telecrime channels-export --output-dir "$COMMIT_DIR" --commit --push-github
  else
    uv run python -m telecrime channels-export --output-dir "$COMMIT_DIR" --commit --push
  fi
else
  uv run python -m telecrime channels-export --output-dir "$EXPORT_DIR"
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] channel export done"
