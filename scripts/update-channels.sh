#!/usr/bin/env bash
# Weekly host script: regenerate the public channel lists and push them to
# the configured git remote (e.g. the public GitHub repo).
#
# Only channels that are both active AND accessible are exported — channels
# reported as deleted/banned/private by Telegram are filtered out. The
# hourly channel_join job keeps those flags fresh via Telegram checks.
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

mkdir -p "$DATA_DIR" "$EXPORT_DIR"

if [[ -z "${TELECRIME_DATABASE_URL:-}" ]]; then
  echo "TELECRIME_DATABASE_URL must be set (source .env or export it)" >&2
  exit 1
fi

# `uv run python` reads the URL from the process environment; assigning the
# shell variable alone (from .env) is not enough — without export the CLI
# aborts with "database_url must be set" under cron.
export TELECRIME_DATABASE_URL

# cron has no interactive git auth: fail fast instead of hanging forever on a
# credential or SSH passphrase prompt.
export GIT_TERMINAL_PROMPT=0
export GIT_SSH_COMMAND="${GIT_SSH_COMMAND:-ssh -o BatchMode=yes}"

# Serialize runs: overlapping exports race on the same channels.md/txt and on
# the git working tree.
LOCK_FILE="${TELECRIME_CHANNEL_EXPORT_LOCK:-$DATA_DIR/.update-channels.lock}"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another channel export is already running — skipping" >&2
  exit 0
fi

# Find uv for cron's minimal PATH.
UV_BIN="${UV_BIN:-$(command -v uv || true)}"
if [[ -z "$UV_BIN" ]]; then
  for candidate in "$HOME/.local/bin/uv" "$REPO_DIR/.venv/bin/uv" /usr/local/bin/uv /usr/bin/uv; do
    if [[ -x "$candidate" ]]; then
      UV_BIN="$candidate"
      break
    fi
  done
fi
if [[ -z "$UV_BIN" ]]; then
  echo "uv not found (set UV_BIN or add it to PATH)" >&2
  exit 1
fi

if [ "$PUSH" = "1" ]; then
  COMMIT_DIR="${TELECRIME_CHANNEL_EXPORT_REPO_DIR:-$REPO_DIR}"
  OUT_DIR="$COMMIT_DIR"
  # Push to the `github` remote if configured, otherwise the default remote.
  # Git credentials must be available to the cron job (e.g. a `~/.git-credentials`
  # file owned by the crontab user, or an SSH key configured in ~/.ssh).
  if git -C "$COMMIT_DIR" remote get-url github >/dev/null 2>&1; then
    PUSH_REMOTE="github"
  else
    PUSH_REMOTE="origin"
  fi
else
  OUT_DIR="$EXPORT_DIR"
  PUSH_REMOTE=""
fi

mkdir -p "$OUT_DIR"

# Generate into a staging dir on the target filesystem, validate, then rename
# into place. The exporter truncates-writes channels.md/txt in place, so a
# crash or the in-container channel_export job writing the same files would
# otherwise leave a partial list behind.
STAGE_DIR="$(mktemp -d "$OUT_DIR/.channels-export.XXXXXX")"
trap 'rm -rf "$STAGE_DIR"' EXIT

EXPORT_TIMEOUT="${TELECRIME_CHANNEL_EXPORT_TIMEOUT:-3600}"
timeout "$EXPORT_TIMEOUT" "$UV_BIN" run python -m telecrime channels-export --output-dir "$STAGE_DIR"

# Same checks as .github/workflows/channels.yml: non-empty and t.me-only.
if [[ ! -s "$STAGE_DIR/channels.md" ]]; then
  echo "channel export produced an empty channels.md — keeping previous files" >&2
  exit 1
fi
if [[ ! -s "$STAGE_DIR/channels.txt" ]]; then
  echo "channel export produced an empty channels.txt — keeping previous files" >&2
  exit 1
fi
bad_lines="$(grep -v '^https://t\.me/' "$STAGE_DIR/channels.txt" | grep -v '^$' || true)"
if [[ -n "$bad_lines" ]]; then
  echo "channels.txt contains non-t.me lines — refusing to install:" >&2
  printf '%s\n' "$bad_lines" >&2
  exit 1
fi

# Atomic on the same filesystem: readers see either the old or the new file.
mv -f "$STAGE_DIR/channels.md" "$OUT_DIR/channels.md"
mv -f "$STAGE_DIR/channels.txt" "$OUT_DIR/channels.txt"

if [ "$PUSH" = "1" ]; then
  git -C "$COMMIT_DIR" add channels.md channels.txt
  if ! git -C "$COMMIT_DIR" commit -m "chore: update channel lists [auto]" --allow-empty; then
    echo "git commit failed" >&2
    exit 1
  fi
  if ! git -C "$COMMIT_DIR" push "$PUSH_REMOTE"; then
    echo "git push to '$PUSH_REMOTE' failed" >&2
    exit 1
  fi
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] channel export done"
