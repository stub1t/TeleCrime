#!/usr/bin/env bash
# Hot-deploy a code change to the running stack with minimal interruption.
#
# Three deploy classes:
#   - PIPELINE:   pipeline subprocess imports it fresh every run. `docker cp`
#                 alone is sufficient — the change takes effect on the next
#                 pipeline subprocess start (within 10 min, or immediately if
#                 you SIGTERM the current subprocess after the cp).
#   - SCHEDULER:  the long-lived worker process imports it once at start;
#                 needs a worker container stop/start (~10s downtime, vs the
#                 full `docker compose build` ~3min).
#   - WEB:        web container restart (~15s).
#
# Usage:
#   scripts/hot-deploy.sh <repo-relative-path> [<path>...]
#
# Examples:
#   scripts/hot-deploy.sh telecrime/pipeline/parse.py
#   scripts/hot-deploy.sh telecrime/scheduler.py
#   scripts/hot-deploy.sh telecrime/notify.py telecrime/pipeline/finalize.py
#
# Per `feedback_docker_restart` memory: stop+start preserves the writable
# layer with the cp'd files. Don't `docker restart` after a cp — that resets
# the container and you'd lose the cp.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO/docker-compose.yml"
DATA_DIR="${TELECRIME_DATA_DIR:-$REPO/data}"

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

dotenv_value() {
    local key="$1"
    if [[ -n "${!key:-}" ]]; then
        printf '%s' "${!key}"
        return
    fi
    if [[ -f "$REPO/.env" ]]; then
        (
            set -a
            # shellcheck disable=SC1091
            . "$REPO/.env"
            printf '%s' "${!key:-}"
        )
    fi
}

container_for() {
    compose ps -q "$1"
}

copy_to_service() {
    local service="$1"
    local rel="$2"
    local container
    container="$(container_for "$service")"
    if [[ -z "$container" ]]; then
        echo "✗ service is not running: $service" >&2
        exit 1
    fi
    docker cp "$REPO/$rel" "$container:/app/$rel"
}

cd "$REPO"
DATA_DIR="${TELECRIME_DATA_DIR:-$(dotenv_value TELECRIME_DATA_DIR)}"
DATA_DIR="${DATA_DIR:-$REPO/data}"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <repo-relative-path> [<path>...]" >&2
    exit 2
fi

WORKER_RESTART=0
WEB_RESTART=0
PIPELINE_KICK=0

classify() {
    case "$1" in
        telecrime/scheduler.py|telecrime/cli.py|telecrime/__main__.py|telecrime/__init__.py)
            echo "scheduler" ;;
        telecrime/web/*)
            echo "web" ;;
        telecrime/*|alembic/*)
            echo "pipeline" ;;
        *)
            echo "unknown" ;;
    esac
}

for rel in "$@"; do
    if [[ ! -f "$REPO/$rel" ]]; then
        echo "✗ missing: $REPO/$rel" >&2
        exit 1
    fi
    cls=$(classify "$rel")
    case "$cls" in
        scheduler)
            echo "→ $rel [scheduler-class]: will hot-cp + restart worker"
            copy_to_service worker "$rel"
            # Also push to web — it imports some scheduler helpers for the
            # dashboard's status panel (read-only helpers).
            copy_to_service web "$rel"
            WORKER_RESTART=1
            ;;
        web)
            echo "→ $rel [web-class]: will hot-cp + restart web"
            copy_to_service web "$rel"
            WEB_RESTART=1
            ;;
        pipeline)
            echo "→ $rel [pipeline-class]: hot-cp only (effective on next pipeline subprocess)"
            copy_to_service worker "$rel"
            copy_to_service web "$rel"
            PIPELINE_KICK=1
            ;;
        unknown)
            echo "✗ don't know how to deploy $rel" >&2
            exit 1
            ;;
    esac
done

if (( WORKER_RESTART )); then
    echo "--- restarting worker (stop+start, ~10s) ---"
    # Per feedback_docker_restart: clear stale shutdown file before AND after
    # the SIGTERM-driven stop, so the new APScheduler doesn't see a leftover
    # file and refuse to run coalesced jobs.
    rm -f "$DATA_DIR/pipeline_shutdown_request.json"
    compose stop -t 5 worker >/dev/null
    rm -f "$DATA_DIR/pipeline_shutdown_request.json"
    compose up -d worker >/dev/null
    sleep 3
    compose ps worker
fi

if (( WEB_RESTART )); then
    echo "--- restarting web (stop+start) ---"
    compose stop -t 5 web >/dev/null
    compose up -d web >/dev/null
    sleep 3
    compose ps web
fi

if (( PIPELINE_KICK )); then
    echo ""
    echo "✓ pipeline-class files copied. The current pipeline subprocess is"
    echo "  still running OLD code. Options to activate the new code:"
    echo "    a) wait — next scheduled pipeline run (≤10 min interval) picks it up"
    echo "    b) finish the current run, scheduler starts new one within 10 min"
    echo "    c) force immediate restart:"
    echo "         docker compose -f \"$COMPOSE_FILE\" exec worker pkill -TERM -f 'telecrime run'"
fi

echo ""
echo "✓ hot-deploy complete."
