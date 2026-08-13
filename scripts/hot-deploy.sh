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
DATA_DIR="${TELECRIME_DATA_DIR:-/mnt/telecrime/data}"

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

declare -a PIPELINE_TARGETS=(telecrime-worker-1 telecrime-web-1)

for rel in "$@"; do
    if [[ ! -f "$REPO/$rel" ]]; then
        echo "✗ missing: $REPO/$rel" >&2
        exit 1
    fi
    cls=$(classify "$rel")
    case "$cls" in
        scheduler)
            echo "→ $rel [scheduler-class]: will hot-cp + restart worker"
            docker cp "$REPO/$rel" "telecrime-worker-1:/app/$rel"
            # Also push to web — it imports some scheduler helpers for the
            # dashboard's status panel (read-only helpers).
            docker cp "$REPO/$rel" "telecrime-web-1:/app/$rel"
            WORKER_RESTART=1
            ;;
        web)
            echo "→ $rel [web-class]: will hot-cp + restart web"
            docker cp "$REPO/$rel" "telecrime-web-1:/app/$rel"
            WEB_RESTART=1
            ;;
        pipeline)
            echo "→ $rel [pipeline-class]: hot-cp only (effective on next pipeline subprocess)"
            for c in "${PIPELINE_TARGETS[@]}"; do
                docker cp "$REPO/$rel" "$c:/app/$rel"
            done
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
    docker stop -t 5 telecrime-worker-1 >/dev/null
    rm -f "$DATA_DIR/pipeline_shutdown_request.json"
    docker start telecrime-worker-1 >/dev/null
    sleep 3
    docker ps --format '  {{.Names}} {{.Status}}' | grep telecrime-worker-1
fi

if (( WEB_RESTART )); then
    echo "--- restarting web (stop+start) ---"
    docker stop -t 5 telecrime-web-1 >/dev/null
    docker start telecrime-web-1 >/dev/null
    sleep 3
    docker ps --format '  {{.Names}} {{.Status}}' | grep telecrime-web-1
fi

if (( PIPELINE_KICK )); then
    echo ""
    echo "✓ pipeline-class files copied. The current pipeline subprocess is"
    echo "  still running OLD code. Options to activate the new code:"
    echo "    a) wait — next scheduled pipeline run (≤10 min interval) picks it up"
    echo "    b) finish the current run, scheduler starts new one within 10 min"
    echo "    c) force immediate restart:"
    echo "         docker top telecrime-worker-1 axo pid,cmd | awk '/telecrime run/{print \$1}' | xargs sudo kill -TERM"
fi

echo ""
echo "✓ hot-deploy complete."
