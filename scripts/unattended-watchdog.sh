#!/usr/bin/env bash
# Telecrime unattended watchdog.
#
# Runs every 10 minutes via cron. Detects and heals:
#   1. Pipeline subprocess hung (frozen progress counters + no DB activity)
#   2. Dead worker containers (not running/restarting)
#   3. Pipeline process crashed but scheduler still thinks it's running
#
# A fresh heartbeat is NOT proof of progress: the heartbeat thread keeps
# ticking even when the pipeline main loop is deadlocked. The reliable signal
# is (a) the progress counters/current_archive actually moving between runs
# and (b) real DB query activity. If both are absent for two consecutive
# checks, the pipeline is hung and gets restarted.
#
# Install:
#   crontab -e
#   */10 * * * * /home/user/TeleCrime/scripts/unattended-watchdog.sh >> /home/user/TeleCrime/data/watchdog.log 2>&1

set -uo pipefail
cd "$(dirname "$0")/.."

LOG=/mnt/telecrime/data/watchdog.log
PROGRESS=/mnt/telecrime/data/pipeline_progress.json
SNAP=/tmp/telecrime-watchdog-snap.txt   # last observed progress signature
STALE_HEARTBEAT_SEC=600      # pipeline must touch progress file this often
NO_DB_ACTIVITY_SEC=300       # pipeline must have an active query this often
HEAL_LOCK=/tmp/telecrime-heal.lock

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >> "$LOG"; }

# Avoid concurrent heal attempts (cron can overlap on slow DB)
exec 9>"$HEAL_LOCK"
if ! flock -n 9; then
  exit 0
fi

# --- 1. Pipeline process running? ---
PIPELINE_PID=$(cat /mnt/telecrime/data/pipeline.pid 2>/dev/null || echo 0)
PIPELINE_ALIVE=0
if [ "$PIPELINE_PID" != "0" ]; then
  if docker exec "$(docker ps -q --filter name=worker)" sh -c "kill -0 $PIPELINE_PID 2>/dev/null" 2>/dev/null; then
    PIPELINE_ALIVE=1
  fi
fi

# --- 2. Fresh heartbeat? ---
HEARTBEAT_AGE=9999
if [ -f "$PROGRESS" ]; then
  HEARTBEAT_AGE=$(python3 -c "
import json, datetime, sys
try:
    d = json.load(open('$PROGRESS'))
    last = datetime.datetime.fromisoformat(d['last_progress_at'])
    age = (datetime.datetime.now(datetime.timezone.utc) - last).total_seconds()
    print(int(age))
except Exception:
    print(9999)
" 2>/dev/null || echo 9999)
fi

# --- 3. Progress signature (counters + current archive) -----------------
# The heartbeat can stay fresh while the main loop is deadlocked, so we also
# track whether the *work* is moving. Signature = creds|dups|archive_index|
# current_archive. Two consecutive identical signatures with no DB activity
# => hung.
SIG=$(python3 -c "
import json, sys
try:
    d = json.load(open('$PROGRESS'))
    print(f\"{d.get('credentials',0)}|{d.get('duplicates',0)}|{d.get('archive_index',0)}|{(d.get('current_archive') or '')[:60]}\")
except Exception:
    print('')
" 2>/dev/null || echo "")

FROZEN=0
if [ -n "$SIG" ] && [ -f "$SNAP" ]; then
  PREV=$(cat "$SNAP")
  if [ "$PREV" = "$SIG" ]; then
    FROZEN=1
  fi
fi
echo "$SIG" > "$SNAP"

# --- 4. Active DB query (pipeline doing real work)? ---
DB_ACTIVE=0
DB_ACTIVITY_AGE=9999
DB_Q=$(timeout 20 docker compose exec -T db psql -U telecrime -t -A -c "
  SELECT COALESCE(EXTRACT(EPOCH FROM (now() - max(query_start)))::int, 9999)
  FROM pg_stat_activity
  WHERE datname='telecrime' AND state='active'
    AND query NOT LIKE '%pg_stat_activity%'
" 2>/dev/null | tr -d ' ')
if [ -n "$DB_Q" ] && [ "$DB_Q" != "9999" ] && [ "$DB_Q" -lt "$NO_DB_ACTIVITY_SEC" ]; then
  DB_ACTIVE=1
fi

log "check: pipeline_pid=$PIPELINE_PID alive=$PIPELINE_ALIVE heartbeat_age=${HEARTBEAT_AGE}s db_active=$DB_ACTIVE frozen=$FROZEN sig=$SIG"

# --- Heal logic ---
NEED_HEAL=0
REASON=""

if [ "$PIPELINE_ALIVE" = "0" ]; then
  # Pipeline process is gone but the pipeline_run table says running → stale
  NEED_HEAL=1
  REASON="pipeline process dead (pid=$PIPELINE_PID)"
elif [ "$HEARTBEAT_AGE" -gt "$STALE_HEARTBEAT_SEC" ] && [ "$DB_ACTIVE" = "0" ]; then
  # Heartbeat stale AND no DB work → hung (e.g. deadlock in I/O)
  NEED_HEAL=1
  REASON="hung pipeline (heartbeat ${HEARTBEAT_AGE}s old, no DB activity)"
elif [ "$FROZEN" = "1" ] && [ "$DB_ACTIVE" = "0" ]; then
  # Fresh heartbeat but the counters/archive have not moved between two
  # consecutive checks AND there is no DB query running → the main loop is
  # deadlocked while the heartbeat thread keeps ticking.
  NEED_HEAL=1
  REASON="hung pipeline (progress frozen for two checks, no DB activity)"
fi

if [ "$NEED_HEAL" = "1" ]; then
  log "HEAL: $REASON — restarting worker container"
  # Force-kill the worker container; docker compose restarts it fresh.
  # The pipeline is crash-safe (per-archive commits) and resumes via the
  # scheduler watchdog + startup recovery.
  CID=$(docker ps -q --filter name=worker)
  if [ -n "$CID" ]; then
    docker kill "$CID" 2>/dev/null
    sleep 5
    docker rm -f "$CID" 2>/dev/null
  fi
  cd /home/user/TeleCrime && docker compose up -d worker 2>/dev/null
  log "HEAL done: worker restarted"
fi

# --- 4. Containers down? ---
for svc in db web worker; do
  if ! docker compose -f /home/user/TeleCrime/docker-compose.yml ps "$svc" 2>/dev/null | grep -q "Up"; then
    log "HEAL: container '$svc' not up — starting"
    cd /home/user/TeleCrime && docker compose up -d "$svc" 2>/dev/null
  fi
done

log "watchdog done"
