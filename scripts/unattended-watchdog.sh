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
# Drive-wedge detection deliberately needs MORE evidence than a hung pipeline:
# a wedged drive looks identical to a hang, but restarting into a wedge churns
# downloads, so a transient D-state writer must never be enough.
#
# Install:
#   crontab -e
#   */10 * * * * /path/to/TeleCrime/scripts/unattended-watchdog.sh >> /path/to/TeleCrime/data/watchdog.log 2>&1

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_DIR/docker-compose.yml"
DATA_DIR="${TELECRIME_DATA_DIR:-$REPO_DIR/data}"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

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

cd "$REPO_DIR"
DATA_DIR="${TELECRIME_DATA_DIR:-$(dotenv_value TELECRIME_DATA_DIR)}"
DATA_DIR="${DATA_DIR:-$REPO_DIR/data}"
mkdir -p "$DATA_DIR"

LOG="$DATA_DIR/watchdog.log"
PROGRESS="$DATA_DIR/pipeline_progress.json"
SNAP=/tmp/telecrime-watchdog-snap.txt   # last observed progress signature
WEDGE_PROC_DIR="${TELECRIME_PROC_DIR:-/proc}"  # overridable for hermetic tests
# A transient D-state `dmcrypt_write*`/`jbd2/*` thread under heavy write load
# is normal; only the SAME thread staying in D-state across this many
# consecutive checks with no DB query activity counts as a wedged drive.
WEDGE_MIN_CHECKS=3
# Heartbeat freshness must match the scheduler's configured stale threshold
# (TELECRIME_PIPELINE_STALE_SECONDS) rather than a hardcoded 600.
STALE_HEARTBEAT_SEC="$(dotenv_value TELECRIME_PIPELINE_STALE_SECONDS)"
STALE_HEARTBEAT_SEC="${STALE_HEARTBEAT_SEC:-1200}"
# A non-numeric value would abort the `[ -gt ]` comparison below (and with
# set -e absent, silently skip the heal), so keep a sane integer.
case "$STALE_HEARTBEAT_SEC" in
  ''|*[!0-9]*) STALE_HEARTBEAT_SEC=1200 ;;
esac
NO_DB_ACTIVITY_SEC=300       # pipeline must have an active query this often
HEAL_LOCK=/tmp/telecrime-heal.lock

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >> "$LOG"; }

# Avoid concurrent heal attempts (cron can overlap on slow DB)
exec 9>"$HEAL_LOCK"
if ! flock -n 9; then
  exit 0
fi

# --- 1. Pipeline process running? ---
PIPELINE_PID=0
if [[ -f "$DATA_DIR/pipeline.pid" ]]; then
  PIPELINE_PID=$(<"$DATA_DIR/pipeline.pid")
fi
# A truncated/garbage pid file must read as "no pid": an empty or non-numeric
# value would otherwise be probed with `kill -0` and treated as a crashed
# pipeline, force-recreating the worker.
case "$PIPELINE_PID" in
  ''|*[!0-9]*) PIPELINE_PID=0 ;;
esac
PIPELINE_ALIVE=0
if [ "$PIPELINE_PID" != "0" ]; then
  WORKER_CONTAINER=$(compose ps -q worker 2>/dev/null)
  if [[ -n "$WORKER_CONTAINER" ]] && docker exec "$WORKER_CONTAINER" sh -c "kill -0 $PIPELINE_PID 2>/dev/null" 2>/dev/null; then
    PIPELINE_ALIVE=1
  fi
fi

# --- 2. Fresh heartbeat? ---
HEARTBEAT_AGE=9999
if [ -f "$PROGRESS" ]; then
  HEARTBEAT_AGE=$(timeout 15 python3 -c "
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
case "$HEARTBEAT_AGE" in
  ''|*[!0-9]*) HEARTBEAT_AGE=9999 ;;
esac

# --- 3. Progress signature (counters + current archive + download) --------
# The heartbeat can stay fresh while the main loop is deadlocked, so we also
# track whether the *work* is moving. Signature = creds|dups|archive_index|
# current_archive|dl_pct. Two consecutive identical signatures with no DB
# activity => hung.
# Note: during a download (stage=acquire) creds/dups/archive_index stay
# frozen while dl_pct advances; including dl_pct avoids false kills of a
# healthy download. A stalled download (dl_pct frozen) is still caught.
SIG=$(timeout 15 python3 -c "
import json, sys
try:
    d = json.load(open('$PROGRESS'))
    arch = (d.get('current_archive') or '')[:60].replace('\n', ' ').replace('\r', ' ')
    print(f\"{d.get('credentials',0)}|{d.get('duplicates',0)}|{d.get('archive_index',0)}|{arch}|{int(d.get('dl_pct') or 0)}\")
except Exception:
    print('')
" 2>/dev/null || echo "")

# MUST be initialized before the conditional below: `set -u` would otherwise
# abort the script with "NOW_TS: unbound variable" whenever the snapshot or
# progress file was missing (e.g. after every reboot — /tmp is tmpfs), i.e.
# exactly before the snapshot write that gets it unstuck. Watchdog stays dead
# forever without this.
NOW_TS=$(date +%s)
FROZEN=0
if [ -n "$SIG" ] && [ -f "$SNAP" ]; then
  # SIG can contain spaces (archive names like "ULP Combo (1).zip"), so the
  # snapshot is stored as two LINES (SIG then timestamp) — a single-line
  # "SIG TS" pair would break read -r on whitespace and silently disable
  # frozen detection whenever a spaced filename was current.
  PREV_SIG=$(sed -n '1p' "$SNAP")
  PREV_TS=$(sed -n '2p' "$SNAP")
  PREV_TS=${PREV_TS:-0}
  # Arithmetic with a non-numeric value under `set -u` aborts the whole
  # watchdog ("unbound variable"), including drive recovery below.
  case "$PREV_TS" in
    ''|*[!0-9]*) PREV_TS=0 ;;
  esac
  # The timestamp must measure how long the SIGNATURE has been unchanged, not
  # how long since the previous invocation. Refreshing it on every run made
  # the 540s age gate unreachable whenever the 5-min monitor loop ran between
  # the 10-min cron checks (each run reset the clock), so a live-but-deadlocked
  # pipeline was never healed. Keep the first-seen timestamp while SIG matches;
  # reset it after a frozen decision so a heal cannot re-fire immediately.
  if [ "$PREV_SIG" = "$SIG" ]; then
    if [ $((NOW_TS - PREV_TS)) -ge 540 ]; then
      FROZEN=1
      PREV_TS=$NOW_TS
    fi
    printf '%s\n%s\n' "$SIG" "$PREV_TS" > "$SNAP"
  else
    printf '%s\n%s\n' "$SIG" "$NOW_TS" > "$SNAP"
  fi
else
  printf '%s\n%s\n' "$SIG" "$NOW_TS" > "$SNAP"
fi

# Pipeline stage — the extract and parse phases are legitimate long-running
# states: 7z/unrar on 50k+ file archives can take 1-2 h, and parsing a
# multi-GB / 500k-line ULP credential file can take 25-40+ min of pure-Python
# regex with no downloads, no DB queries and a frozen signature. Both are
# self-protected (extraction by its proportional timeout; parsing proceeds in
# batches), so treating them as "hung" kills healthy long work — and an
# aborted parse leaves the group EXTRACTED with its credentials never parsed
# (finalize just cleans it) → permanent data loss.
# The exemption is NOT blind for extract: a healthy extraction always has
# current_archive set (the group being worked). An extract that stays frozen
# with an EMPTY current_archive never selected a group — it is wedged before
# any work (e.g. a Telegram session loop in password-candidate extraction)
# and must be healed. (The old "no 7z subprocess" variant was discarded: it
# false-positives on long 7z-free windows like record_outputs hashing.)
STAGE=$(timeout 15 python3 -c "
import json, sys
try:
    print(json.load(open('$PROGRESS')).get('current_stage') or '')
except Exception:
    print('')
" 2>/dev/null || echo "")

CUR_ARCHIVE=$(timeout 15 python3 -c "
import json, sys
try:
    print(json.load(open('$PROGRESS')).get('current_archive') or '')
except Exception:
    print('')
" 2>/dev/null || echo "")

# Active download? A download is network I/O (no DB query) and keeps the
# creds/dups/archive counters frozen while dl_pct advances — a frozen
# signature during an active download is normal, not a hang.
DL_ACTIVE=0
DL_OK=$(timeout 10 python3 -c "
import json, sys
try:
    d = json.load(open('$PROGRESS'))
    if d.get('dl_active') and (d.get('dl_speed') or 0) > 0:
        print(1)
    else:
        print(0)
except Exception:
    print(0)
" 2>/dev/null || echo 0)
if [ "$DL_OK" = "1" ]; then
  DL_ACTIVE=1
fi

# --- 4. Active DB query (pipeline doing real work)? ---
# Pipeline-scoped: the pipeline subprocess tags its connections with
# application_name='telecrime-pipeline' (PGAPPNAME in scheduler.py), so web
# polls/autovacuum cannot mask a genuinely hung pipeline. Fallback to the old
# any-query behaviour while NO pipeline-tagged connection exists (older
# already-running subprocesses, or a pipeline that has not connected yet).
DB_ACTIVE=0
DB_ACTIVITY_AGE=9999
DB_Q=$(timeout 20 docker compose -f "$COMPOSE_FILE" exec -T db psql -U telecrime -t -A -c "
  SELECT COALESCE(EXTRACT(EPOCH FROM (now() - max(query_start)))::int, 9999)
  FROM pg_stat_activity
  WHERE datname='telecrime' AND state='active'
    AND pid <> pg_backend_pid()
    AND query NOT LIKE '%pg_stat_activity%'
    AND (
      application_name = 'telecrime-pipeline'
      OR NOT EXISTS (
        SELECT 1 FROM pg_stat_activity p
        WHERE p.datname='telecrime'
          AND p.application_name = 'telecrime-pipeline'
      )
    )
" 2>/dev/null | tr -d ' ')
# Only a plain integer below the threshold counts as activity. Anything else
# (psql error text on stdout, partial output) must not be coerced by `[ -lt ]`
# — that used to yield DB_ACTIVE=0 and could heal a healthy pipeline.
case "$DB_Q" in
  ''|*[!0-9]*) ;;
  9999) ;;
  *)
    if [ "$DB_Q" -lt "$NO_DB_ACTIVITY_SEC" ]; then
      DB_ACTIVE=1
    fi
    ;;
esac

# --- Internal-volume disk pressure ------------------------------------------
# The `intts` tablespace (both dedup indexes) and pg_wal live on the internal
# root filesystem; when it fills, PostgreSQL writes fail. The external data
# volume is checked by monitor-pipeline.sh. Warn well before full so an
# operator can relocate indexes. Thresholds are env-overridable for tests.
INTERNAL_DISK_WARN_GB="${TELECRIME_INTERNAL_DISK_WARN_GB:-15}"
INTERNAL_TS_PATH="${TELECRIME_PGTS_PATH:-/home/user/recovery/pgts}"
_seen_fs=""
for _disk_path in "$INTERNAL_TS_PATH" /; do
  [ -e "$_disk_path" ] || continue
  _src=$(df -P "$_disk_path" 2>/dev/null | awk 'NR==2{print $1}')
  case "${_src:-}" in ''|*[!A-Za-z0-9/._-]*) continue ;; esac
  case " $_seen_fs " in *" $_src "*) continue ;; esac
  _seen_fs="$_seen_fs $_src"
  _free_gb=$(df -BG "$_disk_path" 2>/dev/null | awk 'NR==2{gsub("G","",$4); print $4}')
  case "${_free_gb:-}" in ''|*[!0-9]*) continue ;; esac
  if [ "$_free_gb" -lt "$INTERNAL_DISK_WARN_GB" ]; then
    log "WARNING: low disk on $_disk_path — ${_free_gb} GB free (intts/pg_wal filesystem)"
  fi
done

log "check: pipeline_pid=$PIPELINE_PID alive=$PIPELINE_ALIVE heartbeat_age=${HEARTBEAT_AGE}s db_active=$DB_ACTIVE dl_active=$DL_ACTIVE frozen=$FROZEN sig=$SIG"

# --- Drive-wedge detection (BEFORE the heal logic) ---
# A hung drive (dm-crypt/jbd2 in persistent D-state) freezes postgres and the
# pipeline's progress writes — the pipeline then LOOKS hung by every signal
# below. Killing/restarting into the wedge churns (resets downloads, fresh run
# into degraded I/O) and cannot help. Transient D-state is normal under heavy
# write load, so a wedge requires the SAME thread in D-state across
# WEDGE_MIN_CHECKS consecutive checks AND no DB query progress. The snapshot
# records "pid count comm" per line; anything not matching (e.g. the old
# pid-only snapshot format) restarts that thread's count at 1.
DRIVE_WEDGED=0
WEDGE_SNAP=/tmp/telecrime-wedge-pids.txt
CURRENT_WEDGED=""
WEDGE_EVIDENCE=""
if [ -d "$WEDGE_PROC_DIR" ]; then
  for _comm in "$WEDGE_PROC_DIR"/[0-9]*/comm; do
    [ -r "$_comm" ] || continue
    _name=$(cat "$_comm" 2>/dev/null) || continue
    case "$_name" in
      dmcrypt_write*|jbd2/*)
        _pid=$(basename "$(dirname "$_comm")")
        if [ -r "$WEDGE_PROC_DIR/$_pid/stat" ] && grep -qE '^[0-9]+ \([^)]*\) D' "$WEDGE_PROC_DIR/$_pid/stat" 2>/dev/null; then
          _count=1
          if [ -f "$WEDGE_SNAP" ]; then
            _prev_count=$(awk -v p="$_pid" -v n="$_name" '$1 == p && $3 == n {print $2; exit}' "$WEDGE_SNAP")
            case "$_prev_count" in
              ''|*[!0-9]*) _prev_count=0 ;;
            esac
            _count=$((_prev_count + 1))
          fi
          if [ -z "$CURRENT_WEDGED" ]; then
            CURRENT_WEDGED="$_pid $_count $_name"
          else
            CURRENT_WEDGED="$CURRENT_WEDGED
$_pid $_count $_name"
          fi
          if [ "$_count" -ge "$WEDGE_MIN_CHECKS" ] && [ "$DB_ACTIVE" = "0" ] && [ -z "$WEDGE_EVIDENCE" ]; then
            DRIVE_WEDGED=1
            WEDGE_EVIDENCE="pid=$_pid comm=$_name count=$_count"
          fi
        fi
        ;;
    esac
  done
fi
printf '%s\n' "$CURRENT_WEDGED" > "$WEDGE_SNAP"
if [ "$DRIVE_WEDGED" = "1" ]; then
  log "CRITICAL: data drive write appears hung ($WEDGE_EVIDENCE in D-state across $WEDGE_MIN_CHECKS consecutive checks, db_active=$DB_ACTIVE) — pausing pipeline heals"
fi

# --- Heal logic ---
NEED_HEAL=0
REASON=""

# During a wedge, a healthy pipeline looks hung (cannot write progress, no DB
# activity — postgres itself is blocked). Stand down until the drive recovers.
if [ "$DRIVE_WEDGED" = "1" ]; then
  log "heal skipped: data drive wedged"
fi
if [ "$DRIVE_WEDGED" = "0" ]; then

# NOTE: a MISSING pid file (PIPELINE_PID=0) is NOT a crash: the pipeline
# removes it on every clean exit, so it is briefly absent during any normal
# stop/restart cycle (manual deploys, scheduler gaps between runs). Healing
# there force-recreates the container mid-restart and silently reverts any
# hot-deployed files. A genuinely crashed subprocess leaves the pid file
# behind, which the PIPELINE_ALIVE=0 branch below catches.
if [ "$PIPELINE_ALIVE" = "0" ] && [ "$PIPELINE_PID" != "0" ]; then
  # Pipeline process is gone but left its pid file → crashed mid-run.
  NEED_HEAL=1
  REASON="pipeline process dead (pid=$PIPELINE_PID)"
elif [ "$HEARTBEAT_AGE" -gt "$STALE_HEARTBEAT_SEC" ] && [ "$DB_ACTIVE" = "0" ] && [ "$PIPELINE_ALIVE" = "0" ]; then
  # Heartbeat stale AND no DB work AND the process is gone (PIPELINE_PID may
  # be 0 when the pid file is missing, e.g. a pipeline started from another
  # container). Requiring PIPELINE_ALIVE=0 prevents a stale progress file
  # left by a crashed run from killing a freshly started healthy pipeline;
  # an alive-but-hung process is caught by the two-consecutive-check FROZEN
  # branch below instead.
  NEED_HEAL=1
  REASON="hung pipeline (heartbeat ${HEARTBEAT_AGE}s old, process not alive, no DB activity)"
elif [ "$FROZEN" = "1" ] && [ "$DB_ACTIVE" = "0" ] && [ "$DL_ACTIVE" = "0" ]; then
  # Fresh heartbeat but the counters/archive/download have not moved between
  # two consecutive checks AND there is no DB query running AND no active
  # download → the main loop is deadlocked while the heartbeat thread ticks.
  # EXCEPT during the extract/parse phases (long subprocess/regex work, see
  # STAGE above) — those are self-protected and must not be killed.
  case "$STAGE" in
    extract) [ -n "$CUR_ARCHIVE" ] || { NEED_HEAL=1; REASON="hung pipeline (extract frozen on no group — current_archive empty)"; } ;;
    parse) : ;;
    *) if [ "$FROZEN" = "1" ]; then
         NEED_HEAL=1
         REASON="hung pipeline (progress frozen for two checks, no DB or download activity)"
       fi ;;
  esac
fi
fi

if [ "$NEED_HEAL" = "1" ]; then
  log "HEAL: $REASON — restarting worker container"
  # Force-kill the worker container; docker compose restarts it fresh.
  # The pipeline is crash-safe (per-archive commits) and resumes via the
  # scheduler watchdog + startup recovery.
  CID=$(compose ps -q worker 2>/dev/null)
  if [ -n "$CID" ]; then
    docker kill "$CID" 2>/dev/null
    sleep 5
    docker rm -f "$CID" 2>/dev/null
  fi
  # --no-deps: `compose up -d worker` would wait on `web: service_healthy`
  # with no timeout — an unhealthy web would park this heal (and hold the
  # flock) indefinitely. db is already running at this point.
  # NB: invoke `docker compose` directly — `timeout` is an external command and
  # cannot run the `compose` shell function, so `timeout 120 compose ...`
  # silently no-op'd (stderr suppressed), leaving HEAL unable to restart the
  # worker exactly when it was needed.
  timeout 120 docker compose -f "$COMPOSE_FILE" up -d --no-deps worker 2>/dev/null
  sleep 5
  if ! compose ps -q worker 2>/dev/null | grep -q .; then
    # First attempt failed (e.g. container create stalled on a wedged volume
    # and the timeout killed compose) — retry once.
    log "HEAL: worker did not start after first attempt — retrying"
    timeout 120 docker compose -f "$COMPOSE_FILE" up -d --no-deps worker 2>/dev/null
    sleep 5
  fi
  if compose ps -q worker 2>/dev/null | grep -q .; then
    log "HEAL done: worker restarted"
  else
    log "HEAL FAILED: worker container not running after retry"
  fi
fi

# --- 4. Containers down? ---
# "Up (unhealthy)" is NOT healthy: the container runs but its healthcheck
# fails (e.g. web's 5s urllib probe timing out) — and since worker depends_on
# web:service_healthy, an unhealthy web blocks every worker heal. Restart
# unhealthy containers too — but with a 30-min cooldown per container: a
# slow-starting/crash-recovering container (or a wedged data drive) must not
# be force-recreated on every 5-10 min cycle, which interrupts recovery and
# severs the running pipeline's DB connections.
# NOTE: DRIVE_WEDGED was computed ABOVE the pipeline-heal logic and must NOT
# be reset here — the container-heal guards below rely on it.
if [ "$DRIVE_WEDGED" = "1" ]; then
  log "CRITICAL: data drive write appears hung ($WEDGE_EVIDENCE) — pausing container heals"
fi

for svc in db web worker; do
  STATUS=$(compose ps "$svc" 2>/dev/null)
  if ! echo "$STATUS" | grep -q "Up" || echo "$STATUS" | grep -q "unhealthy"; then
    LAST=/tmp/telecrime-heal-$svc.last
    NOW_TS=$(date +%s)
    PREV_TS=0
    [ -f "$LAST" ] && PREV_TS=$(cat "$LAST")
    # Non-numeric marker (partial/truncated write) would abort the whole
    # script in the arithmetic below under `set -u`.
    case "$PREV_TS" in
      ''|*[!0-9]*) PREV_TS=0 ;;
    esac
    if [ $((NOW_TS - PREV_TS)) -lt 1800 ]; then
      log "HEAL skipped: '$svc' still unhealthy, last heal $((NOW_TS - PREV_TS))s ago (cooldown 30m)"
      continue
    fi
    if [ "$DRIVE_WEDGED" = "1" ]; then
      log "CRITICAL: '$svc' unhealthy AND data drive write is hung (D-state) — not recreating; drive needs attention"
      continue
    fi
    echo "$NOW_TS" > "$LAST"
    log "HEAL: container '$svc' not healthy (restarting)"
    compose up -d --force-recreate "$svc" 2>/dev/null
    sleep 5
  else
    # Healthy — clear the cooldown marker so a genuinely-new failure isn't
    # held back for 30 min by a previous heal.
    rm -f "/tmp/telecrime-heal-$svc.last"
  fi
done

# --- 5. External drive still mounted? ---
# The DB volume and the data dir live on the LUKS drive at /mnt/telecrime.
# If it drops (USB timeout during heavy I/O) or was never mounted after boot,
# try to bring it back. The mapper may be the crypttab name (telecrime-data)
# or the udisks2-opened luks-UUID name — try any open mapper.
if ! mountpoint -q /mnt/telecrime; then
  log "CRITICAL: /mnt/telecrime not mounted — attempting drive recovery"
  OPEN_MAPPER=$(ls /dev/mapper/ 2>/dev/null | grep -v 'control\|TeleCrime' | head -1)
  RECOVERED=0
  if [ -n "$OPEN_MAPPER" ]; then
    if timeout 30 sudo -n mount "/dev/mapper/$OPEN_MAPPER" /mnt/telecrime 2>/dev/null; then
      RECOVERED=1
    fi
  fi
  if [ "$RECOVERED" = "0" ]; then
    # Volume not open — try the crypttab path (needs the keyfile keyslot).
    timeout 30 sudo -n systemctl start systemd-cryptsetup@telecrime-data.service 2>/dev/null
    sleep 3
    if timeout 30 sudo -n mount /dev/mapper/telecrime-data /mnt/telecrime 2>/dev/null; then
      RECOVERED=1
    fi
  fi
  sleep 2
  if mountpoint -q /mnt/telecrime; then
    log "drive recovered: /mnt/telecrime is mounted again"
    # DB and worker both depend on the bind-mounted volume; restart them so
    # they see the real data instead of the empty root-dir fallback.
    compose up -d db worker 2>/dev/null
  else
    log "drive recovery failed — data dir and DB volume unavailable"
  fi
else
  # Healthy-mount sanity: the DB volume must point at real PG data.
  # postgres_data is root-owned (0700) — plain -e fails with EACCES, so use
  # sudo; fall back to checking the directory entry is visible.
  if ! timeout 15 sudo -n test -e /mnt/telecrime/postgres_data/PG_VERSION 2>/dev/null \
     && [ ! -e /mnt/telecrime/postgres_data ]; then
    log "CRITICAL: /mnt/telecrime mounted but postgres_data missing"
  fi
fi

log "watchdog done"
