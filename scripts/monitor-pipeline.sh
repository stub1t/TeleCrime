#!/usr/bin/env bash
# Continuous pipeline monitor.
#
# Runs the watchdog health/heal checks every ${TELECRIME_MONITOR_INTERVAL}s
# (default 300) and adds RAM/disk pressure warnings. Keeps running until the
# host reboots or the process is killed.
#
# Start:
#   nohup ./scripts/monitor-pipeline.sh > /mnt/telecrime/data/monitor.log 2>&1 &
#
# The cron watchdog (unattended-watchdog.sh) runs in parallel every 10 minutes
# as a fallback; a flock serializes concurrent heal actions.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INTERVAL="${TELECRIME_MONITOR_INTERVAL:-300}"
# Keep the monitor's own log off the data drive: a wedged/failing data volume
# must not block the monitor's logging (2026-09-21: the watchdog hung writing
# its log to the wedged drive and monitoring stopped). The repo normally lives
# on the internal SSD. Overridable via TELECRIME_MONITOR_LOG.
LOG="${TELECRIME_MONITOR_LOG:-$REPO_DIR/data/monitor.log}"

mkdir -p "$(dirname "$LOG")"
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >> "$LOG"; }

log "monitor started (interval ${INTERVAL}s)"

while true; do
  # Bounded watchdog run: a heal (docker kill + compose up) must not stretch
  # the 5-min cadence into a 30-min blind spot.
  timeout 120 "$SCRIPT_DIR/unattended-watchdog.sh" > /dev/null 2>&1 || \
    log "WARNING: watchdog run exceeded 120s or failed"

  # Memory pressure: < 1 GB available means the host is close to swap thrash.
  read -r mem_avail swap_used < <(free -m | awk '/^Mem:/{a=$7} /^Swap:/{s=$3} END{print a, s}')
  if [ -n "${mem_avail:-}" ] && [ "$mem_avail" -lt 1024 ]; then
    log "WARNING: low memory — ${mem_avail} MB available, ${swap_used:-0} MB swap used"
  fi

  # Disk pressure: extraction/parse need headroom; finalize reclaims space.
  # Only measure the LUKS drive while it is actually mounted: df on the
  # unmounted mountpoint would silently report the ROOT filesystem and log a
  # misleading "low disk on /mnt/telecrime" warning.
  if mountpoint -q /mnt/telecrime; then
    free_gb=$(df -BG /mnt/telecrime 2>/dev/null | awk 'NR==2{gsub("G","",$4); print $4}')
    if [ -n "${free_gb:-}" ] && [ "$free_gb" -lt 20 ]; then
      log "WARNING: low disk on /mnt/telecrime — ${free_gb} GB free"
    fi
  fi

  # Internal root hosts the `intts` tablespace (both dedup indexes) and
  # pg_wal; when it fills, PostgreSQL writes fail. Warn at a higher threshold
  # than the bulk-data volume. Dedupe by filesystem source so / and the
  # tablespace path do not warn twice.
  _seen_fs=""
  for _disk_path in "${TELECRIME_PGTS_PATH:-/home/user/recovery/pgts}" /; do
    [ -e "$_disk_path" ] || continue
    _src=$(df -P "$_disk_path" 2>/dev/null | awk 'NR==2{print $1}')
    case "${_src:-}" in ''|*[!A-Za-z0-9/._-]*) continue ;; esac
    case " $_seen_fs " in *" $_src "*) continue ;; esac
    _seen_fs="$_seen_fs $_src"
    _free_gb=$(df -BG "$_disk_path" 2>/dev/null | awk 'NR==2{gsub("G","",$4); print $4}')
    if [ -n "${_free_gb:-}" ] && [ "$_free_gb" -lt "${TELECRIME_INTERNAL_DISK_WARN_GB:-15}" ]; then
      log "WARNING: low disk on $_disk_path — ${_free_gb} GB free (intts/pg_wal filesystem)"
    fi
  done

  sleep "$INTERVAL"
done