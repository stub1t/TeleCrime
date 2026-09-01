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
LOG="${TELECRIME_MONITOR_LOG:-/mnt/telecrime/data/monitor.log}"

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
  free_gb=$(df -BG /mnt/telecrime 2>/dev/null | awk 'NR==2{gsub("G","",$4); print $4}')
  if [ -n "${free_gb:-}" ] && [ "$free_gb" -lt 20 ]; then
    log "WARNING: low disk on /mnt/telecrime — ${free_gb} GB free"
  fi

  sleep "$INTERVAL"
done