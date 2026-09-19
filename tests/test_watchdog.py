"""Behavioral/static tests for ``scripts/unattended-watchdog.sh``.

The production script talks to docker/sudo and keeps state under /tmp and
/mnt/telecrime. Each behavioral test runs a copy of the script in a throwaway
repo with:

  * the hardcoded /tmp state paths rewritten into the temp DATA_DIR,
  * the drive-mount path rewritten to a path that never exists on the host,
  * stub docker/sudo/mountpoint/sleep binaries first on PATH.

Nothing touches the host's containers, mounts or watchdog state, so the tests
are hermetic and fast.
"""

import json
import os
import re
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "unattended-watchdog.sh"


def _write_stub(directory: Path, name: str, body: str) -> None:
    path = directory / name
    path.write_text(f"#!/bin/sh\n{body}")
    path.chmod(0o755)


@pytest.fixture()
def watchdog(tmp_path):
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    data = repo / "data"
    data.mkdir()

    script = repo / "scripts" / "unattended-watchdog.sh"
    source = SCRIPT.read_text()
    # Keep every piece of script state inside the temp dir.
    source = source.replace("/tmp/telecrime-", "$DATA_DIR/telecrime-")
    source = source.replace("/mnt/telecrime", "${DATA_DIR}/mnt-telecrime")
    script.write_text(source)
    (repo / "docker-compose.yml").write_text("services: {}\n")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "docker-calls.log"
    _write_stub(
        bin_dir,
        "docker",
        f"""printf '%s\\n' "$*" >> "{calls}"
case " $* " in
  *" ps -q "*) echo "stub-worker-cid"; exit 0 ;;
  *" ps "*) echo "stubservice Up (healthy)"; exit 0 ;;
  *" exec "*) exit 1 ;;
  *) exit 0 ;;
esac
""",
    )
    _write_stub(bin_dir, "sudo", "exit 1\n")
    _write_stub(bin_dir, "mountpoint", "exit 0\n")
    _write_stub(bin_dir, "sleep", "exit 0\n")

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["TELECRIME_DATA_DIR"] = str(data)
    # Keep the drive-wedge /proc scan hermetic: with the default it would read
    # the host's real /proc and could see (or miss) D-state kernel threads.
    env["TELECRIME_PROC_DIR"] = str(data / "proc")
    env.pop("TELECRIME_PIPELINE_STALE_SECONDS", None)
    return SimpleNamespace(repo=repo, data=data, script=script, env=env, calls=calls)


def _run(watchdog, extra_env=None):
    env = dict(watchdog.env)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", str(watchdog.script)],
        cwd=watchdog.repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _progress_payload(**overrides):
    payload = {
        "credentials": 5,
        "duplicates": 1,
        "archive_index": 2,
        "current_archive": "a.zip",
        "dl_pct": 0,
        "dl_active": False,
        "current_stage": "finalize",
        "last_progress_at": datetime.now(UTC).isoformat(),
    }
    payload.update(overrides)
    return payload


def test_watchdog_survives_empty_progress_and_garbage_inputs(watchdog):
    """An empty progress file, a garbage pid file and a non-numeric stale
    threshold must not abort the script (the unbound-NOW_TS regression)."""
    (watchdog.data / "pipeline_progress.json").write_text("")
    (watchdog.data / "pipeline.pid").write_text("garbage-not-a-pid\n")

    result = _run(watchdog, {"TELECRIME_PIPELINE_STALE_SECONDS": "not-a-number"})

    assert result.returncode == 0, result.stderr
    assert "unbound variable" not in result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "watchdog done" in log
    assert "heartbeat_age=9999s" in log
    assert "pipeline_pid=0 alive=0" in log


def test_watchdog_warns_on_low_internal_disk(watchdog):
    """A nearly-full internal volume (intts/pg_wal) must warn on every check.

    The internal root filesystem was never monitored (only /mnt/telecrime);
    when it fills, PostgreSQL writes fail.
    """
    stub_dir = watchdog.data / "diskbin"
    stub_dir.mkdir()
    _write_stub(
        stub_dir,
        "df",
        'case "$*" in\n'
        '  *-P*) printf "Filesystem 1024-blocks Used Available Capacity Mounted on\\n"\n'
        '        printf "/dev/mapper/root 100000 90000 10000 90%% /\\n" ;;\n'
        '  *) printf "Filesystem 1G-blocks Used Available Use%% Mounted on\\n"\n'
        '     printf "/dev/mapper/root 100 90 8 93%% /\\n" ;;\n'
        "esac\n",
    )
    env_path = f"{stub_dir}:{watchdog.env['PATH']}"

    result = _run(
        watchdog,
        {
            "PATH": env_path,
            "TELECRIME_INTERNAL_DISK_WARN_GB": "15",
            "TELECRIME_PGTS_PATH": str(watchdog.data / "no-such-pgts"),
        },
    )

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "WARNING: low disk on / — 8 GB free (intts/pg_wal filesystem)" in log


def test_watchdog_detects_frozen_progress_from_stale_snapshot(watchdog):
    """Two identical signatures >=540s apart with no DB/download activity are
    reported frozen and healed."""
    (watchdog.data / "pipeline_progress.json").write_text(
        json.dumps(_progress_payload())
    )
    signature = "5|1|2|a.zip|0"
    snapshot = watchdog.data / "telecrime-watchdog-snap.txt"
    snapshot.write_text(f"{signature}\n{int(time.time()) - 1000}\n")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "frozen=1" in log
    assert "HEAL: hung pipeline (progress frozen" in log
    lines = snapshot.read_text().splitlines()
    assert lines[0] == signature
    assert lines[1].isdigit()
    assert int(lines[1]) >= int(time.time()) - 60


def test_watchdog_sanitizes_non_numeric_snapshot_timestamp(watchdog):
    """A truncated/garbage snapshot timestamp must be treated as 0 instead of
    aborting the arithmetic (which would skip all healing)."""
    (watchdog.data / "pipeline_progress.json").write_text(
        json.dumps(_progress_payload())
    )
    snapshot = watchdog.data / "telecrime-watchdog-snap.txt"
    snapshot.write_text("5|1|2|a.zip|0\nnot-a-timestamp\n")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    assert "unbound variable" not in result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "watchdog done" in log
    assert snapshot.read_text().splitlines()[1].isdigit()


def test_watchdog_numeric_sanitization_and_now_ts_order():
    """Static guard: every numeric input consumed by arithmetic has a
    non-numeric sanitization case, and NOW_TS is set before first use."""
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr

    source = SCRIPT.read_text()
    for name in (
        "STALE_HEARTBEAT_SEC",
        "PIPELINE_PID",
        "HEARTBEAT_AGE",
        "PREV_TS",
        "DB_Q",
    ):
        assert re.search(rf'case "\${name}" in', source), (
            f"{name} is used in arithmetic but has no numeric sanitization"
        )

    lines = source.splitlines()
    now_ts_index = next(
        i for i, line in enumerate(lines) if line.startswith("NOW_TS=$(date +%s)")
    )
    snapshot_branch_index = next(
        i
        for i, line in enumerate(lines)
        if 'if [ -n "$SIG" ] && [ -f "$SNAP" ]' in line
    )
    assert now_ts_index < snapshot_branch_index, (
        "NOW_TS must be initialized before the snapshot branch: with `set -u` "
        "a missing snapshot/progress file would abort the whole watchdog"
    )


def test_watchdog_frozen_signature_ages_across_monitor_ticks(watchdog, tmp_path):
    """A signature frozen for >=9 minutes heals even when checks are 5 min apart.

    Regression: every invocation rewrote the snapshot timestamp, so the 540s
    age gate could never be reached under the 5-min monitor loop (each check
    reset the clock) and a live-but-deadlocked pipeline was never healed.
    """
    bin_dir = Path(watchdog.env["PATH"].split(":")[0])
    _write_stub(
        bin_dir,
        "date",
        'case " $* " in\n'
        '  *" +%s "*) printf \'%s\\n\' "${TELECRIME_FAKE_NOW:-0}"; exit 0 ;;\n'
        "esac\n"
        'exec /bin/date "$@"\n',
    )

    (watchdog.data / "pipeline_progress.json").write_text(
        json.dumps(_progress_payload())
    )
    signature = "5|1|2|a.zip|0"
    snapshot = watchdog.data / "telecrime-watchdog-snap.txt"
    t0 = 1_700_000_000
    snapshot.write_text(f"{signature}\n{t0 - 300}\n")

    first = _run(watchdog, {"TELECRIME_FAKE_NOW": str(t0)})
    assert first.returncode == 0, first.stderr
    assert "frozen=0" in (watchdog.data / "watchdog.log").read_text()
    assert snapshot.read_text().splitlines()[1] == str(t0 - 300), (
        "an unchanged signature must keep its first-seen timestamp so the age "
        "keeps accumulating between checks"
    )

    # The drive-wedge check scans the host /proc for persistent D-state
    # threads; on a host that has one, a non-empty previous wedge snapshot
    # would make the second run skip every heal. Clear it so this test only
    # exercises the frozen-signature path.
    (watchdog.data / "telecrime-wedge-pids.txt").write_text("")

    second = _run(watchdog, {"TELECRIME_FAKE_NOW": str(t0 + 300)})
    assert second.returncode == 0, second.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "frozen=1" in log
    assert "HEAL: hung pipeline (progress frozen" in log
    assert snapshot.read_text().splitlines()[1] == str(t0 + 300), (
        "the snapshot timestamp resets after a frozen decision so a heal "
        "cannot re-fire with the same age"
    )


def _write_progress(watchdog, **overrides):
    (watchdog.data / "pipeline_progress.json").write_text(
        json.dumps(_progress_payload(**overrides))
    )


def _write_stale_snapshot(watchdog, signature: str, age_seconds: int = 1000):
    (watchdog.data / "telecrime-watchdog-snap.txt").write_text(
        f"{signature}\n{int(time.time()) - age_seconds}\n"
    )
    # A host thread stuck in D-state would otherwise make the script pause
    # every heal; this test only exercises the pipeline-heal branches.
    (watchdog.data / "telecrime-wedge-pids.txt").write_text("")


def _write_dstate_thread(watchdog, pid: int, name: str) -> None:
    proc = watchdog.data / "proc" / str(pid)
    proc.mkdir(parents=True, exist_ok=True)
    (proc / "comm").write_text(f"{name}\n")
    (proc / "stat").write_text(f"{pid} ({name}) D 1 0 0 0 -1 0 0 0 0 0 0 0 0 0\n")


def _remove_dstate_thread(watchdog, pid: int) -> None:
    shutil.rmtree(watchdog.data / "proc" / str(pid), ignore_errors=True)


def _write_wedge_snapshot(watchdog, *entries: tuple[int, int, str]) -> None:
    lines = [f"{pid} {count} {name}" for pid, count, name in entries]
    (watchdog.data / "telecrime-wedge-pids.txt").write_text("\n".join(lines) + "\n")


def test_watchdog_heals_dead_pipeline_with_pid_file(watchdog):
    """A pid file whose process is gone means the run crashed mid-flight."""
    _write_progress(watchdog)
    (watchdog.data / "pipeline.pid").write_text("4242\n")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "HEAL: pipeline process dead (pid=4242)" in log
    assert "HEAL done: worker restarted" in log
    calls = watchdog.calls.read_text()
    # The old container must be force-killed and removed before restart.
    assert "kill stub-worker-cid" in calls
    assert "rm -f stub-worker-cid" in calls
    # The restart must actually reach docker. `timeout` is an external command
    # and cannot run the `compose` shell function, so `timeout 120 compose ...`
    # silently no-op'd; the heal now invokes `docker compose` directly.
    assert "up -d --no-deps worker" in calls


def test_watchdog_heals_stale_heartbeat_without_pid(watchdog):
    """Heartbeat older than the stale threshold, no process and no DB activity
    heals even when the pid file is absent (pipeline started elsewhere)."""
    from datetime import timedelta

    stale = (datetime.now(UTC) - timedelta(seconds=5000)).isoformat()
    _write_progress(watchdog, last_progress_at=stale)
    _write_stale_snapshot(watchdog, "5|1|2|a.zip|0")

    result = _run(watchdog, {"TELECRIME_PIPELINE_STALE_SECONDS": "1200"})

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "HEAL: hung pipeline (heartbeat" in log


def test_watchdog_frozen_parse_stage_is_not_healed(watchdog):
    """A multi-minute parse is self-protected: killing it discards the file's
    unparsed credentials (finalize would clean the group)."""
    _write_progress(watchdog, current_stage="parse")
    _write_stale_snapshot(watchdog, "5|1|2|a.zip|0")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "frozen=1" in log
    assert "HEAL" not in log


def test_watchdog_frozen_extract_with_archive_is_not_healed(watchdog):
    """A long extraction always names the group it is working; do not kill it."""
    _write_progress(watchdog, current_stage="extract", current_archive="a.zip")
    _write_stale_snapshot(watchdog, "5|1|2|a.zip|0")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "frozen=1" in log
    assert "HEAL" not in log


def test_watchdog_frozen_extract_without_archive_is_healed(watchdog):
    """Extract frozen with no group selected is wedged before starting work."""
    _write_progress(watchdog, current_stage="extract", current_archive="")
    _write_stale_snapshot(watchdog, "5|1|2||0")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "HEAL: hung pipeline (extract frozen on no group" in log


def test_watchdog_active_download_blocks_frozen_heal(watchdog):
    """Counters freeze while a download runs (no DB query); a live download
    with moving speed must not be healed."""
    _write_progress(watchdog, current_stage="acquire", dl_active=True, dl_speed=1.5)
    _write_stale_snapshot(watchdog, "5|1|2|a.zip|0")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "dl_active=1" in log
    assert "HEAL" not in log


def test_watchdog_drive_wedge_needs_three_consecutive_checks(watchdog):
    """Two consecutive D-state sightings of the same thread are not enough.

    Regression: the old detection declared a wedge on the second sighting
    (any shared pid), so transient D-state under write load paused all heals.
    """
    _write_progress(watchdog)
    _write_dstate_thread(watchdog, 4242, "dmcrypt_write/0")
    _write_wedge_snapshot(watchdog, (4242, 1, "dmcrypt_write/0"))

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "CRITICAL: data drive write appears hung" not in log
    # The consecutive-sighting counter still advances.
    assert "4242 2 dmcrypt_write/0" in (
        watchdog.data / "telecrime-wedge-pids.txt"
    ).read_text()


def test_watchdog_drive_wedge_after_three_checks_logs_signature(watchdog):
    """Three consecutive D-state checks of the same thread with no DB query
    activity is a wedge, and the log names the observed signature."""
    _write_progress(watchdog)
    _write_dstate_thread(watchdog, 4242, "dmcrypt_write/0")
    _write_wedge_snapshot(watchdog, (4242, 2, "dmcrypt_write/0"))

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "CRITICAL: data drive write appears hung" in log
    assert "pid=4242" in log
    assert "comm=dmcrypt_write/0" in log
    assert "count=3" in log
    assert "across 3 consecutive checks" in log
    assert "heal skipped: data drive wedged" in log


def test_watchdog_drive_wedge_requires_no_db_activity(watchdog):
    """A persisted D-state thread with an active pipeline DB query is load, not
    a wedge: heal pausing must not trigger."""
    bin_dir = Path(watchdog.env["PATH"].split(":")[0])
    _write_stub(
        bin_dir,
        "docker",
        f"""printf '%s\\n' "$*" >> "{watchdog.calls}"
case " $* " in
  *" ps -q "*) echo "stub-worker-cid"; exit 0 ;;
  *" ps "*) echo "stubservice Up (healthy)"; exit 0 ;;
  *" exec "*) echo "5"; exit 0 ;;
  *) exit 0 ;;
esac
""",
    )
    _write_progress(watchdog)
    _write_dstate_thread(watchdog, 4242, "dmcrypt_write/0")
    _write_wedge_snapshot(watchdog, (4242, 2, "dmcrypt_write/0"))

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "db_active=1" in log
    assert "CRITICAL: data drive write appears hung" not in log


def test_watchdog_drive_wedge_requires_the_same_thread(watchdog):
    """A different D-state thread each check is not sustained evidence."""
    _write_progress(watchdog)
    _write_dstate_thread(watchdog, 9999, "jbd2/sda1-8")
    _write_wedge_snapshot(watchdog, (4242, 2, "dmcrypt_write/0"))

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "CRITICAL: data drive write appears hung" not in log
    snapshot = (watchdog.data / "telecrime-wedge-pids.txt").read_text()
    assert "9999 1 jbd2/sda1-8" in snapshot
    assert "4242" not in snapshot


def test_watchdog_drive_wedge_ignores_legacy_pid_only_snapshot(watchdog):
    """The pre-upgrade pid-only snapshot must not be mistaken for counts.

    ``123 456`` (two pids) previously parsed as pid=123 with count=456, which
    would declare a wedge instantly after deploy.
    """
    _write_progress(watchdog)
    _write_dstate_thread(watchdog, 123, "dmcrypt_write/0")
    (watchdog.data / "telecrime-wedge-pids.txt").write_text("123 456\n")

    result = _run(watchdog)

    assert result.returncode == 0, result.stderr
    log = (watchdog.data / "watchdog.log").read_text()
    assert "CRITICAL: data drive write appears hung" not in log
    assert "123 1 dmcrypt_write/0" in (
        watchdog.data / "telecrime-wedge-pids.txt"
    ).read_text()
