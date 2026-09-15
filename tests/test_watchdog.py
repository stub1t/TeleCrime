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
