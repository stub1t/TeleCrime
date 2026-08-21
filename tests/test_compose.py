"""Tests for the portable Docker Compose contract."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
_COMPOSE_OVERRIDES = {
    "TELECRIME_DATA_DIR",
    "TELECRIME_POSTGRES_VOLUME",
    "TELECRIME_POSTGRES_PORT",
    "TELECRIME_WEB_PORT",
}


def _compose_config(**overrides: str) -> dict:
    if shutil.which("docker") is None:
        pytest.skip("Docker is not installed")

    env = os.environ.copy()
    for key in _COMPOSE_OVERRIDES:
        env.pop(key, None)
    env.update(overrides)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            str(COMPOSE_FILE),
            "config",
            "--format",
            "json",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Docker Compose is unavailable: {result.stderr.strip()}")
    return json.loads(result.stdout)


def test_compose_defaults_are_machine_independent():
    config = _compose_config()

    assert config["services"]["web"]["volumes"][0]["source"] == str(REPO_ROOT / "data")
    assert config["services"]["worker"]["volumes"][0]["source"] == str(REPO_ROOT / "data")
    assert config["services"]["web"]["environment"]["TELECRIME_DATA_DIR"] == "/app/data"
    assert config["volumes"]["postgres_data"]["name"] == "telecrime_postgres_data"
    assert config["volumes"]["postgres_data"].get("external") is not True


def test_compose_custom_storage_and_ports_are_applied():
    config = _compose_config(
        TELECRIME_DATA_DIR="/srv/telecrime/data",
        TELECRIME_POSTGRES_VOLUME="existing_pg",
        TELECRIME_POSTGRES_PORT="15432",
        TELECRIME_WEB_PORT="18000",
    )

    assert config["services"]["web"]["volumes"][0]["source"] == "/srv/telecrime/data"
    assert config["services"]["worker"]["volumes"][0]["source"] == "/srv/telecrime/data"
    assert config["volumes"]["postgres_data"]["name"] == "existing_pg"
    assert config["services"]["db"]["ports"][0]["published"] == "15432"
    assert config["services"]["web"]["ports"][0]["published"] == "18000"
