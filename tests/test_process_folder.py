"""Tests for process_folder.py helpers and standalone import behavior."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from process_folder import ProcessingStats, find_archives, process_archive, process_folder
from telecrime.database import get_session_factory
from telecrime.models import ParsedCredential


@pytest.mark.parametrize(
    ("relative_path", "expected_name"),
    [("top.zip", "top.zip"), ("subdir/nested/logs.7z", "logs.7z")],
)
def test_find_archives_recursively_finds_supported_files(tmp_path, relative_path, expected_name):
    """Archive discovery handles both top-level and nested files."""
    archive = tmp_path / relative_path
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.touch()
    (tmp_path / "readme.txt").touch()

    result = find_archives(tmp_path)

    assert expected_name in {f.name for f in result}
    assert "readme.txt" not in {f.name for f in result}


def test_find_archives_empty_folder(tmp_path):
    """Empty folder returns empty list."""
    assert find_archives(tmp_path) == []


def test_find_archives_split_parts(tmp_path):
    """Split archive parts are detected recursively."""
    sub = tmp_path / "batch"
    sub.mkdir()
    (sub / "archive.part01.rar").touch()
    (sub / "archive.part02.rar").touch()

    result = find_archives(tmp_path)
    names = {f.name for f in result}
    assert "archive.part01.rar" in names
    assert "archive.part02.rar" in names


@pytest.mark.asyncio
async def test_process_folder_uses_configured_data_dir(tmp_path, test_config, monkeypatch):
    """Standalone processing keeps markers and temp files under data_dir."""
    import process_folder as process_folder_module

    source = tmp_path / "archives"
    source.mkdir()
    test_config.data_dir = tmp_path / "configured-data"
    session = MagicMock()
    removed: list[Path] = []

    monkeypatch.setattr(process_folder_module, "load_config", lambda _path: test_config)
    monkeypatch.setattr(process_folder_module, "get_engine", lambda _url: MagicMock())
    monkeypatch.setattr(process_folder_module, "init_db", lambda _engine: None)
    monkeypatch.setattr(
        process_folder_module,
        "get_session_factory",
        lambda _engine: lambda: session,
    )
    monkeypatch.setattr(process_folder_module, "find_archives", lambda _folder: [])
    monkeypatch.setattr(
        process_folder_module.shutil,
        "rmtree",
        lambda path, ignore_errors=False: removed.append(Path(path)),
    )

    stats = await process_folder(source, config_path=tmp_path / "config.toml")

    assert stats.archives_found == 0
    assert (test_config.data_dir / f".processed_{source.name}.txt").exists()
    assert removed == [test_config.data_dir / f".extract_temp_{source.name}"]


@pytest.mark.asyncio
async def test_process_archive_uses_domain_hash_dedup(tmp_path, pg_engine, monkeypatch):
    """Standalone imports deduplicate the same way as the main pipeline."""
    session_factory = get_session_factory(pg_engine)

    archive = tmp_path / "sample.zip"
    archive.touch()
    output_dir = tmp_path / "extract"
    output_dir.mkdir()
    extracted_file = output_dir / "Passwords.txt"
    extracted_file.write_text("dummy")

    creds = [
        SimpleNamespace(
            url="https://a.example/login",
            domain="example.com",
            username="alice",
            password="secret",
            email_domain=None,
            application="Chrome",
            profile="Default",
        ),
        SimpleNamespace(
            url="https://b.example/login",
            domain="example.com",
            username="alice",
            password="secret",
            email_domain=None,
            application="Chrome",
            profile="Default",
        ),
    ]

    class FakeExtractor:
        async def find_matching_files(self, archive_path, extensions, password=None):
            return [extracted_file]

        async def extract(self, archive_path, target_dir, target_extensions, password=None):
            return SimpleNamespace(
                needs_password=False,
                success=True,
                error_message=None,
                extracted_files=[extracted_file],
            )

    monkeypatch.setattr("process_folder.iter_credentials_file", lambda path: iter(creds))
    monkeypatch.setattr("process_folder.detect_stealer_type", lambda filenames: "redline")

    stats = ProcessingStats()
    with session_factory() as session:
        csv_rows = await process_archive(
            archive,
            FakeExtractor(),
            output_dir,
            session,
            stats,
            delete_after=False,
        )

        stored = session.query(ParsedCredential).all()

    assert len(stored) == 1
    assert stored[0].credential_hash == ParsedCredential.compute_hash(
        "example.com", "alice", "secret"
    )
    assert stats.credentials_new == 1
    assert stats.credentials_duplicate == 1
    assert len(csv_rows) == 1
