"""Tests for process_folder.py helpers and standalone import behavior."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from process_folder import ProcessingStats, find_archives, process_archive
from telecrime.database import get_engine, get_session_factory, init_db
from telecrime.models import ParsedCredential


def test_find_archives_top_level(tmp_path):
    """Archives in top-level folder are found."""
    (tmp_path / "a.zip").touch()
    (tmp_path / "b.rar").touch()
    (tmp_path / "readme.txt").touch()

    result = find_archives(tmp_path)
    names = {f.name for f in result}
    assert "a.zip" in names
    assert "b.rar" in names
    assert "readme.txt" not in names


def test_find_archives_recursive(tmp_path):
    """Archives in subdirectories are found recursively."""
    sub = tmp_path / "subdir" / "nested"
    sub.mkdir(parents=True)
    (sub / "logs.7z").touch()
    (tmp_path / "top.zip").touch()

    result = find_archives(tmp_path)
    names = {f.name for f in result}
    assert "logs.7z" in names
    assert "top.zip" in names


def test_find_archives_skips_hidden_extract_temp(tmp_path):
    """Archives inside .extract_temp (temp dir) are still returned by find_archives
    but that dir is created after finding, so it won't exist during a normal run."""
    sub = tmp_path / "channel1"
    sub.mkdir()
    (sub / "pack.zip").touch()

    result = find_archives(tmp_path)
    names = {f.name for f in result}
    assert "pack.zip" in names


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
async def test_process_archive_uses_domain_hash_dedup(tmp_path, monkeypatch):
    """Standalone imports deduplicate the same way as the main pipeline."""
    db_path = tmp_path / "test.db"
    engine = get_engine(f"sqlite:///{db_path}")
    init_db(engine)
    session_factory = get_session_factory(engine)

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
