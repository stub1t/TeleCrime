"""Pytest configuration and fixtures.

Test fixtures use an isolated in-memory SQLite database for speed. Production
code paths are PostgreSQL-only; SQLite is allowed by `get_engine` solely so
these fixtures can construct ephemeral test engines. See test_database.py for
PG-backed coverage of the database module proper.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from telecrime.config import Config
from telecrime.models import Base


@pytest.fixture(autouse=True)
def _isolate_shutdown_request_file(tmp_path, monkeypatch):
    """Point the shutdown-request file at a per-test temp path.

    Without this, scheduler tests read the developer's real
    ``data/pipeline_shutdown_request.json``; ``_pipeline_lock_is_held()`` then
    probes a MagicMock ``data_dir`` and ``Path`` coercion creates
    ``MagicMock/mock.data_dir/<id>/`` junk dirs in the repo root.
    """
    monkeypatch.setenv(
        "TELECRIME_SHUTDOWN_REQUEST_FILE",
        str(tmp_path / "pipeline_shutdown_request.json"),
    )


@pytest.fixture
def in_memory_engine():
    """In-memory SQLite engine with schema created (test fixture only)."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(in_memory_engine) -> Session:
    """Create a database session for testing."""
    SessionLocal = sessionmaker(bind=in_memory_engine, expire_on_commit=False)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def test_config(tmp_path) -> Config:
    """Test configuration with temporary directories."""
    config = Config(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        data_dir=tmp_path / "data",
        downloads_dir=tmp_path / "downloads",
        extracted_dir=tmp_path / "extracted",
    )
    config.ensure_directories()
    return config
