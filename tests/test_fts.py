"""Tests for FTS helpers."""

import pytest

from telecrime.database import get_engine, get_session, init_db
from telecrime.fts import ensure_fts, fts_available, fts_count, fts_search
from telecrime.models import ParsedCredential


def _add_credential(
    session, *, domain: str, username: str, password: str, stealer: str
) -> ParsedCredential:
    cred = ParsedCredential(
        url=f"https://{domain}/login",
        domain=domain,
        username=username,
        password=password,
        stealer_type=stealer,
        credential_hash=ParsedCredential.compute_hash(domain, username, password),
    )
    session.add(cred)
    session.flush()
    return cred


@pytest.mark.skip(reason="exercises removed SQLite FTS5 path; production is PG-only")
def test_fts_search_applies_structured_filters(tmp_path):
    """FTS filtering happens in SQL before limiting results."""
    engine = get_engine(f"sqlite:///{tmp_path / 'fts.db'}")
    init_db(engine)

    with get_session(engine) as session:
        expected = _add_credential(
            session,
            domain="accounts.google.com",
            username="alice",
            password="secret",
            stealer="redline",
        )
        _add_credential(
            session,
            domain="accounts.google.com",
            username="bob",
            password="secret",
            stealer="vidar",
        )

    assert ensure_fts(engine, rebuild=True) is True
    assert fts_available(engine) is True

    with get_session(engine) as session:
        ids = fts_search(
            session,
            "google",
            columns=["domain"],
            limit=5,
            filters={"stealer": "redline"},
        )
        total = fts_count(
            session,
            "google",
            columns=["domain"],
            filters={"stealer": "redline"},
        )

    assert ids == [expected.id]
    assert total == 1
