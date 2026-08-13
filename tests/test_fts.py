"""Tests for FTS helpers."""


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


def test_fts_search_applies_structured_filters(pg_session):
    """FTS filtering happens in SQL before limiting results."""
    expected = _add_credential(
        pg_session,
        domain="accounts.google.com",
        username="alice",
        password="secret",
        stealer="redline",
    )
    _add_credential(
        pg_session,
        domain="accounts.google.com",
        username="bob",
        password="secret",
        stealer="vidar",
    )
    pg_session.commit()

    assert ensure_fts(pg_session.bind) is True
    assert fts_available(pg_session.bind) is True

    ids = fts_search(
        pg_session,
        "google",
        columns=["domain"],
        limit=5,
        filters={"stealer": "redline"},
    )
    total = fts_count(
        pg_session,
        "google",
        columns=["domain"],
        filters={"stealer": "redline"},
    )

    assert ids == [expected.id]
    assert total == 1
