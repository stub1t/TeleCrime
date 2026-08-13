"""Tests for WatchlistItem model."""

from telecrime.database import get_engine, get_session, init_db
from telecrime.models.watchlist import WatchlistItem


def _engine(tmp_path):
    engine = get_engine(f"sqlite:///{tmp_path / 'test.db'}")
    init_db(engine)
    return engine


def test_watchlist_item_defaults(tmp_path):
    """WatchlistItem creates with correct defaults."""
    engine = _engine(tmp_path)
    with get_session(engine) as session:
        item = WatchlistItem(label="test", query="google.com")
        session.add(item)
        session.commit()
        session.refresh(item)

    assert item.id is not None
    assert item.match_type == "any"
    assert item.enabled is True
    assert item.new_count == 0
    assert item.last_known_count == 0
    assert item.last_checked_at is None
    assert item.last_viewed_at is None
    assert item.last_alerted_at is None
    assert item.last_alerted_count == 0


def test_watchlist_item_crud(tmp_path):
    """WatchlistItem can be created, read, updated, and deleted."""
    engine = _engine(tmp_path)

    with get_session(engine) as session:
        item = WatchlistItem(label="bank", query="bank.com", match_type="domain")
        session.add(item)
        session.commit()
        item_id = item.id

    with get_session(engine) as session:
        item = session.get(WatchlistItem, item_id)
        assert item.query == "bank.com"
        item.new_count = 5
        session.commit()

    with get_session(engine) as session:
        item = session.get(WatchlistItem, item_id)
        assert item.new_count == 5
        session.delete(item)
        session.commit()

    with get_session(engine) as session:
        assert session.get(WatchlistItem, item_id) is None


def test_watchlist_item_enabled_filter(tmp_path):
    """Filtering by enabled=True returns only active items."""
    engine = _engine(tmp_path)
    with get_session(engine) as session:
        session.add(WatchlistItem(label="active", query="active.com", enabled=True))
        session.add(WatchlistItem(label="disabled", query="disabled.com", enabled=False))
        session.commit()

    with get_session(engine) as session:
        enabled = session.query(WatchlistItem).filter(WatchlistItem.enabled == True).all()
        assert len(enabled) == 1
        assert enabled[0].label == "active"
