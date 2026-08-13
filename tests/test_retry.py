"""Tests for retry utilities."""

from telecrime.utils.retry import backoff_delay


def test_backoff_delay_increases():
    """Backoff increases with attempt."""
    base = 1.0
    d0 = backoff_delay(0, base, 10.0, jitter=0.0)
    d1 = backoff_delay(1, base, 10.0, jitter=0.0)
    d2 = backoff_delay(2, base, 10.0, jitter=0.0)
    assert d0 == 1.0
    assert d1 == 2.0
    assert d2 == 4.0


def test_backoff_delay_caps():
    """Backoff is capped at max_seconds."""
    d = backoff_delay(10, 1.0, 5.0, jitter=0.0)
    assert d == 5.0
