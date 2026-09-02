"""Shared channel management helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, not_, or_
from sqlalchemy.orm import Query, Session

from telecrime.models import TelegramChannel

# Stealer-log related keywords (channels must match at least one)
STEALER_KEYWORDS = [
    "log",
    "cloud",
    "combo",
    "leak",
    "dump",
    "ulp",
    "stealer",
    "redline",
    "raccoon",
    "vidar",
    "lumma",
    "private",
]

# Channels matching these keywords are excluded
EXCLUDE_KEYWORDS = [
    "_bot",
    "bot_",
    "support",
    "admin",
    "review",
    "soc",
    "dfir",
    "security",
    "fortnite",
    "brawl",
    "gpt",
    "course",
    "skiff",
]


def build_subscription_query(
    session: Session,
    *,
    stealer_only: bool = True,
    filter_pattern: str | None = None,
) -> Query[TelegramChannel]:
    """Build the shared channel subscription candidate query."""
    query: Query[TelegramChannel] = session.query(TelegramChannel).filter(
        TelegramChannel.is_subscribed.is_(False),
        TelegramChannel.is_active.is_(True),
        TelegramChannel.is_accessible.is_(True),
        or_(
            TelegramChannel.username.isnot(None),
            TelegramChannel.invite_link.isnot(None),
        ),
    )

    # Backoff for transient join failures (flood wait, network errors): a
    # candidate that failed a join stays is_accessible=True and was re-selected
    # on every poll — repeated attempts against the same channel risk a
    # Telegram ban. Skip candidates checked within the last 10 minutes.
    _check_cutoff = datetime.now(UTC) - timedelta(minutes=10)
    query = query.filter(
        or_(
            TelegramChannel.last_checked.is_(None),
            TelegramChannel.last_checked < _check_cutoff,
        )
    )

    if stealer_only:
        include_conditions = []
        for keyword in STEALER_KEYWORDS:
            include_conditions.append(
                func.coalesce(TelegramChannel.username, "").ilike(f"%{keyword}%")
            )
            include_conditions.append(
                func.coalesce(TelegramChannel.title, "").ilike(f"%{keyword}%")
            )

        exclude_conditions = []
        for keyword in EXCLUDE_KEYWORDS:
            exclude_conditions.append(
                func.coalesce(TelegramChannel.username, "").ilike(f"%{keyword}%")
            )
            exclude_conditions.append(
                func.coalesce(TelegramChannel.title, "").ilike(f"%{keyword}%")
            )

        query = query.filter(
            or_(*include_conditions),
            not_(or_(*exclude_conditions)),
        )

    if filter_pattern:
        query = query.filter(TelegramChannel.username.ilike(f"%{filter_pattern}%"))

    return query.order_by(TelegramChannel.id)


def mark_channel_checked(channel: TelegramChannel, entity: object | None) -> None:
    """Apply a successful Telegram channel check result."""
    channel.is_active = True
    channel.is_accessible = True
    channel.title = getattr(entity, "title", channel.title)
    channel.member_count = getattr(entity, "participants_count", None)
    channel.last_checked = datetime.now(UTC)
    channel.check_error = None


def mark_channel_check_failed(channel: TelegramChannel, error_message: str) -> None:
    """Apply a failed Telegram channel check result."""
    channel.last_checked = datetime.now(UTC)

    if "No user has" in error_message or "Cannot find" in error_message:
        channel.is_active = False
        channel.check_error = "Channel not found / deleted"
    elif error_message == "Entity not found":
        # get_entity() returned None — channel deleted, banned, or never existed.
        # Mark inaccessible so it is not retried as a join candidate.
        channel.is_accessible = False
        channel.check_error = "Entity not found"
    elif "private" in error_message.lower():
        channel.is_accessible = False
        channel.check_error = "Private channel"
    else:
        channel.check_error = error_message[:200]


def mark_channel_join_result(channel: TelegramChannel, success: bool) -> str:
    """Apply a join attempt result and return a status label."""
    channel.last_checked = datetime.now(UTC)
    if success:
        channel.is_subscribed = True
        channel.check_error = None
        return "joined"

    channel.is_accessible = False
    channel.check_error = "Join failed"
    return "failed"


_PERMANENT_JOIN_ERRORS = (
    # Username resolved to a user account, not a channel
    "cannot cast inputpeeruser",
    # Username deleted or never existed
    "nobody is using this username",
    "username is unacceptable",
    # Channel/group deleted or banned
    "channel/supergroup not found",
    "chat not found",
    "the channel specified is private",
)


def mark_channel_join_failed(channel: TelegramChannel, error_message: str) -> str:
    """Apply a failed join attempt and return a status label."""
    channel.last_checked = datetime.now(UTC)

    if "already" in error_message.lower():
        channel.is_subscribed = True
        channel.check_error = None
        return "already"

    err_lower = error_message.lower()
    # Permanent failures: stop retrying by marking inaccessible
    if any(pat in err_lower for pat in _PERMANENT_JOIN_ERRORS):
        channel.is_accessible = False
    elif "private" in err_lower or "invite" in err_lower:
        channel.is_accessible = False

    channel.check_error = error_message[:200]
    return "failed"
