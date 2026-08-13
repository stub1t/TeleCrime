import logging
import re
import time
from dataclasses import dataclass

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from telecrime.models import (
    Conversation,
    Message,
    ParsedCredential,
    PipelineState,
    TelegramChannel,
)

logger = logging.getLogger(__name__)

# Regex patterns for channel discovery
TELEGRAM_USERNAME_PATTERN = re.compile(
    r"@([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])",  # @username format
    re.IGNORECASE,
)

TELEGRAM_LINK_PATTERN = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:\+([a-zA-Z0-9_-]+)|([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9]))",
    re.IGNORECASE,
)

# Common false positives to filter out
EXCLUDED_USERNAMES = {
    "gmail",
    "yahoo",
    "hotmail",
    "outlook",
    "icloud",
    "mail",
    "email",
    "android",
    "iphone",
    "windows",
    "linux",
    "macos",
    "chrome",
    "firefox",
    "safari",
    "edge",
    "opera",
    "brave",
    "facebook",
    "instagram",
    "twitter",
    "tiktok",
    "youtube",
    "twitch",
    "discord",
    "whatsapp",
    "snapchat",
    "reddit",
    "pinterest",
    "amazon",
    "ebay",
    "paypal",
    "netflix",
    "spotify",
    "google",
    "microsoft",
    "apple",
    "meta",
    "admin",
    "support",
    "help",
    "info",
    "contact",
    "news",
    "username",
    "password",
    "login",
    "user",
    "test",
    "example",
}


@dataclass
class DiscoveredChannel:
    """A discovered channel reference."""

    username: str | None = None
    platform_id: int | None = None
    title: str | None = None
    invite_link: str | None = None
    source: str = "unknown"
    discovered_from: str | None = None


@dataclass
class DiscoveryScanResult:
    """Discovered channel set plus incremental scan checkpoints."""

    channels: dict[str, DiscoveredChannel]
    last_message_id: int
    last_credential_id: int


DISCOVERY_MESSAGE_WATERMARK = "channel_discovery.last_message_id"
DISCOVERY_CREDENTIAL_WATERMARK = "channel_discovery.last_credential_id"


def extract_mentions_from_text(text: str) -> list[str]:
    """Extract @username mentions from text.

    Args:
        text: Text to search for mentions

    Returns:
        List of usernames (without @)
    """
    if not text:
        return []

    matches = TELEGRAM_USERNAME_PATTERN.findall(text)
    usernames = []

    for username in matches:
        username_lower = username.lower()
        # Filter out common false positives
        if username_lower not in EXCLUDED_USERNAMES:
            # Filter out email-like patterns
            if not re.search(rf"[a-zA-Z0-9._%+-]+@{re.escape(username)}", text):
                usernames.append(username_lower)

    return list(set(usernames))


def extract_telegram_links(text: str) -> list[tuple[str | None, str | None]]:
    """Extract t.me links from text.

    Args:
        text: Text to search for links

    Returns:
        List of (invite_hash, username) tuples
    """
    if not text:
        return []

    results: list[tuple[str | None, str | None]] = []
    for match in TELEGRAM_LINK_PATTERN.finditer(text):
        invite_hash = match.group(1)  # +hash format
        username = match.group(2)  # /username format

        if invite_hash:
            results.append((f"https://t.me/+{invite_hash}", None))
        elif username and username.lower() not in EXCLUDED_USERNAMES:
            results.append((None, username.lower()))

    return results


def discover_channels_from_db(session: Session) -> DiscoveryScanResult:
    """Discover channels from all database sources.

    Sources:
    1. Conversations (subscribed channels)
    2. Forwarded message sources
    3. @mentions in message text/captions
    4. @mentions in archive filenames
    5. t.me links in text

    Args:
        session: Database session

    Returns:
        Discovered channels plus watermark values for incremental scans.
    """
    channels: dict[str, DiscoveredChannel] = {}
    last_message_id = _get_state_int(session, DISCOVERY_MESSAGE_WATERMARK)
    last_credential_id = _get_state_int(session, DISCOVERY_CREDENTIAL_WATERMARK)
    max_message_id = (
        session.execute(select(func.max(Message.id)).where(Message.id > last_message_id)).scalar()
        or last_message_id
    )
    max_credential_id = (
        session.execute(
            select(func.max(ParsedCredential.id)).where(ParsedCredential.id > last_credential_id)
        ).scalar()
        or last_credential_id
    )

    # 1. From conversations (subscribed channels)
    logger.info("Discovering channels from conversations...")
    conversations = session.execute(
        select(Conversation)
        .where(Conversation.conversation_type == "channel")
        .execution_options(yield_per=500)
    ).scalars()

    for conv in conversations:
        key = conv.username or f"id:{conv.platform_id}"
        channels[key] = DiscoveredChannel(
            username=conv.username,
            platform_id=conv.platform_id,
            title=conv.title,
            source="subscribed",
            discovered_from="conversation",
        )

    logger.info(f"  Found {len(channels)} subscribed channels")

    # 2. From forwarded messages
    logger.info("Discovering channels from forwarded messages...")
    forwarded = session.execute(
        select(
            Message.forwarded_from_id,
            Message.forwarded_from_name,
            func.count(Message.id).label("count"),
        )
        .where(
            Message.id > last_message_id,
            Message.is_forwarded == True,
            Message.forwarded_from_id != None,
        )
        .group_by(Message.forwarded_from_id, Message.forwarded_from_name)
        .execution_options(yield_per=500)
    )

    forward_count = 0
    for fwd_id, fwd_name, count in forwarded:
        if fwd_id:
            key = f"id:{fwd_id}"
            if key not in channels:
                # Try to extract username from name
                username = None
                if fwd_name:
                    mentions = extract_mentions_from_text(fwd_name)
                    if mentions:
                        username = mentions[0]

                channels[key] = DiscoveredChannel(
                    username=username,
                    platform_id=fwd_id,
                    title=fwd_name,
                    source="forwarded",
                    discovered_from=f"forwarded messages ({count} msgs)",
                )
                forward_count += 1

    logger.info(f"  Found {forward_count} channels from forwarded messages")

    # 3. From @mentions in message text/captions
    logger.info("Discovering channels from @mentions in messages...")
    messages = session.execute(
        select(Message.id, Message.text, Message.caption)
        .where(
            Message.id > last_message_id,
            (Message.text != None) | (Message.caption != None),
        )
        .execution_options(yield_per=1000)
    )

    mention_count = 0
    for message_id, text, caption in messages:
        del message_id
        for content in [text, caption]:
            if content:
                for username in extract_mentions_from_text(content):
                    key = f"@{username}"
                    if key not in channels and "id:" not in str(channels.get(key, "")):
                        channels[key] = DiscoveredChannel(
                            username=username,
                            source="mentioned",
                            discovered_from="message text",
                        )
                        mention_count += 1

                # Also extract t.me links
                for invite_link, link_username in extract_telegram_links(content):
                    if invite_link:
                        key = invite_link
                        if key not in channels:
                            channels[key] = DiscoveredChannel(
                                invite_link=invite_link,
                                source="link",
                                discovered_from="message text",
                            )
                            mention_count += 1
                    elif link_username:
                        key = f"@{link_username}"
                        if key not in channels:
                            channels[key] = DiscoveredChannel(
                                username=link_username,
                                source="link",
                                discovered_from="message text",
                            )
                            mention_count += 1

    logger.info(f"  Found {mention_count} channels from mentions/links")

    # 4. From archive filenames (source_archive in credentials)
    logger.info("Discovering channels from archive filenames...")
    archive_count = 0
    for credential_id, archive_name in session.execute(
        select(ParsedCredential.id, ParsedCredential.source_archive)
        .where(ParsedCredential.id > last_credential_id)
        .execution_options(yield_per=1000)
    ):
        del credential_id
        if archive_name:
            for username in extract_mentions_from_text(archive_name):
                key = f"@{username}"
                if key not in channels:
                    channels[key] = DiscoveredChannel(
                        username=username,
                        source="mentioned",
                        discovered_from=f"archive: {archive_name[:50]}",
                    )
                    archive_count += 1

    logger.info(f"  Found {archive_count} channels from archive names")

    logger.info(f"Total unique channels discovered: {len(channels)}")
    return DiscoveryScanResult(
        channels=channels,
        last_message_id=max_message_id,
        last_credential_id=max_credential_id,
    )


def save_discovered_channels(
    session: Session, channels: dict[str, DiscoveredChannel]
) -> tuple[int, int]:
    """Save discovered channels to database.

    Args:
        session: Database session
        channels: Dict of discovered channels

    Returns:
        (new_count, updated_count)
    """
    new_count = 0
    updated_count = 0

    # Only load DB rows that could match the discovered set — avoids a full
    # table scan of all 6000+ channels on every per-archive incremental call.
    discovered_usernames = [
        c.username.lower() for c in channels.values() if c.username
    ]
    discovered_platform_ids = [
        c.platform_id for c in channels.values() if c.platform_id is not None
    ]
    discovered_invite_links = [
        c.invite_link for c in channels.values() if c.invite_link
    ]

    filter_conditions = []
    if discovered_usernames:
        filter_conditions.append(
            func.lower(TelegramChannel.username).in_(discovered_usernames)
        )
    if discovered_platform_ids:
        filter_conditions.append(TelegramChannel.platform_id.in_(discovered_platform_ids))
    if discovered_invite_links:
        filter_conditions.append(TelegramChannel.invite_link.in_(discovered_invite_links))

    if filter_conditions:
        existing_channels = session.execute(
            select(TelegramChannel).where(or_(*filter_conditions))
        ).scalars().all()
    else:
        existing_channels = []

    by_platform_id = {
        channel.platform_id: channel
        for channel in existing_channels
        if channel.platform_id is not None
    }
    by_username = {
        channel.username.lower(): channel for channel in existing_channels if channel.username
    }
    by_invite_link = {
        channel.invite_link: channel for channel in existing_channels if channel.invite_link
    }

    for key, channel in channels.items():
        del key
        existing = None

        if channel.platform_id is not None:
            existing = by_platform_id.get(channel.platform_id)

        if not existing and channel.username:
            existing = by_username.get(channel.username.lower())

        if not existing and channel.invite_link:
            existing = by_invite_link.get(channel.invite_link)

        if existing is not None:
            # Update if we have more info
            if channel.platform_id is not None and getattr(existing, "platform_id") is None:
                setattr(existing, "platform_id", channel.platform_id)
            if channel.username and not getattr(existing, "username"):
                setattr(existing, "username", channel.username)
            if channel.title and not getattr(existing, "title"):
                setattr(existing, "title", channel.title)
            if channel.source == "subscribed":
                setattr(existing, "is_subscribed", True)
            updated_count += 1
        else:
            # Create new
            db_channel = TelegramChannel(
                platform_id=channel.platform_id,
                username=channel.username,
                title=channel.title,
                invite_link=channel.invite_link,
                source=channel.source,
                discovered_from=channel.discovered_from,
                is_subscribed=(channel.source == "subscribed"),
            )
            session.add(db_channel)
            try:
                # Use a savepoint so an IntegrityError only rolls back this one
                # insert rather than the entire session's pending changes.
                with session.begin_nested():
                    session.flush()
            except IntegrityError:
                # Unique constraint on lower(username) or platform_id — already exists.
                updated_count += 1
                continue
            if channel.platform_id is not None:
                by_platform_id[channel.platform_id] = db_channel
            if channel.username:
                by_username[channel.username.lower()] = db_channel
            if channel.invite_link:
                by_invite_link[channel.invite_link] = db_channel
            new_count += 1

    session.commit()
    return new_count, updated_count


def persist_discovery_state(session: Session, scan_result: DiscoveryScanResult) -> None:
    """Persist channel discovery watermarks after a successful scan."""
    _set_state_int(session, DISCOVERY_MESSAGE_WATERMARK, scan_result.last_message_id)
    _set_state_int(session, DISCOVERY_CREDENTIAL_WATERMARK, scan_result.last_credential_id)
    session.commit()


def update_channel_stats(session: Session) -> None:
    """Update channel statistics from credential data."""
    message_counts = {
        platform_id: count
        for platform_id, count in session.execute(
            select(
                Conversation.platform_id,
                func.count(Message.id),
            )
            .join(Message, Message.conversation_id == Conversation.id, isouter=True)
            .where(Conversation.platform_id != None)
            .group_by(Conversation.platform_id)
        ).all()
    }

    credential_counts = {
        platform_id: count
        for platform_id, count in session.execute(
            select(
                Conversation.platform_id,
                func.count(ParsedCredential.id),
            )
            .join(
                ParsedCredential,
                ParsedCredential.source_conversation_id == Conversation.id,
                isouter=True,
            )
            .where(Conversation.platform_id != None)
            .group_by(Conversation.platform_id)
        ).all()
    }

    channels = session.execute(
        select(TelegramChannel).where(TelegramChannel.platform_id != None)
    ).scalars()

    for channel in channels:
        platform_id = getattr(channel, "platform_id")
        setattr(channel, "messages_seen", message_counts.get(platform_id, 0) or 0)
        setattr(channel, "credentials_extracted", credential_counts.get(platform_id, 0) or 0)

    session.commit()


_DORK_DEFAULT_KEYWORDS = [
    "stealer log",
    "credentials passwords",
    "redline lumma",
    "cloud ulp",
]

_INVITE_RE = re.compile(r"t\.me/(?:joinchat/|\+)([\w-]+)", re.IGNORECASE)
_USERNAME_RE = re.compile(r"t\.me/([A-Za-z][A-Za-z0-9_]{3,30}[A-Za-z0-9])", re.IGNORECASE)
_DDG_URL = "https://html.duckduckgo.com/html/"
_DDG_DELAY = 2.0  # seconds between requests


def _ddg_search(keyword: str) -> str:
    """Fetch DuckDuckGo HTML results for a keyword query."""
    import urllib.parse
    import urllib.request

    params = urllib.parse.urlencode({"q": f"site:t.me/joinchat {keyword}", "kl": "us-en"})
    req = urllib.request.Request(
        f"{_DDG_URL}?{params}",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _extract_invite_links(html: str) -> list[str]:
    """Extract t.me invite links from HTML."""
    links = []
    for match in _INVITE_RE.finditer(html):
        links.append(f"https://t.me/+{match.group(1)}")
    return links


def discover_channels_via_dork(
    session: Session,
    keywords: list[str] | None = None,
    delay: float = _DDG_DELAY,
    _fetch_fn=None,  # Injectable for testing
) -> tuple[int, list[str]]:
    """Search DuckDuckGo for private Telegram invite links.

    Queries ``site:t.me/joinchat <keyword>`` for each keyword and extracts
    invite hashes.  Found links are saved to TelegramChannel with source="dork".

    Args:
        session: Database session.
        keywords: Search terms; defaults to _DORK_DEFAULT_KEYWORDS.
        delay: Seconds between requests to avoid rate-limiting.
        _fetch_fn: Optional callable(keyword) -> html_str for testing.

    Returns:
        (new_count, invite_links) — number of newly saved channels and all links found.
    """
    if keywords is None:
        keywords = _DORK_DEFAULT_KEYWORDS

    fetch = _fetch_fn or _ddg_search

    existing_links = {
        row[0]
        for row in session.execute(
            select(TelegramChannel.invite_link).where(TelegramChannel.invite_link != None)
        ).all()
    }

    found_links: list[str] = []
    new_count = 0

    for i, keyword in enumerate(keywords):
        if i > 0:
            time.sleep(delay)
        try:
            html = fetch(keyword)
        except Exception as e:
            logger.warning("Dork request failed for %r: %s", keyword, e)
            continue

        links = _extract_invite_links(html)
        logger.info("Dork query %r found %d invite links", keyword, len(links))

        for link in links:
            found_links.append(link)
            if link not in existing_links:
                channel = TelegramChannel(
                    invite_link=link,
                    source="dork",
                    discovered_from=f"dork:{keyword}",
                )
                session.add(channel)
                existing_links.add(link)
                new_count += 1

    session.commit()
    return new_count, found_links


def _get_state_int(session: Session, key: str) -> int:
    """Read an integer checkpoint from pipeline state."""
    state = session.get(PipelineState, key)
    if state is None or state.value_int is None:
        return 0
    return state.value_int


def _set_state_int(session: Session, key: str, value: int) -> None:
    """Write an integer checkpoint into pipeline state."""
    state = session.get(PipelineState, key)
    if state is None:
        session.add(PipelineState(key=key, value_int=value))
    else:
        state.value_int = value
