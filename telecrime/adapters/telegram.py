"""Telegram adapter using Telethon."""

import asyncio
import logging
import sqlite3
import time
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    InviteHashExpiredError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import (
    Channel,
    Chat,
    Document,
    DocumentAttributeFilename,
    MessageMediaDocument,
    User,
)
from telethon.tl.types import (
    Message as TelegramMessage,
)

from telecrime.adapters.base import (
    BaseAdapter,
    ConversationInfo,
    FileInfo,
    MessageInfo,
)
from telecrime.config import Config
from telecrime.pipeline.progress import patch_progress

logger = logging.getLogger(__name__)


def _configure_session_sqlite(session_path: Path, client=None) -> None:
    """Enable WAL mode and busy_timeout on the Telethon session SQLite file.

    WAL mode (set via a temporary connection and persisted in the file) lets
    multiple readers coexist with a single writer.  busy_timeout makes SQLite
    retry for up to 5 seconds rather than immediately raising
    "database is locked" when two processes collide on a write.

    We also attempt to apply busy_timeout directly to Telethon's internal
    SQLite connection so the client itself retries instead of failing fast.
    """
    try:
        conn = sqlite3.connect(str(session_path), timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.close()
    except Exception:
        logger.debug("Could not configure session SQLite pragmas", exc_info=True)

    if client is not None:
        try:
            internal_conn = client.session._conn
            if internal_conn is not None:
                internal_conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            logger.debug("Could not set busy_timeout on Telethon session connection", exc_info=True)


class TelegramAdapter(BaseAdapter):
    """Telegram adapter using Telethon library."""

    def __init__(self, config: Config):
        """Initialize Telegram adapter.

        Args:
            config: Application configuration with Telegram credentials
        """
        self.config = config
        self.client: TelegramClient | None = None
        self._connect_lock = asyncio.Lock()
        self._reconnect_since: str | None = None

        if not config.telegram.api_id or not config.telegram.api_hash:
            raise ValueError("Telegram API credentials not configured")

    async def connect(self, timeout: int = 30) -> None:
        """Connect to Telegram.

        Args:
            timeout: Maximum seconds to wait for connection (default 30).
                     Prevents indefinite hangs when session needs re-auth.
        """
        session_path = self.config.data_dir / f"{self.config.telegram.session_name}.session"

        self.client = TelegramClient(
            str(session_path),
            self.config.telegram.api_id,
            self.config.telegram.api_hash,
            timeout=timeout,
            connection_retries=5,
            retry_delay=1,
            auto_reconnect=True,
            flood_sleep_threshold=60,
            request_retries=3,
        )

        try:
            if self.config.telegram.phone:
                await asyncio.wait_for(
                    self.client.start(phone=self.config.telegram.phone),
                    timeout=timeout,
                )
            else:
                # No phone configured — connect and rely on existing session file.
                await asyncio.wait_for(self.client.connect(), timeout=timeout)
                if not await self.client.is_user_authorized():
                    raise ConnectionError(
                        "Telegram session is not authorized. "
                        "Set TELECRIME_TELEGRAM_PHONE and run interactively to log in."
                    )
        except TimeoutError:
            self._set_runtime_note(
                f"Telegram connect timed out after {timeout}s",
                kind="telegram_reconnect",
            )
            # CRITICAL: explicitly disconnect the half-connected client so its
            # background auto_reconnect=True loop stops spinning. Without this,
            # the leaked client keeps holding sockets + tasks for hours, which
            # is what caused 2026-06-04 (9h-idle PG transaction wedged on a
            # leaked Telethon client). Best-effort: swallow disconnect errors.
            try:
                await asyncio.wait_for(self.client.disconnect(), timeout=5)
            except Exception:
                pass
            self.client = None
            raise ConnectionError(
                f"Telegram connection timed out after {timeout}s. "
                "Session may need re-authentication — run interactively first."
            )
        self._clear_runtime_note()
        _configure_session_sqlite(session_path, self.client)
        logger.info("Connected to Telegram")

    async def disconnect(self) -> None:
        """Disconnect from Telegram."""
        if self.client:
            await self.client.disconnect()
            logger.info("Disconnected from Telegram")

    def _set_runtime_note(self, note: str, *, kind: str) -> None:
        if self._reconnect_since is None:
            self._reconnect_since = datetime.now(UTC).isoformat()
        patch_progress(
            runtime_note=note,
            runtime_note_kind=kind,
            runtime_note_since=self._reconnect_since,
        )

    def _clear_runtime_note(self) -> None:
        self._reconnect_since = None
        patch_progress(
            runtime_note=None,
            runtime_note_kind=None,
            runtime_note_since=None,
        )

    @staticmethod
    def _is_retryable_connection_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        patterns = (
            "while disconnected",
            "timed out",
            "timeout",
            "server closed the connection",
            "0 bytes read",
            "connection reset",
            "connection aborted",
            "not connected",
        )
        return any(p in msg for p in patterns)

    # Maximum wall-clock time we will spend on any single _ensure_connected
    # call (including disconnect of the prior client + new connect). Hard cap
    # so a stuck Telethon reconnect can't wedge the caller for hours.
    _ENSURE_CONNECTED_BUDGET_SECONDS: int = 300

    async def _ensure_connected(self, timeout: int = 30, reason: str = "telegram operation") -> None:
        if self.client is not None and self.client.is_connected():
            return

        async with self._connect_lock:
            if self.client is not None and self.client.is_connected():
                return

            self._set_runtime_note(
                f"Waiting for Telegram reconnect during {reason}",
                kind="telegram_reconnect",
            )
            # Hard outer cap so a stuck Telethon (e.g. auto_reconnect spinning
            # forever after a network blip) cannot hold this coroutine
            # indefinitely. The inner `connect(timeout=timeout)` already has
            # its own asyncio.wait_for, but Telethon's background reconnect
            # task can ignore that and keep spinning — this outer cap is the
            # belt-and-braces guarantee. Uses the configured class attribute
            # directly so tests can shrink it without depending on `timeout`.
            budget = self._ENSURE_CONNECTED_BUDGET_SECONDS
            try:
                if self.client is not None:
                    try:
                        await asyncio.wait_for(self.client.disconnect(), timeout=5)
                    except Exception:
                        pass
                await asyncio.wait_for(
                    self.connect(timeout=timeout),
                    timeout=budget,
                )
            except TimeoutError as exc:
                raise ConnectionError(
                    f"Telegram reconnect for {reason!r} exceeded "
                    f"{budget}s budget — aborting"
                ) from exc

    # Maximum wall-clock per attempt of the wrapped Telethon op. Without
    # this, a Telethon call that gets stuck in an internal `await` (network
    # read with auto_reconnect spinning, message-fetch hung after a connection
    # blip, etc.) holds the caller forever — observed today as a 10.3h-idle
    # pg session left by ChannelJoiner.join_conversation while the pipeline
    # was wedged in stage=finalize.  The inner _ensure_connected budget
    # (300s) does NOT cover this path because it only fires on (re)connect.
    _RUN_WITH_RECONNECT_BUDGET_SECONDS: int = 300

    async def _run_with_reconnect(
        self,
        operation: str,
        factory: Callable[[], object],
        *,
        timeout: int = 30,
        retries: int = 2,
    ):
        await self._ensure_connected(timeout=timeout, reason=operation)
        for attempt in range(retries + 1):
            try:
                result = await asyncio.wait_for(
                    factory(),
                    timeout=self._RUN_WITH_RECONNECT_BUDGET_SECONDS,
                )
                self._clear_runtime_note()
                return result
            except asyncio.CancelledError as exc:
                # Telethon cancels in-flight futures when the connection drops.
                # Treat this as a retryable connection error rather than letting
                # it propagate as a BaseException (which bypasses all except Exception
                # guards in the pipeline and kills the subprocess).
                if attempt >= retries:
                    raise ConnectionError(
                        f"{operation} cancelled by Telethon (connection dropped)"
                    ) from exc
                logger.warning(
                    "%s cancelled by Telethon (connection dropped), reconnecting (retry %d/%d)",
                    operation, attempt + 1, retries,
                )
                self._set_runtime_note(
                    f"Waiting for Telegram reconnect during {operation}",
                    kind="telegram_reconnect",
                )
                async with self._connect_lock:
                    if self.client is not None:
                        try:
                            await self.client.disconnect()
                        except Exception:
                            pass
                    await self.connect(timeout=timeout)
                continue
            except Exception as exc:
                if not self._is_retryable_connection_error(exc) or attempt >= retries:
                    raise
                logger.warning(
                    "%s failed due to Telegram connection issue: %s (retry %d/%d)",
                    operation,
                    exc,
                    attempt + 1,
                    retries,
                )
                self._set_runtime_note(
                    f"Waiting for Telegram reconnect during {operation}",
                    kind="telegram_reconnect",
                )
                async with self._connect_lock:
                    if self.client is not None:
                        try:
                            await self.client.disconnect()
                        except Exception:
                            pass
                    await self.connect(timeout=timeout)

        raise RuntimeError(f"{operation} failed after reconnect retries")

    async def iter_conversations(self) -> AsyncIterator[ConversationInfo]:
        """Iterate over all accessible conversations."""
        await self._ensure_connected(reason="iterating conversations")

        try:
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity

                conv_type = self._get_conversation_type(entity)
                is_member = True  # We're in this dialog
                is_accessible = True

                # Get access hash if available
                access_hash = getattr(entity, "access_hash", None)

                yield ConversationInfo(
                    platform_id=dialog.id,
                    access_hash=access_hash,
                    title=dialog.title or dialog.name,
                    username=getattr(entity, "username", None),
                    conversation_type=conv_type,
                    is_member=is_member,
                    is_accessible=is_accessible,
                )
        except asyncio.CancelledError:
            logger.warning("iter_conversations cancelled by Telethon (connection dropped) — stopping iteration")
            return

    def _get_conversation_type(self, entity) -> str:
        """Determine conversation type from entity."""
        if isinstance(entity, User):
            return "user"
        elif isinstance(entity, Chat):
            return "group"
        elif isinstance(entity, Channel):
            if entity.megagroup:
                return "supergroup"
            elif entity.broadcast:
                return "channel"
            return "channel"
        return "unknown"

    async def iter_messages(
        self,
        conversation_id: int,
        min_id: int | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[tuple[MessageInfo, list[FileInfo]]]:
        """Iterate over messages in a conversation."""
        await self._ensure_connected(reason="iterating messages")

        kwargs = {
            "entity": conversation_id,
            "reverse": True,  # Oldest first for deterministic order
        }

        if min_id is not None:
            kwargs["min_id"] = min_id

        if limit is not None:
            kwargs["limit"] = limit

        try:
            async for message in self.client.iter_messages(**kwargs):
                if not isinstance(message, TelegramMessage):
                    continue

                msg_info = self._extract_message_info(message, conversation_id)
                files = self._extract_files(message)

                yield msg_info, files
        except asyncio.CancelledError:
            logger.warning(
                "iter_messages cancelled by Telethon (connection dropped) for conv %s — stopping iteration",
                conversation_id,
            )
            return

    def _extract_message_info(
        self, message: TelegramMessage, conversation_id: int
    ) -> MessageInfo:
        """Extract MessageInfo from Telegram message."""
        # Handle forwarded messages
        is_forwarded = message.fwd_from is not None
        forwarded_from_id = None
        forwarded_from_name = None
        forwarded_message_id = None

        if message.fwd_from:
            fwd = message.fwd_from
            if fwd.from_id:
                # Try to get the channel/user ID
                if hasattr(fwd.from_id, "channel_id"):
                    forwarded_from_id = fwd.from_id.channel_id
                elif hasattr(fwd.from_id, "user_id"):
                    forwarded_from_id = fwd.from_id.user_id

            forwarded_from_name = fwd.from_name
            forwarded_message_id = fwd.channel_post

        # Get timestamp with timezone
        timestamp = message.date
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Extract edit date
        edit_date = None
        if message.edit_date:
            edit_date = message.edit_date
            if edit_date.tzinfo is None:
                edit_date = edit_date.replace(tzinfo=UTC)

        return MessageInfo(
            platform_id=message.id,
            conversation_id=conversation_id,
            timestamp=timestamp,
            text=message.text,
            caption=message.text if message.media else None,
            is_forwarded=is_forwarded,
            forwarded_from_id=forwarded_from_id,
            forwarded_from_name=forwarded_from_name,
            forwarded_message_id=forwarded_message_id,
            views=message.views,
            forwards=message.forwards,
            edit_date=edit_date,
            post_author=message.post_author,
            grouped_id=message.grouped_id,
        )

    def _extract_files(self, message: TelegramMessage) -> list[FileInfo]:
        """Extract file attachments from message."""
        files = []

        if not message.media:
            return files

        if isinstance(message.media, MessageMediaDocument):
            doc = message.media.document
            if isinstance(doc, Document):
                files.append(self._document_to_file_info(doc))

        # Could also handle MessageMediaPhoto if needed
        # elif isinstance(message.media, MessageMediaPhoto):
        #     ...

        return files

    def _document_to_file_info(self, doc: Document) -> FileInfo:
        """Convert Telegram Document to FileInfo."""
        # Get filename from attributes
        filename = None
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                filename = attr.file_name
                break

        return FileInfo(
            platform_file_id=str(doc.id),
            platform_file_unique_id=f"{doc.id}_{doc.access_hash}",
            access_hash=doc.access_hash,
            filename=filename,
            mime_type=doc.mime_type,
            size=doc.size,
        )

    async def download_message_media(
        self,
        conversation_id: int,
        message_id: int,
        destination: Path,
        progress_callback: Callable[[int, int], None] | None = None,
        timeout_seconds: int | None = None,
        stall_seconds: int = 300,
    ) -> bool:
        """Download media from a specific message.

        Uses parallel chunk downloads for large files (premium accounts)
        to saturate available bandwidth.

        Stall detection: if no bytes arrive for `stall_seconds` (default 5 min),
        the download task is cancelled regardless of total timeout.  This catches
        cases where Telethon's reconnection loop hangs indefinitely and absorbs
        asyncio cancellations, which would otherwise defeat asyncio.wait_for.
        """
        try:
            message = await self._run_with_reconnect(
                "fetching message media metadata",
                lambda: self.client.get_messages(conversation_id, ids=message_id),
            )

            if not message:
                raise RuntimeError("Message not found in Telegram (deleted or inaccessible)")
            if not message.media:
                raise RuntimeError("Message has no media attachment")

            last_progress_time = [time.monotonic()]

            def progress(current, total):
                last_progress_time[0] = time.monotonic()
                if progress_callback:
                    progress_callback(current, total)

            download_task = asyncio.create_task(
                self._run_with_reconnect(
                    "downloading message media",
                    lambda: self.client.download_media(
                        message,
                        file=destination,
                        progress_callback=progress,
                    ),
                    timeout=max(timeout_seconds or 30, 30),
                    retries=1,
                )
            )

            deadline = time.monotonic() + timeout_seconds if timeout_seconds else None

            try:
                while not download_task.done():
                    wait_secs = 30.0
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            download_task.cancel()
                            raise TimeoutError(f"Download timed out after {timeout_seconds}s")
                        wait_secs = min(wait_secs, remaining)

                    await asyncio.wait({download_task}, timeout=wait_secs)

                    if download_task.done():
                        break

                    stalled = time.monotonic() - last_progress_time[0]
                    if stalled > stall_seconds:
                        download_task.cancel()
                        raise TimeoutError(
                            f"Download stalled for {stalled:.0f}s with no progress"
                        )

                # Propagate any exception from the task
                download_task.result()

            finally:
                if not download_task.done():
                    download_task.cancel()
                    try:
                        # Use asyncio.wait instead of awaiting directly — Telethon's
                        # reconnection loop can swallow CancelledError, causing an
                        # indefinite hang.  Give it 5 s to honour the cancel, then abandon.
                        await asyncio.wait({download_task}, timeout=5.0)
                    except Exception:
                        pass
                # If the task was cancelled (stall or hard timeout), disconnect so
                # Telethon's auto_reconnect loop doesn't keep firing during subsequent
                # pipeline stages that don't use Telegram.  _ensure_connected will
                # reconnect transparently on the next Telegram operation.
                if download_task.cancelled() and self.client:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass

            if not destination.exists():
                raise RuntimeError("Download task completed but file not found on disk")
            return True

        except TimeoutError:
            raise
        except Exception:
            raise

    async def resolve_forwarded_source(
        self,
        forwarded_from_id: int,
    ) -> ConversationInfo | None:
        """Resolve the source conversation for a forwarded message."""
        try:
            entity = await self._run_with_reconnect(
                "resolving forwarded source",
                lambda: self.client.get_entity(forwarded_from_id),
            )

            conv_type = self._get_conversation_type(entity)
            access_hash = getattr(entity, "access_hash", None)
            username = getattr(entity, "username", None)

            # Check if we're a member
            is_member = False
            try:
                # Try to get dialogs to check membership
                async for dialog in self.client.iter_dialogs():
                    if dialog.id == forwarded_from_id:
                        is_member = True
                        break
            except (Exception, asyncio.CancelledError):
                pass

            return ConversationInfo(
                platform_id=forwarded_from_id,
                access_hash=access_hash,
                title=getattr(entity, "title", None) or getattr(entity, "first_name", None),
                username=username,
                conversation_type=conv_type,
                is_member=is_member,
                is_accessible=True,
            )

        except ChannelPrivateError:
            logger.debug("Channel %d is private", forwarded_from_id)
            return None
        except Exception as e:
            logger.warning("Failed to resolve forwarded source %d: %s", forwarded_from_id, e)
            return None

    async def join_conversation(
        self,
        conversation_id: int,
        username: str | None = None,
    ) -> bool:
        """Attempt to join a conversation."""
        try:
            if username:
                # Try joining by username
                if username.startswith("https://t.me/+") or username.startswith("+"):
                    # Invite link
                    invite_hash = username.split("+")[-1]
                    await self._run_with_reconnect(
                        "joining conversation",
                        lambda: self.client(ImportChatInviteRequest(invite_hash)),
                    )
                else:
                    # Public channel/group
                    await self._run_with_reconnect(
                        "joining conversation",
                        lambda: self.client(JoinChannelRequest(username)),
                    )
            else:
                # Try joining by ID
                entity = await self._run_with_reconnect(
                    "loading conversation entity",
                    lambda: self.client.get_entity(conversation_id),
                )
                await self._run_with_reconnect(
                    "joining conversation",
                    lambda: self.client(JoinChannelRequest(entity)),
                )

            logger.info("Successfully joined conversation %d", conversation_id)
            return True

        except UserAlreadyParticipantError:
            logger.debug("Already a participant in %d", conversation_id)
            return True
        except (ChannelPrivateError, ChatAdminRequiredError):
            logger.warning("Cannot join private channel %d", conversation_id)
            return False
        except InviteHashExpiredError:
            logger.warning("Invite link expired for %d", conversation_id)
            return False
        except Exception as e:
            logger.warning("Failed to join conversation %d: %s", conversation_id, e)
            return False

    async def get_entity(self, target) -> object | None:
        """Fetch a Telegram entity by username string or integer ID.

        Returns the raw Telethon entity object, or None on failure.
        Used for channel existence checks where the caller needs the raw object.
        """
        try:
            return await self._run_with_reconnect(
                "fetching entity",
                lambda: self.client.get_entity(target),
            )
        except Exception as e:
            logger.debug("get_entity(%r) failed: %s", target, e)
            return None
