"""Stage 1: Ingest - enumerate conversations and messages."""

import asyncio
import logging
from datetime import UTC, datetime

from telecrime.adapters.base import ConversationInfo, FileInfo, MessageInfo
from telecrime.database import get_dialect_insert
from telecrime.models import Conversation, FileAttachment, Message
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage

logger = logging.getLogger(__name__)


class IngestStage(PipelineStage):
    """Enumerate conversations and messages, storing metadata in DB."""

    name = "ingest"

    # Priority channels - process first (e.g., channels that delete content after 24h)
    PRIORITY_USERNAMES = [
        "observercloudulpnew",  # Reposts logs, deletes after 24h
    ]

    # Telegram system chats/channels to skip (matched case-insensitively against title)
    SKIP_TITLES = [
        "saved messages",
        "telegram",
        "telegram premium",
    ]

    def __init__(self, message_limit: int | None = None, priority_only: bool = False):
        """Initialize ingest stage.

        Args:
            message_limit: Max messages to fetch per conversation (None = all)
            priority_only: If True, only ingest PRIORITY_USERNAMES channels (fast re-check)
        """
        self.message_limit = message_limit
        self.priority_only = priority_only

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the ingest stage."""
        logger.info("Starting conversation ingestion")

        try:
            # Collect all conversations and sort by priority
            conversations = []
            async for conv_info in ctx.adapter.iter_conversations():
                conversations.append(conv_info)

            # Filter out Telegram system chats
            skip_titles = {t.lower() for t in self.SKIP_TITLES}
            before_count = len(conversations)
            conversations = [c for c in conversations if (c.title or "").lower() not in skip_titles]
            skipped = before_count - len(conversations)
            if skipped:
                logger.info(
                    "Skipped %d system conversations (Saved Messages, Telegram, etc.)", skipped
                )

            # Sort: priority channels first
            priority_set = {u.lower() for u in self.PRIORITY_USERNAMES}

            def priority_key(conv):
                username = (conv.username or "").lower()
                return (0, username) if username in priority_set else (1, username)

            conversations.sort(key=priority_key)

            # For priority_only mode, restrict to priority channels
            if self.priority_only:
                conversations = [
                    c for c in conversations if (c.username or "").lower() in priority_set
                ]
                if conversations:
                    logger.info(
                        "Priority re-ingest: scanning %d channel(s): %s",
                        len(conversations),
                        [c.username for c in conversations],
                    )
                else:
                    logger.debug("Priority re-ingest: no priority channels found in subscriptions")
                    return True
            else:
                priority_found = [
                    c for c in conversations if (c.username or "").lower() in priority_set
                ]
                if priority_found:
                    logger.info(
                        "Processing %d priority channels first: %s",
                        len(priority_found),
                        [c.username for c in priority_found],
                    )

            for conv_info in conversations:
                try:
                    await self._process_conversation(ctx, conv_info)
                    ctx.conversations_processed += 1
                    # Commit after each conversation so we don't hold a long
                    # transaction open across multiple Telegram API calls.
                    ctx.session.commit()
                except (Exception, asyncio.CancelledError) as e:
                    logger.error(
                        "Error processing conversation %s: %s", conv_info.title, e
                    )
                    try:
                        ctx.session.rollback()
                    except Exception:
                        pass

            logger.info("Ingested %d conversations", ctx.conversations_processed)
            return True

        except (Exception, asyncio.CancelledError) as e:
            logger.error("Ingest stage failed: %s", e)
            try:
                ctx.session.rollback()
            except Exception:
                pass
            raise

    async def _process_conversation(
        self, ctx: PipelineContext, conv_info: ConversationInfo
    ) -> None:
        """Process a single conversation."""
        conv = self._upsert_conversation(ctx, conv_info)

        if not conv.is_accessible:
            logger.debug("Skipping inaccessible conversation: %s", conv.title)
            return

        # Fetch messages incrementally from last checkpoint
        min_id = conv.last_ingested_message_id
        message_count = 0
        last_message_platform_id: int | None = None

        async for msg_info, files in ctx.adapter.iter_messages(
            conv_info.platform_id,
            min_id=min_id,
            limit=self.message_limit,
        ):
            await self._process_message(ctx, conv, msg_info, files)
            message_count += 1
            ctx.messages_processed += 1
            last_message_platform_id = msg_info.platform_id

            # Update checkpoint periodically
            if message_count % 100 == 0:
                conv.last_ingested_message_id = last_message_platform_id
                conv.last_ingested_at = datetime.now(UTC)
                ctx.session.flush()

        # Final checkpoint update
        if message_count > 0 and last_message_platform_id is not None:
            conv.last_ingested_message_id = last_message_platform_id
            conv.last_ingested_at = datetime.now(UTC)

        logger.debug(
            "Processed %d messages from conversation: %s",
            message_count,
            conv.title,
        )

    async def _process_message(
        self,
        ctx: PipelineContext,
        conv: Conversation,
        msg_info: MessageInfo,
        files: list[FileInfo],
    ) -> None:
        """Process a single message and its attachments."""
        msg_id = self._insert_message(ctx, conv, msg_info)
        if msg_id is None:
            return

        self._insert_attachments(ctx, msg_id, files)

        logger.debug(
            "Processed message %d with %d files",
            msg_info.platform_id,
            len(files),
        )

    def _upsert_conversation(
        self,
        ctx: PipelineContext,
        conv_info: ConversationInfo,
    ) -> Conversation:
        """Insert or update conversation metadata and return the ORM row."""
        stmt = (
            get_dialect_insert(ctx.session)(Conversation)
            .values(
                platform_id=conv_info.platform_id,
                access_hash=conv_info.access_hash,
                title=conv_info.title,
                username=conv_info.username,
                conversation_type=conv_info.conversation_type,
                is_member=conv_info.is_member,
                is_accessible=conv_info.is_accessible,
            )
            .on_conflict_do_update(
                index_elements=["platform_id"],
                set_={
                    "access_hash": conv_info.access_hash,
                    "title": conv_info.title,
                    "username": conv_info.username,
                    "conversation_type": conv_info.conversation_type,
                    "is_member": conv_info.is_member,
                    "is_accessible": conv_info.is_accessible,
                },
            )
            .returning(Conversation.id)
        )
        conv_id = ctx.session.execute(stmt).scalar_one()
        conv = ctx.session.get(Conversation, conv_id)
        if conv is None:
            raise RuntimeError(f"Failed to load conversation {conv_info.platform_id}")
        return conv

    def _insert_message(
        self,
        ctx: PipelineContext,
        conv: Conversation,
        msg_info: MessageInfo,
    ) -> int | None:
        """Insert one message and return its ID, or None if it already exists."""
        stmt = (
            get_dialect_insert(ctx.session)(Message)
            .values(
                conversation_id=conv.id,
                platform_id=msg_info.platform_id,
                platform_timestamp=msg_info.timestamp,
                text=msg_info.text,
                caption=msg_info.caption,
                is_forwarded=msg_info.is_forwarded,
                forwarded_from_id=msg_info.forwarded_from_id,
                forwarded_from_name=msg_info.forwarded_from_name,
                forwarded_message_id=msg_info.forwarded_message_id,
                views=msg_info.views,
                forwards=msg_info.forwards,
                edit_date=msg_info.edit_date,
                post_author=msg_info.post_author,
                grouped_id=msg_info.grouped_id,
            )
            .on_conflict_do_nothing(
                index_elements=["conversation_id", "platform_id"],
            )
            .returning(Message.id)
        )
        return ctx.session.execute(stmt).scalar_one_or_none()

    def _insert_attachments(
        self,
        ctx: PipelineContext,
        message_id: int,
        files: list[FileInfo],
    ) -> None:
        """Insert all attachments for a newly inserted message."""
        if not files:
            return

        rows = [
            {
                "message_id": message_id,
                "platform_file_id": file_info.platform_file_id,
                "platform_file_unique_id": file_info.platform_file_unique_id,
                "access_hash": file_info.access_hash,
                "filename": file_info.filename,
                "mime_type": file_info.mime_type,
                "size": file_info.size,
            }
            for file_info in files
        ]
        ctx.session.execute(get_dialect_insert(ctx.session)(FileAttachment).values(rows))
        ctx.files_discovered += len(rows)
