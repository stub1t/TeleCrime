"""Stage 5: Enrich - resolve forwarded message origins and membership."""

import logging

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from telecrime.models import Conversation, Message
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage

logger = logging.getLogger(__name__)


class EnrichStage(PipelineStage):
    """Resolve forwarded message origins and handle membership."""

    name = "enrich"

    async def run(self, ctx: PipelineContext) -> bool:
        """Run the enrich stage."""
        logger.info("Starting enrichment (forwarded origin resolution)")

        # Find messages with forwarded content that have archive attachments
        forwarded_messages = ctx.session.execute(
            select(Message)
            .where(
                Message.is_forwarded == True,
                Message.is_processed == False,
            )
            .options(joinedload(Message.attachments))
        ).unique().scalars().all()

        if not forwarded_messages:
            logger.info("No forwarded messages to process")
            return True

        logger.info("Found %d forwarded messages to enrich", len(forwarded_messages))

        for message in forwarded_messages:
            # Only process if message has archive attachments
            has_archives = any(a.is_archive_candidate for a in message.attachments)
            if not has_archives:
                message.is_processed = True
                continue

            if message.forwarded_from_id:
                await self._resolve_origin(ctx, message)

            message.is_processed = True

        ctx.session.commit()
        return True

    async def _resolve_origin(self, ctx: PipelineContext, message: Message) -> None:
        """Resolve the origin conversation of a forwarded message."""
        forwarded_from_id = message.forwarded_from_id
        if forwarded_from_id is None:
            return

        # Check if we already have this conversation
        existing_conv = ctx.session.execute(
            select(Conversation).where(Conversation.platform_id == forwarded_from_id)
        ).scalar_one_or_none()

        if existing_conv:
            if existing_conv.is_accessible:
                logger.debug(
                    "Origin conversation %s already accessible",
                    existing_conv.title or forwarded_from_id,
                )
                return
            elif existing_conv.join_attempted and not existing_conv.join_succeeded:
                logger.debug(
                    "Previously failed to join %s, skipping",
                    existing_conv.title or forwarded_from_id,
                )
                return

        # Try to resolve the source conversation
        try:
            conv_info = await ctx.adapter.resolve_forwarded_source(forwarded_from_id)

            if conv_info is None:
                logger.debug("Could not resolve forwarded source: %d", forwarded_from_id)
                return

            # Create or update conversation record
            if existing_conv is None:
                existing_conv = Conversation(
                    platform_id=conv_info.platform_id,
                    access_hash=conv_info.access_hash,
                    title=conv_info.title,
                    username=conv_info.username,
                    conversation_type=conv_info.conversation_type,
                    is_member=conv_info.is_member,
                    is_accessible=conv_info.is_accessible,
                )
                ctx.session.add(existing_conv)
                ctx.session.flush()
            else:
                existing_conv.title = conv_info.title
                existing_conv.username = conv_info.username
                existing_conv.is_accessible = conv_info.is_accessible

            # If not a member, try to join
            if not conv_info.is_member and not existing_conv.join_attempted:
                await self._attempt_join(ctx, existing_conv, conv_info.username)

        except Exception as e:
            logger.warning("Failed to resolve forwarded source %d: %s", forwarded_from_id, e)

    async def _attempt_join(
        self,
        ctx: PipelineContext,
        conv: Conversation,
        username: str | None,
    ) -> None:
        """Attempt to join a conversation."""
        if ctx.dry_run:
            logger.info("[DRY RUN] Would attempt to join: %s", conv.title or conv.platform_id)
            return

        logger.info("Attempting to join conversation: %s", conv.title or conv.platform_id)

        conv.join_attempted = True

        try:
            success = await ctx.adapter.join_conversation(
                conv.platform_id,
                username=username,
            )

            conv.join_succeeded = success
            if success:
                conv.is_member = True
                conv.is_accessible = True
                logger.info("Successfully joined: %s", conv.title or conv.platform_id)
            else:
                logger.warning("Failed to join: %s", conv.title or conv.platform_id)

        except Exception as e:
            conv.join_succeeded = False
            logger.warning("Exception while joining %s: %s", conv.title or conv.platform_id, e)
