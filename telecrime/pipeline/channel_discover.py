"""Pipeline stage: Discover, verify, and subscribe to Telegram channels.

Channel discovery from the DB runs as a quick upfront stage.
Checking and subscribing happen after each archive completes via the
ChannelJoiner helper (one check or join per archive).
"""

import asyncio
import logging
import os

from sqlalchemy import select

from telecrime.channels.discover import (
    discover_channels_from_db,
    persist_discovery_state,
    save_discovered_channels,
)
from telecrime.channels.service import (
    build_subscription_query,
    mark_channel_check_failed,
    mark_channel_checked,
    mark_channel_join_failed,
    mark_channel_join_result,
)
from telecrime.models import TelegramChannel
from telecrime.pipeline.orchestrator import PipelineContext, PipelineStage

logger = logging.getLogger(__name__)

# Bound every joiner Telegram call. The adapter's reconnect loop can wedge on
# a dead TCP socket (disconnect()/connect() outside the operation budget),
# which previously hung the pipeline main thread for 20+ min after a parse.
_CHANNEL_OP_TIMEOUT_SECONDS = 30


class ChannelDiscoverStage(PipelineStage):
    """Discover new channels from the database (fast, local-only).

    Checking and subscribing are handled by ChannelJoiner during archive processing.
    """

    name = "channel_discover"

    async def run(self, ctx: PipelineContext) -> bool:
        session = ctx.session

        # Only do the fast DB discovery here
        logger.info("Discovering channels from database...")
        scan_result = discover_channels_from_db(session)
        new_count, updated_count = save_discovered_channels(session, scan_result.channels)
        persist_discovery_state(session, scan_result)
        logger.info("Channel discovery: %d new, %d updated", new_count, updated_count)

        if ctx.display and new_count:
            ctx.display.channels_update(0)

        if ctx.notifier and new_count:
            await ctx.notifier.channels_discovered(new_count, 0, 0)

        return True


class ChannelJoiner:
    """Checks and subscribes to channels after each archive completes.

    Call `maybe_act()` once per completed archive. It runs incremental DB
    discovery (picks up new mentions from just-parsed credentials) then
    performs one Telegram check or join.
    """

    def __init__(self, check_batch: int = 0):
        self.channels_checked: int = 0
        self.channels_joined: int = 0
        # Number of channels to verify per maybe_act() call. 0 = default.
        # Overridden by TELECRIME_CHANNEL_CHECK_BATCH.
        self.check_batch = check_batch or int(os.environ.get("TELECRIME_CHANNEL_CHECK_BATCH", "5"))

    async def maybe_act(self, ctx: PipelineContext) -> None:
        """Run incremental channel discovery then verify/join channels."""
        session = ctx.session

        # Step 1: incremental DB scan — picks up @mentions from just-parsed credentials
        scan_result = discover_channels_from_db(session)
        save_discovered_channels(session, scan_result.channels)
        persist_discovery_state(session, scan_result)

        # Step 2: Telegram check/join — skip if adapter doesn't support entity lookup
        if not hasattr(ctx.adapter, "get_entity"):
            return

        # Priority 1: check a batch of channels — the oldest-checked first, so
        # every channel is re-verified periodically and deleted/private ones
        # drop out of the public channel list. Bounded by check_batch to stay
        # inside Telegram's rate limits (get_entity is cheap, ~1 req each).
        unchecked = session.execute(
            select(TelegramChannel)
            .where(
                TelegramChannel.last_checked == None,
                (TelegramChannel.username != None) | (TelegramChannel.platform_id != None),
            )
            .order_by(TelegramChannel.id)
            .limit(self.check_batch)
        ).scalars().all()

        if unchecked:
            for ch in unchecked:
                await self._check_one(ctx, ch)
            session.commit()
            return

        # Priority 2: re-verify the least-recently-checked active channels so
        # the public list stays fresh (channels deleted since their last check
        # are marked is_active=False and filtered out on export).
        stale = session.execute(
            select(TelegramChannel)
            .where(
                TelegramChannel.last_checked.isnot(None),
                TelegramChannel.is_active.is_(True),
                TelegramChannel.is_accessible.is_(True),
                (TelegramChannel.username != None) | (TelegramChannel.platform_id != None),
            )
            .order_by(TelegramChannel.last_checked.asc().nulls_last())
            .limit(self.check_batch)
        ).scalars().all()

        if stale:
            for ch in stale:
                await self._check_one(ctx, ch)
            session.commit()
            return

        # Priority 3: Join one matching unsubscribed channel
        candidate = build_subscription_query(session).limit(1).one_or_none()

        if candidate:
            await self._join_one(ctx, candidate)
            session.commit()

    async def _check_one(self, ctx: PipelineContext, channel: TelegramChannel) -> None:
        """Verify a single channel exists via Telegram."""
        target: int | str
        if channel.username:
            target = f"@{channel.username}"
        elif channel.platform_id:
            target = channel.platform_id
        else:
            return

        try:
            entity = await asyncio.wait_for(
                ctx.adapter.get_entity(target), timeout=_CHANNEL_OP_TIMEOUT_SECONDS
            )
            if entity is not None:
                mark_channel_checked(channel, entity)
                self.channels_checked += 1
                logger.info("Checked channel: %s (active)", channel.display_name)
            else:
                mark_channel_check_failed(channel, "Entity not found")
                logger.info("Checked channel: %s (not found)", channel.display_name)
        except Exception as e:
            mark_channel_check_failed(channel, str(e))
            logger.info("Checked channel: %s (%s)", channel.display_name, channel.check_error)

    async def _join_one(self, ctx: PipelineContext, channel: TelegramChannel) -> None:
        """Subscribe to a single channel."""
        target = channel.username or channel.invite_link
        try:
            success = await asyncio.wait_for(
                ctx.adapter.join_conversation(
                    channel.platform_id or 0,
                    username=target,
                ),
                timeout=_CHANNEL_OP_TIMEOUT_SECONDS,
            )

            if mark_channel_join_result(channel, success) == "joined":
                self.channels_joined += 1
                logger.info("Joined channel: %s", channel.display_name)
            else:
                logger.info("Join attempt for %s: %s", channel.display_name, channel.check_error)

        except Exception as e:
            error_msg = str(e)
            result = mark_channel_join_failed(channel, error_msg)
            if result == "already":
                logger.info("Join attempt for %s: already subscribed", channel.display_name)
            else:
                logger.info("Join attempt for %s: %s", channel.display_name, error_msg[:100])

        if ctx.display:
            ctx.display.channels_update(self.channels_joined)
