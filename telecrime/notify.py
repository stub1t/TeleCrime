"""Notification system for progress updates via Telegram Saved Messages.

Message design
--------------
All messages render in Telegram's HTML parse mode and follow one consistent
skeleton so the feed reads like a log, not a pile of one-off formats:

    <ICON> <b>Title</b>                     ← what happened
    <i>2026-09-03 13:30:00 UTC</i>          ← when
    ──────────────────────────────
    • <b>Key:</b> value                     ← the facts
    • <b>Key:</b> value

Rules:
- `<code>…</code>` is used for verbatim data (file names, queries, IDs) so it
  renders monospace and is tap-to-copy on mobile.
- Anything that came from the outside world (archive names, channel titles,
  watchlist queries, error text) is HTML-escaped via `_esc` before
  interpolation — a `<` in a stealer log cannot break the markup.
- Per-archive parsing results are NOT sent one-by-one: a run of thousands of
  archives would flood the feed. `archive_parsed()` accumulates into a
  digest and `flush()` emits it every N archives or M minutes (env-tunable).
  High-signal events (errors, watchlist hits, start/complete, summaries) are
  still sent immediately.
- Watchlist alert hits **redact password fields** by default — they sync to
  all your devices and a shoulder surfer can read Saved Messages. The
  dashboard shows the full record.
"""

import asyncio
import html
import logging
import os
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from telethon import TelegramClient

    from telecrime.adapters.telegram import TelegramAdapter

logger = logging.getLogger(__name__)

# Bound every Telegram network call so a wedged connection (Telethon
# swallowing CancelledError during a drop/reconnect loop) can never block
# the pipeline main thread forever in a notification send.
_SEND_TIMEOUT_SECONDS = 30

# Digest flush cadence (env-tunable: TELECRIME_NOTIFY_DIGEST_ARCHIVES /
# TELECRIME_NOTIFY_DIGEST_SECONDS).
_DIGEST_ARCHIVES_DEFAULT = 25
_DIGEST_SECONDS_DEFAULT = 20 * 60

_DIVIDER = "─" * 30


def _esc(value: object) -> str:
    """HTML-escape a value for safe interpolation into a notification."""
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def _code(value: object) -> str:
    """Wrap a value in <code>…</code> after HTML-escaping it."""
    return f"<code>{_esc(value)}</code>"


def _trunc(value: str, limit: int = 64) -> str:
    """Truncate a string to `limit` chars with an ellipsis marker."""
    if not value:
        return ""
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _redact_password(password: object) -> str:
    """Default-redact a password field for over-the-wire safety."""
    if password is None or password == "":
        return "—"
    s = str(password)
    n = len(s)
    if n <= 2:
        return "•" * n
    return f"{s[0]}{'•' * (n - 2)}{s[-1]} ({n} chars)"


def _fmt_int(n: Any) -> str:
    """Format an integer with thousands separators; return '—' for missing."""
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return _esc(n)


def _fmt_duration(seconds: Any) -> str:
    """Format a duration in seconds as a compact human string."""
    try:
        s = int(float(seconds))
    except (TypeError, ValueError):
        return "—"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m}m"


def _fmt_rate(count: Any, seconds: Any) -> str:
    """Format a per-second rate."""
    try:
        c = float(count)
        s = float(seconds)
        if s <= 0:
            return "—"
        return f"{c / s:,.0f}/s"
    except (TypeError, ValueError):
        return "—"


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _header(icon: str, title: str) -> str:
    """The standard message header: icon + title + timestamp."""
    return f"{icon} <b>{title}</b>\n<i>{_esc(_now_iso())}</i>\n{_DIVIDER}"


class TelegramNotifier:
    """Send progress notifications to Telegram Saved Messages."""

    # Stages we SUPPRESS from stage_complete announcements (all real pipeline
    # stage names; "channel_join" was never a pipeline stage — kept for
    # symmetry with the scheduler job name, harmless).
    _NOISY_STAGE_COMPLETIONS = frozenset({
        "ingest", "channel_discover", "discover", "plan",
        "channel_join", "enrich",
    })

    def __init__(
        self,
        client: "TelegramClient",
        enabled: bool = True,
        adapter: "TelegramAdapter | None" = None,
    ):
        self.client = client
        self.enabled = enabled
        self.adapter = adapter
        self._me = None
        # Live-status provider (set by the pipeline entry point): called on
        # each digest flush to include a "what is the pipeline doing right
        # now" section (stage, position, rate, queue, disk). Must be cheap —
        # return None to skip.
        self.status_provider = None
        # Watchlist provider (set by the pipeline entry point): called after
        # each digest flush; must return a list of alert dicts (or None).
        # The scheduler's own watchlist job defers while the pipeline runs —
        # this path carries the alerts on the pipeline's Telegram session.
        self.watchlist_provider = None
        # Called after a watchlist alert batch was CONFIRMED sent (advances
        # the alerted window). Set by the pipeline entry point.
        self.watchlist_sent_callback = None
        # Digest accumulator for per-archive parse results.
        try:
            self._digest_archives_cap = max(
                1, int(os.environ.get("TELECRIME_NOTIFY_DIGEST_ARCHIVES", _DIGEST_ARCHIVES_DEFAULT))
            )
        except ValueError:
            self._digest_archives_cap = _DIGEST_ARCHIVES_DEFAULT
        try:
            self._digest_seconds_cap = max(
                30, int(os.environ.get("TELECRIME_NOTIFY_DIGEST_SECONDS", _DIGEST_SECONDS_DEFAULT))
            )
        except ValueError:
            self._digest_seconds_cap = _DIGEST_SECONDS_DEFAULT
        self._digest_archives = 0
        self._digest_new = 0
        self._digest_dups = 0
        self._digest_domains: Counter[str] = Counter()
        self._digest_last_archive: str | None = None
        self._digest_since: float | None = None
        # Archive names already reported this run — a re-parse of the same
        # job (after a wedge) must not double-count it in the digest.
        self._reported_archives: set[str] = set()

    async def _get_me(self, client: "TelegramClient"):
        if self._me is None:
            self._me = await client.get_me()
        return self._me

    async def send(self, message: str, force: bool = False) -> bool:
        """Send an HTML-formatted notification to Saved Messages.

        Returns True when the message was delivered (or notifications are
        disabled), False when it could not be sent — callers that advance
        state on delivery (e.g. the watchlist alert window) must only do so
        on a confirmed send.
        """
        del force  # accepted for API compat; no rate-limit gate
        if not self.enabled:
            logger.info("[NOTIFY] %s", message)
            return True

        try:
            client = self.client
            if self.adapter is not None:
                # The adapter replaces its client instance on reconnect, so a
                # reference captured at construction would send on a dead
                # client forever (observed: every digest failing with "Cannot
                # send requests while disconnected" for the rest of a
                # multi-day run). Re-resolve each send and run the adapter's
                # bounded reconnect when the client is down — but ONLY when
                # the adapter has no operation in flight: forcing a reconnect
                # mid-download kills the download, and the new client then
                # collides with the retrying download on the same session
                # file ("database is locked" / "wrong session ID"). When
                # busy, drop the message instead (the next digest retries).
                client = self.adapter.client
                if (
                    client is None
                    or not client.is_connected()
                ) and getattr(self.adapter, "_active_ops", 0) == 0:
                    await asyncio.wait_for(
                        self.adapter._ensure_connected(
                            timeout=30, reason="sending notification"
                        ),
                        timeout=2 * _SEND_TIMEOUT_SECONDS,
                    )
                    client = self.adapter.client
                    self._me = None
            if client is None or not client.is_connected():
                logger.warning(
                    "Notification not sent: Telegram client disconnected (%s)",
                    _trunc(message, 80),
                )
                return False
            me = await asyncio.wait_for(self._get_me(client), timeout=_SEND_TIMEOUT_SECONDS)
            await asyncio.wait_for(
                client.send_message(me.id, message, parse_mode="html"),
                timeout=_SEND_TIMEOUT_SECONDS,
            )
            logger.debug("Notification sent: %s", _trunc(message, 80))
            return True
        except (Exception, asyncio.CancelledError) as e:
            logger.warning("Failed to send notification: %s", e)
            return False

    # -------------------------------------------------------------- digests

    async def archive_parsed(
        self,
        archive_name: str,
        new_credentials: int,
        duplicates: int,
        unique_domains: int,
        top_domains: list[tuple[str, int]] | None = None,
    ):
        """Accumulate per-archive parse results into a progress digest.

        Individual archives are NOT messaged (a 2,000-archive run would
        flood Saved Messages). Results accumulate until `flush()` decides the
        digest is due (every N archives or M minutes).
        """
        del unique_domains
        if archive_name in self._reported_archives:
            return
        self._reported_archives.add(archive_name)
        self._digest_archives += 1
        self._digest_new += new_credentials or 0
        self._digest_dups += duplicates or 0
        if top_domains:
            for domain, count in top_domains:
                if domain:
                    self._digest_domains[domain] += count
        self._digest_last_archive = archive_name

        now = asyncio.get_event_loop().time()
        if self._digest_since is None:
            self._digest_since = now
        if (
            self._digest_archives >= self._digest_archives_cap
            or (now - self._digest_since) >= self._digest_seconds_cap
        ):
            await self.flush()

    async def flush(self):
        """Send the accumulated progress digest (if any) and reset."""
        if not self._digest_archives:
            return
        new = self._digest_new
        dups = self._digest_dups
        total = new + dups
        dedup_pct = ""
        if total:
            dedup_pct = f" ({100.0 * dups / total:.0f}% dedup)"

        lines = [
            _header("📊", "Progress digest"),
            "",
            f"• <b>Archives parsed:</b> {_fmt_int(self._digest_archives)}",
            f"• <b>New credentials:</b> {_fmt_int(new)}",
            f"• <b>Duplicates:</b> {_fmt_int(dups)}{dedup_pct}",
        ]
        if self._digest_last_archive:
            lines.append(
                f"• <b>Last archive:</b> {_code(_trunc(self._digest_last_archive, 60))}"
            )

        # Live status section: what the pipeline is doing right now. The
        # provider is called with a short budget — a wedged drive or slow
        # query must not stall the digest.
        if self.status_provider is not None:
            try:
                status = await asyncio.wait_for(
                    self.status_provider(), timeout=10
                )
            except Exception:
                status = None
            if status:
                lines.append("")
                lines.append("🟢 <b>Pipeline</b>")
                _stage = status.get("stage") or "—"
                _idx = status.get("archive_index")
                _tot = status.get("archive_total")
                _pos = (
                    f"{_fmt_int(_idx)} / {_fmt_int(_tot)}"
                    if _idx is not None and _tot is not None
                    else "—"
                )
                # Credentials per minute since the last digest flush.
                _rate = None
                if self._digest_since:
                    _mins = max(
                        1.0, (asyncio.get_event_loop().time() - self._digest_since) / 60
                    )
                    _rate = new / _mins
                lines.append(f"• <b>Stage:</b> {_esc(_stage)}")
                lines.append(f"• <b>Position:</b> {_pos}")
                if _rate is not None:
                    lines.append(f"• <b>Rate:</b> {_rate:,.0f} creds/min")
                if status.get("pending") is not None:
                    lines.append(f"• <b>Queue:</b> {_fmt_int(status['pending'])} downloads")
                if status.get("free_disk_gb") is not None:
                    lines.append(f"• <b>Free disk:</b> {status['free_disk_gb']:,.0f} GB")
                if status.get("errors"):
                    lines.append(f"• <b>Errors:</b> ⚠️ {_fmt_int(status['errors'])}")
                if status.get("current_archive"):
                    lines.append(
                        f"• <b>Current:</b> {_code(_trunc(status['current_archive'], 55))}"
                    )

        if self._digest_domains:
            top = self._digest_domains.most_common(5)
            lines.append("")
            lines.append("<b>Top domains</b>")
            for domain, count in top:
                lines.append(f"• {_esc(_trunc(domain, 48))} — {_fmt_int(count)}")

        self._digest_archives = 0
        self._digest_new = 0
        self._digest_dups = 0
        self._digest_domains.clear()
        self._digest_last_archive = None
        self._digest_since = None
        await self.send("\n".join(lines))

        # Watchlist hits on the pipeline's session: the scheduler's
        # watchlist job defers while the pipeline runs, so this is the only
        # path alerts reach Saved Messages during multi-day runs.
        if self.watchlist_provider is not None:
            try:
                alerts = await asyncio.wait_for(
                    self.watchlist_provider(), timeout=45
                )
            except Exception as _w:
                alerts = None
            if alerts:
                try:
                    sent = await self.watchlist_alerts(alerts)
                except Exception as _we:
                    sent = False
                    logger.warning("Watchlist alert send failed: %s", _we)
                if sent and self.watchlist_sent_callback is not None:
                    # Advance the alerted window only on a confirmed delivery —
                    # otherwise a transient send failure would drop the hits.
                    try:
                        await asyncio.wait_for(
                            self.watchlist_sent_callback(alerts), timeout=45
                        )
                    except Exception as _a:
                        logger.warning("Watchlist window advance failed: %s", _a)
        # Keep _reported_archives: a digest flush mid-run must not re-report
        # archives already counted once this run.

    # ------------------------------------------------------------------ stages

    async def stage_start(self, stage_name: str):
        """Stage start: suppressed entirely — was extreme noise.

        Stage transitions are visible in the dashboard's progress panel and in
        worker logs; flooding Saved Messages on every transition (5+ per
        pipeline run × N runs/day) wasn't useful. Kept as a no-op so callers
        don't need to be touched.
        """
        del stage_name

    async def stage_complete(self, stage_name: str, stats: dict | None = None):
        """Announce completion of meaningful stages only.

        Most stage completions are too noisy to surface (see _NOISY_STAGE_COMPLETIONS).
        """
        if stage_name in self._NOISY_STAGE_COMPLETIONS:
            return
        lines = [_header("✅", f"Stage complete — {_esc(stage_name)}")]
        if stats:
            lines.append("")
            for key, value in stats.items():
                lines.append(f"• <b>{_esc(key)}:</b> {_esc(value)}")
        await self.send("\n".join(lines))

    # --------------------------------------------------------------- downloads

    async def downloading(self, channel: str, filename: str, size_mb: float):
        """Per-file download announcement (only for files ≥ 50 MB).

        Smaller files complete fast enough that the notification arrives after
        the download already finished, so we skip them.
        """
        if size_mb is None or size_mb < 50:
            return
        msg = (
            f"{_header('📥', 'Downloading')}\n"
            f"{_code(_trunc(filename, 80))}\n\n"
            f"• <b>Channel:</b> {_esc(_trunc(channel, 50))}\n"
            f"• <b>Size:</b> {size_mb:,.1f} MB"
        )
        await self.send(msg)

    async def error(self, message: str, stage: str | None = None):
        """Pipeline error — formatted with stage and timestamp."""
        header = "❌ <b>Pipeline error</b>"
        if stage:
            header += f" — <i>{_esc(stage)}</i>"
        body = (
            f"{header}\n"
            f"<i>{_esc(_now_iso())}</i>\n{_DIVIDER}\n\n"
            f"<pre>{_esc(_trunc(message, 800))}</pre>"
        )
        await self.send(body)

    # ----------------------------------------------------------- channel disc

    async def channels_discovered(
        self,
        new_discovered: int,
        checked: int,
        joined: int,
    ):
        """Channel discovery / join summary — suppressed when nothing happened."""
        if not new_discovered and not joined:
            return
        lines = [_header("📡", "Channels")]
        if new_discovered:
            lines.append(f"• <b>New discovered:</b> {_fmt_int(new_discovered)}")
        if checked:
            lines.append(f"• <b>Checked:</b> {_fmt_int(checked)}")
        if joined:
            lines.append(f"• <b>Joined:</b> {_fmt_int(joined)}")
        await self.send("\n".join(lines))

    # ----------------------------------------------------- pipeline lifecycle

    async def pipeline_start(
        self,
        target_extensions: list[str],
        *,
        queue_size: int | None = None,
        free_disk_gb: float | None = None,
    ):
        """Pipeline run started — header includes queue + disk snapshot."""
        lines = [_header("🚀", "Pipeline started")]
        lines.append(f"• <b>Targets:</b> {_esc(', '.join(target_extensions or [])) or '—'}")
        if queue_size is not None:
            lines.append(f"• <b>Queue:</b> {_fmt_int(queue_size)} pending archives")
        if free_disk_gb is not None:
            lines.append(f"• <b>Free disk:</b> {free_disk_gb:,.1f} GB")
        await self.send("\n".join(lines))

    async def pipeline_complete(self, stats: dict):
        """Pipeline run completed — flush pending digest, then formatted stats."""
        # Flush any accumulated per-archive digest FIRST so the feed shows
        # progress up to the end, then the final summary.
        await self.flush()

        archives = stats.get("archives_extracted") or stats.get("archives") or 0
        creds = stats.get("credentials_parsed") or stats.get("credentials") or 0
        dups = stats.get("duplicates_skipped") or stats.get("duplicates") or 0
        errors = stats.get("errors") or 0
        elapsed = stats.get("elapsed_seconds")

        lines = [_header("🏁", "Pipeline complete")]
        lines.append(f"• <b>Archives:</b> {_fmt_int(archives)}")
        lines.append(f"• <b>Credentials:</b> {_fmt_int(creds)} new, {_fmt_int(dups)} dups")
        lines.append(f"• <b>Errors:</b> {_fmt_int(errors)}")
        if elapsed is not None:
            lines.append(f"• <b>Duration:</b> {_fmt_duration(elapsed)}")
            lines.append(f"• <b>Rate:</b> {_fmt_rate(creds, elapsed)} creds")
        # Surface anything else the caller passed but we didn't recognise.
        _known = {
            "archives_extracted", "archives",
            "credentials_parsed", "credentials",
            "duplicates_skipped", "duplicates",
            "errors", "elapsed_seconds",
        }
        for key in stats:
            if key in _known:
                continue
            lines.append(f"• <b>{_esc(key)}:</b> {_esc(stats[key])}")
        await self.send("\n".join(lines))

    async def activity_summary(self, window_label: str, new_unique_credentials: int):
        """Hourly/daily summary of new unique credentials."""
        msg = (
            f"{_header('📊', f'{_esc(window_label)} summary')}\n"
            f"• <b>New unique credentials:</b> {_fmt_int(new_unique_credentials)}"
        )
        await self.send(msg)

    # ------------------------------------------------------------- watchlist

    async def watchlist_alerts(self, alerts: list[dict]) -> bool:
        """Watchlist hits — passwords redacted by default for over-the-wire safety.

        Returns True when delivered (or nothing to send), False on a failed
        send — the caller must only advance the alerted window on True.
        """
        if not alerts:
            return True

        lines = [_header("🚨", f"Watchlist hits — {len(alerts)} item(s)")]
        for alert in alerts[:8]:
            label = _esc(_trunc(str(alert.get("label", "")), 60))
            query = _code(_trunc(str(alert.get("query", "")), 60))
            new = _fmt_int(alert.get("new_matches", 0))
            lines.append("")
            lines.append(f"<b>+{new}</b> — {label}")
            lines.append(f"<i>query:</i> {query}")

            hits = alert.get("hits") or []
            for hit in hits[:5]:
                source = _esc(_trunc(
                    str(hit.get("source_archive") or hit.get("source_file") or "—"), 60
                ))
                domain = _esc(_trunc(
                    str(hit.get("domain") or hit.get("url") or "—"), 80
                ))
                username = _esc(_trunc(str(hit.get("username") or "—"), 60))
                # Redact password — see module docstring.
                pwd = _esc(_redact_password(hit.get("password")))
                lines.append(
                    f"  • <b>{domain}</b>\n"
                    f"    user: {_code(username)}\n"
                    f"    pwd:  {pwd}\n"
                    f"    src:  {_code(source)}"
                )
            hidden = int(alert.get("new_matches", 0)) - len(hits)
            if hidden > 0:
                lines.append(f"  …and {_fmt_int(hidden)} more new hits")
        if len(alerts) > 8:
            lines.append(f"\n…and {len(alerts) - 8} more watchlist items")
        return await self.send("\n".join(lines))
