"""Notification system for progress updates via Telegram Saved Messages.

All messages render in Telegram's HTML parse mode (broader tag set than
Markdown V2, no special-character escaping in literal text required outside
the few HTML reserved chars). Each notification follows a consistent layout:

    <ICON> <b>Title</b>
    <i>optional subtitle</i>

    • Key: value
    • Key: value

`<code>…</code>` is used for verbatim data (file names, queries, IDs) so it
renders in monospace and can be tap-to-copy on mobile. Values that came from
the outside world (archive names, channel titles, watchlist queries, error
text) are HTML-escaped via `_esc` before interpolation so a `<` in a stealer
log can't break the markup.

Watchlist alert hits **redact password fields** by default — they sync to all
your devices and a shoulder surfer can read your Saved Messages. The dashboard
shows the full record.
"""

import asyncio
import html
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)

# Bound every Telegram network call so a wedged connection (Telethon
# swallowing CancelledError during a drop/reconnect loop) can never block
# the pipeline main thread forever in a notification send.
_SEND_TIMEOUT_SECONDS = 30


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


class TelegramNotifier:
    """Send progress notifications to Telegram Saved Messages."""

    # Stages we explicitly announce as "completed" (others are noise).
    _NOISY_STAGE_COMPLETIONS = frozenset({
        "ingest", "channel_discover", "discover", "plan",
        "channel_join", "enrich",
    })

    def __init__(self, client: "TelegramClient", enabled: bool = True):
        self.client = client
        self.enabled = enabled
        self._me = None

    async def _get_me(self):
        if self._me is None:
            self._me = await self.client.get_me()
        return self._me

    async def send(self, message: str, force: bool = False):
        """Send an HTML-formatted notification to Saved Messages."""
        del force  # accepted for API compat; no rate-limit gate
        if not self.enabled:
            logger.info("[NOTIFY] %s", message)
            return

        try:
            me = await asyncio.wait_for(self._get_me(), timeout=_SEND_TIMEOUT_SECONDS)
            await asyncio.wait_for(
                self.client.send_message(me.id, message, parse_mode="html"),
                timeout=_SEND_TIMEOUT_SECONDS,
            )
            logger.debug("Notification sent: %s", _trunc(message, 80))
        except (Exception, asyncio.CancelledError) as e:
            logger.warning("Failed to send notification: %s", e)

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
        lines = [f"✅ <b>Stage complete: {_esc(stage_name)}</b>"]
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
            "📥 <b>Downloading</b>\n"
            f"{_code(_trunc(filename, 80))}\n\n"
            f"• <b>Channel:</b> {_esc(_trunc(channel, 50))}\n"
            f"• <b>Size:</b> {size_mb:,.1f} MB"
        )
        await self.send(msg)

    async def archive_parsed(
        self,
        archive_name: str,
        new_credentials: int,
        duplicates: int,
        unique_domains: int,
        top_domains: list[tuple[str, int]] | None = None,
    ):
        """One message per archive when parsing completes.

        Suppressed when the archive had zero hits at all (all-duplicate
        archives flooded Saved Messages with empty notifications).
        """
        if not new_credentials and not duplicates:
            return
        total = (new_credentials or 0) + (duplicates or 0)
        dedup_pct = ""
        if total:
            pct = 100.0 * (duplicates or 0) / total
            dedup_pct = f" ({pct:.0f}% dedup)"
        lines = [
            f"🔑 <b>{_esc(_trunc(archive_name, 80))}</b>",
            "",
            f"• <b>New:</b> {_fmt_int(new_credentials)}"
            f"  <b>Dups:</b> {_fmt_int(duplicates)}{dedup_pct}",
            f"• <b>Unique domains:</b> {_fmt_int(unique_domains)}",
        ]
        if top_domains:
            lines.append("")
            lines.append("<b>Top</b>")
            for domain, cnt in top_domains[:5]:
                lines.append(f"• {_esc(_trunc(domain, 48))} — {_fmt_int(cnt)}")
        await self.send("\n".join(lines))

    async def error(self, message: str, stage: str | None = None):
        """Pipeline error — formatted with stage and timestamp."""
        header = "❌ <b>Pipeline error</b>"
        if stage:
            header += f" — <i>{_esc(stage)}</i>"
        body = (
            f"{header}\n"
            f"<i>{_esc(_now_iso())}</i>\n\n"
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
        lines = ["📡 <b>Channels</b>"]
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
        lines = [
            "🚀 <b>Pipeline started</b>",
            f"<i>{_esc(_now_iso())}</i>",
            "",
            f"• <b>Targets:</b> {_esc(', '.join(target_extensions or [])) or '—'}",
        ]
        if queue_size is not None:
            lines.append(f"• <b>Queue:</b> {_fmt_int(queue_size)} pending archives")
        if free_disk_gb is not None:
            lines.append(f"• <b>Free disk:</b> {free_disk_gb:,.1f} GB")
        await self.send("\n".join(lines))

    async def pipeline_complete(self, stats: dict):
        """Pipeline run completed — formatted stats + derived rate."""
        archives = stats.get("archives_extracted") or stats.get("archives") or 0
        creds = stats.get("credentials_parsed") or stats.get("credentials") or 0
        dups = stats.get("duplicates_skipped") or stats.get("duplicates") or 0
        errors = stats.get("errors") or 0
        elapsed = stats.get("elapsed_seconds")

        lines = [
            "🏁 <b>Pipeline complete</b>",
            f"<i>{_esc(_now_iso())}</i>",
            "",
            f"• <b>Archives:</b> {_fmt_int(archives)}",
            f"• <b>Credentials:</b> {_fmt_int(creds)} new, {_fmt_int(dups)} dups",
            f"• <b>Errors:</b> {_fmt_int(errors)}",
        ]
        if elapsed is not None:
            lines.append(f"• <b>Duration:</b> {_fmt_duration(elapsed)}")
            lines.append(f"• <b>Rate:</b> {_fmt_rate(creds, elapsed)} creds")
        # Surface anything else the caller passed but we didn't recognise.
        for key in stats:
            if key in {
                "archives_extracted", "archives",
                "credentials_parsed", "credentials",
                "duplicates_skipped", "duplicates",
                "errors", "elapsed_seconds",
            }:
                continue
            lines.append(f"• <b>{_esc(key)}:</b> {_esc(stats[key])}")
        await self.send("\n".join(lines))

    async def activity_summary(self, window_label: str, new_unique_credentials: int):
        """Hourly/daily summary of new unique credentials."""
        msg = (
            f"📊 <b>{_esc(window_label)} summary</b>\n"
            f"<i>{_esc(_now_iso())}</i>\n\n"
            f"• <b>New unique credentials:</b> {_fmt_int(new_unique_credentials)}"
        )
        await self.send(msg)

    # ------------------------------------------------------------- watchlist

    async def watchlist_alerts(self, alerts: list[dict]):
        """Watchlist hits — passwords redacted by default for over-the-wire safety."""
        if not alerts:
            return

        lines = [f"🚨 <b>Watchlist hits — {len(alerts)} item(s)</b>"]
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
        await self.send("\n".join(lines))


# Global notifier instance (set during pipeline init)
_notifier: TelegramNotifier | None = None


