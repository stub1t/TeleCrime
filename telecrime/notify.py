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

    # Stages we explicitly announce as "completed" (others are noise).
    _NOISY_STAGE_COMPLETIONS = frozenset({
        "ingest", "channel_discover", "discover", "plan",
        "channel_join", "enrich",
    })

    def __init__(self, client: "TelegramClient", enabled: bool = True):
        self.client = client
        self.enabled = enabled
        self._me = None
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
            f"{_header('📊', f'{window_label} summary')}\n"
            f"• <b>New unique credentials:</b> {_fmt_int(new_unique_credentials)}"
        )
        await self.send(msg)

    # ------------------------------------------------------------- watchlist

    async def watchlist_alerts(self, alerts: list[dict]):
        """Watchlist hits — passwords redacted by default for over-the-wire safety."""
        if not alerts:
            return

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
        await self.send("\n".join(lines))
