"""Rich live display for pipeline output."""

import logging
import time
from collections import deque
from typing import TYPE_CHECKING

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from telecrime.pipeline.orchestrator import PipelineContext

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

# Ordered pipeline stages for progress display
STAGE_ORDER = [
    "ingest",
    "channel_discover",
    "discover",
    "plan",
    "acquire",
    "enrich",
    "extract",
    "parse",
    "finalize",
]

STAGE_LABELS = {
    "ingest": "Ingest",
    "channel_discover": "Channels",
    "discover": "Discover",
    "plan": "Plan",
    "acquire": "Acquire",
    "enrich": "Enrich",
    "extract": "Extract",
    "parse": "Parse",
    "finalize": "Finalize",
}


class PipelineDisplay:
    """Compact Rich live panel that updates in-place during pipeline runs."""

    def __init__(self, console: Console | None = None):
        self.console = console or Console()
        self._live: Live | None = None

        # Stage tracking
        self._current_stage: str | None = None
        self._completed_stages: set[str] = set()
        self._failed_stages: set[str] = set()

        # Download tracking
        self._dl_filename: str | None = None
        self._dl_pct: float = 0.0
        self._dl_speed: float = 0.0
        self._dl_eta: str = ""
        self._dl_active: bool = False

        # Archive tracking
        self._archive_current: int = 0
        self._archive_total: int = 0
        self._archive_name: str = ""

        # Counters
        self._creds: int = 0
        self._dups: int = 0
        self._messages: int = 0
        self._errors: int = 0
        self._channels_joined: int = 0

        # Recent per-archive results (deque keeps last 3)
        self._recent_results: deque[str] = deque(maxlen=3)

        # Logging filter
        self._log_filters: list[tuple[logging.Logger, int]] = []

        # Timing
        self._start_time: float = 0.0

    def start(self) -> "PipelineDisplay":
        """Start the live display and suppress pipeline loggers."""
        self._start_time = time.time()
        if not self.console.is_terminal:
            return self  # non-TTY: no live panel, logging stays active
        self._suppress_loggers()
        self._live = Live(
            self._render(),
            console=self.console,
            refresh_per_second=4,
            transient=False,
        )
        self._live.start()
        return self

    def stop(self) -> None:
        """Stop the live display and restore loggers."""
        if self._live:
            self._live.stop()
            self._live = None
        self._restore_loggers()

    def __enter__(self) -> "PipelineDisplay":
        return self.start()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()

    def _suppress_loggers(self) -> None:
        """Set telecrime loggers to WARNING while display is active."""
        for name in [
            "telecrime",
            "telecrime.pipeline",
            "telecrime.pipeline.orchestrator",
            "telecrime.pipeline.acquire",
            "telecrime.pipeline.extract",
            "telecrime.pipeline.parse",
            "telecrime.pipeline.ingest",
            "telecrime.pipeline.discover",
            "telecrime.pipeline.plan",
            "telecrime.pipeline.enrich",
            "telecrime.pipeline.finalize",
            "telecrime.pipeline.channel_discover",
            "telecrime.adapters",
            "telecrime.adapters.telegram",
            "telecrime.extractor",
            "telecrime.extractor.seven_zip",
            "telecrime.stealer",
            "telecrime.stealer.parser",
            "telecrime.channels",
            "telecrime.channels.discover",
            "telecrime.notify",
        ]:
            log = logging.getLogger(name)
            self._log_filters.append((log, log.level))
            log.setLevel(logging.WARNING)

    def _restore_loggers(self) -> None:
        """Restore original log levels."""
        for log, level in self._log_filters:
            log.setLevel(level)
        self._log_filters.clear()

    def _update(self) -> None:
        """Push a new render to the live display."""
        if self._live:
            self._live.update(self._render())

    def _elapsed(self) -> float:
        return time.time() - self._start_time if self._start_time else 0.0

    def _elapsed_str(self) -> str:
        elapsed = self._elapsed()
        if elapsed >= 60:
            return f"{int(elapsed // 60)}m{int(elapsed % 60):02d}s"
        return f"{int(elapsed)}s"

    def _render(self) -> Panel:
        """Build the display panel."""
        grid = Table.grid(padding=(0, 1))
        grid.add_column(style="bold cyan", min_width=10)
        grid.add_column(min_width=52)

        # Stage progress dots + label — spinner advances with wall clock at 4fps
        spinner_char = SPINNER[int(time.time() * 4) % len(SPINNER)]
        stage_dots = Text()
        for stage_name in STAGE_ORDER:
            if stage_name in self._failed_stages:
                stage_dots.append("✗", style="red")
            elif stage_name in self._completed_stages:
                stage_dots.append("✓", style="green")
            elif stage_name == self._current_stage:
                stage_dots.append(spinner_char, style="yellow bold")
            else:
                stage_dots.append("○", style="dim")
            stage_dots.append(" ", style="")

        if self._current_stage and self._current_stage in STAGE_ORDER:
            stage_label = STAGE_LABELS.get(self._current_stage, self._current_stage)
            stage_index = STAGE_ORDER.index(self._current_stage) + 1
            stage_dots.append(f" {stage_label} [{stage_index}/{len(STAGE_ORDER)}]", style="bold white")
        elif not self._current_stage and self._completed_stages:
            stage_dots.append(" Done", style="green bold")

        grid.add_row("Stage", stage_dots)

        # Archive line (only in sequential mode when we have archive info)
        if self._archive_total > 0:
            archive_text = Text()
            archive_text.append(f"[{self._archive_current}/{self._archive_total}] ", style="bold")

            # Mini 12-char progress bar
            pct = self._archive_current / self._archive_total
            bar_width = 12
            filled = int(bar_width * pct)
            empty = bar_width - filled
            archive_text.append("█" * filled, style="green")
            archive_text.append("░" * empty, style="dim")
            archive_text.append(f" {pct*100:3.0f}%  ", style="bold")

            if self._archive_name:
                name = self._archive_name
                if len(name) > 35:
                    name = name[:32] + "..."
                archive_text.append(name, style="dim")
            grid.add_row("Archive", archive_text)

        # Download progress bar
        if self._dl_active:
            bar_width = 20
            filled = int(bar_width * self._dl_pct / 100)
            empty = bar_width - filled
            dl_text = Text()
            dl_text.append("█" * filled, style="green")
            dl_text.append("░" * empty, style="dim")
            dl_text.append(f" {self._dl_pct:4.0f}%", style="bold")
            if self._dl_speed > 0:
                dl_text.append(f"  {self._dl_speed:.1f}MB/s", style="cyan")
            if self._dl_eta:
                dl_text.append(f"  {self._dl_eta}", style="dim")
            grid.add_row("Download", dl_text)

        # Recent archive results (most recent last = shown at bottom = most visible)
        if self._recent_results:
            results_list = list(self._recent_results)
            for i, result in enumerate(results_list):
                style = "green" if i == len(results_list) - 1 else "dim"
                grid.add_row("" if i > 0 else "Results", Text(result, style=style))

        # Separator
        grid.add_row("", Text("─" * 52, style="dim"))

        # Counters row with optional rate
        elapsed = self._elapsed()
        counters = Text()
        counters.append(f"Creds {self._creds:,}", style="green bold")
        if elapsed > 30 and self._creds > 0:
            rate = self._creds / (elapsed / 60)
            counters.append(f" ({rate:,.0f}/min)", style="dim")
        counters.append("   ", style="dim")
        counters.append(f"Dups {self._dups:,}", style="yellow")
        counters.append("   ", style="dim")
        counters.append(f"Msgs {self._messages:,}", style="blue")
        counters.append("   ", style="dim")
        counters.append(f"Errors {self._errors}", style="red" if self._errors > 0 else "dim")
        if self._channels_joined > 0:
            counters.append("   ", style="dim")
            counters.append(f"Channels +{self._channels_joined}", style="magenta")
        grid.add_row("", counters)

        return Panel(
            grid,
            title="[bold white]Telecrime[/bold white]",
            subtitle=self._elapsed_str(),
            border_style="bright_black",
        )

    # --- Public API called by pipeline stages ---

    def stage_start(self, name: str) -> None:
        """Mark a stage as currently running.

        Auto-completes the previous stage if one was active.
        Removes name from completed so the spinner shows even for cycled stages.
        """
        if self._current_stage and self._current_stage != name:
            self._completed_stages.add(self._current_stage)
        self._current_stage = name
        self._completed_stages.discard(name)
        self._dl_active = False
        self._update()

    def stage_complete(self, name: str) -> None:
        """Mark a stage as completed."""
        self._completed_stages.add(name)
        if self._current_stage == name:
            self._current_stage = None
        self._update()

    def stage_error(self, name: str) -> None:
        """Mark a stage as failed."""
        self._failed_stages.add(name)
        self._errors += 1
        self._update()

    def set_archive_total(self, total: int) -> None:
        """Set the total number of archives to process."""
        self._archive_total = total
        self._update()

    def archive_start(self, name: str) -> None:
        """Mark the start of processing an archive."""
        self._archive_current += 1
        self._archive_name = name
        self._update()

    def archive_complete(self, name: str, creds: int, dups: int = 0) -> None:
        """Mark an archive as done and update credential count.

        Resets per-archive stage indicators so the next archive starts fresh.
        """
        # Mark current stage done
        if self._current_stage:
            self._completed_stages.add(self._current_stage)
            self._current_stage = None
        # Do NOT update _dups here — update_dups() already maintains the cumulative
        # count from ctx.duplicates_skipped. Adding dups again would double-count.
        # _creds is already up-to-date from update_creds() calls during parse.
        self._dl_active = False

        # Append to recent results deque
        short = name if len(name) <= 32 else name[:29] + "..."
        self._recent_results.append(f"{short}: +{creds:,} new, {dups:,} dups")

        self._update()

    def download_start(self, filename: str, size_mb: float) -> None:
        """Start tracking a download."""
        self._dl_filename = filename
        self._dl_pct = 0.0
        self._dl_speed = 0.0
        self._dl_eta = ""
        self._dl_active = True
        self._update()

    def download_progress(self, pct: float, speed_mbps: float, eta: str) -> None:
        """Update download progress."""
        self._dl_pct = pct
        self._dl_speed = speed_mbps
        self._dl_eta = eta
        self._update()

    def download_complete(self) -> None:
        """Mark download as finished."""
        self._dl_pct = 100.0
        self._dl_active = False
        self._update()

    def update_messages(self, count: int) -> None:
        """Update the messages counter."""
        self._messages = count
        self._update()

    def update_creds(self, count: int) -> None:
        """Set the total credentials counter."""
        self._creds = count
        self._update()

    def update_counts(self, creds: int, dups: int) -> None:
        """Set both counters with a single redraw."""
        self._creds = creds
        self._dups = dups
        self._update()

    def add_error(self) -> None:
        """Increment the error counter."""
        self._errors += 1
        self._update()

    def channels_update(self, joined: int) -> None:
        """Update channel joined counter."""
        self._channels_joined = joined
        self._update()

    def print_summary(self, ctx: "PipelineContext") -> None:
        """Print final summary. Call after stop()."""
        self.console.print()
        self.console.print("[bold green]Pipeline completed![/bold green]")

        elapsed_str = self._elapsed_str()

        summary = Table.grid(padding=(0, 2))
        summary.add_column(style="cyan", min_width=20)
        summary.add_column(style="bold")

        summary.add_row("Duration", elapsed_str)
        summary.add_row("Conversations", f"{ctx.conversations_processed:,}")
        summary.add_row("Messages", f"{ctx.messages_processed:,}")
        summary.add_row("Files discovered", f"{ctx.files_discovered:,}")
        summary.add_row("Files downloaded", f"{ctx.files_downloaded:,}")
        summary.add_row("Archives extracted", f"{ctx.archives_extracted:,}")
        summary.add_row("Credentials parsed", f"{ctx.credentials_parsed:,}")
        if ctx.duplicates_skipped:
            summary.add_row("Duplicates skipped", f"{ctx.duplicates_skipped:,}")

        if ctx.errors:
            summary.add_row("Errors", f"[red]{len(ctx.errors)}[/red]")

        self.console.print(summary)

        if ctx.errors:
            self.console.print(f"\n[yellow]Errors ({len(ctx.errors)}):[/yellow]")
            for error in ctx.errors[:10]:
                self.console.print(f"  - {error}")

    def finish(self, ctx: "PipelineContext") -> None:
        """Stop display and print summary (convenience)."""
        self.stop()
        self.print_summary(ctx)
