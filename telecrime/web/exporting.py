"""Export and serialization helpers for the web dashboard."""

import csv
import io
import re
from collections.abc import Iterable
from datetime import datetime
from html import unescape

# Characters openpyxl (and Excel) reject outright: C0 controls except TAB,
# LF and CR. NUL also truncates CSV fields in several parsers. Values are
# sanitized in _serialize_value so CSV, JSON, Markdown and XLSX exports all
# stay valid for credentials containing control bytes.
_ILLEGAL_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _serialize_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return _ILLEGAL_CONTROL_CHARS.sub("", value)
    return value


def _strip_markdown(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value
    text = re.sub(r"!?\[([^\]]+)\]\([^\)]+\)", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"\1", text)
    text = re.sub(r"(?<!_)_([^_]+)_(?!_)", r"\1", text)
    return unescape(text)


def _export_value(value: object, *, no_markdown: bool) -> object:
    serialized = _serialize_value(value)
    if no_markdown:
        return _strip_markdown(serialized)
    return serialized


def _serialize_row(obj, fields: list[str], *, no_markdown: bool = False) -> dict[str, object]:
    data = {}
    for field in fields:
        data[field] = _export_value(getattr(obj, field), no_markdown=no_markdown)
    return data


def _csv_stream(
    headers: list[str],
    rows: Iterable[Iterable[object]],
    *,
    no_markdown: bool = False,
):
    """Stream an RFC 4180 CSV document (quoting handles commas/quotes/newlines).

    The ``csv`` module is used instead of hand-rolled quoting so embedded
    delimiters, quotes and line breaks can never desynchronize columns or
    rows. ``rows`` may be any iterable (including a generator) so callers can
    stream large result sets without materializing them.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(headers)
    yield buffer.getvalue()
    for row in rows:
        buffer.seek(0)
        buffer.truncate(0)
        writer.writerow(
            [
                "" if value is None else str(_export_value(value, no_markdown=no_markdown))
                for value in row
            ]
        )
        yield buffer.getvalue()


def _markdown_cell(value: object, *, no_markdown: bool) -> str:
    rendered = _export_value(value, no_markdown=no_markdown)
    text = "" if rendered is None else str(rendered)
    # Escape backslashes first, otherwise an existing escape (e.g. "a\|b")
    # would be double-escaped and the trailing "\|" still split the cell.
    text = text.replace("\\", "\\\\")
    text = text.replace("|", r"\|")
    text = text.replace("\r\n", "<br>").replace("\n", "<br>").replace("\r", "<br>")
    return text


def _markdown_table(
    title: str, headers: list[str], rows: list[list[object]], *, no_markdown: bool
) -> str:
    lines = [f"## {title}", ""]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(_markdown_cell(value, no_markdown=no_markdown) for value in row)
            + " |"
        )
    lines.append("")
    return "\n".join(lines)
