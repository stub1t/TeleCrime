"""Export and serialization helpers for the web dashboard."""

import re
from datetime import datetime
from html import unescape


def _serialize_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
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


def _csv_stream(headers: list[str], rows: list[list[object]], *, no_markdown: bool = False):
    yield ",".join(headers) + "\n"
    for row in rows:
        escaped = []
        for value in row:
            text = "" if value is None else str(_export_value(value, no_markdown=no_markdown))
            text = text.replace('"', '""')
            escaped.append(f'"{text}"')
        yield ",".join(escaped) + "\n"


def _markdown_cell(value: object, *, no_markdown: bool) -> str:
    rendered = _export_value(value, no_markdown=no_markdown)
    text = "" if rendered is None else str(rendered)
    text = text.replace("|", r"\|")
    text = text.replace("\r\n", "<br>").replace("\n", "<br>")
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
