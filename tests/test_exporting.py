"""Tests for CSV export serialization in ``telecrime.web.exporting``.

Regression coverage for standards-compliant CSV quoting (embedded commas,
quotes and newlines) and for control-byte sanitization of credential fields.
"""

import csv
import io

from telecrime.web.exporting import _csv_stream


def _parse_stream(headers, rows):
    document = "".join(_csv_stream(headers, rows))
    return list(csv.reader(io.StringIO(document))), document


def test_csv_quotes_passwords_with_commas_quotes_and_newlines():
    """Embedded delimiters, quotes and line breaks must not desync columns;
    a standards CSV reader must round-trip the exact password."""
    headers = ["username", "password"]
    rows = [
        ("alice", "pa,ss"),
        ("bob", 'pa"ss'),
        ("carol", "line1\nline2"),
        ("dave", "cr\rlf\r\nend"),
    ]

    parsed, document = _parse_stream(headers, rows)

    assert parsed[0] == headers
    assert [tuple(row) for row in parsed[1:]] == [
        ("alice", "pa,ss"),
        ("bob", 'pa"ss'),
        ("carol", "line1\nline2"),
        ("dave", "cr\rlf\r\nend"),
    ]
    assert '"pa,ss"' in document
    assert '"pa""ss"' in document


def test_csv_strips_illegal_control_bytes_but_keeps_tab_lf_cr():
    """NUL and other C0 controls (except TAB/LF/CR) are removed; TAB, LF and
    CR are legal CSV content and survive field quoting."""
    headers = ["password"]
    rows = [
        ("pa\x00ss",),
        ("ctrl\x07char",),
        ("bell\x1f\x0b\x0c",),
        ("tab\tand\nnew\nline",),
    ]

    parsed, _ = _parse_stream(headers, rows)

    assert [row[0] for row in parsed[1:]] == [
        "pass",
        "ctrlchar",
        "bell",
        "tab\tand\nnew\nline",
    ]


def test_csv_none_values_become_empty_fields():
    parsed, _ = _parse_stream(["a", "b"], [(None, "x"), ("y", None)])
    assert parsed == [["a", "b"], ["", "x"], ["y", ""]]
