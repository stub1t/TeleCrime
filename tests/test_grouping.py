"""Tests for multi-part archive grouping."""

from unittest.mock import MagicMock

import pytest

from telecrime.grouping.normalize import normalize_group_key
from telecrime.grouping.patterns import (
    extract_base_and_part,
    group_by_pattern,
)


class TestExtractBaseAndPart:
    """Tests for extract_base_and_part function."""

    @pytest.mark.parametrize(
        "filename, base, part, total",
        [
            ("archive.part1.rar", "archive", 1, None),
            ("archive.part01.rar", "archive", 1, None),
            ("archive.part123.rar", "archive", 123, None),
            ("archive.part1.zip", "archive", 1, None),
            ("archive.part2.7z", "archive", 2, None),
            ("archive.part01.zip", "archive", 1, None),
            ("archive.r00", "archive", 0, None),
            ("archive.r01", "archive", 1, None),
            ("archive.r99", "archive", 99, None),
            ("archive.7z.001", "archive", 1, None),
            ("archive.7z.002", "archive", 2, None),
            ("archive.zip.001", "archive", 1, None),
            ("archive.z01", "archive", 1, None),
            ("archive.001", "archive", 1, None),
            ("large_file.003", "large_file", 3, None),
            ("archive_vol1.zip", "archive", 1, None),
            ("data-volume-2.rar", "data-volume-2", None, None),
            ("archive_1of3.zip", "archive", 1, 3),
            ("data-2of5.rar", "data", 2, 5),
            ("ARCHIVE.PART1.RAR", "ARCHIVE", 1, None),
            ("Data.7Z.001", "Data", 1, None),
        ],
    )
    def test_extract_base_and_part(self, filename, base, part, total):
        """Pure filename table for extract_base_and_part."""
        result_base, result_part, result_total = extract_base_and_part(filename)
        assert result_base == base
        assert result_part == part
        assert result_total == total

    def test_no_match(self):
        """Test non-split archive filenames."""
        base, part, total = extract_base_and_part("archive.zip")
        assert base is None
        assert part is None

        base, part, total = extract_base_and_part("document.pdf")
        assert base is None
        assert part is None

        base, part, total = extract_base_and_part("")
        assert base is None
        assert part is None

    def test_case_insensitivity(self):
        """Test case insensitive matching."""
        base, part, _ = extract_base_and_part("ARCHIVE.PART1.RAR")
        assert base is not None  # Should detect regardless of case
        assert part == 1

        base, part, _ = extract_base_and_part("Data.7Z.001")
        assert base is not None
        assert part == 1


class TestGroupByPattern:
    """Tests for group_by_pattern function."""

    def _make_attachment(self, filename, attachment_id=None):
        """Create a mock FileAttachment."""
        mock = MagicMock()
        mock.filename = filename
        mock.id = attachment_id or id(mock)
        mock.detected_base_name = None
        mock.detected_part_number = None
        return mock

    def test_single_file(self):
        """Test grouping a single file."""
        attachments = [self._make_attachment("archive.zip")]
        results = group_by_pattern(attachments)

        assert len(results) == 1
        assert results[0].base_name == "archive.zip"
        assert len(results[0].attachments) == 1
        assert results[0].expected_parts == 1

    def test_multi_part_rar(self):
        """Test grouping multi-part RAR."""
        attachments = [
            self._make_attachment("book.part1.rar", 1),
            self._make_attachment("book.part2.rar", 2),
            self._make_attachment("book.part3.rar", 3),
        ]
        results = group_by_pattern(attachments)

        # Should create one multi-part group
        multi_part = [r for r in results if len(r.attachments) > 1]
        assert len(multi_part) == 1
        assert multi_part[0].expected_parts >= 3  # At least 3 parts
        assert len(multi_part[0].attachments) == 3

    def test_multi_part_7z(self):
        """Test grouping multi-part 7z."""
        attachments = [
            self._make_attachment("data.7z.001", 1),
            self._make_attachment("data.7z.002", 2),
        ]
        results = group_by_pattern(attachments)

        multi_part = [r for r in results if len(r.attachments) > 1]
        assert len(multi_part) == 1
        assert multi_part[0].expected_parts >= 2  # At least 2 parts

    def test_mixed_archives(self):
        """Test grouping mixed archive types."""
        attachments = [
            self._make_attachment("book.part1.rar", 1),
            self._make_attachment("book.part2.rar", 2),
            self._make_attachment("other.zip", 3),
            self._make_attachment("data.7z.001", 4),
            self._make_attachment("data.7z.002", 5),
        ]
        results = group_by_pattern(attachments)

        # Should have 3 groups: book (2 parts), other (1), data (2 parts)
        assert len(results) == 3

        # Find the groups
        book_group = next((r for r in results if "book" in r.base_name.lower()), None)
        data_group = next((r for r in results if "data" in r.base_name.lower()), None)
        other_group = next((r for r in results if "other" in r.base_name.lower()), None)

        assert book_group is not None
        assert len(book_group.attachments) == 2

        assert data_group is not None
        assert len(data_group.attachments) == 2

        assert other_group is not None
        assert len(other_group.attachments) == 1

    def test_empty_input(self):
        """Test grouping with no attachments."""
        results = group_by_pattern([])
        assert results == []

    def test_confusable_filenames_group_together(self):
        attachments = [
            self._make_attachment("PegasusCloud.part1.rar", 1),
            self._make_attachment("PеgasusCloud.part2.rar", 2),
            self._make_attachment("PegаsusCloud.part3.rar", 3),
        ]
        results = group_by_pattern(attachments)
        multi_part = [r for r in results if len(r.attachments) > 1]
        assert len(multi_part) == 1
        assert len(multi_part[0].attachments) == 3


def test_normalize_group_key_folds_common_confusables():
    assert normalize_group_key("PegasusCloud") == normalize_group_key("PеgasusCloud")
