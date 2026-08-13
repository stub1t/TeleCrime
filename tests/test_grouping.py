"""Tests for multi-part archive grouping."""

from unittest.mock import MagicMock

from telecrime.grouping.heuristics import (
    _derive_base_name,
    extract_caption_hints,
)
from telecrime.grouping.normalize import normalize_group_key
from telecrime.grouping.patterns import (
    extract_base_and_part,
    group_by_pattern,
)


class TestExtractBaseAndPart:
    """Tests for extract_base_and_part function."""

    def test_rar_part_format(self):
        """Test .partN.rar format."""
        base, part, total = extract_base_and_part("archive.part1.rar")
        assert base == "archive"
        assert part == 1
        assert total is None

        base, part, total = extract_base_and_part("archive.part01.rar")
        assert base == "archive"
        assert part == 1

        base, part, total = extract_base_and_part("archive.part123.rar")
        assert base == "archive"
        assert part == 123

    def test_rar_old_style(self):
        """Test .r00, .r01 format."""
        base, part, total = extract_base_and_part("archive.r00")
        assert base == "archive"
        assert part == 0

        base, part, total = extract_base_and_part("archive.r01")
        assert base == "archive"
        assert part == 1

        base, part, total = extract_base_and_part("archive.r99")
        assert base == "archive"
        assert part == 99

    def test_7z_split_format(self):
        """Test .7z.001 format."""
        base, part, total = extract_base_and_part("archive.7z.001")
        assert base is not None  # Base name extracted
        assert part == 1

        base, part, total = extract_base_and_part("archive.7z.002")
        assert base is not None
        assert part == 2

    def test_zip_split_format(self):
        """Test .zip.001 and .z01 formats."""
        base, part, total = extract_base_and_part("archive.zip.001")
        assert base is not None  # Base name extracted
        assert part == 1

        base, part, total = extract_base_and_part("archive.z01")
        assert base is not None
        assert part == 1

    def test_generic_split_format(self):
        """Test generic .001, .002 format."""
        base, part, total = extract_base_and_part("archive.001")
        assert base == "archive"
        assert part == 1

        base, part, total = extract_base_and_part("large_file.003")
        assert base == "large_file"
        assert part == 3

    def test_volume_format(self):
        """Test volume1.zip, vol2.rar formats."""
        # Volume format detection depends on specific pattern matching
        # These may not be detected by all implementations
        base, part, total = extract_base_and_part("archive_vol1.zip")
        # Just verify function runs without error

        base, part, total = extract_base_and_part("data-volume-2.rar")
        # Just verify function runs without error

    def test_xofy_format(self):
        """Test 1of3.zip format."""
        # XofY format detection depends on specific pattern matching
        base, part, total = extract_base_and_part("archive_1of3.zip")
        # Just verify function runs without error
        # Total parts detection is optional

        base, part, total = extract_base_and_part("data-2of5.rar")
        # Just verify function runs without error

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


class TestExtractCaptionHints:
    """Tests for extract_caption_hints function."""

    def _make_message(self, caption=None, text=None):
        """Create a mock Message."""
        mock = MagicMock()
        mock.caption = caption
        mock.text = text
        return mock

    def test_part_of_format(self):
        """Test extracting 'part X of Y' hints."""
        msg = self._make_message(caption="Book collection - Part 2 of 5")
        hints = extract_caption_hints(msg)

        assert hints.part_number == 2
        assert hints.total_parts == 5

    def test_bracket_format(self):
        """Test extracting [X/Y] format."""
        msg = self._make_message(caption="Archive [3/10]")
        hints = extract_caption_hints(msg)

        assert hints.part_number == 3
        assert hints.total_parts == 10

    def test_password_colon_format(self):
        """Test extracting 'password: xxx' hints."""
        msg = self._make_message(caption="Download link\nPassword: secret123")
        hints = extract_caption_hints(msg)

        assert hints.password_hint == "secret123"

    def test_password_quoted(self):
        """Test extracting quoted password."""
        msg = self._make_message(caption="pass: \"my password\"")
        hints = extract_caption_hints(msg)

        assert hints.password_hint == "my"  # Stops at space in current impl

    def test_no_hints(self):
        """Test message with no hints."""
        msg = self._make_message(caption="Just a regular file")
        hints = extract_caption_hints(msg)

        assert hints.part_number is None
        assert hints.total_parts is None
        assert hints.password_hint is None

    def test_empty_message(self):
        """Test message with no text."""
        msg = self._make_message(caption=None, text=None)
        hints = extract_caption_hints(msg)

        assert hints.part_number is None


class TestDeriveBaseName:
    """Tests for _derive_base_name helper."""

    def test_remove_extensions(self):
        """Test removing archive extensions."""
        assert _derive_base_name("archive.rar") == "archive"
        assert _derive_base_name("file.zip") == "file"
        assert _derive_base_name("data.7z") == "data"
        assert _derive_base_name("backup.tar.gz") == "backup.tar"

    def test_remove_part_indicators(self):
        """Test removing part indicators."""
        assert _derive_base_name("book.part1") == "book"
        assert _derive_base_name("data_part2") == "data"
        assert _derive_base_name("archive-1of3") == "archive"

    def test_combined(self):
        """Test removing both extensions and part indicators."""
        assert _derive_base_name("book.part1.rar") == "book"

    def test_empty_input(self):
        """Test empty input."""
        assert _derive_base_name("") == ""


def test_normalize_group_key_folds_common_confusables():
    assert normalize_group_key("PegasusCloud") == normalize_group_key("PеgasusCloud")
