"""Tests for password extraction and ranking."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

from telecrime.passwords.extractor import (
    _is_valid_password,
    _strip_telegram_markdown,
    extract_inline_passwords,
    extract_passwords_from_text,
    load_password_file,
    normalize_password,
)
from telecrime.passwords.ranker import (
    compute_score,
    deduplicate_candidates,
    get_ranking_reason,
    rank_passwords,
)
from telecrime.states import PasswordScope


def test_load_password_file_defaults_to_configured_data_dir(tmp_path, monkeypatch):
    data_dir = tmp_path / "runtime"
    data_dir.mkdir()
    (data_dir / "passwords.txt").write_text("secret123\n")
    monkeypatch.setenv("TELECRIME_DATA_DIR", str(data_dir))

    assert load_password_file() == ["secret123"]


class TestExtractPasswordsFromText:
    """Tests for extract_passwords_from_text function."""

    def test_password_colon_format(self):
        """Test 'password: xxx' format."""
        results = extract_passwords_from_text("Download here\npassword: secret123")

        assert len(results) >= 1
        passwords = [p for p, _ in results]
        assert "secret123" in passwords

    def test_pass_colon_format(self):
        """Test 'pass: xxx' format."""
        results = extract_passwords_from_text("File attached\npass: mypass")

        passwords = [p for p, _ in results]
        assert "mypass" in passwords

    def test_pwd_equals_format(self):
        """Test 'pwd = xxx' format."""
        results = extract_passwords_from_text("Archive\npwd = test123")

        passwords = [p for p, _ in results]
        assert "test123" in passwords

    def test_quoted_password(self):
        """Test quoted password extraction."""
        results = extract_passwords_from_text("password: \"complex pass\"")

        passwords = [p for p, _ in results]
        assert "complex pass" in passwords

    def test_backtick_quoted(self):
        """Test backtick quoted password."""
        results = extract_passwords_from_text("password: `special`")

        passwords = [p for p, _ in results]
        assert "special" in passwords

    def test_no_password(self):
        """Test text without password."""
        results = extract_passwords_from_text("Just a regular message with no secrets")

        # Should return empty or only low-confidence guesses
        high_confidence = [(p, c) for p, c in results if c > 0.7]
        assert len(high_confidence) == 0

    def test_empty_text(self):
        """Test empty text."""
        results = extract_passwords_from_text("")
        assert results == []

    def test_password_with_special_chars(self):
        """Test password with special characters."""
        results = extract_passwords_from_text("pass: P@ssw0rd!123")

        passwords = [p for p, _ in results]
        assert "P@ssw0rd!123" in passwords

    def test_password_trims_trailing_punctuation(self):
        """Test password value trimming for punctuation."""
        results = extract_passwords_from_text("password: secret123,")

        passwords = [p for p, _ in results]
        assert "secret123" in passwords

    def test_standalone_requires_digit_or_symbol(self):
        """Test standalone token must include digit or symbol."""
        results = extract_passwords_from_text("cloud\nupdate\nsecret\nonlyletters\npass123")

        passwords = [p for p, _ in results]
        assert "pass123" in passwords
        assert "onlyletters" not in passwords

    def test_telegram_markdown_bold_stripped(self):
        """Regression: Telegram ****@Channel**** markdown must not appear in extracted password."""
        # Real-world message from LogsPlanet channel re-posting BHF Cloud archives
        msg = (
            "****🔑**** Password: ****@BHFCloud****\n"
            "**[Admin](http://t.me/KolyvanAdmin)**"
        )
        results = extract_passwords_from_text(msg)
        passwords = [p for p, _ in results]
        # Must extract the clean value, not the markdown-wrapped one
        assert "@BHFCloud" in passwords
        # Must NOT contain any asterisks
        for pwd in passwords:
            assert "*" not in pwd, f"Markdown asterisks leaked into password: {pwd!r}"

    def test_strip_telegram_markdown_bold(self):
        """_strip_telegram_markdown removes ** and **** markers."""
        assert _strip_telegram_markdown("**bold**") == "bold"
        assert _strip_telegram_markdown("****bold****") == "bold"
        assert _strip_telegram_markdown("****🔑**** Password: ****@BHFCloud****") == "🔑 Password: @BHFCloud"

    def test_strip_telegram_markdown_links(self):
        """_strip_telegram_markdown keeps both link text and URL."""
        stripped = _strip_telegram_markdown("[Admin](http://t.me/KolyvanAdmin)")
        assert "Admin" in stripped
        assert "http://t.me/KolyvanAdmin" in stripped

    def test_strip_telegram_markdown_italic(self):
        """_strip_telegram_markdown removes __ italic markers."""
        assert _strip_telegram_markdown("__italic__") == "italic"

    def test_emoji_only_marker_lower_confidence(self):
        """Test emoji-only marker reduces confidence."""
        results = extract_passwords_from_text("🔑 secret123")

        for pwd, confidence in results:
            if pwd == "secret123":
                assert confidence < 0.8

    def test_inline_passwords_extract(self):
        """Test inline password extraction for filenames."""
        results = extract_inline_passwords("archive_pass=Secr3t.zip")
        passwords = [p for p, _ in results]
        assert "Secr3t" in passwords

    def test_inline_passwords_no_match(self):
        """Test inline password extraction ignores non-matches."""
        results = extract_inline_passwords("archive_final.zip")
        assert results == []

    def test_multiple_passwords(self):
        """Test multiple passwords in text."""
        text = """
        First archive: password: first123
        Second archive: pass: second456
        """
        results = extract_passwords_from_text(text)

        passwords = [p for p, _ in results]
        assert "first123" in passwords
        assert "second456" in passwords

    def test_confidence_higher_for_explicit(self):
        """Test that explicit passwords have higher confidence."""
        results = extract_passwords_from_text("password: explicit")

        # Explicit passwords should have confidence >= 0.8
        for pwd, confidence in results:
            if pwd == "explicit":
                assert confidence >= 0.8

    def test_multiple_passwords_plus_separated(self):
        """Test extraction of multiple passwords separated by ' + '."""
        results = extract_passwords_from_text("pass: @PegasusCloud + @EuropeCloud")
        passwords = [p for p, _ in results]
        assert "@PegasusCloud" in passwords
        assert "@EuropeCloud" in passwords

    def test_multiple_passwords_slash_separated(self):
        """Test extraction of multiple passwords separated by ' / '."""
        results = extract_passwords_from_text("pass: abc123 / xyz789")
        passwords = [p for p, _ in results]
        assert "abc123" in passwords
        assert "xyz789" in passwords

    def test_multiple_passwords_three_tokens(self):
        """Test extraction of three passwords on one line."""
        results = extract_passwords_from_text("pass: @AlphaCloud + @BetaCloud + @GammaCloud")
        passwords = [p for p, _ in results]
        assert "@AlphaCloud" in passwords
        assert "@BetaCloud" in passwords
        assert "@GammaCloud" in passwords


class TestIsValidPassword:
    """Tests for _is_valid_password helper."""

    def test_valid_password(self):
        """Test valid passwords."""
        assert _is_valid_password("secret123") is True
        assert _is_valid_password("P@ssw0rd!") is True
        assert _is_valid_password("abc") is True  # Minimum 3 chars

    def test_too_short(self):
        """Test too short passwords."""
        assert _is_valid_password("ab") is False
        assert _is_valid_password("") is False

    def test_too_long(self):
        """Test too long passwords."""
        assert _is_valid_password("a" * 101) is False

    def test_common_false_positives(self):
        """Test common false positives are rejected."""
        assert _is_valid_password("password") is False
        assert _is_valid_password("the") is False
        assert _is_valid_password("file") is False
        assert _is_valid_password("download") is False
        assert _is_valid_password("zip") is False
        assert _is_valid_password("rar") is False
        assert _is_valid_password("part001") is False

    def test_urls_rejected(self):
        """Test URLs are rejected."""
        assert _is_valid_password("http://example.com") is False
        assert _is_valid_password("www.example.com") is False


class TestNormalizePassword:
    """Tests for normalize_password function."""

    def test_strip_whitespace(self):
        """Test whitespace stripping."""
        assert normalize_password("  secret  ") == "secret"
        assert normalize_password("\tsecret\n") == "secret"

    def test_remove_quotes(self):
        """Test quote removal."""
        assert normalize_password("\"secret\"") == "secret"
        assert normalize_password("'secret'") == "secret"
        assert normalize_password("`secret`") == "secret"

    def test_preserve_inner_content(self):
        """Test inner content is preserved."""
        assert normalize_password("pass word") == "pass word"
        assert normalize_password("pass\"word") == "pass\"word"


class TestComputeScore:
    """Tests for compute_score function."""

    def _make_candidate(self, scope, confidence=0.5, succeeded=0, failed=0):
        """Create a mock PasswordCandidate."""
        mock = MagicMock()
        mock.scope = scope
        mock.confidence = confidence
        mock.times_succeeded = succeeded
        mock.times_failed = failed
        return mock

    def test_message_scope_highest(self):
        """Test MESSAGE scope has highest base score."""
        msg_candidate = self._make_candidate(PasswordScope.MESSAGE, 1.0)
        nearby_candidate = self._make_candidate(PasswordScope.NEARBY, 1.0)

        msg_score = compute_score(msg_candidate)
        nearby_score = compute_score(nearby_candidate)

        assert msg_score > nearby_score

    def test_confidence_affects_score(self):
        """Test confidence affects score."""
        high_conf = self._make_candidate(PasswordScope.MESSAGE, 0.9)
        low_conf = self._make_candidate(PasswordScope.MESSAGE, 0.3)

        assert compute_score(high_conf) > compute_score(low_conf)

    def test_success_history_boosts_score(self):
        """Test successful history boosts score."""
        no_history = self._make_candidate(PasswordScope.NEARBY, 0.5, 0, 0)
        success_history = self._make_candidate(PasswordScope.NEARBY, 0.5, 3, 0)

        assert compute_score(success_history) > compute_score(no_history)

    def test_failure_history_penalizes(self):
        """Test failure history penalizes score."""
        no_history = self._make_candidate(PasswordScope.NEARBY, 0.5, 0, 0)
        failure_history = self._make_candidate(PasswordScope.NEARBY, 0.5, 0, 5)

        assert compute_score(failure_history) < compute_score(no_history)

    def test_score_bounded(self):
        """Test score is bounded between 0 and 1."""
        # Best case
        best = self._make_candidate(PasswordScope.MESSAGE, 1.0, 10, 0)
        assert 0 <= compute_score(best) <= 1.0

        # Worst case
        worst = self._make_candidate(PasswordScope.GLOBAL, 0.1, 0, 10)
        assert 0 <= compute_score(worst) <= 1.0


class TestRankPasswords:
    """Tests for rank_passwords function."""

    def _make_candidate(self, value, scope, confidence=0.5):
        """Create a mock PasswordCandidate."""
        mock = MagicMock()
        mock.value = value
        mock.scope = scope
        mock.confidence = confidence
        mock.times_succeeded = 0
        mock.times_failed = 0
        mock.extraction_method = "test"
        return mock

    def test_ranking_order(self):
        """Test passwords are ranked in correct order."""
        candidates = [
            self._make_candidate("nearby", PasswordScope.NEARBY, 0.5),
            self._make_candidate("message", PasswordScope.MESSAGE, 0.9),
            self._make_candidate("global", PasswordScope.GLOBAL, 0.3),
        ]

        ranked = rank_passwords(candidates)

        # Message should be first (highest score)
        assert ranked[0].candidate.value == "message"
        # Global should be last (lowest score)
        assert ranked[-1].candidate.value == "global"

    def test_empty_input(self):
        """Test ranking with no candidates."""
        ranked = rank_passwords([])
        assert ranked == []


class TestGetRankingReason:
    """Tests for get_ranking_reason function."""

    def _make_candidate(self, scope, method="test", succeeded=0, failed=0, confidence=0.5):
        """Create a mock PasswordCandidate."""
        mock = MagicMock()
        mock.scope = scope
        mock.extraction_method = method
        mock.times_succeeded = succeeded
        mock.times_failed = failed
        mock.confidence = confidence
        return mock

    def test_scope_in_reason(self):
        """Test scope is mentioned in reason."""
        candidate = self._make_candidate(PasswordScope.MESSAGE)
        reason = get_ranking_reason(candidate)

        assert "caption" in reason.lower() or "message" in reason.lower()

    def test_success_mentioned(self):
        """Test success count is mentioned."""
        candidate = self._make_candidate(PasswordScope.MESSAGE, succeeded=5)
        reason = get_ranking_reason(candidate)

        assert "succeeded" in reason.lower()
        assert "5" in reason

    def test_high_confidence_mentioned(self):
        """Test high confidence is mentioned."""
        candidate = self._make_candidate(PasswordScope.MESSAGE, confidence=0.95)
        reason = get_ranking_reason(candidate)

        assert "high confidence" in reason.lower()


class TestDeduplicateCandidates:
    """Tests for deduplicate_candidates function."""

    def _make_candidate(self, value):
        """Create a mock PasswordCandidate."""
        mock = MagicMock()
        mock.value = value
        return mock

    def test_removes_duplicates(self):
        """Test duplicate values are removed."""
        candidates = [
            self._make_candidate("password1"),
            self._make_candidate("password2"),
            self._make_candidate("password1"),  # Duplicate
        ]

        unique = deduplicate_candidates(candidates)

        assert len(unique) == 2
        values = [c.value for c in unique]
        assert "password1" in values
        assert "password2" in values

    def test_keeps_first_occurrence(self):
        """Test first occurrence is kept (for ranked lists)."""
        first = self._make_candidate("test")
        second = self._make_candidate("test")

        unique = deduplicate_candidates([first, second])

        assert len(unique) == 1
        assert unique[0] is first

    def test_case_insensitive(self):
        """Test deduplication is case-insensitive."""
        candidates = [
            self._make_candidate("Password"),
            self._make_candidate("password"),
            self._make_candidate("PASSWORD"),
        ]

        unique = deduplicate_candidates(candidates)

        assert len(unique) == 1

    def test_empty_input(self):
        """Test empty input."""
        assert deduplicate_candidates([]) == []


class TestExtractPasswordsFromContext:
    """Tests for extract_passwords_from_context (the production entry point)."""

    def test_channel_username_is_high_priority(self, session, test_config):
        """Channel username variations become high-confidence candidates."""
        from telecrime.models import Conversation, Message
        from telecrime.passwords.extractor import extract_passwords_from_context

        conv = Conversation(
            platform_id=1, username="examplecloud", conversation_type="channel"
        )
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="archive file",
        )
        session.add(msg)
        session.flush()

        import asyncio
        candidates = asyncio.run(extract_passwords_from_context(
            session, msg, test_config
        ))

        values = {c.value for c in candidates}
        assert "examplecloud" in values
        assert "@examplecloud" in values
        assert all(c.scope == PasswordScope.MESSAGE for c in candidates)
        assert all(c.confidence == 0.95 for c in candidates)

    def test_caption_password(self, session, test_config):
        """Passwords marked in the caption are extracted."""
        from telecrime.models import Conversation, Message
        from telecrime.passwords.extractor import extract_passwords_from_context

        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="Download link\npassword: secret123",
        )
        session.add(msg)
        session.flush()

        import asyncio
        candidates = asyncio.run(extract_passwords_from_context(
            session, msg, test_config
        ))

        values = {c.value for c in candidates}
        assert "secret123" in values
        caption_c = next(c for c in candidates if c.extraction_method == "caption")
        assert caption_c.scope == PasswordScope.MESSAGE

    def test_attachment_filename_password(self, session, test_config):
        """Passwords embedded in the archive filename are extracted."""
        from telecrime.models import Conversation, Message
        from telecrime.passwords.extractor import extract_passwords_from_context

        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="file",
        )
        session.add(msg)
        session.flush()

        import asyncio
        candidates = asyncio.run(extract_passwords_from_context(
            session, msg, test_config,
            attachment_filename="logs_pass_9f3x2k.zip",
        ))

        values = {c.value for c in candidates}
        assert "9f3x2k" in values
        file_c = next(c for c in candidates if c.extraction_method == "filename")
        assert file_c.scope == PasswordScope.MESSAGE

    def test_nearby_message_passwords_lower_confidence(self, session, test_config):
        """Passwords from nearby messages get NEARBY scope and lower confidence."""
        from telecrime.models import Conversation, Message
        from telecrime.passwords.extractor import extract_passwords_from_context

        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        main_msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="here is the archive",
        )
        session.add(main_msg)
        nearby = Message(
            conversation_id=conv.id,
            platform_id=9,
            platform_timestamp=datetime.now(UTC),
            text="password: nearbypass99",
        )
        session.add(nearby)
        session.flush()

        import asyncio
        candidates = asyncio.run(extract_passwords_from_context(
            session, main_msg, test_config
        ))

        nearby_c = [c for c in candidates if c.extraction_method == "nearby"]
        assert any(c.value == "nearbypass99" for c in nearby_c)
        for c in nearby_c:
            assert c.scope == PasswordScope.NEARBY
            assert c.confidence < 0.9

    def test_duplicates_across_sources_are_deduped(self, session, test_config):
        """The same password from multiple sources appears only once."""
        from telecrime.models import Conversation, Message
        from telecrime.passwords.extractor import extract_passwords_from_context

        conv = Conversation(
            platform_id=1, username="dupcloud", conversation_type="channel"
        )
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="password: dupcloud",
        )
        session.add(msg)
        session.flush()

        import asyncio
        candidates = asyncio.run(extract_passwords_from_context(
            session, msg, test_config
        ))

        values = [c.value for c in candidates]
        assert values.count("dupcloud") == 1
