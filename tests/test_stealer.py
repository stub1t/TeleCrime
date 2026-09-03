"""Tests for stealer log parsing."""

import pytest

from telecrime.stealer.models import Credential, StealerLog
from telecrime.stealer.parser import (
    parse_credentials_text,
    parse_system_info,
)
from telecrime.stealer.patterns import (
    detect_stealer_type,
    is_credential_file,
    is_system_info_file,
)


class TestCredentialModel:
    """Tests for Credential dataclass."""

    def test_domain_extraction(self):
        """Test domain is extracted from URL."""
        cred = Credential(
            url="https://login.example.com/auth",
            username="user",
            password="pass",
        )
        assert cred.domain == "login.example.com"

    def test_email_domain_extraction(self):
        """Test email domain is extracted from username."""
        cred = Credential(
            url="https://example.com",
            username="user@gmail.com",
            password="pass",
        )
        assert cred.email_domain == "gmail.com"

    def test_domain_extraction_non_http_scheme_without_path(self):
        """Fallback strips scheme even when the URL has no path component."""
        cred = Credential(url="ftp://example.com", username="user", password="pass")
        assert cred.domain == "example.com"

    def test_domain_extraction_schemeless_url_with_path(self):
        """Fallback takes the host part of a schemeless URL (urlparse behaviour)."""
        cred = Credential(url="example.com/path", username="user", password="pass")
        assert cred.domain == "example.com"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        cred = Credential(
            url="https://example.com",
            username="user",
            password="pass",
            application="Chrome",
        )
        d = cred.to_dict()
        assert d["url"] == "https://example.com"
        assert d["username"] == "user"
        assert d["application"] == "Chrome"


class TestCredentialHash:
    """Tests for ParsedCredential.compute_hash."""

    def test_consistent_hash(self):
        """Same inputs always produce the same hash."""
        from telecrime.models.credential import ParsedCredential

        h1 = ParsedCredential.compute_hash("example.com", "user", "pass")
        h2 = ParsedCredential.compute_hash("example.com", "user", "pass")
        assert h1 == h2

    def test_hash_is_64_char_hex(self):
        """Hash is a 64-character hex string (SHA256)."""
        from telecrime.models.credential import ParsedCredential

        h = ParsedCredential.compute_hash("example.com", "user", "pass")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_different_inputs_different_hash(self):
        """Different domains, users, and passwords produce different hashes."""
        from telecrime.models.credential import ParsedCredential

        h1 = ParsedCredential.compute_hash("a.com", "user", "pass")
        h2 = ParsedCredential.compute_hash("b.com", "user", "pass")
        h3 = ParsedCredential.compute_hash("a.com", "other", "pass")
        h4 = ParsedCredential.compute_hash("a.com", "user", "other")
        assert len({h1, h2, h3, h4}) == 4

    def test_empty_inputs(self):
        """Empty strings produce a valid hash."""
        from telecrime.models.credential import ParsedCredential

        h = ParsedCredential.compute_hash("", "", "")
        assert len(h) == 64

    def test_unicode_inputs(self):
        """Unicode inputs produce a valid hash."""
        from telecrime.models.credential import ParsedCredential

        h = ParsedCredential.compute_hash("example.com", "user@example.com", "p\u00e4ssw\u00f6rd")
        assert len(h) == 64


class TestPatterns:
    """Tests for file pattern matching."""

    def test_credential_file_patterns(self):
        """Test credential file pattern matching."""
        assert is_credential_file("Passwords.txt")
        assert is_credential_file("passwords.txt")
        assert is_credential_file("All Passwords.txt")
        assert is_credential_file("AllPasswords_list.txt")
        assert is_credential_file("_AllPasswords_list.txt")
        assert is_credential_file("Google_[Chrome]_Default.txt")
        assert is_credential_file("Mozilla_[Firefox]_default.txt")

        # Should not match
        assert not is_credential_file("readme.txt")
        assert not is_credential_file("notes.txt")

    def test_system_info_patterns(self):
        """Test system info file pattern matching."""
        assert is_system_info_file("SystemInfo.txt")
        assert is_system_info_file("System Information.txt")
        assert is_system_info_file("UserInformation.txt")
        assert is_system_info_file("PC Info.txt")

        assert not is_system_info_file("passwords.txt")

    def test_find_credential_files(self):
        """Filename gate: credential files qualify, system info/readme don't."""
        from telecrime.stealer.patterns import is_credential_file

        files = [
            "victim1/Passwords.txt",
            "victim1/Cookies.txt",
            "victim1/SystemInfo.txt",
            "victim2/All Passwords.txt",
            "readme.txt",
        ]
        matches = [f for f in files if is_credential_file(f)]
        assert len(matches) == 2
        assert "victim1/Passwords.txt" in matches
        assert "victim2/All Passwords.txt" in matches

    @pytest.mark.parametrize(
        "files, expected",
        [
            (["DomainDetects.txt", "Passwords.txt", "InstalledBrowsers.txt"], "redline"),
            (["MachineInfo.txt", "Passwords.txt"], "raccoon"),
        ],
    )
    def test_detect_stealer_type_by_files(self, files, expected):
        """Detect stealer family from file signatures."""
        assert detect_stealer_type(files) == expected

    def test_detect_stealer_from_content(self):
        """Test detection from content signatures."""
        files = ["Passwords.txt"]
        content = "=== REDLINE STEALER ==="
        assert detect_stealer_type(files, content) == "redline"


class TestCredentialParser:
    """Tests for credential parsing."""

    def test_parse_labeled_format(self):
        """Test parsing labeled format (Soft/Host/Login/Password)."""
        text = """
Soft: Google Chrome [Default]
Host: https://accounts.google.com
Login: user@gmail.com
Password: TEST_PASSWORD_123

Soft: Mozilla Firefox
Host: https://twitter.com
Login: twitteruser
Password: twitterpass
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 2

        assert creds[0].url == "https://accounts.google.com"
        assert creds[0].username == "user@gmail.com"
        assert creds[0].password == "TEST_PASSWORD_123"
        assert creds[0].application == "Google Chrome [Default]"

        assert creds[1].url == "https://twitter.com"
        assert creds[1].username == "twitteruser"
        assert creds[1].password == "twitterpass"

    def test_parse_url_username_password_format(self):
        """Test parsing URL/Username/Password format."""
        text = """
URL: https://facebook.com
Username: fbuser
Password: fbpass

URL: https://instagram.com
Username: instauser
Password: instapass
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 2
        assert creds[0].url == "https://facebook.com"
        assert creds[0].username == "fbuser"

    def test_parse_bracket_header_format(self):
        """Test parsing ["Browser" = "Profile"] format."""
        text = """
["Chrome" = "Default"]
Hostname: https://netflix.com
Username: viewer@email.com
Password: watchme123
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 1
        assert creds[0].url == "https://netflix.com"
        assert creds[0].application == "Chrome"
        assert creds[0].profile == "Default"

    @pytest.mark.parametrize(
        "text, url, username, password",
        [
            ("https://example.com:user1:pass1\nhttps://other.com:user2:pass2",
             "https://example.com", "user1", "pass1"),
            ("https://host:8080:user1:pass1", "https://host:8080", "user1", "pass1"),
            ("https://site1.com | admin | secret\nhttps://site2.com | root | toor",
             "https://site1.com", "admin", "secret"),
            ("https://login.example.com;user@example.com;p@ssw0rd",
             "https://login.example.com", "user@example.com", "p@ssw0rd"),
        ],
    )
    def test_separator_formats_dual_path(self, tmp_path, text, url, username, password):
        """Each separator format parses identically through the text parser AND
        the production streaming path (regression: the streaming colon branch
        had zero coverage and drifted from the text parser)."""
        from telecrime.stealer.parser import iter_credentials_file

        # Text entry point
        creds = parse_credentials_text(text)
        assert creds
        assert creds[0].url == url
        assert creds[0].username == username
        assert creds[0].password == password

        # Streaming entry point (production)
        f = tmp_path / "Passwords.txt"
        f.write_text(text + "\n", encoding="utf-8")
        streamed = list(iter_credentials_file(f))
        assert streamed
        assert streamed[0].url == url
        assert streamed[0].username == username
        assert streamed[0].password == password

    def test_parse_semicolon_password_with_semicolon(self):
        """Password field may contain semicolons after the second delimiter."""
        text = "https://example.com;user;p;a;s;s\n"
        creds = parse_credentials_text(text)
        assert len(creds) == 1
        assert creds[0].password == "p;a;s;s"

    def test_parse_strips_marketplace_boilerplate_from_fields(self):
        text = """
https://example.com:user@example.com:secret123 | https://t.me/SampleCloud You can buy dm @SampleCloud
https://other.example:user@example.com[to buy @seller]:secret456
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 2
        assert creds[0].username == "user@example.com"
        assert creds[0].password == "secret123"
        assert creds[1].username == "user@example.com"
        assert creds[1].password == "secret456"

    def test_parse_strips_uppercase_marketplace_boilerplate(self):
        """Promo regexes are case-insensitive; the fast-path trigger must be too."""
        text = """
https://example.com:user@example.com:secret123 | HTTPS://T.ME/X YOU CAN BUY DM @X
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 1
        assert creds[0].username == "user@example.com"
        assert creds[0].password == "secret123"

    def test_garbage_sentinels_fit_length_guard(self):
        """_is_garbage_credential skips the sentinel lookup for values >15 chars;
        every sentinel must fit within that guard or it would never match."""
        from telecrime.stealer.parser import _GARBAGE_PASSWORDS, _GARBAGE_USERNAMES

        assert all(len(v) <= 15 for v in _GARBAGE_USERNAMES | _GARBAGE_PASSWORDS)

    def test_deduplication(self):
        """Test that duplicate credentials are removed."""
        text = """
URL: https://example.com
Username: user
Password: pass

URL: https://example.com
Username: user
Password: pass
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 1

    def test_empty_input(self):
        """Test parsing empty input."""
        creds = parse_credentials_text("")
        assert creds == []

    def test_no_valid_credentials(self):
        """Test input with no valid credentials."""
        text = """
This is just some random text
without any credentials
"""
        creds = parse_credentials_text(text)
        assert creds == []


class TestSystemInfoParser:
    """Tests for system info parsing."""

    def test_parse_system_info(self):
        """Test parsing system information."""
        text = """
Hostname: DESKTOP-ABC123
Username: JohnDoe
IP: 192.168.1.100
Country: United States
HWID: ABC123DEF456
OS: Windows 10 Pro
CPU: Intel Core i7-9700K
GPU: NVIDIA GeForce RTX 2080
RAM: 16 GB
Timezone: UTC-5
Language: en-US
Screen: 1920x1080
"""
        info = parse_system_info(text)

        assert info.hostname == "DESKTOP-ABC123"
        assert info.username == "JohnDoe"
        assert info.ip_address == "192.168.1.100"
        assert info.country == "United States"
        assert info.hwid == "ABC123DEF456"
        assert info.os == "Windows 10 Pro"
        assert info.cpu == "Intel Core i7-9700K"
        assert info.gpu == "NVIDIA GeForce RTX 2080"

    def test_parse_system_info_variations(self):
        """Test parsing with field name variations."""
        text = """
ComputerName: WORKSTATION1
User: admin
PublicIP: 8.8.8.8
OperatingSystem: Windows 11
"""
        info = parse_system_info(text)

        assert info.hostname == "WORKSTATION1"
        assert info.username == "admin"
        assert info.ip_address == "8.8.8.8"
        assert info.os == "Windows 11"


class TestStealerLog:
    """Tests for StealerLog model."""

    def test_credential_count(self):
        """Test credential count property."""
        log = StealerLog(
            credentials=[
                Credential(url="https://a.com", username="u1", password="p1"),
                Credential(url="https://b.com", username="u2", password="p2"),
            ]
        )
        assert log.credential_count == 2

    def test_unique_domains(self):
        """Test unique domains property."""
        log = StealerLog(
            credentials=[
                Credential(url="https://google.com/a", username="u1", password="p1"),
                Credential(url="https://google.com/b", username="u2", password="p2"),
                Credential(url="https://facebook.com", username="u3", password="p3"),
            ]
        )
        assert log.unique_domains == {"google.com", "facebook.com"}

    def test_to_dict(self):
        """Test conversion to dictionary."""
        log = StealerLog(
            stealer_name="redline",
            source_archive="log.zip",
            credentials=[
                Credential(url="https://a.com", username="u", password="p"),
            ],
        )
        d = log.to_dict()

        assert d["stealer_name"] == "redline"
        assert d["credential_count"] == 1
        assert len(d["credentials"]) == 1


class TestParserRobustness:
    """Regression tests for parser edge cases (E1 improvements)."""

    def test_windows_path_url_rejected(self):
        """HOST: C:\\path should not produce credentials — it's a Windows path, not a URL."""
        text = """
Soft: Chrome
Host: C:\\Users\\user\\AppData\\Local\\Google\\Chrome\\User Data
Login: user@example.com
Password: secret123
"""
        creds = parse_credentials_text(text)
        assert creds == [], "Windows path in HOST field should not produce a credential"

    def test_url_with_null_chars_stripped(self):
        """URLs containing null bytes should be cleaned before producing a credential."""
        text = "URL: https://example.com\x00\nUsername: user\nPassword: pass\n"
        creds = parse_credentials_text(text)
        assert len(creds) == 1
        assert "\x00" not in creds[0].url

    def test_empty_value_continuation(self):
        """Password on its own line after 'Password:' (empty value) should be captured."""
        text = """
URL: https://example.com
Username: admin
Password:
sup3rsecret!
"""
        creds = parse_credentials_text(text)
        assert len(creds) == 1
        assert creds[0].password == "sup3rsecret!"

    def test_binary_file_skipped(self, tmp_path):
        """Files containing null-byte sequences should be skipped silently."""
        from telecrime.stealer.parser import iter_credentials_file

        binary_file = tmp_path / "binary.txt"
        binary_file.write_bytes(b"\x00\x00\x00MZ\x90\x00some binary content")
        creds = list(iter_credentials_file(binary_file))
        assert creds == [], "Binary file should yield zero credentials"

    def test_url_scheme_required(self):
        """Only http:// and https:// URLs should be accepted; ftp:// and bare domains rejected."""
        text = """
URL: ftp://files.example.com
Username: user
Password: pass

URL: example.com
Username: user2
Password: pass2
"""
        creds = parse_credentials_text(text)
        assert creds == [], "Non-HTTP(S) URLs should not produce credentials"


class TestExpandedStealerDetection:
    """Tests for expanded stealer family detection (E2)."""

    # File-based signatures
    @pytest.mark.parametrize(
        "files, expected",
        [
            (["userinfo.txt", "passwords.txt", "cookies.txt"], "vidar"),
            (["aurora_passwords.txt", "cookies.txt"], "aurora"),
            (["mystic_passwords.txt"], "mystic"),
            (["doen_passwords.txt", "Passwords.txt"], "doenerium"),
            (["cryptbot_passwords.txt"], "cryptbot"),
            (["cinoshi_passwords.txt"], "cinoshi"),
            (["titan_stealer_passwords.txt"], "titan"),
        ],
    )
    def test_detect_stealer_by_files(self, files, expected):
        assert detect_stealer_type(files) == expected

    # Content-based signatures
    @pytest.mark.parametrize(
        "content, expected",
        [
            ("Aurora Stealer v1.0", "aurora"),
            ("MysticStealer build 2024", "mystic"),
            ("Doenerium Grabber", "doenerium"),
            ("titan stealer panel", "titan"),
        ],
    )
    def test_detect_stealer_from_content(self, content, expected):
        assert detect_stealer_type(["Passwords.txt"], content_sample=content) == expected

    # SystemInfo self-identification (highest priority)
    def test_sysinfo_overrides_file_signature(self):
        """SystemInfo stealer name takes priority over file-based detection."""
        files = ["MachineInfo.txt", "Passwords.txt"]  # would normally → raccoon
        result = detect_stealer_type(files, sysinfo_stealer="lumma")
        assert result == "lumma"

    def test_sysinfo_self_id(self):
        assert detect_stealer_type(["Passwords.txt"], sysinfo_stealer="RedLine") == "redline"

    # SystemInfo parser expansion
    def test_parse_sysinfo_stealer_field(self):
        """Stealer self-identification field in SystemInfo should be captured."""
        from telecrime.stealer.parser import parse_system_info
        text = "Hostname: DESKTOP\nStealer: Lumma 5.0\nIP: 1.2.3.4\n"
        info = parse_system_info(text)
        assert info.stealer_name == "Lumma 5.0"

    def test_parse_sysinfo_multiword_fields(self):
        """Multi-word field names like 'Computer Name' should be parsed."""
        from telecrime.stealer.parser import parse_system_info
        text = "Computer Name: WORKSTATION-42\nExternal IP: 8.8.8.8\nOS Version: Windows 11\n"
        info = parse_system_info(text)
        assert info.hostname == "WORKSTATION-42"
        assert info.ip_address == "8.8.8.8"
        assert info.os == "Windows 11"


class TestRoundTenValidation:
    """Tests for the round-10 validation fixes."""

    def test_cp1252_file_decodes_via_fallback(self, tmp_path):
        """A cp1252-encoded credential file must decode (latin-1 fallback),
        NOT be misdetected as utf-16 (round-10 fix: chain ordering)."""
        from telecrime.stealer.parser import iter_credentials_file

        f = tmp_path / "Passwords.txt"
        # "https://example.com;user;pass\xe9" in cp1252 (é = \xe9)
        f.write_bytes(b"https://example.com;alice;p\xe9ss\n")
        creds = list(iter_credentials_file(f))
        assert len(creds) == 1
        assert creds[0].password == "p\xe9ss"

    def test_utf16_bom_file_decodes(self, tmp_path):
        """A UTF-16 file WITH BOM must decode via the explicit BOM path."""
        from telecrime.stealer.parser import iter_credentials_file

        f = tmp_path / "Passwords.txt"
        f.write_bytes(
            "https://example.com;alice;p@ss1\n".encode("utf-16")
        )
        creds = list(iter_credentials_file(f))
        assert len(creds) == 1
        assert creds[0].url == "https://example.com"

    def test_promo_strip_requires_separator(self):
        """Legit passwords containing 'buy'/'t.me' must NOT be truncated."""
        from telecrime.stealer.parser import _clean_credential_field

        assert _clean_credential_field("I want to buy stuff") == "I want to buy stuff"
        assert _clean_credential_field("to buy stuff") == "to buy stuff"
        assert _clean_credential_field("my t.me/abc password") == "my t.me/abc password"
        # Marketplace boilerplate AFTER a pipe separator is still stripped.
        assert _clean_credential_field(
            "secret123 | https://t.me/SampleCloud You can buy dm @SampleCloud"
        ) == "secret123"

    def test_url_userinfo_stripped(self):
        """user:pass@ in a URL must not leak into the domain/hashes."""
        from telecrime.stealer.parser import parse_credential_lines

        creds = list(parse_credential_lines(
            iter(["https://alice:secret@example.com;u;p"]), "f.txt"
        ))
        assert creds
        assert creds[0].url == "https://example.com"
        assert creds[0].domain == "example.com"

    def test_both_empty_credential_is_garbage(self):
        from telecrime.stealer.parser import _is_garbage_credential

        assert _is_garbage_credential("", "") is True
        # url+password-only rows stay valid
        assert _is_garbage_credential("", "pass123") is False
