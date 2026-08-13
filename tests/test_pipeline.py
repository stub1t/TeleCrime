"""Tests for pipeline stages and orchestration."""

import subprocess
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from telecrime.models import PipelineRun
from telecrime.pipeline.acquire import AcquireStage
from telecrime.pipeline.discover import DiscoverStage
from telecrime.pipeline.extract import ExtractStage
from telecrime.pipeline.ingest import IngestStage
from telecrime.pipeline.lock import pipeline_run_lock
from telecrime.pipeline.orchestrator import (
    Pipeline,
    PipelineContext,
    PipelineStage,
    create_default_pipeline,
    run_sequential_pipeline,
)
from telecrime.pipeline.parse import ParseStage
from telecrime.states import ExtractionStatus, GroupStatus


class MockStage(PipelineStage):
    """Mock pipeline stage for testing."""

    name = "mock"

    def __init__(self, should_succeed=True, should_raise=False):
        self.should_succeed = should_succeed
        self.should_raise = should_raise
        self.was_called = False

    async def run(self, ctx):
        self.was_called = True
        if self.should_raise:
            raise RuntimeError("Test error")
        return self.should_succeed


class TestPipelineContext:
    """Tests for PipelineContext dataclass."""

    def test_default_values(self, session, test_config):
        """Test default context values."""
        adapter = MagicMock()
        ctx = PipelineContext(
            config=test_config,
            session=session,
            adapter=adapter,
        )

        assert ctx.dry_run is False
        assert ctx.conversations_processed == 0
        assert ctx.messages_processed == 0
        assert ctx.files_discovered == 0
        assert ctx.errors == []


class TestPipeline:
    """Tests for Pipeline class."""

    def test_add_stage(self, session, test_config):
        """Test adding stages to pipeline."""
        adapter = MagicMock()
        pipeline = Pipeline(test_config, session, adapter)

        stage1 = MockStage()
        stage2 = MockStage()

        result = pipeline.add_stage(stage1)

        # Should return self for chaining
        assert result is pipeline
        assert len(pipeline.stages) == 1

        pipeline.add_stage(stage2)
        assert len(pipeline.stages) == 2

    @pytest.mark.asyncio
    async def test_run_executes_stages(self, session, test_config):
        """Test pipeline runs all stages."""
        adapter = MagicMock()
        pipeline = Pipeline(test_config, session, adapter)

        stage1 = MockStage()
        stage2 = MockStage()
        pipeline.add_stage(stage1).add_stage(stage2)

        await pipeline.run()

        assert stage1.was_called
        assert stage2.was_called

    @pytest.mark.asyncio
    async def test_run_continues_on_failure(self, session, test_config):
        """Test pipeline continues when stage returns False."""
        adapter = MagicMock()
        pipeline = Pipeline(test_config, session, adapter)

        stage1 = MockStage(should_succeed=False)
        stage2 = MockStage(should_succeed=True)
        pipeline.add_stage(stage1).add_stage(stage2)

        await pipeline.run()

        # Both stages should have been called
        assert stage1.was_called
        assert stage2.was_called

    @pytest.mark.asyncio
    async def test_run_continues_on_exception(self, session, test_config):
        """Test pipeline continues when stage raises exception."""
        adapter = MagicMock()
        pipeline = Pipeline(test_config, session, adapter)

        stage1 = MockStage(should_raise=True)
        stage2 = MockStage()
        pipeline.add_stage(stage1).add_stage(stage2)

        ctx = await pipeline.run()

        # Both stages should have been called
        assert stage1.was_called
        assert stage2.was_called
        # Error should be recorded
        assert len(ctx.errors) == 1

    @pytest.mark.asyncio
    async def test_run_passes_dry_run(self, session, test_config):
        """Test dry_run flag is passed to context."""
        adapter = MagicMock()
        pipeline = Pipeline(test_config, session, adapter)

        ctx = await pipeline.run(dry_run=True)

        assert ctx.dry_run is True

    @pytest.mark.asyncio
    async def test_run_persists_pipeline_run_summary(self, session, test_config):
        """Batch pipeline runs persist a summary row."""

        class OkStage(PipelineStage):
            name = "ok"

            async def run(self, ctx):
                ctx.credentials_parsed = 5
                return True

        class BoomStage(PipelineStage):
            name = "boom"

            async def run(self, ctx):
                raise RuntimeError("stage failed")

        pipeline = Pipeline(test_config, session, MagicMock())
        pipeline.add_stage(OkStage()).add_stage(BoomStage())

        ctx = await pipeline.run(dry_run=True)
        run = session.query(PipelineRun).order_by(PipelineRun.id.desc()).first()

        assert ctx.errors
        assert run is not None
        assert run.mode == "batch"
        assert run.dry_run == 1
        assert run.credentials_parsed == 5
        assert run.status == "failed"

    @pytest.mark.asyncio
    async def test_run_rolls_back_failed_stage_and_continues(self, session, test_config):
        """A stage that breaks the session does not poison later stages."""
        from telecrime.models import Conversation

        class BrokenStage(PipelineStage):
            name = "broken"

            async def run(self, ctx):
                ctx.session.add(Conversation(platform_id=111, conversation_type="channel"))
                ctx.session.flush()
                ctx.session.add(Conversation(platform_id=111, conversation_type="channel"))
                ctx.session.flush()
                return True

        class AfterStage(PipelineStage):
            name = "after"

            async def run(self, ctx):
                ctx.messages_processed = 7
                return True

        pipeline = Pipeline(test_config, session, MagicMock())
        pipeline.add_stage(BrokenStage()).add_stage(AfterStage())

        ctx = await pipeline.run()
        run = session.query(PipelineRun).order_by(PipelineRun.id.desc()).first()

        assert ctx.messages_processed == 7
        assert len(ctx.errors) == 1
        assert run is not None
        assert run.status == "failed"

    @pytest.mark.asyncio
    async def test_sequential_pipeline_records_failed_initial_stage(
        self, session, test_config, monkeypatch
    ):
        """Sequential pipeline keeps going and records failures from early stages."""

        class FailingStage(PipelineStage):
            name = "ingest"

            async def run(self, ctx):
                raise RuntimeError("ingest failed")

        class OkStage(PipelineStage):
            def __init__(self, name):
                self.name = name

            async def run(self, ctx):
                return True

        monkeypatch.setattr("telecrime.pipeline.ingest.IngestStage", lambda: FailingStage())
        monkeypatch.setattr(
            "telecrime.pipeline.channel_discover.ChannelDiscoverStage",
            lambda: OkStage("channel_discover"),
        )
        monkeypatch.setattr(
            "telecrime.pipeline.discover.DiscoverStage", lambda: OkStage("discover")
        )
        monkeypatch.setattr("telecrime.pipeline.plan.PlanStage", lambda: OkStage("plan"))
        monkeypatch.setattr("telecrime.pipeline.acquire.AcquireStage", lambda: MagicMock())
        monkeypatch.setattr("telecrime.pipeline.extract.ExtractStage", lambda: MagicMock())
        monkeypatch.setattr("telecrime.pipeline.parse.ParseStage", lambda: MagicMock())
        monkeypatch.setattr("telecrime.pipeline.finalize.FinalizeStage", lambda: MagicMock())
        monkeypatch.setattr("telecrime.pipeline.channel_discover.ChannelJoiner", lambda: None)
        monkeypatch.setattr(
            "telecrime.extractor.seven_zip.SevenZipExtractor", lambda *args, **kwargs: MagicMock()
        )

        ctx = await run_sequential_pipeline(
            test_config, session, MagicMock(), dry_run=True, limit=1
        )
        run = session.query(PipelineRun).order_by(PipelineRun.id.desc()).first()

        assert ctx.errors
        assert run is not None
        assert run.mode == "sequential"
        assert run.status == "failed"

    @pytest.mark.asyncio
    async def test_sequential_pipeline_fails_cleanly_when_locked(self, session, test_config):
        """The pipeline lock blocks other processes from starting a run."""
        with pipeline_run_lock(test_config.data_dir):
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        "from telecrime.pipeline.lock import pipeline_run_lock; "
                        f"data_dir = Path({str(test_config.data_dir)!r}); "
                        "ctx = pipeline_run_lock(data_dir); ctx.__enter__()"
                    ),
                ],
                cwd=Path(__file__).parent.parent,
                capture_output=True,
                text=True,
            )

        assert result.returncode != 0
        assert "Another pipeline run is already active" in result.stderr


class TestIngestStage:
    """Tests for IngestStage."""

    def test_init_with_limit(self):
        """Test initialization with message limit."""
        stage = IngestStage(message_limit=100)
        assert stage.message_limit == 100

    @pytest.mark.asyncio
    async def test_process_conversation_creates_record(self, session, test_config):
        """Test processing conversation creates database record."""
        from telecrime.adapters.base import ConversationInfo
        from telecrime.models import Conversation

        # Create a proper async iterator mock
        conv_info = ConversationInfo(
            platform_id=12345,
            access_hash=None,
            title="Test Channel",
            username="testchannel",
            conversation_type="channel",
            is_member=True,
            is_accessible=True,
        )

        adapter = MagicMock()
        adapter.iter_conversations = MagicMock(return_value=AsyncIteratorMock([conv_info]))
        adapter.iter_messages = MagicMock(return_value=AsyncIteratorMock([]))

        ctx = PipelineContext(
            config=test_config,
            session=session,
            adapter=adapter,
        )

        stage = IngestStage()
        await stage.run(ctx)

        # Verify conversation was created
        conv = session.query(Conversation).filter_by(platform_id=12345).first()
        assert conv is not None
        assert conv.title == "Test Channel"

    @pytest.mark.asyncio
    async def test_process_message_is_idempotent(self, session, test_config):
        """Duplicate message ingests do not create extra rows or attachments."""
        from telecrime.adapters.base import FileInfo, MessageInfo
        from telecrime.models import Conversation, FileAttachment, Message

        conv = Conversation(
            platform_id=12345,
            title="Test Channel",
            conversation_type="channel",
            is_member=True,
            is_accessible=True,
        )
        session.add(conv)
        session.flush()

        msg_info = MessageInfo(
            platform_id=999,
            conversation_id=12345,
            timestamp=datetime.now(UTC),
            text="hello",
            caption=None,
            is_forwarded=False,
            forwarded_from_id=None,
            forwarded_from_name=None,
            forwarded_message_id=None,
        )
        files = [
            FileInfo(
                platform_file_id="file123",
                platform_file_unique_id="unique123",
                access_hash=1,
                filename="dump.zip",
                mime_type="application/zip",
                size=123,
            )
        ]

        adapter = MagicMock()
        ctx = PipelineContext(config=test_config, session=session, adapter=adapter)
        stage = IngestStage()

        await stage._process_message(ctx, conv, msg_info, files)
        await stage._process_message(ctx, conv, msg_info, files)

        assert (
            session.query(Message).filter_by(conversation_id=conv.id, platform_id=999).count() == 1
        )
        assert session.query(FileAttachment).count() == 1


class TestDiscoverStage:
    """Tests for DiscoverStage."""

    def test_classify_txt_credential_file(self):
        """Direct credential .txt files are classified as archive_type='txt'."""
        from types import SimpleNamespace

        stage = DiscoverStage()
        for name in ["Passwords.txt", "All Passwords.txt", "[ULP]combo.txt", "combo_list.txt"]:
            attachment = SimpleNamespace(filename=name, mime_type="text/plain", size=1024)
            is_archive, archive_type, part_info = stage._classify_attachment(attachment)
            assert is_archive is True, f"{name} should be classified as archive candidate"
            assert archive_type == "txt", f"{name} should have archive_type='txt'"
            assert part_info is None

    def test_classify_txt_non_credential_file(self):
        """Generic .txt files that don't match credential patterns are not classified."""
        from types import SimpleNamespace

        stage = DiscoverStage()
        for name in ["README.txt", "notes.txt", "info.txt"]:
            attachment = SimpleNamespace(filename=name, mime_type="text/plain", size=1024)
            is_archive, archive_type, part_info = stage._classify_attachment(attachment)
            assert is_archive is False, f"{name} should not be classified as archive candidate"

    def test_classify_zip_still_works(self):
        """Standard archive classification is unaffected by .txt changes."""
        from types import SimpleNamespace

        stage = DiscoverStage()
        attachment = SimpleNamespace(filename="archive.zip", mime_type="application/zip", size=1024)
        is_archive, archive_type, part_info = stage._classify_attachment(attachment)
        assert is_archive is True
        assert archive_type == "zip"


class TestExtractStageDirect:
    """Tests for ExtractStage direct .txt handling."""

    @pytest.mark.asyncio
    async def test_handle_direct_txt_links_file(self, session, test_config, tmp_path):
        """_handle_direct_txt hardlinks a .txt file and records extracted output."""
        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            ExtractedOutput,
            ExtractionJob,
            FileAttachment,
            Message,
        )

        # Minimal DB objects
        conv = Conversation(platform_id=99, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=1,
            platform_timestamp=datetime.now(UTC),
            text="ulp drop",
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(
            message_id=msg.id,
            platform_file_id="ulp-file",
            filename="Passwords.txt",
            archive_type="txt",
        )
        session.add(attachment)
        session.flush()

        # Write a real txt file simulating a downloaded credential dump
        dl_path = tmp_path / "downloads" / "Passwords.txt"
        dl_path.parent.mkdir(parents=True, exist_ok=True)
        dl_path.write_text("https://example.com;alice;secret\n", encoding="utf-8")

        artifact = DownloadArtifact(attachment_id=attachment.id, local_path=str(dl_path))
        session.add(artifact)
        session.flush()
        group = ArchiveGroup(
            fingerprint="ulp-group",
            base_name="Passwords.txt",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.READY,
        )
        session.add(group)
        session.flush()
        session.add(ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0))
        session.flush()

        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.PENDING)
        session.add(job)
        session.flush()
        session.commit()

        # Reload with relationships
        from sqlalchemy.orm import joinedload

        from telecrime.models import DownloadArtifact
        group = session.get(
            ArchiveGroup,
            group.id,
            options=[
                joinedload(ArchiveGroup.parts)
                .joinedload(ArchiveGroupPart.artifact)
                .joinedload(DownloadArtifact.attachment)
            ],
        )
        job = session.get(ExtractionJob, job.id)

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = ExtractStage()
        result = await stage._handle_direct_txt(ctx, group, job, dl_path)

        assert result is True
        assert group.status == GroupStatus.EXTRACTED
        assert job.status == ExtractionStatus.COMPLETED

        # Output file should exist in extracted_dir
        outputs = session.query(ExtractedOutput).filter_by(job_id=job.id).all()
        assert len(outputs) == 1
        assert outputs[0].output_filename == "Passwords.txt"
        assert Path(outputs[0].output_path).exists()

    @pytest.mark.asyncio
    async def test_low_disk_leaves_group_ready_not_failed(
        self, session, test_config, tmp_path, monkeypatch
    ):
        """Transient low disk must NOT mark the group FAILED.

        A FAILED group would have its archive parts deleted by finalize's
        cleanup sweep — silently destroying a healthy group over long
        disk-pressured unattended runs. The group must stay
        READY (and the job PENDING) so it is retried once space is reclaimed.
        """
        from sqlalchemy.orm import joinedload

        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            ExtractionJob,
            FileAttachment,
            Message,
        )

        conv = Conversation(platform_id=199, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=2,
            platform_timestamp=datetime.now(UTC),
            text="archive drop",
        )
        session.add(msg)
        session.flush()
        attachment = FileAttachment(
            message_id=msg.id,
            platform_file_id="zip-file",
            filename="dump.zip",
            archive_type="zip",
        )
        session.add(attachment)
        session.flush()

        dl_path = tmp_path / "downloads" / "dump.zip"
        dl_path.parent.mkdir(parents=True, exist_ok=True)
        dl_path.write_bytes(b"PK\x03\x04 not really a zip but enough for the guard")

        artifact = DownloadArtifact(attachment_id=attachment.id, local_path=str(dl_path))
        session.add(artifact)
        session.flush()
        group = ArchiveGroup(
            fingerprint="zip-group",
            base_name="dump.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.READY,
        )
        session.add(group)
        session.flush()
        session.add(ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0))
        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.PENDING)
        session.add(job)
        session.flush()
        session.commit()

        from telecrime.models import DownloadArtifact
        group = session.get(
            ArchiveGroup,
            group.id,
            options=[
                joinedload(ArchiveGroup.parts)
                .joinedload(ArchiveGroupPart.artifact)
                .joinedload(DownloadArtifact.attachment)
            ],
        )
        job_id = job.id

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = ExtractStage()
        monkeypatch.setattr(stage, "_has_sufficient_disk", lambda _ctx: False)

        result = await stage._extract_group(ctx, group, MagicMock())

        assert result is False
        assert group.status == GroupStatus.READY
        assert session.get(ExtractionJob, job_id).status == ExtractionStatus.PENDING


class TestPlanStage:
    """Tests for PlanStage."""


class TestAcquireStage:
    """Tests for AcquireStage."""

    def test_sanitize_filename(self):
        """Test filename sanitization."""
        stage = AcquireStage()

        assert stage._sanitize_filename("normal.zip") == "normal.zip"
        assert stage._sanitize_filename("file/with/slashes.zip") == "file_with_slashes.zip"
        assert stage._sanitize_filename("file\\backslash.zip") == "file_backslash.zip"
        assert stage._sanitize_filename("") == "unnamed_file"
        # None is handled by the caller, not _sanitize_filename directly

        # Test length limit
        long_name = "a" * 300 + ".zip"
        sanitized = stage._sanitize_filename(long_name)
        assert len(sanitized) <= 200

    @pytest.mark.asyncio
    async def test_compute_hash(self, tmp_path):
        """SHA256 hash runs in executor and matches direct computation."""
        import hashlib
        stage = AcquireStage()
        data = b"hello telecrime" * 1000
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        result = await stage._compute_hash(f)
        assert result == hashlib.sha256(data).hexdigest()

    def test_recover_marks_completed_when_file_on_disk(self, session, tmp_path):
        """Artifact whose final file exists is marked COMPLETED, not re-downloaded."""
        from telecrime.models import (
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.states import DownloadStatus

        downloads_dir = tmp_path / "downloads"
        downloads_dir.mkdir()

        conv = Conversation(platform_id=99, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=99,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()
        fa = FileAttachment(message_id=msg.id, filename="archive.rar", platform_file_id="fid_99")
        session.add(fa)
        session.flush()

        # Simulate crash: DOWNLOADING but the final file is already on disk
        final_file = downloads_dir / "archive.rar"
        final_file.write_bytes(b"fake archive content")
        artifact = DownloadArtifact(
            attachment_id=fa.id,
            status=DownloadStatus.DOWNLOADING,
            temp_path=None,
        )
        session.add(artifact)
        session.commit()

        recovered = AcquireStage().recover_stuck_downloads(session, downloads_dir)

        assert recovered == 1
        session.refresh(artifact)
        assert artifact.status == DownloadStatus.COMPLETED
        assert artifact.local_path == str(final_file)

    def test_recover_resets_to_pending_when_file_absent(self, session, tmp_path):
        """Artifact with no file on disk is reset to PENDING for re-download."""
        from telecrime.models import (
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.states import DownloadStatus

        downloads_dir = tmp_path / "downloads"
        downloads_dir.mkdir()
        tmp_dir = downloads_dir / ".tmp"
        tmp_dir.mkdir()

        conv = Conversation(platform_id=100, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=100,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()
        fa = FileAttachment(message_id=msg.id, filename="missing.rar", platform_file_id="fid_100")
        session.add(fa)
        session.flush()

        # Partial file that should be deleted
        partial = tmp_dir / "tmpXXXXXX.partial"
        partial.write_bytes(b"incomplete data")
        artifact = DownloadArtifact(
            attachment_id=fa.id,
            status=DownloadStatus.DOWNLOADING,
            temp_path=str(partial),
        )
        session.add(artifact)
        session.commit()

        recovered = AcquireStage().recover_stuck_downloads(session, downloads_dir)

        assert recovered == 1
        session.refresh(artifact)
        assert artifact.status == DownloadStatus.PENDING
        assert artifact.temp_path is None
        assert not partial.exists()  # partial file cleaned up

    @pytest.mark.asyncio
    async def test_update_group_statuses_only_touches_requested_groups(self, session, test_config):
        """Acquire status updates can target just the groups changed by downloads."""
        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id,
            platform_id=10,
            platform_timestamp=datetime.now(UTC),
            text="msg",
        )
        session.add(msg)
        session.flush()

        def add_group(group_name: str, artifact_status: DownloadStatus) -> ArchiveGroup:
            group = ArchiveGroup(
                fingerprint=group_name,
                expected_part_count=1,
                detected_part_count=1,
                status=GroupStatus.INCOMPLETE,
            )
            session.add(group)
            session.flush()
            attachment = FileAttachment(message_id=msg.id, platform_file_id=f"file-{group_name}")
            session.add(attachment)
            session.flush()
            artifact = DownloadArtifact(attachment_id=attachment.id, status=artifact_status)
            session.add(artifact)
            session.flush()
            session.add(ArchiveGroupPart(group_id=group.id, artifact_id=artifact.id, part_index=0))
            session.flush()
            return group

        ready_group = add_group("ready-group", DownloadStatus.COMPLETED)
        untouched_group = add_group("untouched-group", DownloadStatus.PENDING)
        session.commit()

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = AcquireStage()

        await stage._update_group_statuses(ctx, {ready_group.id})
        session.commit()

        session.refresh(ready_group)
        session.refresh(untouched_group)
        assert ready_group.status == GroupStatus.READY
        assert untouched_group.status == GroupStatus.INCOMPLETE


class TestParseStage:
    """Tests for ParseStage bulk insert behavior."""

    @pytest.mark.asyncio
    async def test_parse_job_outputs_bulk_inserts_and_dedups(self, session, test_config, tmp_path):
        """Bulk parse inserts only new credential hashes and counts duplicates."""
        from telecrime.models import ArchiveGroup, ExtractedOutput, ExtractionJob, ParsedCredential

        credential_file = tmp_path / "Passwords.txt"
        credential_file.write_text("placeholder")

        group = ArchiveGroup(
            fingerprint="group123",
            base_name="archive.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.EXTRACTED,
        )
        session.add(group)
        session.flush()

        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()

        output = ExtractedOutput(
            job_id=job.id,
            output_path=str(credential_file),
            output_filename="Passwords.txt",
            output_hash="hash123",
        )
        session.add(output)
        session.flush()

        existing_hash = ParsedCredential.compute_hash("example.com", "alice", "secret")
        session.add(
            ParsedCredential(
                url="https://old.example/login",
                domain="example.com",
                username="alice",
                password="secret",
                credential_hash=existing_hash,
            )
        )
        session.commit()

        job = session.get(ExtractionJob, job.id)
        adapter = MagicMock()
        ctx = PipelineContext(config=test_config, session=session, adapter=adapter)
        stage = ParseStage()

        creds = [
            SimpleNamespace(
                url="https://one.example/login",
                domain="example.com",
                username="alice",
                password="secret",
                email_domain=None,
                application="Chrome",
                profile="Default",
            ),
            SimpleNamespace(
                url="https://two.example/login",
                domain="another.com",
                username="bob",
                password="pw1",
                email_domain=None,
                application="Chrome",
                profile="Default",
            ),
            SimpleNamespace(
                url="https://three.example/login",
                domain="another.com",
                username="bob",
                password="pw1",
                email_domain=None,
                application="Chrome",
                profile="Default",
            ),
        ]

        with patch("telecrime.pipeline.parse.iter_credentials_file", return_value=iter(creds)):
            new_count, dup_count = await stage._parse_job_outputs(ctx, job, False)

        stored = session.query(ParsedCredential).filter_by(extraction_job_id=job.id).all()
        assert new_count == 1
        assert dup_count == 2
        assert len(stored) == 1
        assert stored[0].credential_hash == ParsedCredential.compute_hash(
            "another.com", "bob", "pw1"
        )


class TestCreateDefaultPipeline:
    """Tests for create_default_pipeline factory."""

    def test_creates_all_stages(self, session, test_config):
        """Test factory creates all expected stages."""
        adapter = MagicMock()
        pipeline = create_default_pipeline(test_config, session, adapter)

        # Should have 9 stages (including channel_discover)
        assert len(pipeline.stages) == 9

        # Verify stage types
        stage_names = [s.name for s in pipeline.stages]
        assert "ingest" in stage_names
        assert "channel_discover" in stage_names
        assert "discover" in stage_names
        assert "plan" in stage_names
        assert "acquire" in stage_names
        assert "enrich" in stage_names
        assert "extract" in stage_names
        assert "parse" in stage_names
        assert "finalize" in stage_names


class AsyncIteratorMock:
    """Helper to create async iterators from lists."""

    def __init__(self, items):
        self.items = items
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


class TestParseStageChunkedInsert:
    """Tests for ParseStage chunked bulk insert."""

    @pytest.mark.asyncio
    async def test_bulk_insert_chunks_large_batches(self, session, test_config, tmp_path):
        """_bulk_insert_credentials splits large batches into smaller chunks."""
        from telecrime.models import ArchiveGroup, ExtractedOutput, ExtractionJob, ParsedCredential
        from telecrime.pipeline.parse import _INSERT_CHUNK_SIZE

        group = ArchiveGroup(
            fingerprint="chunk-test",
            base_name="archive.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.EXTRACTED,
        )
        session.add(group)
        session.flush()

        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()

        credential_file = tmp_path / "Passwords.txt"
        credential_file.write_text("placeholder")
        output = ExtractedOutput(
            job_id=job.id,
            output_path=str(credential_file),
            output_filename="Passwords.txt",
            output_hash="hash123",
        )
        session.add(output)
        session.flush()

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = ParseStage()

        # Create more rows than _INSERT_CHUNK_SIZE so chunking is exercised
        rows = []
        for i in range(_INSERT_CHUNK_SIZE + 100):
            rows.append(
                {
                    "url": f"https://site{i}.com/login",
                    "domain": f"site{i}.com",
                    "username": f"user{i}",
                    "password": "secret",
                    "credential_hash": ParsedCredential.compute_hash(
                        f"site{i}.com", f"user{i}", "secret"
                    ),
                    "extraction_job_id": job.id,
                    "source_file": str(credential_file),
                    "source_archive": group.base_name,
                    "source_conversation_id": None,
                    "source_message_id": None,
                    "stealer_type": None,
                }
            )

        inserted = stage._bulk_insert_credentials(ctx, rows)
        assert len(inserted) == len(rows)

        # Verify all rows landed in the DB
        count = session.query(ParsedCredential).count()
        assert count == len(rows)

    @pytest.mark.asyncio
    async def test_bulk_insert_continues_after_chunk_failure(self, session, test_config, tmp_path):
        """A failing chunk does not abort the whole batch."""
        from telecrime.models import ArchiveGroup, ExtractionJob, ParsedCredential

        group = ArchiveGroup(
            fingerprint="fail-test",
            base_name="archive.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.EXTRACTED,
        )
        session.add(group)
        session.flush()

        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = ParseStage()

        # Pre-seed one hash so the first chunk has a conflict
        existing_hash = ParsedCredential.compute_hash("site0.com", "user0", "secret")
        session.add(
            ParsedCredential(
                url="https://site0.com/login",
                domain="site0.com",
                username="user0",
                password="secret",
                credential_hash=existing_hash,
                extraction_job_id=job.id,
            )
        )
        session.commit()

        rows = [
            {
                "url": "https://site0.com/login",
                "domain": "site0.com",
                "username": "user0",
                "password": "secret",
                "credential_hash": existing_hash,
                "extraction_job_id": job.id,
                "source_file": "/tmp/f.txt",
                "source_archive": group.base_name,
                "source_conversation_id": None,
                "source_message_id": None,
                "stealer_type": None,
            },
            {
                "url": "https://site1.com/login",
                "domain": "site1.com",
                "username": "user1",
                "password": "secret",
                "credential_hash": ParsedCredential.compute_hash("site1.com", "user1", "secret"),
                "extraction_job_id": job.id,
                "source_file": "/tmp/f.txt",
                "source_archive": group.base_name,
                "source_conversation_id": None,
                "source_message_id": None,
                "stealer_type": None,
            },
        ]

        inserted = stage._bulk_insert_credentials(ctx, rows)
        # Only the second row should be newly inserted
        assert len(inserted) == 1
        assert inserted[0]["credential_hash"] == rows[1]["credential_hash"]

    @pytest.mark.asyncio
    async def test_bulk_insert_savepoint_preserves_prior_chunks_on_failure(
        self, session, test_config, tmp_path, monkeypatch
    ):
        """A mid-batch chunk failure must not discard chunks inserted earlier.

        Regression: before the SAVEPOINT-per-chunk fix, ctx.session.rollback()
        on a failed chunk discarded the WHOLE transaction including prior
        successful chunks — silent data loss while the return count claimed
        the rows had been inserted.
        """
        from telecrime.models import ArchiveGroup, ExtractionJob, ParsedCredential
        from telecrime.pipeline import parse as parse_mod

        group = ArchiveGroup(
            fingerprint="sp-test",
            base_name="sp.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.EXTRACTED,
        )
        session.add(group)
        session.flush()
        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()
        session.commit()

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = ParseStage()

        # 1-row chunks so we can target the middle chunk precisely.
        monkeypatch.setattr(parse_mod, "_INSERT_CHUNK_SIZE", 1)

        def _row(host: str) -> dict:
            return {
                "url": f"https://{host}/login",
                "domain": host,
                "username": "u",
                "password": "p",
                "credential_hash": ParsedCredential.compute_hash(host, "u", "p"),
                "extraction_job_id": job.id,
                "source_file": "/tmp/f.txt",
                "source_archive": group.base_name,
                "source_conversation_id": None,
                "source_message_id": None,
                "stealer_type": None,
            }

        rows = [_row("a.example.com"), _row("b.example.com"), _row("c.example.com")]

        # Make the middle chunk's INSERT raise. SAVEPOINT semantics: the first
        # row is committed-to-outer-tx, the middle is rolled back to savepoint,
        # the third proceeds normally.
        original_execute = session.execute
        call_count = {"n": 0}

        def fake_execute(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("synthetic chunk failure")
            return original_execute(*args, **kwargs)

        monkeypatch.setattr(session, "execute", fake_execute)

        inserted = stage._bulk_insert_credentials(ctx, rows)
        # Restore real execute so the commit/query below work.
        monkeypatch.setattr(session, "execute", original_execute)
        session.commit()

        # Two of three chunks inserted; the bad one was skipped, not the others.
        assert len(inserted) == 2
        hashes_inserted = {row["credential_hash"] for row in inserted}
        assert rows[0]["credential_hash"] in hashes_inserted
        assert rows[2]["credential_hash"] in hashes_inserted

        # Critically: the successful rows are actually durable, not silently
        # discarded by a rollback of the surrounding transaction.
        durable_hashes = {
            r[0]
            for r in session.query(ParsedCredential.credential_hash).all()
        }
        assert rows[0]["credential_hash"] in durable_hashes
        assert rows[2]["credential_hash"] in durable_hashes
        assert rows[1]["credential_hash"] not in durable_hashes


class TestParseParallelChunking:
    """Tests for the parallel parse line-chunking used by large files."""

    def test_iter_line_chunks_never_splits_labeled_blocks(self, tmp_path):
        """Chunks are cut only at blank/separator lines, so labeled credentials
        never fall across two workers."""
        from telecrime.pipeline.parse import _iter_line_chunks

        lines: list[str] = []
        for i in range(10):
            # Labeled block of 4 lines, blank-separated
            lines.append("Soft: Chrome")
            lines.append(f"Host: https://example{i}.com")
            lines.append(f"Login: u{i}")
            lines.append(f"Password: p{i}")
            lines.append("")

        fh = StringIO("\n".join(lines))
        raw_line_count = sum(1 for _ in StringIO("\n".join(lines)))
        chunks = list(_iter_line_chunks(fh, chunk_lines=5))
        # No chunk may end mid-block: every chunk's last line is blank/separator
        # (or is the file tail).
        for idx, chunk in enumerate(chunks):
            if idx < len(chunks) - 1:
                last = chunk[-1].strip()
                assert last == "" or _iter_line_chunks.__globals__["_CHUNK_BOUNDARY_RE"].match(last), (
                    f"chunk {idx} ends mid-block: {chunk[-3:]}"
                )
        assert sum(len(c) for c in chunks) == raw_line_count

    def test_iter_line_chunks_hard_cut_on_unbounded_combo_list(self, tmp_path):
        """A combo list with no blank lines still gets chunked (hard cut) and
        keeps every line (combo lines are line-independent)."""
        from telecrime.pipeline.parse import _iter_line_chunks

        lines = [f"https://site{i}.com;u{i};p{i}" for i in range(150)]
        fh = StringIO("\n".join(lines))
        chunks = list(_iter_line_chunks(fh, chunk_lines=10))
        # Hard-cut fallback kicks in at 4×chunk_lines; 150 lines → 4+ chunks.
        assert len(chunks) >= 4
        assert sum(len(c) for c in chunks) == len(lines)

    def test_parse_lines_chunk_worker_matches_sequential(self, tmp_path):
        """Parsing a chunk through the worker yields the same credentials as the
        sequential file parser on the same lines."""
        from telecrime.pipeline.parse import _parse_lines_chunk_worker
        from telecrime.stealer.parser import parse_credential_lines

        lines = [
            "Soft: Chrome\nHost: https://example.com\nLogin: alice\nPassword: secret",
            "",
            "https://site.com;bob;pw123",
            "https://pipe.com | carol | pw456",
        ]
        worker_result = _parse_lines_chunk_worker((lines, "/tmp/x.txt"))
        sequential = [
            (c.url, c.username, c.password, c.application, c.profile)
            for c in parse_credential_lines(iter(lines), "/tmp/x.txt")
        ]
        assert worker_result == sequential
        assert len(worker_result) >= 2


class TestAcquireStaleCleanup:
    """Tests for AcquireStage stale group cleanup."""

    def test_cleanup_stale_groups_skips_groups_with_completed_parts(self, session, tmp_path):
        """Groups with at least one COMPLETED part are never cleaned up."""
        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.pipeline.acquire import AcquireStage
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=1, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=10,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()

        group = ArchiveGroup(
            fingerprint="old-partial",
            expected_part_count=2,
            detected_part_count=2,
            status=GroupStatus.INCOMPLETE,
        )
        session.add(group)
        session.flush()

        # One completed, one pending
        for i, status in enumerate([DownloadStatus.COMPLETED, DownloadStatus.PENDING]):
            att = FileAttachment(message_id=msg.id, platform_file_id=f"file-{i}")
            session.add(att)
            session.flush()
            art = DownloadArtifact(attachment_id=att.id, status=status)
            session.add(art)
            session.flush()
            session.add(ArchiveGroupPart(group_id=group.id, artifact_id=art.id, part_index=i))

        session.commit()
        cleaned = AcquireStage().cleanup_stale_incomplete_groups(session, max_age_days=0)
        assert cleaned == 0
        session.refresh(group)
        assert group.status == GroupStatus.INCOMPLETE

    def test_cleanup_stale_groups_cleans_old_zero_progress_groups(self, session, tmp_path):
        """Old INCOMPLETE groups with zero completed parts are marked FAILED_TERMINAL."""
        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.pipeline.acquire import AcquireStage
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=2, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=20,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()

        group = ArchiveGroup(
            fingerprint="old-stale",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.INCOMPLETE,
        )
        # Force updated_at to be very old
        from datetime import timedelta
        group.updated_at = datetime.now(UTC) - timedelta(days=60)
        session.add(group)
        session.flush()

        att = FileAttachment(message_id=msg.id, platform_file_id="file-stale")
        session.add(att)
        session.flush()
        art = DownloadArtifact(attachment_id=att.id, status=DownloadStatus.PENDING)
        session.add(art)
        session.flush()
        session.add(ArchiveGroupPart(group_id=group.id, artifact_id=art.id, part_index=0))
        session.commit()

        cleaned = AcquireStage().cleanup_stale_incomplete_groups(session, max_age_days=30)
        assert cleaned == 1
        session.refresh(group)
        session.refresh(art)
        assert group.status == GroupStatus.FAILED_TERMINAL
        assert art.status == DownloadStatus.FAILED_TERMINAL


class TestOrchestratorStaleGroups:
    """Tests for orchestrator stale-group deprioritisation."""

    def test_next_pending_artifact_skips_stale_zero_progress_groups(self, session, tmp_path):
        """Artifacts from old INCOMPLETE groups with 0 completed parts are skipped."""
        from datetime import timedelta

        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.pipeline.orchestrator import _next_pending_artifact
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=3, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=30,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()

        # Stale group (60 days old, 0 completed parts)
        stale_group = ArchiveGroup(
            fingerprint="stale",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.INCOMPLETE,
            updated_at=datetime.now(UTC) - timedelta(days=60),
        )
        session.add(stale_group)
        session.flush()
        stale_att = FileAttachment(message_id=msg.id, platform_file_id="file-stale", filename="stale.zip")
        session.add(stale_att)
        session.flush()
        stale_art = DownloadArtifact(attachment_id=stale_att.id, status=DownloadStatus.PENDING)
        session.add(stale_art)
        session.flush()
        session.add(ArchiveGroupPart(group_id=stale_group.id, artifact_id=stale_art.id, part_index=0))

        # Fresh group (new, 0 completed parts)
        fresh_group = ArchiveGroup(
            fingerprint="fresh",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.INCOMPLETE,
        )
        session.add(fresh_group)
        session.flush()
        fresh_att = FileAttachment(message_id=msg.id, platform_file_id="file-fresh", filename="fresh.zip")
        session.add(fresh_att)
        session.flush()
        fresh_art = DownloadArtifact(attachment_id=fresh_att.id, status=DownloadStatus.PENDING)
        session.add(fresh_art)
        session.flush()
        session.add(ArchiveGroupPart(group_id=fresh_group.id, artifact_id=fresh_art.id, part_index=0))
        session.commit()

        result = _next_pending_artifact(session)
        assert result is not None
        assert result.id == fresh_art.id

    def test_next_pending_artifact_prefers_newer_groups(self, session, tmp_path):
        """When multiple fresh groups exist, newer updated_at is preferred."""
        from datetime import timedelta

        from telecrime.models import (
            ArchiveGroup,
            ArchiveGroupPart,
            Conversation,
            DownloadArtifact,
            FileAttachment,
            Message,
        )
        from telecrime.pipeline.orchestrator import _next_pending_artifact
        from telecrime.states import DownloadStatus, GroupStatus

        conv = Conversation(platform_id=4, conversation_type="channel")
        session.add(conv)
        session.flush()
        msg = Message(
            conversation_id=conv.id, platform_id=40,
            platform_timestamp=datetime.now(UTC),
        )
        session.add(msg)
        session.flush()

        for i, days_ago in enumerate([1, 5]):
            group = ArchiveGroup(
                fingerprint=f"group-{i}",
                expected_part_count=1,
                detected_part_count=1,
                status=GroupStatus.INCOMPLETE,
                updated_at=datetime.now(UTC) - timedelta(days=days_ago),
            )
            session.add(group)
            session.flush()
            att = FileAttachment(message_id=msg.id, platform_file_id=f"file-{i}", filename="archive.zip")
            session.add(att)
            session.flush()
            art = DownloadArtifact(attachment_id=att.id, status=DownloadStatus.PENDING)
            session.add(art)
            session.flush()
            session.add(ArchiveGroupPart(group_id=group.id, artifact_id=art.id, part_index=0))

        session.commit()
        result = _next_pending_artifact(session)
        # Should pick the group updated 1 day ago (newer)
        assert result is not None
        assert result.attachment.platform_file_id == "file-0"


class TestFinalizeStageCredentialCount:
    """Tests for FinalizeStage.credential_count denormalization."""

    @pytest.mark.asyncio
    async def test_finalize_sets_credential_count(self, session, test_config, tmp_path):
        """_finalize_extracted_group records the credential count on the group."""
        from telecrime.models import (
            ArchiveGroup,
            ExtractedOutput,
            ExtractionJob,
            ParsedCredential,
        )
        from telecrime.pipeline.finalize import FinalizeStage

        group = ArchiveGroup(
            fingerprint="count-test",
            base_name="archive.zip",
            expected_part_count=1,
            detected_part_count=1,
            status=GroupStatus.EXTRACTED,
        )
        session.add(group)
        session.flush()

        job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
        session.add(job)
        session.flush()

        cred_file = tmp_path / "Passwords.txt"
        cred_file.write_text("placeholder")
        output = ExtractedOutput(
            job_id=job.id,
            output_path=str(cred_file),
            output_filename="Passwords.txt",
            output_hash="hash-count",
        )
        session.add(output)

        for i in range(3):
            session.add(ParsedCredential(
                url=f"https://site{i}.com/",
                domain=f"site{i}.com",
                username=f"user{i}",
                password="secret",
                credential_hash=ParsedCredential.compute_hash(f"site{i}.com", f"user{i}", "secret"),
                extraction_job_id=job.id,
            ))
        session.commit()

        ctx = PipelineContext(config=test_config, session=session, adapter=MagicMock())
        stage = FinalizeStage()
        # Bypass file-cleanup I/O by patching _cleanup_archives and _cleanup_extracted_files
        stage._cleanup_archives = AsyncMock()
        stage._cleanup_extracted_files = AsyncMock()
        # _record_first_seen needs outputs; stub it out to avoid complex setup
        stage._record_first_seen = AsyncMock()

        await stage._finalize_extracted_group(ctx, group)
        session.commit()

        session.refresh(group)
        assert group.credential_count == 3
        assert group.status.value == "cleaned"
