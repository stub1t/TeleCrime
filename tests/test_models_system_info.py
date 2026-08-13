"""Tests for SystemInfoRecord model."""

import pytest
from sqlalchemy.exc import IntegrityError

from telecrime.database import get_engine, get_session, init_db
from telecrime.models import ExtractionJob
from telecrime.models.archive_group import ArchiveGroup
from telecrime.models.system_info import SystemInfoRecord
from telecrime.states import ExtractionStatus, GroupStatus


def _engine(tmp_path):
    engine = get_engine(f"sqlite:///{tmp_path / 'test.db'}")
    init_db(engine)
    return engine


def _make_job(session) -> int:
    """Create a minimal ArchiveGroup + ExtractionJob and return the job id."""
    group = ArchiveGroup(
        fingerprint="test.zip",
        base_name="test.zip",
        status=GroupStatus.INCOMPLETE,
        expected_part_count=1,
        detected_part_count=1,
    )
    session.add(group)
    session.flush()
    job = ExtractionJob(group_id=group.id, status=ExtractionStatus.COMPLETED)
    session.add(job)
    session.flush()
    return job.id


def test_system_info_record_persists_all_fields(tmp_path):
    """SystemInfoRecord saves all fields correctly."""
    engine = _engine(tmp_path)
    with get_session(engine) as session:
        job_id = _make_job(session)
        record = SystemInfoRecord(
            extraction_job_id=job_id,
            hostname="DESKTOP-ABC123",
            username="victim",
            ip_address="1.2.3.4",
            country="US",
            hwid="HWID-XYZ",
            os="Windows 10",
            cpu="Intel i7",
            gpu="NVIDIA GTX",
            ram="16GB",
            timezone="UTC-5",
            language="en-US",
            screen_size="1920x1080",
            stealer_name="lumma",
        )
        session.add(record)
        session.commit()
        record_id = record.id

    with get_session(engine) as session:
        r = session.get(SystemInfoRecord, record_id)
        assert r.hostname == "DESKTOP-ABC123"
        assert r.country == "US"
        assert r.ip_address == "1.2.3.4"
        assert r.stealer_name == "lumma"


def test_system_info_record_unique_per_job(tmp_path):
    """Two SystemInfoRecord rows with the same extraction_job_id raise IntegrityError."""
    engine = _engine(tmp_path)
    with get_session(engine) as session:
        job_id = _make_job(session)
        session.add(SystemInfoRecord(extraction_job_id=job_id, country="US"))
        session.commit()

    with pytest.raises(IntegrityError):
        with get_session(engine) as session:
            session.add(SystemInfoRecord(extraction_job_id=job_id, country="UK"))
            session.commit()


def test_system_info_cascade_delete(tmp_path):
    """Deleting ExtractionJob cascades to SystemInfoRecord."""
    engine = _engine(tmp_path)
    with get_session(engine) as session:
        job_id = _make_job(session)
        session.add(SystemInfoRecord(extraction_job_id=job_id, country="DE"))
        session.commit()

    with get_session(engine) as session:
        assert session.query(SystemInfoRecord).filter_by(extraction_job_id=job_id).count() == 1
        job = session.get(ExtractionJob, job_id)
        session.delete(job)
        session.commit()

    with get_session(engine) as session:
        assert session.query(SystemInfoRecord).filter_by(extraction_job_id=job_id).count() == 0
