#!/usr/bin/env python3
"""Process a folder of downloaded archives - extract credentials to database.

This standalone script processes archives without the Telegram pipeline.
All credentials are saved to the PostgreSQL database (ParsedCredential table).

Usage:
    uv run python process_folder.py /path/to/downloads
    uv run python process_folder.py /path/to/downloads --delete-after
    uv run python process_folder.py /path/to/downloads --output results.csv
    uv run python process_folder.py /path/to/downloads --config /path/to/config.toml
"""

import argparse
import asyncio
import csv
import logging
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

# Add the project root to path
sys.path.insert(0, str(Path(__file__).parent))

from telecrime.config import load_config
from telecrime.database import get_dialect_insert, get_engine, get_session_factory, init_db
from telecrime.extractor.seven_zip import SevenZipExtractor
from telecrime.models import ParsedCredential
from telecrime.stealer.parser import (
    iter_credentials_file,
    truncate_field,
)
from telecrime.stealer.patterns import detect_stealer_type, is_credential_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Archive extensions to process
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2"}


@dataclass
class ProcessingStats:
    """Track processing statistics."""

    archives_found: int = 0
    archives_processed: int = 0
    archives_failed: int = 0
    archives_empty: int = 0
    credentials_found: int = 0
    credentials_new: int = 0
    credentials_duplicate: int = 0
    unique_domains: set = field(default_factory=set)
    bytes_processed: int = 0
    bytes_deleted: int = 0


def find_archives(folder: Path) -> list[Path]:
    """Find all archive files in a folder (recursively)."""
    archives = []
    for f in folder.rglob("*"):
        if f.is_file():
            # Check extension
            suffix = f.suffix.lower()
            if suffix in ARCHIVE_EXTENSIONS:
                archives.append(f)
            # Also check for split archives like .part01.rar
            elif ".part" in f.name.lower() and f.name.lower().endswith(".rar"):
                archives.append(f)
            # Check for .001, .002 etc split files
            elif suffix and suffix[1:].isdigit():
                archives.append(f)

    return sorted(archives)


def is_split_part(archive: Path) -> tuple[bool, bool]:
    """Check if archive is a split part.

    Returns:
        (is_split, is_first_part) - True if split, True if it's the first part
    """
    name = archive.name.lower()
    import re

    # .part01.rar, .part02.rar etc
    if ".part" in name and name.endswith(".rar"):
        match = re.search(r"\.part(\d+)\.rar$", name)
        if match:
            part_num = int(match.group(1))
            return True, part_num == 1

    # .7z.001, .7z.002 etc
    if ".7z." in name:
        match = re.search(r"\.7z\.(\d+)$", name)
        if match:
            part_num = int(match.group(1))
            return True, part_num == 1

    # .zip.001, .zip.002 etc
    if ".zip." in name:
        match = re.search(r"\.zip\.(\d+)$", name)
        if match:
            part_num = int(match.group(1))
            return True, part_num == 1

    return False, True


async def process_archive(
    archive: Path,
    extractor: SevenZipExtractor,
    output_dir: Path,
    session,
    stats: ProcessingStats,
    delete_after: bool = False,
    collect_csv: bool = True,
) -> list[dict]:
    """Process a single archive and extract credentials.

    Returns:
        List of credential dicts (for optional CSV export). Empty when
        collect_csv is False — avoids materializing huge ULP batches in RAM.
    """
    credentials_for_csv: list[dict] = []
    archive_size = archive.stat().st_size
    stats.bytes_processed += archive_size

    # Check if this is a non-first split part (skip, will be processed with first part)
    is_split, is_first = is_split_part(archive)
    if is_split and not is_first:
        logger.debug("Skipping non-first split part: %s", archive.name)
        return credentials_for_csv

    logger.info("Processing: %s (%.1f MB)", archive.name, archive_size / 1024 / 1024)

    # Create temp extraction directory
    extract_dir = output_dir / f"extract_{archive.stem}"
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Try to list contents first
        matching_files = await extractor.find_matching_files(archive, [".txt"], password=None)

        if matching_files:
            logger.info("  Found %d .txt files in archive", len(matching_files))

        # Extract .txt files only
        result = await extractor.extract(
            archive,
            extract_dir,
            target_extensions=[".txt"],
            password=None,
        )

        if result.needs_password:
            logger.warning("  Archive needs password: %s", archive.name)
            stats.archives_failed += 1
            if delete_after:
                _delete_archive_and_parts(archive, stats)
            return credentials_for_csv

        if not result.success:
            logger.error("  Extraction failed: %s - %s", archive.name, result.error_message)
            stats.archives_failed += 1
            if delete_after:
                _delete_archive_and_parts(archive, stats)
            return credentials_for_csv

        if not result.extracted_files:
            logger.info("  No .txt files extracted from: %s", archive.name)
            stats.archives_empty += 1
            stats.archives_processed += 1
            if delete_after:
                _delete_archive_and_parts(archive, stats)
            return credentials_for_csv

        logger.info("  Extracted %d files", len(result.extracted_files))

        # Detect stealer type from extracted files
        all_filenames = [f.name for f in result.extracted_files]
        stealer_type = detect_stealer_type(all_filenames)
        if stealer_type:
            logger.info("  Detected stealer type: %s", stealer_type)

        # Find and parse credential files
        for txt_file in result.extracted_files:
            if not is_credential_file(txt_file.name):
                continue

            logger.debug("  Parsing credential file: %s", txt_file.name)

            try:
                file_found_count = 0
                seen_in_file: set[str] = set()
                batch: list = []
                batch_size = 2000
                txt_file_name = txt_file.name
                txt_file_path = str(txt_file)

                def flush_batch(items):
                    nonlocal file_found_count
                    if not items:
                        return

                    rows_by_hash: dict[str, dict] = {}
                    for cred in items:
                        domain_value = truncate_field(cred.domain or cred.url or "", 255) or ""
                        username_value = truncate_field(cred.username, 255) or ""
                        password_value = truncate_field(cred.password, 255) or ""
                        credential_hash = ParsedCredential.compute_hash(
                            domain_value, username_value, password_value
                        )
                        if credential_hash in seen_in_file or credential_hash in rows_by_hash:
                            stats.credentials_duplicate += 1
                            continue
                        rows_by_hash[credential_hash] = {
                            "url": truncate_field(cred.url, 1024) or "",
                            "domain": truncate_field(cred.domain, 255),
                            "username": username_value,
                            "password": password_value,
                            "email_domain": truncate_field(cred.email_domain, 255),
                            "application": truncate_field(cred.application, 100),
                            "profile": truncate_field(cred.profile, 100),
                            "source_file": txt_file_path,
                            "source_archive": archive.name,
                            "stealer_type": truncate_field(stealer_type, 50),
                            "credential_hash": credential_hash,
                        }
                        if cred.domain:
                            stats.unique_domains.add(cred.domain)

                    if not rows_by_hash:
                        return

                    insert = get_dialect_insert(session)
                    stmt = (
                        insert(ParsedCredential)
                        .values(list(rows_by_hash.values()))
                        .on_conflict_do_nothing(
                            index_elements=[ParsedCredential.credential_hash]
                        )
                        .returning(ParsedCredential.credential_hash)
                    )
                    inserted_hashes = {row[0] for row in session.execute(stmt)}

                    new_count = len(inserted_hashes)
                    stats.credentials_found += new_count
                    stats.credentials_new += new_count
                    stats.credentials_duplicate += len(rows_by_hash) - new_count
                    file_found_count += new_count
                    seen_in_file.update(rows_by_hash.keys())

                    if collect_csv:
                        for h in inserted_hashes:
                            row = rows_by_hash[h]
                            credentials_for_csv.append({
                                **{k: row[k] for k in (
                                    "url", "domain", "username", "password",
                                    "application", "profile", "source_archive", "stealer_type",
                                )},
                                "source_file": txt_file_name,
                            })

                    session.commit()
                    session.expire_all()

                for cred in iter_credentials_file(txt_file):
                    batch.append(cred)
                    if len(batch) >= batch_size:
                        flush_batch(batch)
                        batch = []
                flush_batch(batch)

                if file_found_count:
                    logger.info(
                        "    Found %d new credentials in %s",
                        file_found_count,
                        txt_file.name,
                    )

            except Exception as e:
                logger.error("    Error parsing %s: %s", txt_file.name, e)

        stats.archives_processed += 1

        # Delete archive if requested
        if delete_after:
            _delete_archive_and_parts(archive, stats)

        return credentials_for_csv

    finally:
        # Cleanup extraction directory
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)


def _delete_archive_and_parts(archive: Path, stats: ProcessingStats):
    """Delete an archive and any related split parts."""
    import re

    deleted_size = 0

    # Delete main archive
    if archive.exists():
        deleted_size += archive.stat().st_size
        archive.unlink()
        logger.info("  Deleted: %s", archive.name)

    # Find and delete related split parts
    parent = archive.parent

    # For .part01.rar style, find all .partXX.rar files
    if ".part" in archive.name.lower():
        match = re.match(r"(.+?)\.part\d+\.rar$", archive.name, re.IGNORECASE)
        if match:
            base = match.group(1)
            for f in parent.glob(f"{base}.part*.rar"):
                if f.exists() and f != archive:
                    deleted_size += f.stat().st_size
                    f.unlink()
                    logger.info("  Deleted split part: %s", f.name)

    # For .7z.001 style
    elif ".7z." in archive.name.lower():
        match = re.match(r"(.+\.7z)\.\d+$", archive.name, re.IGNORECASE)
        if match:
            base = match.group(1)
            for f in parent.glob(f"{base}.*"):
                if f.exists() and f.suffix[1:].isdigit():
                    deleted_size += f.stat().st_size
                    f.unlink()
                    logger.info("  Deleted split part: %s", f.name)

    # For .zip.001 style
    elif ".zip." in archive.name.lower():
        match = re.match(r"(.+\.zip)\.\d+$", archive.name, re.IGNORECASE)
        if match:
            base = match.group(1)
            for f in parent.glob(f"{base}.*"):
                if f.exists() and f.suffix[1:].isdigit():
                    deleted_size += f.stat().st_size
                    f.unlink()
                    logger.info("  Deleted split part: %s", f.name)

    stats.bytes_deleted += deleted_size


async def process_folder(
    folder: Path,
    output_file: Path | None = None,
    delete_after: bool = False,
    limit: int | None = None,
    db_url: str | None = None,
    config_path: Path | None = None,
) -> tuple[ProcessingStats, list[dict]]:
    """Process all archives in a folder."""
    stats = ProcessingStats()
    all_credentials = []

    # Setup database
    config = load_config(config_path)
    if db_url:
        config.database_url = db_url

    engine = get_engine(config.database_url)
    init_db(engine)
    session_factory = get_session_factory(engine)

    # Find archives
    archives = find_archives(folder)
    stats.archives_found = len(archives)

    logger.info("Found %d archive files in %s", len(archives), folder)
    logger.info("Database: %s", config.database_url)

    processed_marker = Path("data") / f".processed_{folder.name}.txt"
    processed_marker.parent.mkdir(parents=True, exist_ok=True)
    already_processed: set[str] = set()
    if processed_marker.exists():
        already_processed.update(
            line.strip() for line in processed_marker.read_text().splitlines() if line.strip()
        )
    # Probe DB for any remaining candidates using an indexed IN-lookup
    # (cheap) rather than a DISTINCT scan of the whole credentials table.
    candidate_names = [a.name for a in archives if a.name not in already_processed]
    if candidate_names:
        with session_factory() as resume_session:
            for chunk_start in range(0, len(candidate_names), 500):
                chunk = candidate_names[chunk_start:chunk_start + 500]
                already_processed.update(
                    name for (name,) in resume_session.query(
                        ParsedCredential.source_archive
                    ).filter(ParsedCredential.source_archive.in_(chunk)).distinct().all()
                    if name
                )
    before_skip = len(archives)
    archives = [a for a in archives if a.name not in already_processed]
    skipped = before_skip - len(archives)
    if skipped:
        logger.info("Resume: skipping %d archives already processed", skipped)

    if limit:
        archives = archives[:limit]
        logger.info("Processing first %d archives (--limit)", limit)

    # Setup extractor
    extractor = SevenZipExtractor()

    # data/ keeps temp output off the (possibly read-only) source folder.
    output_dir = Path("data") / f".extract_temp_{folder.name}"
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        session = session_factory()
        # Line-buffered — writes flush on '\n' without an explicit flush per archive.
        marker_fp = processed_marker.open("a", encoding="utf-8", buffering=1)
        try:
            for i, archive in enumerate(archives, 1):
                logger.info("\n[%d/%d] Processing archive...", i, len(archives))

                try:
                    creds = await process_archive(
                        archive,
                        extractor,
                        output_dir,
                        session,
                        stats,
                        delete_after,
                        collect_csv=bool(output_file),
                    )
                    if output_file:
                        all_credentials.extend(creds)
                except Exception as e:
                    logger.error("Error processing %s: %s", archive.name, e)
                    stats.archives_failed += 1

                marker_fp.write(archive.name + "\n")

                if i % 10 == 0:
                    logger.info(
                        "\n--- Progress: %d/%d archives, %d credentials (%d new, %d dup), %d domains ---\n",
                        stats.archives_processed,
                        stats.archives_found,
                        stats.credentials_found,
                        stats.credentials_new,
                        stats.credentials_duplicate,
                        len(stats.unique_domains),
                    )

                # Recycle session periodically to bound the ORM identity map.
                if i % 25 == 0:
                    session.commit()
                    session.close()
                    session = session_factory()
        finally:
            marker_fp.close()
            session.close()

    finally:
        # Cleanup temp directory
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)

    # Write output file if specified
    if output_file and all_credentials:
        write_credentials_csv(all_credentials, output_file)
        logger.info("Wrote %d credentials to %s", len(all_credentials), output_file)

    return stats, all_credentials


def write_credentials_csv(credentials: list[dict], output_file: Path):
    """Write credentials to a CSV file."""
    if not credentials:
        return

    fieldnames = [
        "url",
        "domain",
        "username",
        "password",
        "application",
        "profile",
        "source_archive",
        "source_file",
        "stealer_type",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(credentials)


def print_summary(stats: ProcessingStats, credentials: list[dict]):
    """Print processing summary."""
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Archives found:      {stats.archives_found}")
    print(f"Archives processed:  {stats.archives_processed}")
    print(f"Archives empty:      {stats.archives_empty}")
    print(f"Archives failed:     {stats.archives_failed}")
    print(f"Data processed:      {stats.bytes_processed / 1024 / 1024 / 1024:.2f} GB")
    if stats.bytes_deleted:
        print(f"Data deleted:        {stats.bytes_deleted / 1024 / 1024 / 1024:.2f} GB")
    print()
    print(f"Credentials found:   {stats.credentials_found}")
    print(f"  New (saved to DB): {stats.credentials_new}")
    print(f"  Duplicates:        {stats.credentials_duplicate}")
    print(f"Unique domains:      {len(stats.unique_domains)}")

    if credentials:
        # Top domains
        domain_counts = Counter(c["domain"] for c in credentials if c.get("domain"))
        print("\nTop 20 domains:")
        for domain, count in domain_counts.most_common(20):
            print(f"  {domain}: {count}")

        # Stealer types
        stealer_counts = Counter(
            c.get("stealer_type") for c in credentials if c.get("stealer_type")
        )
        if stealer_counts:
            print("\nStealer types detected:")
            for stealer, count in stealer_counts.most_common():
                print(f"  {stealer}: {count}")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Process downloaded archives to extract credentials to database"
    )
    parser.add_argument(
        "folder",
        type=Path,
        help="Folder containing downloaded archives",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        help="Config file path",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Optional: also export to CSV file",
    )
    parser.add_argument(
        "--delete-after",
        action="store_true",
        help="Delete archives after successful extraction",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        help="Only process first N archives",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=str,
        help="Database URL (default: from TELECRIME_DATABASE_URL or config.toml)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.folder.exists():
        print(f"Error: Folder not found: {args.folder}")
        sys.exit(1)

    if not args.folder.is_dir():
        print(f"Error: Not a directory: {args.folder}")
        sys.exit(1)

    # Run processing
    stats, credentials = asyncio.run(
        process_folder(
            args.folder,
            output_file=args.output,
            delete_after=args.delete_after,
            limit=args.limit,
            db_url=args.database,
            config_path=args.config,
        )
    )

    # Print summary
    print_summary(stats, credentials)

    # Set exit code based on results
    if stats.archives_failed > 0 and stats.archives_processed == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
