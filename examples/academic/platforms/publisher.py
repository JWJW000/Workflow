"""Atomic and conflict-free PDF publisher.

Handles:
1. Strict validation before publish
2. Quarantine of invalid / error files to .doi_download_state/quarantine/<platform>/
3. Content-hash deduplication (no-op if identical content already published)
4. Conflict isolation if different content exists for the same DOI (quarantine as doi_content_conflict)
5. Atomic publish via same-filesystem rename to oapdf/
"""

from __future__ import annotations

import hashlib
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .base import FailureClass
from .content_filter import record_exclusion
from .validation import ValidationResult, sanitize_filename_doi, validate_pdf


@dataclass
class PublishOutcome:
    published: bool
    final_path: Optional[Path] = None
    status: str = "failed"  # downloaded | duplicate_skipped | quarantined | conflict_quarantined
    bytes_count: int = 0
    failure_class: Optional[str] = None
    message: str = ""


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(64 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _publish_staged_pdf_unlocked(
    staged_path: Path,
    output_dir: Path,
    quarantine_dir: Path,
    expected_doi: str,
    expected_title: str = "",
    platform: str = "unknown",
) -> PublishOutcome:
    """Atomically validate and publish a staged PDF file into output_dir.

    If invalid, moves file to quarantine_dir/<platform>/.
    If already exists with same SHA256, unlinks staged_path and returns duplicate_skipped.
    If already exists with different SHA256, isolates staged file to quarantine.
    """
    if not staged_path.is_file():
        return PublishOutcome(
            published=False,
            status="failed",
            failure_class=FailureClass.DOWNLOAD_NOT_TRIGGERED.value,
            message="No staged file was produced; inspect retained workflow artifacts for the page or navigation failure",
        )

    # 1. Strict validation
    val = validate_pdf(staged_path, expected_doi=expected_doi, expected_title=expected_title)
    if not val.is_valid:
        if val.failure_class == FailureClass.NON_ARTICLE:
            record_exclusion(quarantine_dir.parent / 'non_article_manifest.jsonl', expected_doi, expected_title, val.error_message)
        # Move to quarantine
        quarantine_target_dir = quarantine_dir / platform
        quarantine_target_dir.mkdir(parents=True, exist_ok=True)
        fail_name = f"{val.failure_class.value if val.failure_class else 'invalid'}_{staged_path.name}"
        quarantine_file = quarantine_target_dir / fail_name
        try:
            shaged_stat_size = staged_path.stat().st_size
            shutil.move(str(staged_path), str(quarantine_file))
        except OSError:
            shaged_stat_size = 0
        return PublishOutcome(
            published=False,
            status="non_article" if val.failure_class == FailureClass.NON_ARTICLE else "quarantined",
            bytes_count=shaged_stat_size,
            failure_class=val.failure_class.value if val.failure_class else "invalid_pdf",
            message=val.error_message,
        )

    # 2. Destination path computation
    target_filename = sanitize_filename_doi(expected_doi)
    destination = output_dir / target_filename
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3. Check existing file
    staged_sha = file_sha256(staged_path)
    if destination.exists():
        dest_val = validate_pdf(destination, expected_doi=expected_doi, expected_title=expected_title)
        if dest_val.is_valid:
            dest_sha = file_sha256(destination)
            if dest_sha == staged_sha:
                # Identical content already published
                staged_path.unlink(missing_ok=True)
                return PublishOutcome(
                    published=True,
                    final_path=destination,
                    status="duplicate_skipped",
                    bytes_count=destination.stat().st_size,
                    message="Identical valid PDF already present in output",
                )
            else:
                # Content conflict: different content for same DOI!
                quarantine_target_dir = quarantine_dir / platform
                quarantine_target_dir.mkdir(parents=True, exist_ok=True)
                conflict_file = quarantine_target_dir / f"conflict_{target_filename}"
                shutil.move(str(staged_path), str(conflict_file))
                return PublishOutcome(
                    published=False,
                    status="conflict_quarantined",
                    bytes_count=conflict_file.stat().st_size,
                    failure_class="doi_content_conflict",
                    message=f"Conflict with existing file in output: {destination}",
                )
        else:
            # Existing file in destination is invalid! Quarantine it
            quarantine_target_dir = quarantine_dir / platform
            quarantine_target_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(destination), str(quarantine_target_dir / f"corrupt_replaced_{target_filename}"))

    # 4. Atomic publish
    # Use temporary file on the same filesystem as destination for atomic rename
    temp_dest = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(str(staged_path), str(temp_dest))
        temp_dest.replace(destination)
        staged_path.unlink(missing_ok=True)
        return PublishOutcome(
            published=True,
            final_path=destination,
            status="downloaded",
            bytes_count=destination.stat().st_size,
            message="Published successfully",
        )
    except OSError as e:
        temp_dest.unlink(missing_ok=True)
        return PublishOutcome(
            published=False,
            status="failed",
            failure_class="publish_error",
            message=str(e),
        )


def publish_staged_pdf(
    staged_path: Path,
    output_dir: Path,
    quarantine_dir: Path,
    expected_doi: str,
    expected_title: str = "",
    platform: str = "unknown",
) -> PublishOutcome:
    """Serialize publication for a DOI across all downloader processes."""
    target_filename = sanitize_filename_doi(expected_doi)
    claim_dir = output_dir.parent / ".doi_download_state" / "claims" / "publish"
    claim_dir.mkdir(parents=True, exist_ok=True)
    claim = claim_dir / f"{target_filename}.claim"
    deadline = time.monotonic() + 30.0
    while True:
        try:
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(f"{os.getpid()}\n")
            break
        except FileExistsError:
            try:
                if time.time() - claim.stat().st_mtime > 6 * 3600:
                    claim.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                return PublishOutcome(
                    published=False,
                    status="publish_busy",
                    failure_class="publish_busy",
                    message=f"another process is publishing {expected_doi}",
                )
            time.sleep(0.1)
    try:
        return _publish_staged_pdf_unlocked(
            staged_path,
            output_dir,
            quarantine_dir,
            expected_doi,
            expected_title,
            platform,
        )
    finally:
        claim.unlink(missing_ok=True)
