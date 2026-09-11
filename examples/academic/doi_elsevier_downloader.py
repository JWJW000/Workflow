#!/usr/bin/env python3
"""Batch Downloader for Elsevier/ScienceDirect via Tianjin University EDS.

Features:
- Single long-lived Chrome instance per batch (prevents repeated popups and profile lockouts).
- Reads DOI candidates, queries CrossRef for PII, and constructs direct S3/EDS links.
- Strictly validates PDF structure and content before publishing to oapdf/.
- Updates runtime state (.doi_download_state/runtime/elsevier.json) for http://localhost:8899/ dashboard.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from contextlib import nullcontext
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from platforms.base import Work
from platforms.publisher import publish_staged_pdf
from platforms.runtime_state import PlatformRuntimeState, save_runtime_state, trip_needs_login
from platforms.tju_login import TjuLoginError, ensure_tju_login
from platforms.validation import MIN_PDF_BYTES, TRAILER_EOF_BYTES, sanitize_filename_doi

TJU_EDS_HOST = "https://cfbfh253cb3a601b84ef2sbwnowbbpncov6xw6fgac.eds.tju.edu.cn"
PROFILE_DIR = ".drission-workflow/profiles/doi-tju-eds"


try:
    from platforms.content_filter import skip_row
except ImportError:
    from examples.academic.platforms.content_filter import skip_row


def resolve_elsevier_pii(doi: str, timeout: int = 6) -> Optional[str]:
    """Resolve PII code for an Elsevier DOI via CrossRef API."""
    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi.strip(), safe='/:._-')}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "AcademicDownloader/1.0 (mailto:researcher@tju.edu.cn)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            links = data.get("message", {}).get("link", [])
            for item in links:
                u = item.get("URL", "")
                if "PII:" in u:
                    pii = u.split("PII:")[1].split("?")[0].strip()
                    if pii:
                        return pii
    except Exception:
        pass
    return None


def is_probable_pdf(path: Path) -> bool:
    """Cheap startup check; full parsing remains mandatory before publication."""
    try:
        size = path.stat().st_size
        if size < MIN_PDF_BYTES or path.name.startswith("._"):
            return False
        with path.open("rb") as handle:
            if not handle.read(5).startswith(b"%PDF-"):
                return False
            handle.seek(max(0, size - TRAILER_EOF_BYTES))
            return b"%%EOF" in handle.read(TRAILER_EOF_BYTES)
    except OSError:
        return False


def load_existing_stems(output_dir: Path) -> set[str]:
    """Index published names without reparsing the entire multi-platform corpus.

    Every newly published file already passes ``publish_staged_pdf``'s strict
    parser.  Reopening tens of thousands of PDFs from an external disk here
    added about a minute to every calibration and emitted noisy parser warnings.
    Corpus integrity audits remain a separate maintenance operation.
    """
    existing: set[str] = set()
    if output_dir.is_dir():
        for p in output_dir.glob("*.pdf"):
            if not p.name.startswith("._"):
                existing.add(p.name.lower())
    return existing


# Common placeholder / non-article titles in ScienceDirect that have no downloadable full-text PDF
NON_ARTICLE_TITLE_KEYWORDS = (
    "editorial board",
    "table of contents",
    "author index",
    "subject index",
    "cumulative index",
    "corrigendum to",
    "erratum to",
    "inside front cover",
    "inside back cover",
    "outside front cover",
    "outside back cover",
    "blank page",
    "publisher's note",
    "announcement",
    "calendar",
    "guide for authors",
)


def is_non_article_title(title: str) -> bool:
    t = title.lower().strip()
    return any(keyword in t for keyword in NON_ARTICLE_TITLE_KEYWORDS)


def iter_elsevier_candidates(input_dir: Path, existing: set[str]) -> Iterator[Dict[str, str]]:
    seen: set[str] = set()
    for cpath in sorted(input_dir.glob("*.csv")):
        if cpath.name.startswith("._"):
            continue
        with cpath.open(encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if skip_row(input_dir, row):
                    continue
                doi = (row.get("doi") or "").strip()
                if not doi or not doi.lower().startswith(("10.1016/", "10.1053/")):
                    continue
                key = doi.lower()
                if key in seen:
                    continue
                seen.add(key)
                fname = sanitize_filename_doi(doi).lower()
                if fname in existing:
                    continue
                title = (row.get("title") or "Untitled").strip()
                if is_non_article_title(title):
                    continue
                yield {
                    "doi": doi,
                    "title": title,
                    "publisher": (row.get("publisher") or "Elsevier").strip(),
                    "filename": fname,
                }


def run_elsevier_downloader(target_dir: Path, limit: int = 0, batch_size: int = 25) -> None:
    input_dir = target_dir / "total_journals" if (target_dir / "total_journals").is_dir() else target_dir
    output_dir = target_dir / "oapdf"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = target_dir / ".doi_download_state" / "elsevier_buaa_webvpn_manifest.jsonl"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    quarantine_dir = target_dir / ".doi_download_state" / "quarantine"
    browser_stage_root = target_dir / ".doi_download_state" / "browser_batches" / "elsevier"
    browser_stage_root.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[2]
    runner_bin = root / "target" / "debug" / "drission-workflow"

    runtime = PlatformRuntimeState(
        platform_id="elsevier",
        state="running",
        reason=f"Tianjin University EDS; limit={limit or 'all'}",
    )
    save_runtime_state(target_dir, runtime)

    print("Checking Tianjin Resource Access 1 login in shared Chrome...", flush=True)
    try:
        ensure_tju_login("eds", timeout=90)
    except TjuLoginError as error:
        reason = str(error)
        trip_needs_login(target_dir, "elsevier", reason)
        print(f"Stopped before downloads: {reason}", flush=True)
        return

    print("Indexing existing files in oapdf...")
    existing = load_existing_stems(output_dir)
    print(f"Skipping {len(existing)} existing PDFs.")

    target_label = str(limit) if limit else "all"
    print(f"Collecting Elsevier candidates (target: {target_label})...")
    candidates_iter = iter_elsevier_candidates(input_dir, existing)

    total_success = 0
    total_failed = 0

    while True:
        # Collect batch
        batch: List[Dict[str, str]] = []
        for c in candidates_iter:
            batch.append(c)
            if len(batch) >= batch_size or (limit and (total_success + total_failed + len(batch)) >= limit):
                break

        if not batch:
            print("All pending Elsevier candidates downloaded or processed!")
            break

        print(f"\n--- Starting batch of {len(batch)} articles (Total so far: {total_success} ok, {total_failed} fail) ---")
        # Pre-resolve PIIs
        valid_items = []
        for item in batch:
            pii = resolve_elsevier_pii(item["doi"])
            if pii:
                item["pii"] = pii
                # Warm the full article page before asking the same browser
                # session to load the PDF endpoint.
                item["url"] = f"{TJU_EDS_HOST}/science/article/pii/{pii}"
                # The warmed page establishes the institutional/session state.
                # Navigation to this URL is then performed by the same visible
                # Chrome session; the driver keeps PDF-viewer responses as files.
                item["pdf_url"] = (
                    f"{TJU_EDS_HOST}/science/article/pii/{pii}"
                    "/pdfft?isDTMRedir=true&download=true"
                )
                valid_items.append(item)
            else:
                print(f"  ✗ CrossRef could not resolve PII for {item['doi']}")
                total_failed += 1
                rec = {
                    "doi": item["doi"],
                    "title": item["title"],
                    "status": "resolve_failed",
                    "file": None,
                    "bytes": 0,
                    "error": "CrossRef PII not found",
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
                with open(manifest_file, "a", encoding="utf-8") as mf:
                    mf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        if not valid_items:
            continue

        # Execute single long-running Chrome batch
        with nullcontext(tempfile.mkdtemp(
            prefix="tju_eds_batch_", dir=browser_stage_root
        )) as tmp_dir:
            tmp_path = Path(tmp_dir)
            payload_file = tmp_path / "payload.json"
            payload_file.write_text(json.dumps({"articles": valid_items}, ensure_ascii=False), encoding="utf-8")

            cmd = [
                str(runner_bin),
                "run",
                str(root / "examples" / "templates" / "doi-elsevier-tju-eds-batch-downloader.yaml"),
                "--inputs", str(payload_file),
                "--artifacts", str(tmp_path),
            ]
            t0 = time.monotonic()
            # Stream the real workflow error to the dashboard log.  The old
            # capture_output path hid LOCATOR_NOT_FOUND / CDP errors and left
            # only a misleading generic failure for every DOI in the batch.
            proc = subprocess.run(cmd, cwd=str(root), check=False)
            print(
                f"Workflow finished: exit={proc.returncode}, "
                f"elapsed={time.monotonic() - t0:.1f}s, artifacts={tmp_path}",
                flush=True,
            )

            if proc.returncode != 0:
                print(f"Workflow failed (exit={proc.returncode}); retaining {tmp_path} and recording missing PDFs before continuing", flush=True)

            # Move and validate downloaded files
            for item in valid_items:
                fname = item["filename"]
                found_file = next(
                    (path for path in tmp_path.rglob(fname) if path.is_file()),
                    None,
                )

                if found_file and found_file.stat().st_size >= 1024:
                    outcome = publish_staged_pdf(
                        staged_path=found_file,
                        output_dir=output_dir,
                        quarantine_dir=quarantine_dir,
                        expected_doi=item["doi"],
                        expected_title=item["title"],
                        platform="elsevier",
                    )
                    if outcome.published:
                        total_success += 1
                        print(
                            f"  ✓ {outcome.status}: {fname} "
                            f"({outcome.bytes_count // 1024} KB)"
                        )
                        rec = {
                            "doi": item["doi"],
                            "title": item["title"],
                            "pii": item["pii"],
                            "status": outcome.status,
                            "file": str(outcome.final_path),
                            "bytes": outcome.bytes_count,
                            "workflow_exit_code": proc.returncode,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        }
                    else:
                        total_failed += 1
                        print(f"  ✗ {outcome.status}: {outcome.message}")
                        rec = {
                            "doi": item["doi"],
                            "title": item["title"],
                            "status": outcome.status,
                            "bytes": outcome.bytes_count,
                            "failure_class": outcome.failure_class,
                            "error": outcome.message,
                            "workflow_exit_code": proc.returncode,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        }
                else:
                    total_failed += 1
                    print(f"  ✗ Failed to acquire PDF stream for {item['doi']}")
                    rec = {
                        "doi": item["doi"],
                        "title": item["title"],
                        "status": "browser_failed",
                        "bytes": 0,
                        "failure_class": "workflow_failed" if proc.returncode else "download_not_triggered",
                        "error": (
                            f"workflow exit code {proc.returncode}; no PDF produced"
                            if proc.returncode
                            else "download was not triggered for this DOI"
                        ),
                        "workflow_exit_code": proc.returncode,
                        "recorded_at": datetime.now(timezone.utc).isoformat(),
                    }

                with open(manifest_file, "a", encoding="utf-8") as mf:
                    mf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Update runtime stats
        runtime.recent_attempts = total_success + total_failed
        runtime.recent_successes = total_success
        runtime.last_success_at = datetime.now(timezone.utc).isoformat() if total_success else None
        save_runtime_state(target_dir, runtime)

        if limit and (total_success + total_failed) >= limit:
            break

        # Inter-batch rest cooldown (prevent IP rate limiting)
        print("Batch complete. Resting 120 seconds to keep TJU EDS IP healthy...")
        time.sleep(120)

    runtime.state = "failed" if total_failed and not total_success else "stopped"
    runtime.reason = f"Completed run: successes={total_success}, failures={total_failed}"
    save_runtime_state(target_dir, runtime)
    print(f"\nAll batches finished. Total Success: {total_success} | Total Failed: {total_failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elsevier TJU EDS Downloader")
    parser.add_argument("target", nargs="?", default="/Volumes/PortableSSD/doi撞库202608028")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    try:
        run_elsevier_downloader(Path(args.target).resolve(), limit=args.limit, batch_size=args.batch_size)
    except Exception as e:
        save_runtime_state(
            Path(args.target).resolve(),
            PlatformRuntimeState(platform_id="elsevier", state="failed", reason=str(e))
        )
        raise
