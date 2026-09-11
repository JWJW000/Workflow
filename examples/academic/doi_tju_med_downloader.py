#!/usr/bin/env python3
"""Batch Downloader for High-Value Medical & Association Journals via Tianjin University Resource Access 2 (p.lib.tju.edu.cn).

Candidate platforms (institutional proxy routes must be verified separately):
- BMJ (10.1136/)
- RSNA Radiology (10.1148/)
- Oxford University Press (10.1093/)
- LWW / ACG (10.14309/)
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))

from platforms.publisher import publish_staged_pdf
from platforms.runtime_state import PlatformRuntimeState, save_runtime_state, trip_needs_login
from platforms.tju_login import TjuLoginError, ensure_tju_login
from platforms.validation import sanitize_filename_doi

VALID_PREFIXES = ("10.1136/", "10.1148/", "10.1093/", "10.14309/")
PROFILE_DIR = ".drission-workflow/profiles/doi-tju-audit"


def verified_proxy_url(row: Dict[str, str]) -> str:
    """Use an explicitly verified portal route; a public DOI is not a proxy."""
    url = (row.get("tju_proxy_url") or "").strip()
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if (parsed.scheme == "https" and (host == "p.lib.tju.edu.cn" or host.endswith(".p.lib.tju.edu.cn"))
                and parsed.port in (None, 443) and not parsed.username and not parsed.password):
            return url
    except ValueError:
        pass
    return ""


try:
    from platforms.content_filter import skip_row
except ImportError:
    from examples.academic.platforms.content_filter import skip_row


def load_existing_stems(output_dir: Path) -> set[str]:
    existing: set[str] = set()
    if output_dir.is_dir():
        for p in output_dir.glob("*.pdf"):
            if not p.name.startswith("._") and p.stat().st_size >= 1024:
                existing.add(p.name.lower())
    return existing


def iter_candidates(input_dir: Path, existing: set[str]) -> Iterator[Dict[str, str]]:
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
                if not doi:
                    continue
                doi_lower = doi.lower()
                if not doi_lower.startswith(VALID_PREFIXES):
                    continue
                if doi_lower in seen:
                    continue
                seen.add(doi_lower)

                fname = sanitize_filename_doi(doi).lower()
                if fname in existing:
                    continue

                title = (row.get("title") or "Untitled").strip()
                proxy_url = verified_proxy_url(row)
                if not proxy_url:
                    continue
                yield {
                    "doi": doi,
                    "title": title,
                    "publisher": (row.get("publisher") or "").strip(),
                    "filename": fname,
                    "url": proxy_url,
                }


def run_tju_med_downloader(target_dir: Path, limit: int = 0, batch_size: int = 20, *, platform_id: str = "tju_med", candidates_factory=iter_candidates, workflow_name: str = "doi-tju-med-batch-downloader.yaml") -> None:
    input_dir = target_dir / "total_journals" if (target_dir / "total_journals").is_dir() else target_dir
    output_dir = target_dir / "oapdf"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = target_dir / ".doi_download_state" / f"{platform_id}_manifest.jsonl"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    quarantine_dir = target_dir / ".doi_download_state" / "quarantine"
    browser_stage_root = target_dir / ".doi_download_state" / "browser_batches" / platform_id
    browser_stage_root.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[2]
    runner_bin = root / "target" / "debug" / "drission-workflow"

    runtime = PlatformRuntimeState(
        platform_id=platform_id,
        state="running",
        reason=f"TJU Resource 2 (BMJ/RSNA/OUP/LWW); limit={limit or 'all'}",
    )
    save_runtime_state(target_dir, runtime)

    if next(candidates_factory(input_dir, load_existing_stems(output_dir)), None) is None:
        runtime.state = "unavailable"
        runtime.reason = "没有待下载且已验证的天大代理文章地址；门户登录不等于出版社全文授权，不能以公网 DOI 冒充代理下载。"
        save_runtime_state(target_dir, runtime)
        print(runtime.reason, flush=True)
        return

    print("Checking Tianjin Resource Access 2 login in shared Chrome...", flush=True)
    try:
        ensure_tju_login("resource2", timeout=90)
    except TjuLoginError as error:
        reason = str(error)
        trip_needs_login(target_dir, platform_id, reason)
        print(f"Stopped before downloads: {reason}", flush=True)
        return

    print("Indexing existing files in oapdf...")
    existing = load_existing_stems(output_dir)
    print(f"Skipping {len(existing)} existing PDFs.")

    target_label = str(limit) if limit else "all"
    print(f"Collecting TJU Medical/Association candidates (BMJ, RSNA, OUP, LWW, target: {target_label})...")
    candidates_iter = candidates_factory(input_dir, existing)

    total_success = 0
    total_failed = 0

    while True:
        batch: List[Dict[str, str]] = []
        for c in candidates_iter:
            batch.append(c)
            if len(batch) >= batch_size or (limit and (total_success + total_failed + len(batch)) >= limit):
                break

        if not batch:
            print("All pending candidates downloaded or processed!")
            break

        print(f"\n--- Starting batch of {len(batch)} articles (Total so far: {total_success} ok, {total_failed} fail) ---")

        with tempfile.TemporaryDirectory(prefix="tju_med_batch_", dir=browser_stage_root) as tmp_dir:
            tmp_path = Path(tmp_dir)
            payload_file = tmp_path / "payload.json"
            payload_file.write_text(json.dumps({"articles": batch}, ensure_ascii=False), encoding="utf-8")

            cmd = [
                str(runner_bin),
                "run",
                str(root / "examples" / "templates" / workflow_name),
                "--inputs", str(payload_file),
                "--artifacts", str(tmp_path),
            ]
            print(
                "Automatic login preflight passed; starting DOI downloads in the same shared browser.",
                flush=True,
            )
            proc = subprocess.run(cmd, cwd=str(root), check=False)

            # Login/navigation preflight is deliberately fatal.  Never turn a
            # failed gate into twenty false per-DOI failures or continue into
            # another batch with an unauthenticated browser.
            if proc.returncode != 0:
                reason = (
                    f"TJU Resource 2 login/session gate failed (workflow exit "
                    f"{proc.returncode}); complete login in the opened profile and retry"
                )
                trip_needs_login(target_dir, platform_id, reason)
                print(f"Stopped before downloads: {reason}", flush=True)
                return

            for item in batch:
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
                        platform=platform_id,
                    )
                    if outcome.published:
                        total_success += 1
                        print(f"  ✓ {outcome.status}: {fname} ({outcome.bytes_count // 1024} KB)")
                        rec = {
                            "doi": item["doi"],
                            "title": item["title"],
                            "status": outcome.status,
                            "file": str(outcome.final_path),
                            "bytes": outcome.bytes_count,
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
                            "error": outcome.message,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        }
                else:
                    total_failed += 1
                    print(f"  ✗ Failed to acquire PDF stream for {item['doi']}")
                    rec = {"doi": item["doi"], "title": item["title"], "status": "browser_failed"}

                with open(manifest_file, "a", encoding="utf-8") as mf:
                    mf.write(json.dumps(rec, ensure_ascii=False) + "\n")

        runtime.recent_attempts = total_success + total_failed
        runtime.recent_successes = total_success
        runtime.last_success_at = datetime.now(timezone.utc).isoformat() if total_success else None
        save_runtime_state(target_dir, runtime)

        if limit and (total_success + total_failed) >= limit:
            break

        print("Batch complete. Resting 45 seconds before next batch...")
        time.sleep(45)

    runtime.state = "stopped"
    runtime.reason = f"Finished run: successes={total_success}, failures={total_failed}"
    save_runtime_state(target_dir, runtime)
    print(f"\nAll batches finished. Total Success: {total_success} | Total Failed: {total_failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TJU Medical & Association Batch Downloader")
    parser.add_argument("target", nargs="?", default="/Volumes/PortableSSD/doi撞库202608028")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    run_tju_med_downloader(Path(args.target).resolve(), limit=args.limit, batch_size=args.batch_size)
