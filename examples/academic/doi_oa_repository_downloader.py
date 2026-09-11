#!/usr/bin/env python3
"""Concurrent Fast Lane downloader for OA articles with open repository URLs.

Features:
- Queries OpenAlex for best_oa_location direct PDF URLs for OA records
- Bounded thread pool with 12 workers
- Per-host rate limiting and semaphore control
- Strict PDF validation and atomic publishing
- Host-level circuit breaking upon consecutive errors
- Progress and throughput metrics output
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from platforms.base import Work, FailureClass
from platforms.oa_repository import OARepositoryAdapter
from platforms.publisher import publish_staged_pdf
from platforms.runtime_state import PlatformRuntimeState, save_runtime_state
from platforms.validation import validate_pdf, sanitize_filename_doi


try:
    from platforms.content_filter import skip_row
except ImportError:
    from examples.academic.platforms.content_filter import skip_row


def append_manifest_record(
    manifest_file: Path, manifest_lock: threading.Lock, record: Dict[str, Any]
) -> None:
    with manifest_lock:
        with manifest_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def acquire_doi_claim(target_dir: Path, filename: str) -> Optional[Path]:
    """Claim one DOI across concurrently running downloader processes."""
    claim_dir = target_dir / ".doi_download_state" / "claims" / "oa_repository"
    claim_dir.mkdir(parents=True, exist_ok=True)
    claim = claim_dir / f"{filename}.claim"
    try:
        fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        # Recover claims left by a process that died more than six hours ago.
        try:
            if time.time() - claim.stat().st_mtime <= 6 * 3600:
                return None
            claim.unlink()
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except (FileExistsError, FileNotFoundError, OSError):
            return None
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(f"{os.getpid()}\n")
    return claim


def load_completed_stems(target_dir: Path, manifest_file: Path) -> set[str]:
    """Pre-load stems of all existing valid PDFs and successful manifest entries to skip already downloaded works."""
    completed: set[str] = set()
    for folder_name in ("oapdf", "期刊文件 1000"):
        d = target_dir / folder_name
        if d.is_dir():
            for p in d.glob("*.pdf"):
                if not p.name.startswith("._") and p.stat().st_size >= 1024:
                    name_lower = p.name.lower()
                    completed.add(name_lower)
                    if name_lower.endswith(".pdf"):
                        completed.add(name_lower[:-4])
    if manifest_file.is_file():
        try:
            with manifest_file.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        if record.get("status") in ("downloaded", "duplicate_skipped"):
                            doi = str(record.get("doi") or "").strip().lower()
                            if doi:
                                completed.add(doi)
                                stem = sanitize_filename_doi(doi).lower()
                                completed.add(stem)
                                if stem.endswith(".pdf"):
                                    completed.add(stem[:-4])
                    except Exception:
                        pass
        except Exception:
            pass
    return completed


def known_public_pdf_url(doi: str) -> str:
    doi = doi.strip().lower()
    match = re.fullmatch(r'10\.1371/journal\.(pbio|ppat|pmed)\.(\d+)', doi)
    if match:
        journal = {'pbio': 'plosbiology', 'ppat': 'plospathogens', 'pmed': 'plosmedicine'}[match[1]]
        return f'https://journals.plos.org/{journal}/article/file?' + urllib.parse.urlencode({'id': doi, 'type': 'printable'})
    match = re.fullmatch(r'10\.5194/(acp|hess)-(\d+)-(\d+)-(\d{4})', doi)
    if match:
        journal, volume, page, year = match.groups()
        return f'https://{journal}.copernicus.org/articles/{volume}/{page}/{year}/{journal}-{volume}-{page}-{year}.pdf'
    return ''


def resolve_oa_row(row: Dict[str, str]) -> Optional[Work]:
    url = known_public_pdf_url(row['doi'])
    if url:
        return Work(doi=row['doi'], title=row['title'], publisher=row['publisher'], is_oa=True,
                    oa_urls=(url,), openalex_pdf_url=url)
    return query_oa_pdf_url(row['doi'], row['title'], row['publisher'])


def iter_oa_csv_rows(input_dir: Path, skip_stems: Optional[set[str]] = None) -> Iterator[Dict[str, str]]:
    seen: set[str] = set()
    skip = skip_stems or set()
    for fpath in sorted(p for p in input_dir.glob("*.csv") if not p.name.startswith("._")):
        with fpath.open(encoding="utf-8-sig", errors="replace") as handle:
            for row in csv.DictReader(handle):
                if skip_row(input_dir, row):
                    continue
                doi = (row.get("doi") or "").strip()
                key = doi.lower()
                if not doi or key in seen:
                    continue
                seen.add(key)
                # Check if already downloaded
                fname = sanitize_filename_doi(doi).lower()
                stem = fname[:-4] if fname.endswith(".pdf") else fname
                if key in skip or fname in skip or stem in skip:
                    continue
                yield {
                    "doi": doi,
                    "title": row.get("title", ""),
                    "publisher": row.get("publisher", ""),
                }


class HostLimiter:
    def __init__(self, max_concurrent: int = 2, min_interval: float = 1.0) -> None:
        self.max_concurrent = max_concurrent
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._semaphores: Dict[str, threading.BoundedSemaphore] = {}
        self._last_access: Dict[str, float] = {}
        self._fail_counts: Dict[str, int] = {}

    def get_host(self, url: str) -> str:
        return (urllib.parse.urlparse(url).hostname or "unknown").lower()

    def is_breaker_tripped(self, host: str) -> bool:
        with self._lock:
            return self._fail_counts.get(host, 0) >= 5

    def record_outcome(self, host: str, success: bool) -> None:
        with self._lock:
            if success:
                self._fail_counts[host] = 0
            else:
                self._fail_counts[host] = self._fail_counts.get(host, 0) + 1

    def acquire(self, url: str) -> threading.BoundedSemaphore:
        host = self.get_host(url)
        with self._lock:
            if host not in self._semaphores:
                self._semaphores[host] = threading.BoundedSemaphore(self.max_concurrent)
            sem = self._semaphores[host]
            now = time.monotonic()
            last = self._last_access.get(host, 0.0)
            scheduled = max(now, last + self.min_interval)
            self._last_access[host] = scheduled
        wait = scheduled - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        return sem


# Host domains known to block headless HTTP requests with 403/Cloudflare
STRICT_BOT_HOSTS = {
    "sciencedirect.com",
    "www.sciencedirect.com",
    "jamanetwork.com",
    "www.acpjournals.org",
    "acpjournals.org",
    "tandfonline.com",
    "www.tandfonline.com",
    "onlinelibrary.wiley.com",
    "academic.oup.com",
    "asmedigitalcollection.asme.org",
    "aacrjournals.org",
    "pubs.acs.org",
    "ieeexplore.ieee.org",
}

OPEN_REPO_KEYWORDS = (
    "europepmc", "ncbi", "pmc", "arxiv", "zenodo", "figshare",
    "biorxiv", "medrxiv", "osf.io", "hal.science", "researchgate",
    "citeseerx", "dspace", "handle", "repo", "univ", "edu", "ac.",
    "core.ac.uk", "semanticscholar"
)


def query_oa_pdf_url(doi: str, title: str, publisher: str) -> Optional[Work]:
    """Query OpenAlex for direct PDF URLs, prioritizing institutional and open repositories."""
    api_url = f"https://api.openalex.org/works/doi:{urllib.parse.quote(doi, safe='/:._-')}?mailto=drission-workflow@local"
    req = urllib.request.Request(api_url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            locations = data.get("locations") or []
            repo_urls: list[str] = []
            other_urls: list[str] = []
            blocked_urls: list[str] = []
            primary_landing = f"https://doi.org/{doi}"

            best_oa = data.get("best_oa_location") or {}
            if best_oa.get("landing_page_url"):
                primary_landing = best_oa.get("landing_page_url")

            for loc in locations:
                pdf_url = loc.get("pdf_url")
                if not pdf_url or not pdf_url.startswith("http") or pdf_url.endswith(".html"):
                    continue
                host = (urllib.parse.urlparse(pdf_url).hostname or "").lower()
                if any(repo in host for repo in OPEN_REPO_KEYWORDS):
                    if pdf_url not in repo_urls:
                        repo_urls.append(pdf_url)
                elif any(b_host in host for b_host in STRICT_BOT_HOSTS):
                    if pdf_url not in blocked_urls:
                        blocked_urls.append(pdf_url)
                else:
                    if pdf_url not in other_urls:
                        other_urls.append(pdf_url)

            # Fallback to best_oa if not already categorized
            best_pdf = best_oa.get("pdf_url")
            if best_pdf and best_pdf.startswith("http") and not best_pdf.endswith(".html"):
                host = (urllib.parse.urlparse(best_pdf).hostname or "").lower()
                if any(repo in host for repo in OPEN_REPO_KEYWORDS):
                    if best_pdf not in repo_urls:
                        repo_urls.append(best_pdf)
                elif any(b_host in host for b_host in STRICT_BOT_HOSTS):
                    if best_pdf not in blocked_urls:
                        blocked_urls.append(best_pdf)
                else:
                    if best_pdf not in other_urls:
                        other_urls.append(best_pdf)

            # Only accept open repositories and direct non-blocked hosts for HTTP Fast Lane
            candidate_urls = repo_urls + other_urls
            if candidate_urls:
                return Work(
                    doi=doi,
                    title=title,
                    publisher=publisher,
                    is_oa=True,
                    openalex_pdf_url=candidate_urls[0],
                    oa_urls=tuple(candidate_urls),
                    openalex_landing_url=primary_landing,
                )
    except Exception:
        pass
    return None


def download_single_oa(
    work: Work,
    target_dir: Path,
    limiter: HostLimiter,
    manifest_file: Path,
    manifest_lock: threading.Lock,
    output_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    output_dir = output_dir or target_dir / "oapdf"
    staging_dir = target_dir / ".doi_download_tmp" / "oa_repository"
    quarantine_dir = target_dir / ".doi_download_state" / "quarantine"

    filename = sanitize_filename_doi(work.doi)
    final_path = output_dir / filename
    claim = acquire_doi_claim(target_dir, filename)
    if claim is None:
        record = {
            "doi": work.doi,
            "title": work.title,
            "publisher": work.publisher,
            "status": "claimed_elsewhere",
            "file": None,
            "bytes": 0,
            "failure_class": None,
            "error": "another downloader process owns this DOI",
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        append_manifest_record(manifest_file, manifest_lock, record)
        return record

    try:
        # Re-check after obtaining the claim so two processes cannot publish the
        # same DOI between the initial existence check and download.
        if final_path.is_file():
            val = validate_pdf(final_path, expected_doi=work.doi, expected_title=work.title)
            if val.is_valid:
                record = {
                    "doi": work.doi,
                    "title": work.title,
                    "publisher": work.publisher,
                    "is_oa": True,
                    "access_route": "direct_http",
                    "landing_url": work.openalex_landing_url or f"https://doi.org/{work.doi}",
                    "pdf_url": work.openalex_pdf_url,
                    "status": "duplicate_skipped",
                    "file": str(final_path),
                    "bytes": val.size_bytes,
                    "failure_class": None,
                    "error": None,
                    "duration_ms": 0,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
                append_manifest_record(manifest_file, manifest_lock, record)
                return record

        urls_to_try = list(work.oa_urls) if work.oa_urls else ([work.openalex_pdf_url] if work.openalex_pdf_url else [])
        if not urls_to_try:
            record = {
                "doi": work.doi,
                "title": work.title,
                "publisher": work.publisher,
                "status": "failed",
                "file": None,
                "bytes": 0,
                "failure_class": "no_candidate_url",
                "error": "no valid candidate PDF URL found",
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            append_manifest_record(manifest_file, manifest_lock, record)
            return record

        staging_dir.mkdir(parents=True, exist_ok=True)
        staging_path = staging_dir / f"{filename}.{os.getpid()}.{threading.get_ident()}.tmp"
        t0 = time.monotonic()
        status = "failed"
        failure_class = None
        bytes_count = 0
        err_msg = ""
        successful_url = ""

        for candidate_url in urls_to_try:
            host = limiter.get_host(candidate_url)
            if limiter.is_breaker_tripped(host):
                continue

            sem = limiter.acquire(candidate_url)
            with sem:
                try:
                    req = urllib.request.Request(
                        candidate_url,
                        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36", "Accept": "application/pdf"},
                    )
                    with urllib.request.urlopen(req, timeout=18) as resp, staging_path.open("wb") as out:
                        shutil.copyfileobj(resp, out)

                    pub_res = publish_staged_pdf(
                        staged_path=staging_path,
                        output_dir=output_dir,
                        quarantine_dir=quarantine_dir,
                        expected_doi=work.doi,
                        expected_title=work.title,
                        platform="oa_repository",
                    )
                    if pub_res.published:
                        status = pub_res.status
                        bytes_count = pub_res.bytes_count
                        limiter.record_outcome(host, True)
                        successful_url = candidate_url
                        break
                    else:
                        status = pub_res.status
                        failure_class = pub_res.failure_class
                        err_msg = pub_res.message
                        if failure_class == FailureClass.NON_ARTICLE.value:
                            break
                        limiter.record_outcome(host, False)
                except Exception as e:
                    failure_class = "download_error"
                    err_msg = str(e)
                    limiter.record_outcome(host, False)
                finally:
                    staging_path.unlink(missing_ok=True)

        if not successful_url:
            successful_url = urls_to_try[0]

        duration_ms = int((time.monotonic() - t0) * 1000)
        record = {
            "doi": work.doi,
            "title": work.title,
            "publisher": work.publisher,
            "is_oa": True,
            "access_route": "direct_http",
            "landing_url": work.openalex_landing_url or f"https://doi.org/{work.doi}",
            "pdf_url": successful_url,
            "status": status,
            "file": str(final_path) if status in ("downloaded", "duplicate_skipped") else None,
            "bytes": bytes_count,
            "failure_class": failure_class,
            "error": err_msg if status not in ("downloaded", "duplicate_skipped") else None,
            "duration_ms": duration_ms,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }

        append_manifest_record(manifest_file, manifest_lock, record)
        return record
    finally:
        claim.unlink(missing_ok=True)


def public_direct_first(works: List[Work], target_dir: Path, output_dir: Path):
    """Try public PDFs regardless of CSV OA flags; preserve originals for fallback."""
    cache_file = target_dir / '.doi_download_state' / 'public_direct_first.jsonl'
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache = {}
    if cache_file.exists():
        for line in cache_file.read_text().splitlines():
            try:
                row = json.loads(line)
                cache[row['doi'].lower()] = row
            except (ValueError, KeyError):
                continue
    lock = threading.Lock()
    limiter = HostLimiter(max_concurrent=2, min_interval=1.0)
    attempts = cache_file.with_name('oa_repository_direct_manifest.jsonl')

    def attempt(work):
        destination = output_dir / sanitize_filename_doi(work.doi)
        if destination.is_file() and validate_pdf(destination, work.doi, work.title).is_valid:
            return {'doi': work.doi, 'title': work.title, 'status': 'duplicate_skipped',
                    'file': str(destination), 'bytes': destination.stat().st_size}
        previous = cache.get(work.doi.lower(), {})
        if previous.get('status') == 'browser_fallback' and time.time() - previous.get('checked_at', 0) < 86400:
            return None
        resolved = resolve_oa_row({'doi': work.doi, 'title': work.title, 'publisher': work.publisher})
        result = download_single_oa(resolved, target_dir, limiter, attempts, lock, output_dir) if resolved else None
        success = result and result.get('status') in ('downloaded', 'duplicate_skipped')
        append_manifest_record(cache_file, lock, {
            'doi': work.doi, 'checked_at': time.time(),
            'status': 'downloaded' if success else 'browser_fallback',
            'error': result.get('error') if result else 'No public PDF URL resolved',
        })
        return result if success else None

    remaining, successful = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for work, result in zip(works, pool.map(attempt, works)):
            if result:
                successful.append(result)
            else:
                remaining.append(work)
    return remaining, successful


def run_oa_downloader(target_dir: Path, limit: int = 100, workers: int = 12,
                      input_dir: Optional[Path] = None, stop_requested=None) -> Dict[str, int]:
    input_dir = input_dir or (target_dir / "total_journals" if (target_dir / "total_journals").is_dir() else target_dir)
    manifest_file = target_dir / ".doi_download_state" / "oa_repository_direct_manifest.jsonl"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = target_dir / ".doi_download_tmp" / "oa_repository"
    staging_dir.mkdir(parents=True, exist_ok=True)

    limiter = HostLimiter(max_concurrent=2, min_interval=1.0)
    manifest_lock = threading.Lock()
    runtime = PlatformRuntimeState(
        platform_id="oa_repository",
        state="running",
        reason=f"input={input_dir.name}, limit={limit or 'all'}, workers={workers}",
    )
    save_runtime_state(target_dir, runtime)

    print("Indexing existing downloaded PDFs to skip...")
    skip_stems = load_completed_stems(target_dir, manifest_file)
    print(f"Loaded {len(skip_stems)} existing downloaded files/DOIs to skip.")

    target_label = str(limit) if limit else "all"
    print(f"Reading OA candidates from CSVs to collect {target_label} works with direct PDF URLs...")
    candidates: List[Work] = []
    rows = iter(iter_oa_csv_rows(input_dir, skip_stems=skip_stems))
    exhausted = False
    while not exhausted and (not limit or len(candidates) < limit):
        chunk: List[Dict[str, str]] = []
        for _ in range(300):
            try:
                chunk.append(next(rows))
            except StopIteration:
                exhausted = True
                break
        if not chunk:
            break
        print(f"Resolving {len(chunk)} OA candidates via verified public links or OpenAlex...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as resolver_pool:
            for res in resolver_pool.map(
                resolve_oa_row,
                chunk,
            ):
                if res:
                    candidates.append(res)
                    if limit and len(candidates) >= limit:
                        break

    print(f"Executing concurrent download for {len(candidates)} candidates with {workers} workers...")
    t_start = time.monotonic()
    success_count = 0
    failure_count = 0

    def download(work):
        if stop_requested and stop_requested():
            raise InterruptedError('OA month sequence paused')
        return download_single_oa(work, target_dir, limiter, manifest_file, manifest_lock)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(download, w)
            for w in candidates
        ]
        for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            res = fut.result()
            if res.get("status") in ("downloaded", "duplicate_skipped"):
                success_count += 1
            else:
                failure_count += 1
            if idx % 10 == 0 or idx == len(candidates):
                elapsed = time.monotonic() - t_start
                rate = round(idx / elapsed, 1) if elapsed > 0 else 0
                print(f"Progress: {idx}/{len(candidates)} | Success: {success_count} | Failed: {failure_count} | Speed: {rate} docs/s")

    runtime.state = "completed"
    runtime.reason = f"input={input_dir.name}, attempts={len(candidates)}, successes={success_count}"
    runtime.recent_attempts = len(candidates)
    runtime.recent_successes = success_count
    runtime.last_success_at = datetime.now(timezone.utc).isoformat() if success_count else None
    save_runtime_state(target_dir, runtime)
    print(f"OA Fast Lane calibration complete. Manifest logged at: {manifest_file}")
    return {'attempts': len(candidates), 'successes': success_count, 'failed': failure_count}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concurrent OA Fast Lane Downloader")
    parser.add_argument("target", nargs="?", default="/Volumes/PortableSSD/doi撞库202608028")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--single-month", action="store_true", help="Run only the supplied CSV batch, including calibration")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    if not 1 <= args.workers <= 32:
        parser.error("--workers must be between 1 and 32")

    target = Path(args.target).expanduser().resolve()
    try:
        if not args.single_month and (target.parent / 'oa_months.enabled').exists():
            sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'outputs/journal_2026_download'))
            from run_oa_months import run_months
            run_months(target, workers=args.workers)
        else:
            run_oa_downloader(target, limit=args.limit, workers=args.workers)
    except Exception as error:
        save_runtime_state(
            target,
            PlatformRuntimeState(
                platform_id="oa_repository", state="failed", reason=str(error)
            ),
        )
        raise
