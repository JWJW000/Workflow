#!/usr/bin/env python3
"""Download OA or campus-access DOI records through the project downloader.

The Python layer streams CSV rows, revalidates DOI metadata with OpenAlex,
batches verified OA inputs, and records resume state. Browser download is
performed by the Rust ``driver-drission`` implementation.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import concurrent.futures
import csv
import hashlib
import json
import os
import random
import re
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    from doi_oa_repository_downloader import public_direct_first
except ImportError:
    from examples.academic.doi_oa_repository_downloader import public_direct_first

try:
    from platforms.base import FailureClass, Work
    from platforms.routing import resolve_platform_for_work, SPRINGER_EMBO_PREFIXES
    from platforms.registry import get_platform_spec, load_registry
    from platforms.runtime_state import PlatformRuntimeState, save_runtime_state
except ImportError:
    from examples.academic.platforms.base import FailureClass, Work
    from examples.academic.platforms.routing import resolve_platform_for_work, SPRINGER_EMBO_PREFIXES
    from examples.academic.platforms.registry import get_platform_spec, load_registry
    from examples.academic.platforms.runtime_state import PlatformRuntimeState, save_runtime_state


OPENALEX_API = "https://api.openalex.org/works/doi:"
OPENALEX_WORKS_API = "https://api.openalex.org/works"
OPENALEX_MAILTO = "drission-workflow@local"
OPENALEX_BATCH_SIZE = 40
UNPAYWALL_API = "https://api.unpaywall.org/v2/"
UNPAYWALL_EMAIL = "drission-workflow@example.com"
HTTP_CHUNK_SIZE = 1024 * 1024
OA_CACHE_VERSION = 3
BUAA_IEEE_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421f9f244993f20645f6c0dc7a59d50267b1ab4a9"
)
BUAA_SPRINGER_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421fcfe4f976923784277068ea98a1b203a54"
)
BUAA_SCIENCEDIRECT_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421e7e056d234336155700b8ca891472636a6d29e640e"
)
BUAA_ACS_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421e0e2438f69316b4330079bab"
)
BUAA_APS_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421fcfe4f976931784330079bab"
)
BUAA_APS_JOURNALS_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421faf8548e29316443300999bfd65a3132"
)
BUAA_AAA_WEBVPN_PREFIX = (
    "https://d.buaa.edu.cn/https/"
    "77726476706e69737468656265737421e0e243902e336944770787bfd6542234e0fc78bd2c7e"
)

CAMPUS_TRIAL_SHARES = (
    ("elsevier_cell", 0.40),
    ("nature", 0.20),
    ("wiley", 0.15),
    ("springer", 0.15),
    ("easy_other", 0.10),
)
CAMPUS_PLATFORM_CHOICES = tuple(sorted(set(load_registry().keys()) | {
    "elsevier_cell",
    "nature",
    "wiley",
    "springer",
    "ieee",
    "acs",
    "aps",
    "siam",
    "annual_reviews",
    "aaa",
    "cambridge",
    "rsc",
    "easy_other",
}))


try:
    from platforms.content_filter import skip_row
except ImportError:
    from examples.academic.platforms.content_filter import skip_row


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def visible_csv_files(directory: Path, pattern: str) -> list[Path]:
    return sorted(
        path for path in directory.glob(pattern) if not path.name.startswith("._")
    )


def csv_files(input_dir: Path) -> list[Path]:
    """Find the current refreshed DOI dataset, with legacy compatibility.

    Normal commands continue to receive the DOI project root. When its
    ``total_journals`` directory contains refreshed parts, those files are the
    authoritative input. Passing ``total_journals`` itself is also supported.
    The old merged files are consulted only when no refreshed dataset exists.
    """
    refreshed_dir = input_dir / "total_journals"
    if refreshed_dir.is_dir():
        refreshed = visible_csv_files(
            refreshed_dir, "reversed_dois_refreshed_part_*.csv"
        )
        if refreshed:
            return refreshed

    refreshed = visible_csv_files(
        input_dir, "reversed_dois_refreshed_part_*.csv"
    )
    if refreshed:
        return refreshed

    return visible_csv_files(input_dir, "reversed_dois_merged_part_*.csv")


def iter_oa_rows(input_dir: Path, oa_mode: str = "oa") -> Iterable[Work]:
    for path in csv_files(input_dir):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if skip_row(input_dir, row):
                    continue
                is_oa_str = (row.get("is_oa") or "").strip().lower()
                is_oa = is_oa_str in ("true", "1", "t")
                if oa_mode == "oa" and not is_oa:
                    continue
                if oa_mode == "non_oa" and is_oa:
                    continue
                doi = (row.get("doi") or "").strip()
                if not doi:
                    continue
                openalex_pdf_url = (row.get("openalex_pdf_url") or row.get("pdf_url") or "").strip()
                openalex_landing_url = (row.get("openalex_landing_url") or row.get("url") or f"https://doi.org/{doi}").strip()
                yield Work(
                    doi=doi,
                    title=(row.get("title") or "Untitled").strip(),
                    publisher=(row.get("publisher") or "").strip(),
                    journal=(row.get("journal") or "").strip(),
                    issn=(row.get("issn") or "").strip(),
                    is_oa=is_oa,
                    openalex_pdf_url=openalex_pdf_url,
                    openalex_landing_url=openalex_landing_url,
                    url=f"https://doi.org/{doi}",
                )


def prioritize_retry_rows(works: Iterable[Work], retry_dois: set[str]) -> Iterable[Work]:
    # Stable ordering preserves the existing month order within each group.
    return sorted(works, key=lambda work: work.doi.lower() not in retry_dois) if retry_dois else works


def campus_platform(work: Work) -> str | None:
    """Resolve platform using the unified platforms.routing module."""
    plat = resolve_platform_for_work(work)
    if plat == "elsevier":
        return "elsevier_cell"
    return plat


def campus_trial_rows(
    input_dir: Path,
    total: int,
    exclude: set[str] | None = None,
    excluded_platforms: set[str] | None = None,
    oa_mode: str = "oa",
) -> list[Work]:
    excluded_platforms = excluded_platforms or set()
    active_shares = [
        item for item in CAMPUS_TRIAL_SHARES if item[0] not in excluded_platforms
    ]
    if not active_shares:
        raise ValueError("at least one campus platform must remain enabled")
    share_total = sum(share for _, share in active_shares)
    quotas: dict[str, int] = {}
    assigned = 0
    for index, (platform, share) in enumerate(active_shares):
        amount = (
            total - assigned
            if index == len(active_shares) - 1
            else round(total * share / share_total)
        )
        quotas[platform] = amount
        assigned += amount
    selected: dict[str, list[Work]] = {platform: [] for platform in quotas}
    for work in iter_oa_rows(input_dir, oa_mode):
        if exclude and work.doi.lower() in exclude:
            continue
        platform = campus_platform(work)
        if platform not in selected or len(selected[platform]) >= quotas[platform]:
            continue
        selected[platform].append(work)
        if all(len(selected[key]) >= quotas[key] for key in quotas):
            break
    for platform, _ in active_shares:
        values = selected[platform]
        print(f"Campus trial selection: {platform}={len(values)}/{quotas[platform]}")
    # Interleave platforms so every early browser batch is representative.
    cycle = {
        platform: max(1, round(20 * share / share_total))
        for platform, share in active_shares
    }
    offsets = {platform: 0 for platform in selected}
    rows: list[Work] = []
    while len(rows) < sum(len(values) for values in selected.values()):
        before = len(rows)
        for platform, _ in active_shares:
            start = offsets[platform]
            end = min(start + cycle[platform], len(selected[platform]))
            rows.extend(selected[platform][start:end])
            offsets[platform] = end
        if len(rows) == before:
            break
    return rows


def doi_from_safe_filename(name: str) -> str:
    """Inverse of doi_filename() for ordinary DOI names (one slash)."""
    stem = name[:-4] if name.lower().endswith(".pdf") else name
    if "_" not in stem:
        return stem
    prefix, rest = stem.split("_", 1)
    return f"{prefix}/{rest}"


def doi_filename(doi: str) -> str:
    normalized = urllib.parse.unquote(doi.strip())
    normalized = re.sub(
        r"(?i)^https?://(?:dx\.)?doi\.org/", "", normalized
    )
    normalized = re.sub(r"(?i)^doi:\s*", "", normalized).lower()
    stem = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", normalized)
    stem = re.sub(r"\s+", "_", stem).strip(" ._") or "unknown-doi"

    # The workflow engine caps a path component at 120 characters. Preserve
    # ordinary DOI names verbatim (apart from unsafe characters); exceptionally
    # long DOI names retain a short digest suffix so truncation cannot collide.
    if len(stem) > 115:
        digest = hashlib.sha256(normalized.encode()).hexdigest()[:8]
        stem = f"{stem[:106]}_{digest}"
    while len(f"{stem}.pdf".encode("utf-8")) > 240:
        stem = stem[:-1]
    return f"{stem}.pdf"


def safe_filename(work: Work) -> str:
    return doi_filename(work.doi)


def is_known_placeholder_pdf(path: Path, size: int) -> bool:
    """Reject publisher-generated PDFs that only say no PDF is available or policy placeholders."""
    # AAA's page footer links to a generic authorship policy. Older downloader
    # builds could mistake it for the article PDF and rename it to every DOI.
    if size == 85126:
        return True

    if not 32 * 1024 <= size <= 512 * 1024:
        return False
    converter = shutil.which("pdftotext")
    if converter:
        try:
            result = subprocess.run(
                [converter, str(path), "-"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
            text = result.stdout.decode("utf-8", errors="ignore").lower()
            if "article in pdf format" in text and "sorry" in text:
                return True
            if (
                "publications ethics task force" in text
                or "authorship & publications ethics policy" in text
                or "aaa publications ethics policy" in text
                or ("part a: authorship" in text and "pub-004" in text)
            ):
                return True
        except (OSError, subprocess.SubprocessError):
            pass
    # Portable fallback for Nature's known one-page placeholder template and AAA policy template.
    try:
        data = path.read_bytes()
    except OSError:
        return False
    if (
        b"Acrobat Distiller 11.0.9" in data
        and b"/MediaBox[0 0 334.488 255.118]" in data
    ):
        return True
    if b"Publications Ethics Task Force" in data or b"PUB-004" in data:
        return True
    return False


# Common non-article placeholder title keywords in academic indexes (e.g. IEEE, AAA, Nature)
PLACEHOLDER_TITLE_PATTERNS = (
    "[blank page]",
    "blank page",
    "[front cover]",
    "front cover",
    "[back cover]",
    "back cover",
    "table of contents",
    "author index",
    "subject index",
    "cumulative index",
    "editorial board",
)


def is_placeholder_work(work: Work) -> bool:
    t = (work.title or "").lower().strip()
    return any(p in t for p in PLACEHOLDER_TITLE_PATTERNS)


def looks_like_pdf(path: Path) -> bool:
    """Fast existence check used when skipping already-downloaded DOIs."""
    try:
        if not path.is_file() or path.stat().st_size < 800:
            return False
        with path.open("rb") as handle:
            return handle.read(4) == b"%PDF"
    except OSError:
        return False


def is_valid_pdf(path: Path) -> bool:
    try:
        if not path.is_file():
            return False
        # Import strict validation from platforms package
        try:
            from platforms.validation import validate_pdf as platform_validate_pdf
            res = platform_validate_pdf(path)
            if not res.is_valid:
                return False
        except ImportError:
            size = path.stat().st_size
            if size < 800:
                return False
            with path.open("rb") as handle:
                if handle.read(4) != b"%PDF":
                    return False
        return not is_known_placeholder_pdf(path, path.stat().st_size)
    except OSError:
        return False


def load_completed(
    manifest: Path,
    output_dir: Path,
    include_validation_terminal: bool = True,
) -> set[str]:
    completed: set[str] = set()
    if not manifest.is_file():
        return completed
    with manifest.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            status = item.get("status")
            doi = str(item.get("doi") or "")
            if status == "downloaded" and doi:
                desired = output_dir / doi_filename(doi)
                if not looks_like_pdf(desired):
                    recorded = Path(str(item.get("file") or ""))
                    try:
                        within_output = (
                            recorded.resolve().parent == output_dir.resolve()
                        )
                    except OSError:
                        within_output = False
                    if within_output and looks_like_pdf(recorded):
                        recorded.replace(desired)
                terminal = looks_like_pdf(desired)
            else:
                terminal = (
                    item.get("status") == "no_pdf_available"
                    or (
                        include_validation_terminal
                        and item.get("status") in {"not_oa", "doi_not_found"}
                        and item.get("validation_version") == OA_CACHE_VERSION
                    )
                )
            if terminal and doi:
                completed.add(doi.lower())
                if status == "downloaded" and item.get("source_doi"):
                    completed.add(str(item["source_doi"]).strip().lower())
    return completed


def load_manifest_dois_by_status(manifest: Path, statuses: set[str]) -> set[str]:
    """Return DOI keys already recorded with one of the requested statuses."""
    matched: set[str] = set()
    if not manifest.is_file():
        return matched
    with manifest.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            doi = str(item.get("doi") or "").strip().lower()
            if doi and item.get("status") in statuses:
                matched.add(doi)
    return matched


def load_deferred_dois(manifest: Path, now: datetime | None = None, statuses: set[str] | None = None) -> set[str]:
    """Give failed records a day to recover instead of starving fresh DOI rows."""
    now = now or datetime.now(timezone.utc)
    latest: dict[str, dict] = {}
    if not manifest.is_file():
        return set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        doi = str(item.get("doi") or "").strip().lower()
        if doi:
            latest[doi] = item
    deferred = set()
    for doi, item in latest.items():
        if item.get("status") not in (statuses if statuses is not None else {
            "browser_failed", "direct_failed", "resolve_failed", "quarantined", "publication_pending"
        }):
            continue
        try:
            recorded = datetime.fromisoformat(item["recorded_at"])
            if recorded.tzinfo is None:
                recorded = recorded.replace(tzinfo=timezone.utc)
        except (KeyError, ValueError, TypeError):
            continue
        if (now - recorded).total_seconds() < 86400:
            deferred.add(doi)
    return deferred


def browser_batches(works: list[Work], batch_size: int) -> Iterable[list[Work]]:
    """Probe a small batch before committing the remaining browser work."""
    probe_size = min(3, batch_size)
    if works:
        yield works[:probe_size]
    for offset in range(probe_size, len(works), batch_size):
        yield works[offset : offset + batch_size]


def load_oa_cache(path: Path) -> dict[str, dict]:
    cached: dict[str, dict] = {}
    if not path.is_file():
        return cached
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            doi = str(item.get("doi") or "").lower()
            if (
                doi
                and item.get("cache_version") == OA_CACHE_VERSION
                and item.get("status") in {"oa", "not_oa", "doi_not_found"}
            ):
                cached[doi] = item
    return cached


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def json_request(url: str, user_agent: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": user_agent},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(3):
        try:
            with opener.open(request, timeout=20) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                raise
            if attempt == 2:
                raise
            delay = min(10.0, 1.0 + attempt * 2.0 + random.random())
            time.sleep(delay)
        except (OSError, ValueError):
            if attempt == 2:
                raise
            time.sleep(1.0 + attempt + random.random())
    raise RuntimeError("metadata request exhausted retries")


def add_url(values: list[str], candidate: object) -> None:
    if not candidate:
        return
    url = str(candidate).strip()
    if url.startswith(("http://", "https://")) and url not in values:
        values.append(url)


def merge_oa_url_lists(*groups: Iterable[object]) -> list[str]:
    urls: list[str] = []
    for group in groups:
        if not group:
            continue
        if isinstance(group, (str, bytes)):
            add_url(urls, group)
            continue
        for item in group:
            add_url(urls, item)
    return urls


def normalize_openalex_doi(value: object) -> str:
    doi = str(value or "").strip()
    doi = re.sub(r"(?i)^https?://(?:dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"(?i)^doi:\s*", "", doi)
    return doi.lower()


def metadata_from_openalex_work(value: dict, work: Work | None = None) -> dict:
    fallback_doi = work.doi if work is not None else ""
    fallback_title = work.title if work is not None else "Untitled"
    fallback_publisher = work.publisher if work is not None else ""
    doi = normalize_openalex_doi(value.get("doi")) or fallback_doi
    open_access = value.get("open_access") or {}
    best = value.get("best_oa_location") or {}
    primary = value.get("primary_location") or {}
    locations = value.get("locations") or []
    actual_title = str(value.get("title") or fallback_title).strip() or "Untitled"
    source = best.get("source") or primary.get("source") or {}
    publisher = str(source.get("display_name") or fallback_publisher).strip()
    pdf_urls: list[str] = []
    landing_urls: list[str] = []
    add_url(pdf_urls, best.get("pdf_url"))
    add_url(pdf_urls, primary.get("pdf_url"))
    for location in locations:
        if not isinstance(location, dict) or location.get("is_oa") is False:
            continue
        add_url(pdf_urls, location.get("pdf_url"))
    add_url(landing_urls, open_access.get("oa_url"))
    add_url(landing_urls, best.get("landing_page_url"))
    add_url(landing_urls, primary.get("landing_page_url"))
    for location in locations:
        if isinstance(location, dict) and location.get("is_oa") is not False:
            add_url(landing_urls, location.get("landing_page_url"))
    is_oa = bool(open_access.get("is_oa"))
    if not is_oa:
        return {
            "doi": doi or fallback_doi,
            "status": "not_oa",
            "title": actual_title,
            "publisher": publisher,
            "oa_status": open_access.get("oa_status"),
            "landing_url": (
                str(primary.get("landing_page_url") or "").strip()
                or f"https://doi.org/{doi or fallback_doi}"
            ),
        }
    oa_urls = pdf_urls + [url for url in landing_urls if url not in pdf_urls]
    if not oa_urls:
        oa_urls.append(f"https://doi.org/{doi or fallback_doi}")
    return {
        "doi": doi or fallback_doi,
        "status": "oa",
        "title": actual_title,
        "publisher": publisher,
        "oa_status": open_access.get("oa_status"),
        "oa_urls": oa_urls,
        "landing_url": landing_urls[0] if landing_urls else f"https://doi.org/{doi or fallback_doi}",
    }


def merge_unpaywall(metadata: dict, work: Work) -> dict:
    encoded_doi = urllib.parse.quote(work.doi, safe="/():;._-")
    unpaywall_url = (
        f"{UNPAYWALL_API}{encoded_doi}?email="
        f"{urllib.parse.quote(UNPAYWALL_EMAIL)}"
    )
    try:
        unpaywall = json_request(
            unpaywall_url,
            f"drission-workflow/0.1 (mailto:{UNPAYWALL_EMAIL})",
        )
    except Exception:
        return metadata
    pdf_urls = list(metadata.get("oa_urls") or [])
    landing_urls: list[str] = []
    add_url(landing_urls, metadata.get("landing_url"))
    locations = []
    if isinstance(unpaywall.get("best_oa_location"), dict):
        locations.append(unpaywall["best_oa_location"])
    locations.extend(unpaywall.get("oa_locations") or [])
    for location in locations:
        if not isinstance(location, dict):
            continue
        add_url(pdf_urls, location.get("url_for_pdf"))
        add_url(landing_urls, location.get("url"))
    if unpaywall.get("is_oa") and metadata.get("status") != "oa":
        metadata = dict(metadata)
        metadata["status"] = "oa"
    if pdf_urls or landing_urls:
        metadata = dict(metadata)
        oa_urls = merge_oa_url_lists(pdf_urls, landing_urls)
        if oa_urls:
            metadata["oa_urls"] = oa_urls
        if landing_urls:
            metadata["landing_url"] = landing_urls[0]
    return metadata


def openalex_filter_url(dois: list[str]) -> str:
    filter_value = "|".join(dois)
    return (
        f"{OPENALEX_WORKS_API}?filter=doi:{urllib.parse.quote(filter_value, safe='|/:._-')}"
        f"&per-page={max(len(dois), 1)}"
        f"&select=doi,title,open_access,best_oa_location,primary_location,locations"
        f"&mailto={urllib.parse.quote(OPENALEX_MAILTO)}"
    )


def openalex_lookup(work: Work, skip_unpaywall: bool = False) -> dict:
    encoded_doi = urllib.parse.quote(work.doi, safe="/():;._-")
    fields = "doi,title,open_access,best_oa_location,primary_location,locations"
    url = (
        f"{OPENALEX_API}{encoded_doi}?select={fields}"
        f"&mailto={urllib.parse.quote(OPENALEX_MAILTO)}"
    )
    try:
        value = json_request(
            url, f"drission-workflow/0.1 (mailto:{OPENALEX_MAILTO})"
        )
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return {"doi": work.doi, "status": "doi_not_found"}
        raise
    metadata = metadata_from_openalex_work(value, work)
    if skip_unpaywall:
        return metadata
    return merge_unpaywall(metadata, work)


def openalex_lookup_batch(works: list[Work], skip_unpaywall: bool = True) -> dict[str, dict]:
    """Resolve many DOIs with one OpenAlex request per chunk instead of one per DOI."""
    found: dict[str, dict] = {}
    by_doi = {work.doi.lower(): work for work in works}
    dois = list(by_doi)
    for offset in range(0, len(dois), OPENALEX_BATCH_SIZE):
        chunk = dois[offset : offset + OPENALEX_BATCH_SIZE]
        try:
            payload = json_request(
                openalex_filter_url(chunk),
                f"drission-workflow/0.1 (mailto:{OPENALEX_MAILTO})",
            )
            results = payload.get("results") if isinstance(payload, dict) else None
            if not isinstance(results, list):
                raise ValueError("OpenAlex batch response missing results")
            for value in results:
                if not isinstance(value, dict):
                    continue
                work = by_doi.get(normalize_openalex_doi(value.get("doi")))
                if work is None:
                    continue
                found[work.doi.lower()] = metadata_from_openalex_work(value, work)
        except Exception:
            for doi in chunk:
                work = by_doi[doi]
                try:
                    found[doi] = openalex_lookup(work, skip_unpaywall=True)
                except Exception as error:  # noqa: BLE001 - returned for resumable scans
                    found[doi] = {
                        "doi": work.doi,
                        "status": "metadata_failed",
                        "error": f"{type(error).__name__}: {error}",
                    }
        for doi in chunk:
            if doi not in found:
                work = by_doi[doi]
                found[doi] = {"doi": work.doi, "status": "doi_not_found"}
    if not skip_unpaywall:
        for doi, work in by_doi.items():
            metadata = found.get(doi)
            if metadata is None or metadata.get("status") == "metadata_failed":
                continue
            if has_direct_pdf_url(metadata):
                continue
            found[doi] = merge_unpaywall(metadata, work)
    return found


def has_direct_pdf_url(metadata: dict) -> bool:
    for url in metadata.get("oa_urls") or []:
        lowered = str(url).lower()
        if (
            lowered.endswith(".pdf")
            or "/pdf" in lowered
            or "pdf?" in lowered
            or "format=pdf" in lowered
            or "type=printable" in lowered
        ):
            return True
    return False


def synthetic_oa_metadata(work: Work) -> dict:
    constructed = campus_pdf_candidates(
        work.doi, work.openalex_landing_url or work.landing_url or work.url
    )
    landing = (
        work.openalex_landing_url
        or work.landing_url
        or work.url
        or f"https://doi.org/{work.doi}"
    )
    urls = merge_oa_url_lists(
        [work.openalex_pdf_url] if work.openalex_pdf_url else [],
        work.oa_urls,
        constructed,
        [landing],
    )
    return {
        "doi": work.doi,
        "status": "oa",
        "title": work.title,
        "publisher": work.publisher,
        "oa_urls": urls,
        "landing_url": landing,
        "source": "csv_oa_flag",
    }


def metadata_from_cache_or_csv(work: Work, cache: dict[str, dict]) -> dict:
    synthetic = synthetic_oa_metadata(work)
    cached = cache.get(work.doi.lower())
    if not cached:
        return synthetic
    if cached.get("status") == "oa":
        merged = dict(cached)
        merged["status"] = "oa"
        merged["oa_urls"] = merge_oa_url_lists(cached.get("oa_urls"), synthetic.get("oa_urls"))
        merged["title"] = cached.get("title") or synthetic["title"]
        merged["publisher"] = cached.get("publisher") or synthetic["publisher"]
        merged["landing_url"] = cached.get("landing_url") or synthetic["landing_url"]
        return merged
    return synthetic


def load_all_oa_caches(state_dir: Path, primary: Path) -> dict[str, dict]:
    cache = load_oa_cache(primary)
    if not state_dir.is_dir():
        return cache
    for path in sorted(state_dir.glob("*openalex_oa_cache.jsonl")):
        if path.name.startswith("._") or path.resolve() == primary.resolve():
            continue
        extra = load_oa_cache(path)
        for key, value in extra.items():
            existing = cache.get(key)
            if existing is None:
                cache[key] = value
                continue
            if existing.get("status") != "oa" and value.get("status") == "oa":
                cache[key] = value
                continue
            if existing.get("status") == "oa" and value.get("status") == "oa":
                merged = dict(existing)
                merged["oa_urls"] = merge_oa_url_lists(
                    existing.get("oa_urls"), value.get("oa_urls")
                )
                cache[key] = merged
    return cache


def resolve_metadata_batch(
    works: list[Work],
    cache: dict[str, dict],
    cache_path: Path,
    workers: int,
    skip_unpaywall: bool = False,
) -> list[tuple[Work, dict]]:
    results: list[tuple[Work, dict] | None] = [None] * len(works)
    missing: list[tuple[int, Work]] = []
    for index, work in enumerate(works):
        metadata = cache.get(work.doi.lower())
        if metadata is None:
            missing.append((index, work))
        else:
            results[index] = (work, metadata)

    if missing:
        fetched = openalex_lookup_batch(
            [work for _, work in missing], skip_unpaywall=skip_unpaywall
        )
        now = datetime.now(timezone.utc).isoformat()
        for index, work in missing:
            metadata = fetched.get(work.doi.lower()) or {
                "doi": work.doi,
                "status": "metadata_failed",
                "error": "OpenAlex batch returned no record",
            }
            if metadata.get("status") in {"oa", "not_oa", "doi_not_found"}:
                metadata["cache_version"] = OA_CACHE_VERSION
                metadata["checked_at"] = now
                append_jsonl(cache_path, metadata)
                cache[work.doi.lower()] = metadata
            results[index] = (work, metadata)
    return [item for item in results if item is not None]


def campus_pdf_candidates(doi: str, landing_url: str = "") -> tuple[str, ...]:
    normalized = doi.strip().lower()
    encoded = urllib.parse.quote(normalized, safe="/():;._-")
    suffix = normalized.split("/", 1)[1] if "/" in normalized else normalized
    candidates: list[str] = []
    if normalized.startswith("10.1038/") and not normalized.startswith(SPRINGER_EMBO_PREFIXES):
        add_url(candidates, f"https://www.nature.com/articles/{suffix}.pdf")
    if normalized.startswith(("10.1002/", "10.1111/")):
        add_url(
            candidates,
            f"https://onlinelibrary.wiley.com/doi/pdfdirect/{encoded}",
        )
    if normalized.startswith(("10.1007/", "10.1186/", "10.1057/") + SPRINGER_EMBO_PREFIXES):
        add_url(
            candidates,
            f"https://link.springer.com/content/pdf/{encoded}.pdf",
        )
    if normalized.startswith("10.1109/"):
        for candidate in ieee_pdf_candidates(landing_url):
            add_url(candidates, candidate)
    if normalized.startswith("10.1021/"):
        add_url(candidates, f"https://pubs.acs.org/doi/pdf/{encoded}")
        add_url(candidates, f"https://pubs.acs.org/doi/epdf/{encoded}")
    if normalized.startswith("10.1103/"):
        add_url(candidates, f"https://link.aps.org/doi/pdf/{encoded}")
    if normalized.startswith("10.1137/"):
        add_url(candidates, f"https://epubs.siam.org/doi/pdf/{encoded}")
    if normalized.startswith("10.1146/"):
        add_url(candidates, f"https://www.annualreviews.org/doi/pdf/{encoded}")
    return tuple(candidates)


def ieee_pdf_candidates(landing_url: str) -> tuple[str, ...]:
    host = urllib.parse.urlparse(landing_url).hostname or ""
    if host not in {"ieeexplore.ieee.org", "d.buaa.edu.cn"}:
        return ()
    match = re.search(r"(?:/document/|[?&]arnumber=)(\d+)", landing_url)
    if not match:
        return ()
    article_number = match.group(1)
    return (
        "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?"
        f"tp=&arnumber={article_number}",
        "https://ieeexplore.ieee.org/stamp/stamp.jsp?"
        f"tp=&arnumber={article_number}",
    )


def buaa_ieee_webvpn_url(url: str) -> str:
    """Route an IEEE Xplore URL through BUAA's authenticated WebVPN."""
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() != "ieeexplore.ieee.org":
        return url
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{BUAA_IEEE_WEBVPN_PREFIX}{path}{query}"


def buaa_springer_webvpn_url(url: str) -> str:
    """Route a SpringerLink URL through BUAA's authenticated WebVPN."""
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() != "link.springer.com":
        return url
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{BUAA_SPRINGER_WEBVPN_PREFIX}{path}{query}"


def resolve_nature_vpn_work(work: Work, route: str = 'fudan') -> Work:
    """Build Nature URLs for the selected campus route."""
    normalized = work.doi.strip().lower()
    suffix = normalized.split("/", 1)[1]
    article_url = f"https://www.nature.com/articles/{suffix}"
    pdf_url = f"{article_url}.pdf"
    if route == 'buaa':
        try:
            from platforms.webvpn import webvpn_https
        except ImportError:
            from examples.academic.platforms.webvpn import webvpn_https
        article_url = webvpn_https('www.nature.com', f'/articles/{suffix}')
        pdf_url = f'{article_url}.pdf'
    return Work(
        doi=work.doi,
        title=work.title,
        publisher=work.publisher,
        url=pdf_url,
        oa_urls=(pdf_url,),
        landing_url=article_url,
    )


def buaa_sciencedirect_webvpn_url(url: str) -> str:
    """Route a ScienceDirect URL through BUAA's authenticated WebVPN."""
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() != "www.sciencedirect.com":
        return url
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{BUAA_SCIENCEDIRECT_WEBVPN_PREFIX}{path}{query}"


def buaa_fixed_host_url(url: str, hostname: str, prefix: str) -> str:
    """Route one known publisher hostname through BUAA WebVPN."""
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() != hostname:
        return url
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{prefix}{path}{query}"


ACS_NON_ARTICLE_TITLES = {
    "ad",
    "ads",
    "toc",
    "advertisement",
    "back cover",
    "blank page",
    "contents",
    "cover",
    "front cover",
    "issue information",
    "masthead",
    "table of contents",
}


def acs_non_article_reason(work: Work) -> str | None:
    """Return a terminal reason for ACS records without article PDFs."""
    doi = work.doi.strip().lower()
    # ACS assigns DOI-like identifiers ending in .s001, .s002, ... to
    # Supporting Information files.  They do not have a normal /doi/abs page;
    # routing them as articles produces ACS's generic 404 page.  A future SI
    # downloader can retrieve these from the parent article attachment list.
    if re.search(r"\.s\d+$", doi):
        return "ACS supplementary-material record"
    normalized = re.sub(r"\s+", " ", (work.title or "").strip().lower())
    normalized = normalized.rstrip(" .:;-")
    if normalized in ACS_NON_ARTICLE_TITLES:
        return f"ACS non-article record: {normalized}"
    return None


def resolve_acs_webvpn_work(work: Work) -> Work:
    doi = work.doi.strip().lower()
    # ACS retired the legacy `/doi/abs/{doi}` route during its platform
    # migration.  It now renders the generic "The page you're looking for
    # cannot be found" page even for valid, institutionally entitled papers.
    # `/doi/{doi}` is the stable article landing route and exposes the current
    # PDF/EPDF controls for the real-browser workflow.
    landing = buaa_fixed_host_url(
        f"https://pubs.acs.org/doi/{doi}",
        "pubs.acs.org",
        BUAA_ACS_WEBVPN_PREFIX,
    )
    return replace(work, url=landing, oa_urls=(landing,), landing_url=landing)


def cambridge_non_article_reason(work: Work) -> str | None:
    doi = work.doi.strip().lower()
    if not doi.startswith("10.1017/"):
        return None
    if "cbo978" in doi or doi.startswith("10.1017/978"):
        return "cambridge_book"
    title = (work.title or "").strip().lower()
    if title in {"cover", "front matter", "back matter", "index"}:
        return "cambridge_issue_furniture"
    return None


def resolve_cambridge_webvpn_work(work: Work) -> Work:
    try:
        from platforms.cambridge import CambridgeAdapter
    except ImportError:
        from examples.academic.platforms.cambridge import CambridgeAdapter
    resolved = CambridgeAdapter().resolve(work)
    landing = resolved.landing_url
    return replace(work, url=landing, oa_urls=(landing,), landing_url=landing)


def resolve_rsc_webvpn_work(work: Work) -> Work:
    try:
        from platforms.rsc import RSCAdapter
    except ImportError:
        from examples.academic.platforms.rsc import RSCAdapter
    resolved = RSCAdapter().resolve(work)
    pdf = resolved.pdf_url or resolved.landing_url
    landing = resolved.landing_url
    return replace(
        work,
        url=landing,
        oa_urls=tuple(item for item in (landing, pdf) if item),
        landing_url=landing,
    )


APS_JOURNAL_ROUTES = {
    "physreva": ("pra", "PhysRevA"),
    "physrevb": ("prb", "PhysRevB"),
    "physrevc": ("prc", "PhysRevC"),
    "physrevd": ("prd", "PhysRevD"),
    "physreve": ("pre", "PhysRevE"),
    "physrevlett": ("prl", "PhysRevLett"),
    "revmodphys": ("rmp", "RevModPhys"),
    "physrevx": ("prx", "PhysRevX"),
    "prxquantum": ("prxquantum", "PRXQuantum"),
    "physrevapplied": ("prapplied", "PhysRevApplied"),
    "physrevfluids": ("prfluids", "PhysRevFluids"),
    "physrevmaterials": ("prmaterials", "PhysRevMaterials"),
    "physrevresearch": ("prresearch", "PhysRevResearch"),
    "physrevphyseducres": ("prper", "PhysRevPhysEducRes"),
    "physrevaccelbeams": ("prab", "PhysRevAccelBeams"),
}


def canonical_aps_route(doi: str) -> tuple[str, str] | None:
    """Return the APS journal path and canonical-case DOI for legacy DOI forms."""
    clean = doi.strip()
    if "/" not in clean:
        return None
    registrant, suffix = clean.split("/", 1)
    journal_code, separator, remainder = suffix.partition(".")
    route = APS_JOURNAL_ROUTES.get(journal_code.lower())
    if route is None or not separator or not remainder:
        return None
    journal_path, canonical_journal = route
    return journal_path, f"{registrant.lower()}/{canonical_journal}.{remainder}"


def aps_route_from_landing_url(url: str) -> tuple[str, str] | None:
    """Extract journal path and canonical DOI from an APS redirect target."""
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() != "journals.aps.org":
        return None
    match = re.match(r"^/([^/]+)/(?:abstract|pdf)/(.+)$", parsed.path)
    if not match:
        return None
    journal_path = match.group(1)
    doi = urllib.parse.unquote(match.group(2))
    if not doi.lower().startswith("10.1103/"):
        return None
    return journal_path, doi


def build_aps_webvpn_work(work: Work, journal_path: str, doi: str) -> Work:
    encoded_doi = urllib.parse.quote(doi, safe="/():;._-")
    landing = buaa_fixed_host_url(
        f"https://journals.aps.org/{journal_path}/abstract/{encoded_doi}",
        "journals.aps.org",
        BUAA_APS_JOURNALS_WEBVPN_PREFIX,
    )
    pdf = buaa_fixed_host_url(
        f"https://journals.aps.org/{journal_path}/pdf/{encoded_doi}",
        "journals.aps.org",
        BUAA_APS_JOURNALS_WEBVPN_PREFIX,
    )
    # Opaque DOIs may still be accepted manuscripts with no published PDF.
    # Let the publisher resolve the current publication stage in the browser.
    if canonical_aps_route(doi) is None:
        landing = f"{BUAA_APS_WEBVPN_PREFIX}/doi/{encoded_doi}"
    return replace(work, url=pdf, oa_urls=(pdf,), landing_url=landing)


def resolve_aps_webvpn_work(work: Work) -> tuple[Work | None, str | None]:
    """Resolve both legacy and new opaque APS DOIs to a real journal route."""
    deterministic = canonical_aps_route(work.doi)
    if deterministic is not None:
        return build_aps_webvpn_work(work, *deterministic), None

    # Opaque APS DOIs no longer encode the journal. Use supplied journal
    # metadata before the unauthenticated resolver, which can return 403.
    journal_key = re.sub(r"[^a-z0-9]", "", work.journal.casefold())
    journal_key = journal_key.replace("physicalreview", "physrev")
    journal_key = {
        "physrevletters": "physrevlett",
        "reviewsofmodernphysics": "revmodphys",
        "physrevphysicseducationresearch": "physrevphyseducres",
        "physrevacceleratorsandbeams": "physrevaccelbeams",
    }.get(journal_key, journal_key)
    metadata_route = APS_JOURNAL_ROUTES.get(journal_key)
    if work.doi.strip().lower().startswith("10.1103/") and metadata_route:
        return build_aps_webvpn_work(work, metadata_route[0], work.doi.strip()), None

    resolver = f"https://doi.org/{urllib.parse.quote(work.doi.strip(), safe='/():;._-')}"
    opener = urllib.request.build_opener()
    error_text = "DOI did not redirect to a journals.aps.org article"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            resolver,
            method=method,
            headers={"User-Agent": "Mozilla/5.0 Chrome/152 Safari/537.36"},
        )
        try:
            with opener.open(request, timeout=20) as response:
                landing_url = response.geturl()
        except urllib.error.HTTPError as error:
            landing_url = error.geturl()
            error_text = f"HTTP {error.code}: {landing_url}"
        except OSError as error:
            error_text = f"{type(error).__name__}: {error}"
            continue
        resolved = aps_route_from_landing_url(landing_url)
        if resolved is not None:
            return build_aps_webvpn_work(work, *resolved), None
    return None, error_text


def resolve_aps_webvpn_batch(
    works: list[Work], workers: int, vpn_headers: dict[str, str] | None = None
) -> list[tuple[Work, Work | None, str | None]]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        resolved = executor.map(resolve_aps_webvpn_work, works)
        return [
            (original, routed, error)
            for original, (routed, error) in zip(works, resolved)
        ]


def resolve_aaa_webvpn_work(work: Work) -> tuple[Work | None, str | None]:
    """Resolve a 10.2308 DOI to its AAA article page and route it via WebVPN."""
    opener = urllib.request.build_opener()
    resolver = f"https://doi.org/{urllib.parse.quote(work.doi, safe='/():;._-')}"
    error_text = "DOI did not redirect to publications.aaahq.org"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            resolver,
            method=method,
            headers={"User-Agent": "Mozilla/5.0 Chrome/152 Safari/537.36"},
        )
        try:
            with opener.open(request, timeout=20) as response:
                landing = response.geturl()
        except urllib.error.HTTPError as error:
            landing = error.geturl()
        except OSError as error:
            error_text = f"{type(error).__name__}: {error}"
            continue
        if (urllib.parse.urlparse(landing).hostname or "").lower() != "publications.aaahq.org":
            continue
        routed = buaa_fixed_host_url(
            landing, "publications.aaahq.org", BUAA_AAA_WEBVPN_PREFIX
        )
        return (
            Work(
                doi=work.doi, title=work.title, publisher=work.publisher,
                url=routed, oa_urls=(routed,), landing_url=routed,
            ),
            None,
        )
    return None, error_text


def resolve_aaa_webvpn_batch(
    works: list[Work], workers: int, vpn_headers: dict[str, str] | None = None
) -> list[tuple[Work, Work | None, str | None]]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        resolved = executor.map(resolve_aaa_webvpn_work, works)
        return [
            (original, routed, error)
            for original, (routed, error) in zip(works, resolved)
        ]


def resolve_elsevier_webvpn_work(work: Work) -> tuple[Work | None, str | None]:
    """Resolve an Elsevier DOI to a PII and BUAA ScienceDirect URLs."""
    # DOI resolution is metadata-only and may need the host's normal network
    # proxy. The resulting PDF URL is still downloaded exclusively in Chrome
    # after being rewritten under BUAA WebVPN.
    opener = urllib.request.build_opener()
    resolver = f"https://doi.org/{urllib.parse.quote(work.doi, safe='/():;._-')}"
    landing_url = ""
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            resolver,
            method=method,
            headers={"User-Agent": "Mozilla/5.0 Chrome/140 Safari/537.36"},
        )
        try:
            with opener.open(request, timeout=20) as response:
                landing_url = response.geturl()
        except urllib.error.HTTPError as error:
            landing_url = error.geturl()
        except OSError:
            continue
        match = re.search(r"/pii/([A-Za-z0-9]+)", landing_url, re.I)
        if match:
            pii = match.group(1).upper()
            article_url = f"https://www.sciencedirect.com/science/article/pii/{pii}"
            pdf_urls = (
                f"{article_url}/pdf",
                f"{article_url}/pdfft?isDTMRedir=true&download=true",
            )
            routed = tuple(buaa_sciencedirect_webvpn_url(url) for url in pdf_urls)
            return (
                Work(
                    doi=work.doi,
                    title=work.title,
                    publisher=work.publisher,
                    url=routed[0],
                    oa_urls=routed,
                    landing_url=buaa_sciencedirect_webvpn_url(article_url),
                ),
                None,
            )
    return None, "DOI redirect did not expose an Elsevier PII"


def resolve_elsevier_webvpn_batch(
    works: list[Work], workers: int, vpn_headers: dict[str, str] | None = None
) -> list[tuple[Work, Work | None, str | None]]:
    def resolve(work: Work) -> tuple[Work, Work | None, str | None]:
        routed, error = resolve_elsevier_webvpn_work(work)
        return work, routed, error

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(resolve, works))


def resolve_springer_webvpn_work(work: Work) -> Work:
    """Build deterministic Springer article/PDF URLs under BUAA WebVPN."""
    normalized = work.doi.strip()
    encoded = urllib.parse.quote(normalized, safe="/():;._-")
    return replace(
        work,
        url=buaa_springer_webvpn_url(
            f"https://link.springer.com/content/pdf/{encoded}.pdf"
        ),
        oa_urls=(
            buaa_springer_webvpn_url(
                f"https://link.springer.com/content/pdf/{encoded}.pdf"
            ),
        ),
        landing_url=buaa_springer_webvpn_url(
            f"https://link.springer.com/{'chapter' if '_' in normalized and normalized.startswith('10.1007/978-') else 'article'}/{encoded}"
        ),
    )


_ieee_resolution = threading.local()


def ieee_resolution_note(message: str) -> None:
    if hasattr(_ieee_resolution, "errors"):
        _ieee_resolution.errors.append(message)


def ieee_page_candidates(url: str, body: bytes, doi: str) -> tuple[str, ...]:
    try:
        from platforms.ieee_metadata import article_urls
    except ImportError:
        from examples.academic.platforms.ieee_metadata import article_urls
    for link in article_urls(body.decode("utf-8", errors="replace"), doi):
        candidates = ieee_pdf_candidates(link)
        if candidates:
            return candidates
    host = urllib.parse.urlparse(url).hostname or "unknown"
    ieee_resolution_note(f"{host}: page has no DOI-matched IEEE article metadata")
    return ()


def ieee_crossref_candidates(doi: str) -> tuple[str, ...]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    url = "https://api.crossref.org/works/" + urllib.parse.quote(doi, safe="")
    try:
        with opener.open(url, timeout=15) as response:
            message = json.loads(response.read(2_000_000))["message"]
        if str(message.get("DOI", "")).lower() != doi.lower():
            ieee_resolution_note("Crossref: DOI mismatch")
            return ()
        kind = message.get("type", "")
        if kind in {"book", "edited-book", "monograph", "reference-book", "book-series", "book-set"}:
            ieee_resolution_note(f"unsupported_document_type: Crossref identifies {kind}, not an article")
            return ()
        urls = [message.get("resource", {}).get("primary", {}).get("URL", "")]
        urls += [link.get("URL", "") for link in message.get("link", [])]
        for link in urls:
            if candidates := ieee_pdf_candidates(link):
                return candidates
        ieee_resolution_note(f"Crossref: type={kind or 'unknown'}, no IEEE article-number URL")
    except urllib.error.HTTPError as error:
        ieee_resolution_note(f"Crossref: HTTP {error.code}")
    except (OSError, ValueError, KeyError, TypeError) as error:
        ieee_resolution_note(f"Crossref: {type(error).__name__}")
    return ()


def ieee_vpn_resolver_candidates(doi: str, headers: dict[str, str]) -> tuple[str, ...]:
    """Keep DOI resolution and publisher redirects inside the authenticated VPN."""
    doi_prefix = "https://d.buaa.edu.cn/https/77726476706e69737468656265737421f4f848d228226f"
    def wrap(url):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "https":
            raise ValueError("Resolver redirected to an unsupported scheme")
        if parsed.hostname == "d.buaa.edu.cn":
            return url
        if parsed.hostname == "doi.org":
            return doi_prefix + parsed.path + ("?" + parsed.query if parsed.query else "")
        if parsed.hostname == "ieeexplore.ieee.org":
            return buaa_ieee_webvpn_url(url)
        raise ValueError("Resolver redirected outside the supported IEEE route")
    class VPNRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):
            return super().redirect_request(req, fp, code, msg, hdrs, wrap(newurl))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), VPNRedirect())
    request = urllib.request.Request(doi_prefix + '/' + urllib.parse.quote(doi, safe='/'), headers=headers)
    try:
        with opener.open(request, timeout=20) as response:
            return ieee_pdf_candidates(response.geturl()) or ieee_page_candidates(response.geturl(), response.read(2_000_000), doi)
    except urllib.error.HTTPError as error:
        ieee_resolution_note(f"VPN DOI resolver: HTTP {error.code}")
        return ieee_pdf_candidates(error.geturl())
    except (OSError, ValueError) as error:
        ieee_resolution_note(f"VPN DOI resolver: {type(error).__name__}" + (f" ({error})" if isinstance(error, ValueError) else ""))
        return ()


def resolve_ieee_webvpn_work(work: Work, vpn_headers: dict[str, str] | None = None) -> tuple[Work | None, str | None]:
    """Resolve a DOI to its IEEE article number and BUAA WebVPN URLs."""
    _ieee_resolution.errors = []
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    candidates = next(
        (found for url in (work.landing_url, work.openalex_landing_url,
                          work.openalex_pdf_url, work.url, *work.oa_urls)
         if url and (found := ieee_pdf_candidates(url))),
        (),
    )
    if not candidates:
        candidates = resolve_ieee_pdf_candidates(opener, work.doi)
    if not candidates and vpn_headers:
        candidates = ieee_vpn_resolver_candidates(work.doi, vpn_headers)
    if not candidates:
        candidates = ieee_crossref_candidates(work.doi)
    if not candidates:
        if not vpn_headers:
            ieee_resolution_note("authenticated VPN session unavailable")
        return None, "; ".join(_ieee_resolution.errors) or "No DOI-matched IEEE article number found"
    match = re.search(r"[?&]arnumber=(\d+)", candidates[0])
    if not match:
        return None, "resolved IEEE PDF URL has no article number"
    article_number = match.group(1)
    routed = tuple(buaa_ieee_webvpn_url(url) for url in candidates)
    return (
        Work(
            doi=work.doi,
            title=work.title,
            publisher=work.publisher,
            url=routed[0],
            oa_urls=routed,
            landing_url=(
                f"{BUAA_IEEE_WEBVPN_PREFIX}/document/{article_number}/"
            ),
        ),
        None,
    )


def resolve_ieee_webvpn_batch(
    works: list[Work], workers: int, vpn_headers: dict[str, str] | None = None
) -> list[tuple[Work, Work | None, str | None]]:
    def resolve(work: Work) -> tuple[Work, Work | None, str | None]:
        routed, error = resolve_ieee_webvpn_work(work, vpn_headers)
        return work, routed, error

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(resolve, works))


def resolve_ieee_pdf_candidates(
    opener: urllib.request.OpenerDirector,
    doi: str,
) -> tuple[str, ...]:
    resolver = f"https://doi.org/{doi}"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(
            resolver,
            method=method,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/140.0.0.0 Safari/537.36"
                )
            },
        )
        landing_url = ""
        body = b""
        try:
            with opener.open(request, timeout=20) as response:
                landing_url = response.geturl()
                if method == "GET" and not ieee_pdf_candidates(landing_url):
                    body = response.read(2_000_000)
        except urllib.error.HTTPError as error:
            landing_url = error.geturl()
            ieee_resolution_note(f"direct DOI {method}: HTTP {error.code} at {urllib.parse.urlparse(landing_url).hostname}")
        except OSError as error:
            ieee_resolution_note(f"direct DOI {method}: {type(error).__name__}")
            continue
        candidates = ieee_pdf_candidates(landing_url)
        if not candidates and body:
            candidates = ieee_page_candidates(landing_url, body, doi)
        if candidates:
            return candidates
    return ()


def resolved_work(work: Work, metadata: dict, campus_vpn: bool = False) -> Work | None:
    if metadata.get("status") != "oa" and not (
        campus_vpn and metadata.get("status") == "not_oa"
    ):
        return None
    urls = metadata.get("oa_urls") or [metadata.get("oa_url")]
    oa_urls = tuple(
        str(url).strip()
        for url in urls
        if url and str(url).strip().startswith(("http://", "https://"))
    )
    if campus_vpn:
        routed = campus_pdf_candidates(
            work.doi,
            str(metadata.get("landing_url") or ""),
        )
        oa_urls = tuple(dict.fromkeys((*routed, *oa_urls)))
    if not oa_urls:
        oa_urls = (f"https://doi.org/{work.doi}",)
    return replace(
        work,
        title=str(metadata.get("title") or work.title),
        publisher=str(metadata.get("publisher") or work.publisher),
        url=oa_urls[0],
        oa_urls=oa_urls,
        landing_url=str(
            metadata.get("landing_url") or f"https://doi.org/{work.doi}"
        ),
    )


class HostLimiter:
    def __init__(self, per_host: int, min_interval: float = 0.0) -> None:
        self.per_host = per_host
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.semaphores: dict[str, threading.BoundedSemaphore] = {}
        self._next_ok: dict[str, float] = {}

    def for_url(self, url: str) -> threading.BoundedSemaphore:
        host = (urllib.parse.urlparse(url).hostname or "unknown").lower()
        wait = 0.0
        with self.lock:
            sem = self.semaphores.setdefault(
                host, threading.BoundedSemaphore(self.per_host)
            )
            if self.min_interval > 0:
                now = time.monotonic()
                scheduled = max(now, self._next_ok.get(host, 0.0))
                self._next_ok[host] = scheduled + self.min_interval
                wait = scheduled - now
        if wait > 0:
            time.sleep(wait)
        return sem


def _host_needs_insecure_ssl(url: str) -> bool:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    return host == "d.buaa.edu.cn" or host.endswith(".buaa.edu.cn")


def _safe_request_error(url: str, exc: BaseException) -> str:
    host = urllib.parse.urlparse(url).hostname or "unknown-host"
    text = str(exc)
    lowered = text.lower()
    if "cookie" in lowered or "authorization" in lowered or "ticket" in lowered:
        return f"{host}: {type(exc).__name__}"
    return f"{host}: {type(exc).__name__}: {text[:180]}"


def direct_download(
    work: Work,
    output_dir: Path,
    limiter: HostLimiter,
    request_timeout: int = 40,
    attempts: int = 2,
    extra_headers: dict[str, str] | None = None,
    ssl_context: ssl.SSLContext | None = None,
) -> tuple[Work, Path | None, str | None]:
    destination = output_dir / safe_filename(work)
    if is_valid_pdf(destination):
        return work, destination, None
    handlers: list[urllib.request.BaseHandler] = [urllib.request.ProxyHandler({})]
    if ssl_context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=ssl_context))
    opener = urllib.request.build_opener(*handlers)
    errors: list[str] = []
    urls = work.oa_urls or (work.url,)
    if work.doi.lower().startswith("10.1109/") and not any(
        "arnumber=" in url for url in urls
    ):
        routed = resolve_ieee_pdf_candidates(opener, work.doi)
        urls = tuple(dict.fromkeys((*routed, *urls)))
    for url_index, url in enumerate(urls):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/140.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1"
            ),
            "Referer": work.landing_url or f"https://doi.org/{work.doi}",
        }
        if extra_headers:
            cookie_header = extra_headers.get("Cookie") or extra_headers.get("cookie")
            headers.update(
                {
                    key: value
                    for key, value in extra_headers.items()
                    if key.lower() != "cookie"
                }
            )
            host = (urllib.parse.urlparse(url).hostname or "").lower()
            if cookie_header and "buaa.edu.cn" in host:
                headers["Cookie"] = cookie_header
        request = urllib.request.Request(url, headers=headers)
        for attempt in range(attempts):
            temporary = destination.with_name(
                f".{destination.name}.{os.getpid()}.{threading.get_ident()}."
                f"{url_index}.{attempt}.part"
            )
            retry_delay = 0.5 + random.random()
            try:
                with limiter.for_url(url):
                    with opener.open(request, timeout=request_timeout) as response:
                        content_type = (
                            response.headers.get("Content-Type") or ""
                        ).lower()
                        first = response.read(HTTP_CHUNK_SIZE)
                        try:
                            from platforms.chrome_session import body_looks_like_login
                        except ImportError:
                            from examples.academic.platforms.chrome_session import (
                                body_looks_like_login,
                            )
                        if "buaa.edu.cn" in (
                            urllib.parse.urlparse(url).hostname or ""
                        ).lower() and body_looks_like_login(first, content_type):
                            errors.append("login_expired: HTML response instead of PDF")
                            break
                        if not first.startswith(b"%PDF") or "text/html" in content_type:
                            host = urllib.parse.urlparse(url).hostname or "unknown-host"
                            errors.append(
                                f"{host}: non-PDF "
                                f"({content_type or 'unknown content type'})"
                            )
                            break
                        with temporary.open("wb") as handle:
                            handle.write(first)
                            while chunk := response.read(HTTP_CHUNK_SIZE):
                                handle.write(chunk)
                try:
                    from platforms.validation import validate_pdf
                    from platforms.content_filter import record_exclusion
                except ImportError:
                    from examples.academic.platforms.validation import validate_pdf
                    from examples.academic.platforms.content_filter import record_exclusion
                validation = validate_pdf(temporary, expected_doi=work.doi, expected_title=work.title)
                if validation.failure_class == FailureClass.NON_ARTICLE:
                    state_dir = output_dir.parent / '.doi_download_state'
                    record_exclusion(state_dir / 'non_article_manifest.jsonl', work.doi, work.title, validation.error_message)
                    quarantine = state_dir / 'quarantine' / 'non_article'
                    quarantine.mkdir(parents=True, exist_ok=True)
                    os.replace(temporary, quarantine / safe_filename(work))
                    return work, None, 'non_article: ' + validation.error_message
                if validation.is_valid:
                    os.replace(temporary, destination)
                    return work, destination, None
                errors.append("downloaded body failed PDF validation")
            except urllib.error.HTTPError as error:
                host = urllib.parse.urlparse(url).hostname or "unknown-host"
                errors.append(f"{host}: HTTP {error.code}")
                if error.code in {401, 403} and "buaa.edu.cn" in host.lower():
                    errors.append("login_expired: HTTP authentication failure")
                if error.code in {403, 420, 429}:
                    retry_after = error.headers.get("Retry-After")
                    try:
                        retry_delay = min(30.0, max(2.0, float(retry_after)))
                    except (TypeError, ValueError):
                        retry_delay = min(
                            30.0,
                            (8.0 if error.code == 420 else 3.0) * (attempt + 1)
                            + random.random(),
                        )
            except Exception as error:  # noqa: BLE001 - browser fallback handles it
                errors.append(_safe_request_error(url, error))
            finally:
                temporary.unlink(missing_ok=True)
            if attempt + 1 < attempts:
                time.sleep(retry_delay)
    error_text = "; ".join(errors[-4:]) or "all OA URLs failed"
    return work, None, error_text


def direct_download_iter(
    works: list[Work],
    output_dir: Path,
    workers: int,
    per_host: int,
    request_timeout: int = 40,
    attempts: int = 2,
    extra_headers: dict[str, str] | None = None,
    ssl_context: ssl.SSLContext | None = None,
    min_interval: float = 0.0,
):
    limiter = HostLimiter(per_host, min_interval=min_interval)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                direct_download,
                work,
                output_dir,
                limiter,
                request_timeout,
                attempts,
                extra_headers,
                ssl_context,
            )
            for work in works
        ]
        for future in concurrent.futures.as_completed(futures):
            yield future.result()


def direct_download_batch(
    works: list[Work],
    output_dir: Path,
    workers: int,
    per_host: int,
    request_timeout: int = 40,
    attempts: int = 2,
    extra_headers: dict[str, str] | None = None,
    ssl_context: ssl.SSLContext | None = None,
    min_interval: float = 0.0,
) -> list[tuple[Work, Path | None, str | None]]:
    return list(
        direct_download_iter(
            works,
            output_dir,
            workers,
            per_host,
            request_timeout,
            attempts,
            extra_headers,
            ssl_context,
            min_interval,
        )
    )


def append_manifest(manifest: Path, record: dict) -> None:
    append_jsonl(manifest, record)


def record_non_oa(manifest: Path, work: Work, metadata: dict) -> None:
    append_manifest(
        manifest,
        {
            "doi": work.doi,
            "title": metadata.get("title") or work.title,
            "publisher": metadata.get("publisher") or work.publisher,
            "candidate_title": work.title,
            "status": metadata.get("status") or "metadata_failed",
            "validation_version": OA_CACHE_VERSION,
            "oa_status": metadata.get("oa_status"),
            "file": None,
            "bytes": 0,
            "engine": "openalex-validation",
            "error": metadata.get("error"),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def publish_pdf(source: Path, destination: Path) -> bool:
    """Atomically mirror a staged PDF into a browser-monitored output dir."""
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{threading.get_ident()}.publish"
    )
    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, destination)
        return True
    except OSError:
        return False
    finally:
        temporary.unlink(missing_ok=True)


def commit_direct_success(
    work: Work,
    staged_path: Path,
    output_dir: Path,
    manifest: Path,
    parallel_safe: bool,
    engine: str,
) -> Path | None:
    """Copy one staged PDF into oapdf and append a downloaded manifest row."""
    reported_path = staged_path
    if parallel_safe:
        final_path = output_dir / safe_filename(work)
        if publish_pdf(staged_path, final_path):
            reported_path = final_path
        else:
            print(f"[publish deferred] {work.doi}: staged at {staged_path}")
            return None
    record_direct_download(
        manifest,
        work,
        staged_path if reported_path == staged_path else reported_path,
        reported_path,
        engine=engine,
    )
    if (
        parallel_safe
        and reported_path != staged_path
        and looks_like_pdf(reported_path)
    ):
        staged_path.unlink(missing_ok=True)
    print(f"[direct OK] {work.doi}: {reported_path.name}")
    return reported_path


def flush_direct_successes(
    pending: list[tuple[Work, Path]],
    output_dir: Path,
    manifest: Path,
    parallel_safe: bool,
    engine: str,
    publish_every: int,
    completed: set[str] | None = None,
    force: bool = False,
) -> int:
    """Move staged successes into oapdf in groups of publish_every.

    force=True publishes the leftover group smaller than publish_every.
    publish_every<=0 publishes the whole pending list when force=True.
    """
    flushed = 0
    while pending and (
        force or (publish_every > 0 and len(pending) >= publish_every)
    ):
        count = len(pending) if force or publish_every <= 0 else publish_every
        chunk = pending[:count]
        del pending[:count]
        for work, path in chunk:
            published = commit_direct_success(
                work, path, output_dir, manifest, parallel_safe, engine
            )
            if published is None:
                pending.append((work, path))
                continue
            flushed += 1
            if completed is not None:
                completed.add(work.doi.lower())
        if force:
            break
    return flushed


def ingest_existing_staging(
    staging_dir: Path,
    output_dir: Path,
    manifest: Path,
    completed: set[str],
    parallel_safe: bool,
    engine: str,
    publish_every: int,
) -> int:
    """Publish valid PDFs left in staging by an interrupted run."""
    if not staging_dir.is_dir():
        return 0
    pending: list[tuple[Work, Path]] = []
    for path in sorted(
        item
        for item in staging_dir.glob("*.pdf")
        if not item.name.startswith("._")
    ):
        if not looks_like_pdf(path):
            continue
        doi = doi_from_safe_filename(path.name)
        key = doi.lower()
        final_path = output_dir / path.name
        if looks_like_pdf(final_path):
            completed.add(key)
            continue
        pending.append(
            (
                Work(doi=doi, title=doi, url=f"https://doi.org/{doi}"),
                path,
            )
        )
    ingested = 0
    ingested += flush_direct_successes(
        pending,
        output_dir,
        manifest,
        parallel_safe,
        engine,
        publish_every,
        completed,
        force=False,
    )
    ingested += flush_direct_successes(
        pending,
        output_dir,
        manifest,
        parallel_safe,
        engine,
        publish_every,
        completed,
        force=True,
    )
    return ingested


def record_direct_download(
    manifest: Path,
    work: Work,
    source_path: Path,
    reported_path: Path | None = None,
    engine: str = "concurrent-direct-http",
) -> None:
    try:
        byte_count = source_path.stat().st_size
    except OSError:
        byte_count = 0
    append_manifest(
        manifest,
        {
            "doi": work.doi,
            "title": work.title,
            "publisher": work.publisher,
            "landing_url": work.url,
            "status": "downloaded",
            "file": str(reported_path or source_path),
            "bytes": byte_count,
            "engine": engine,
            "error": None,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def runner_prefix(root: Path, override: Path | None) -> list[str]:
    if override is not None:
        if not override.is_file():
            raise SystemExit(f"runner not found: {override}")
        return [str(override)]
    binary = root / "target" / "debug" / "drission-workflow"
    if binary.is_file():
        return [str(binary)]
    installed = shutil.which("drission-workflow")
    if installed:
        return [installed]
    return ["cargo", "run", "-p", "workflow-cli", "--"]


def save_browser_failure_details(root: Path, run_id: str, output_dir: Path) -> None:
    """Preserve the runner's per-article errors instead of losing them at batch exit."""
    import sqlite3
    try:
        with sqlite3.connect(f"file:{root / '.drission-workflow/workflows.db'}?mode=ro", uri=True) as db:
            row = db.execute("SELECT inputs_json FROM runs WHERE id=?", (run_id,)).fetchone()
            if not row:
                return
            articles = json.loads(row[0]).get("articles", [])
            failures = {}
            for step, code, message in db.execute(
                    "SELECT step_id,error_code,error_message FROM step_runs "
                    "WHERE run_id=? AND error_code IS NOT NULL ORDER BY step_id", (run_id,)):
                match = re.search(r"\[(\d+)\]", step)
                if match and int(match[1]) < len(articles):
                    doi = articles[int(match[1])]["doi"].lower()
                    failures.setdefault(doi, []).append({"step": step, "code": code, "message": message})
                elif not match:
                    failures.setdefault("__batch__", []).append({"step": step, "code": code, "message": message})
            (output_dir / "workflow_failures.json").write_text(json.dumps(failures, ensure_ascii=False))
    except (sqlite3.Error, OSError, ValueError, KeyError) as error:
        print(f"Could not preserve workflow error details: {error}")


def run_batch(
    root: Path,
    workflow: Path,
    runner: list[str],
    output_dir: Path,
    batch: list[Work],
) -> int:
    try:
        from platforms.webvpn import assert_not_forbidden_webvpn_url
    except ImportError:
        from examples.academic.platforms.webvpn import assert_not_forbidden_webvpn_url
    # Reuse the task page before attaching the workflow, including on batch retries.
    if os.environ.get("DRISSION_SHARED_BROWSER_ENDPOINT") and batch:
        try:
            from platforms.chrome_session import shared_debug_port, open_start_page
        except ImportError:
            from examples.academic.platforms.chrome_session import shared_debug_port, open_start_page
        port = shared_debug_port()
        if port:
            open_start_page(port, batch[0].landing_url or batch[0].url, clean_empty_tabs=True)
    articles = [
        {
            "doi": work.doi,
            "url": assert_not_forbidden_webvpn_url(work.url),
            "landingUrl": work.landing_url or f"https://doi.org/{work.doi}",
            "filename": safe_filename(work),
        }
        for work in batch
    ]
    input_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".json", delete=False
        ) as handle:
            json.dump({"articles": articles}, handle, ensure_ascii=False)
            input_path = Path(handle.name)
        command = runner + [
            "run",
            str(workflow),
            "--inputs",
            str(input_path),
            "--artifacts",
            str(output_dir),
        ]
        print(f"Starting real-browser batch: {len(batch)} DOI(s)")
        (output_dir / "articles.json").write_text(json.dumps({"articles": articles}, ensure_ascii=False), encoding="utf-8")
        browser_env = os.environ.copy()
        for name in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        ):
            browser_env.pop(name, None)
        result = subprocess.run(command, cwd=root, env=browser_env, check=False,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout, end="", flush=True)
        run_match = re.search(r"^run: ([a-f0-9-]+)$", result.stdout, re.MULTILINE)
        if run_match:
            save_browser_failure_details(root, run_match[1], output_dir)
        return result.returncode
    finally:
        if input_path is not None:
            input_path.unlink(missing_ok=True)


def record_batch(
    manifest: Path,
    artifact_dir: Path,
    output_dir: Path,
    batch: list[Work],
    returncode: int,
) -> tuple[int, int]:
    downloaded = failed = 0
    now = datetime.now(timezone.utc).isoformat()
    quarantine_dir = manifest.parent / "quarantine"
    try:
        workflow_failures = json.loads((artifact_dir / "workflow_failures.json").read_text())
    except (OSError, ValueError):
        workflow_failures = {}

    try:
        from platforms.publisher import publish_staged_pdf
        from platforms.download_watcher import wait_for_browser_download
    except ImportError:
        from examples.academic.platforms.publisher import publish_staged_pdf
        from examples.academic.platforms.download_watcher import wait_for_browser_download

    for work in batch:
        filename = safe_filename(work)
        artifact_path = artifact_dir / filename
        final_path = output_dir / filename
        plat = campus_platform(work) or "browser"

        evidence_path = artifact_dir / f"{work.doi.split('/', 1)[-1]}.access.json"
        try:
            evidence = json.loads(evidence_path.read_text())
        except (OSError, ValueError):
            evidence = {}
        if (not artifact_path.exists()
                and evidence.get("status") in {"institution_access_unavailable", "publication_pending"}
                and str(evidence.get("doi", "")).lower() == work.doi.lower()):
            append_manifest(manifest, {
                "doi": work.doi, "title": work.title, "publisher": work.publisher,
                "landing_url": work.landing_url or work.url,
                "status": evidence["status"], "file": None, "bytes": 0,
                "engine": "browser-workflow", "failure_class": evidence["status"],
                "error": evidence.get("message"), "evidence_url": evidence.get("url"),
                "artifact_dir": str(artifact_dir), "recorded_at": now,
            })
            print(f"Deferred {work.doi}: {evidence['status']}")
            continue

        if (returncode and workflow_failures and not artifact_path.exists()
                and work.doi.lower() not in workflow_failures):
            append_manifest(manifest, {
                "doi": work.doi, "title": work.title, "publisher": work.publisher,
                "landing_url": work.landing_url or work.url,
                "status": "retry_pending", "file": None, "bytes": 0,
                "engine": "browser-workflow", "error": "Batch stopped before this DOI completed; retained for retry",
                "recorded_at": now,
            })
            continue

        # The workflow already waits for the click action.  Do not add a fixed
        # timeout for a DOI that produced no download at all.  Continue watching
        # only when Chrome created a final or partial target.
        pending_paths = (
            artifact_path,
            artifact_dir / f"{filename}.crdownload",
            artifact_dir / f"{filename}.tmp",
        )
        if any(path.exists() for path in pending_paths):
            wait_res = wait_for_browser_download(
                download_dir=artifact_dir,
                expected_filename=filename,
                timeout_seconds=180.0 if plat == "rsc" else 120.0,
                check_interval=2.0,
                stability_checks=3,
            )
            if wait_res.completed and wait_res.final_file:
                artifact_path = wait_res.final_file
        pub_res = publish_staged_pdf(
            staged_path=artifact_path,
            output_dir=output_dir,
            quarantine_dir=quarantine_dir,
            expected_doi=work.doi,
            expected_title=work.title,
            platform=plat,
        )

        if pub_res.published:
            downloaded += 1
            status = "downloaded"
            error = None
            reported_file = str(pub_res.final_path or final_path)
            byte_count = pub_res.bytes_count
        else:
            if pub_res.failure_class != FailureClass.NON_ARTICLE.value:
                failed += 1
            if pub_res.failure_class == FailureClass.NO_PDF_AVAILABLE.value:
                status = "no_pdf_available"
            else:
                status = pub_res.status if pub_res.status != "failed" else "browser_failed"
            error = pub_res.message or (f"workflow exit code {returncode}" if returncode else "no valid PDF produced")
            reported_file = None
            byte_count = pub_res.bytes_count

        failure_class = pub_res.failure_class
        if not pub_res.published and returncode and not artifact_path.exists() and pub_res.failure_class != FailureClass.NON_ARTICLE.value:
            failure_class = "workflow_failed"
            error = f"workflow exit code {returncode}; no PDF produced: {error}"

        step_failures = workflow_failures.get(work.doi.lower(), [])
        if not pub_res.published and failure_class in {"workflow_failed", "download_not_triggered"} and step_failures:
            error = " | ".join(f"{item['step']}: {item['message']}" for item in step_failures)

        append_manifest(
            manifest,
            {
                "doi": work.doi,
                "title": work.title,
                "publisher": work.publisher,
                "landing_url": work.landing_url or work.url,
                "status": status,
                "file": reported_file,
                "bytes": byte_count,
                "engine": "browser-workflow",
                "source_doi": work.extra.get("source_doi", work.doi),
                "failure_class": failure_class,
                "workflow_exit_code": returncode,
                "artifact_dir": str(artifact_dir) if not pub_res.published else None,
                "error": error,
                "recorded_at": now,
            },
        )
    return downloaded, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download DOI records from total_journals with optional real-Chrome access"
    )
    parser.add_argument(
        "input",
        type=Path,
        help="DOI project root containing total_journals (legacy CSVs also supported)",
    )
    parser.add_argument("--output", type=Path, help="output directory (default: <input>/oapdf)")
    parser.add_argument(
        "--state-prefix",
        default="",
        help="prefix manifest/cache filenames for a parallel task",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="state/log directory (default: <input>/.doi_download_state)",
    )
    parser.add_argument(
        "--parallel-safe",
        action="store_true",
        help="stage direct PDFs outside the browser-monitored output before publishing",
    )
    parser.add_argument(
        "--oa-mode",
        choices=("oa", "non_oa", "all"),
        default="oa",
        help="select CSV rows by OA flag (default: oa)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="attempt at most N metadata-validated records (0 means all)",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="DOIs per persistent-browser run (1-500)")
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="retry failures from the last 24 hours after repairing access",
    )
    parser.add_argument(
        "--metadata-workers",
        type=int,
        default=8,
        help="concurrent OpenAlex lookups (1-16, default: 8)",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=12,
        help="concurrent direct PDF downloads (1-32, default: 12)",
    )
    parser.add_argument(
        "--per-host",
        type=int,
        default=2,
        help="maximum concurrent direct requests per hostname (1-4, default: 2)",
    )
    parser.add_argument(
        "--direct-timeout",
        type=int,
        default=40,
        help="seconds per direct HTTP request (5-120, default: 40)",
    )
    parser.add_argument(
        "--direct-attempts",
        type=int,
        default=2,
        help="attempts per direct PDF URL (1-3, default: 2)",
    )
    parser.add_argument("--runner", type=Path, help="path to drission-workflow executable")
    parser.add_argument(
        "--workflow",
        type=Path,
        help="browser fallback workflow override",
    )
    parser.add_argument(
        "--campus-vpn",
        action="store_true",
        help="allow institution-access PDFs even when OpenAlex says the DOI is not OA",
    )
    parser.add_argument(
        "--campus-trial",
        type=int,
        default=0,
        metavar="N",
        help="select an N-DOI publisher-balanced campus VPN trial (implies --campus-vpn)",
    )
    parser.add_argument(
        "--exclude-campus-platform",
        action="append",
        default=[],
        choices=CAMPUS_PLATFORM_CHOICES,
        help="skip a campus platform; may be specified more than once",
    )
    parser.add_argument(
        "--only-campus-platform",
        action="append",
        default=[],
        choices=CAMPUS_PLATFORM_CHOICES,
        help="process only these campus platforms; may be specified more than once",
    )
    parser.add_argument(
        "--direct-only",
        action="store_true",
        help="try direct HTTP candidates only; never launch Chrome",
    )
    parser.add_argument(
        "--browser-only",
        action="store_true",
        help="skip unauthenticated direct HTTP and send every verified DOI to Chrome",
    )
    parser.add_argument(
        "--session-http",
        action="store_true",
        help="download IEEE WebVPN PDFs over HTTP using the live Chrome login cookies",
    )
    parser.add_argument(
        "--skip-oa-metadata",
        action="store_true",
        help="do not call OpenAlex/Unpaywall; use the CSV OA flag, local caches, and constructed PDF URLs",
    )
    parser.add_argument(
        "--skip-unpaywall",
        action="store_true",
        help="resolve OA URLs with OpenAlex only; skip the extra Unpaywall round trip",
    )
    parser.add_argument(
        "--chrome-profile",
        type=Path,
        help="persistent Chrome profile used by --session-http",
    )
    parser.add_argument(
        "--publish-every",
        type=int,
        default=20,
        help="copy staged PDFs into oapdf after every N successes (0 = wait until the batch ends)",
    )
    parser.add_argument(
        "--ieee-webvpn",
        action="store_true",
        help="route IEEE article/PDF URLs through the logged-in BUAA WebVPN profile",
    )
    parser.add_argument(
        "--springer-webvpn",
        action="store_true",
        help="route SpringerLink article/PDF URLs through a logged-in BUAA WebVPN profile",
    )
    parser.add_argument(
        "--nature-vpn",
        action="store_true",
        help="download Nature URLs in real Chrome over an active Fudan EasyConnect VPN",
    )
    parser.add_argument('--nature-route', choices=['fudan', 'buaa'], default='fudan')
    parser.add_argument(
        "--elsevier-webvpn",
        action="store_true",
        help="route ScienceDirect article/PDF URLs through a logged-in BUAA WebVPN profile",
    )
    parser.add_argument("--acs-webvpn", action="store_true", help="route ACS DOI pages through BUAA WebVPN")
    parser.add_argument("--aps-webvpn", action="store_true", help="route APS DOI pages through BUAA WebVPN")
    parser.add_argument("--aaa-webvpn", action="store_true", help="route American Accounting Association DOI pages through BUAA WebVPN")
    parser.add_argument("--cambridge-webvpn", action="store_true", help="route Cambridge Core DOI pages through BUAA WebVPN")
    parser.add_argument("--rsc-webvpn", action="store_true", help="route RSC DOI pages through BUAA WebVPN")
    parser.add_argument("--scan-only", action="store_true", help="only count rows selected by --oa-mode")
    args = parser.parse_args()
    args.input = args.input.expanduser().resolve()
    args.output = args.output.expanduser().resolve() if args.output else args.input / "oapdf"
    args.state_dir = (
        args.state_dir.expanduser().resolve()
        if args.state_dir
        else args.input / ".doi_download_state"
    )
    args.runner = args.runner.expanduser().resolve() if args.runner else None
    if not args.input.is_dir() or not csv_files(args.input):
        parser.error(
            "no refreshed total_journals or legacy merged CSV files in: "
            f"{args.input}"
        )
    if args.limit < 0 or not 1 <= args.batch_size <= 500:
        parser.error("--limit must be >= 0 and --batch-size must be between 1 and 500")
    if args.campus_trial < 0:
        parser.error("--campus-trial must be >= 0")
    if args.state_prefix and not re.fullmatch(r"[A-Za-z0-9_.-]+", args.state_prefix):
        parser.error("--state-prefix may contain only letters, digits, dot, underscore, and hyphen")
    if args.campus_trial:
        args.campus_vpn = True
    if args.direct_only and args.browser_only:
        parser.error("--direct-only and --browser-only cannot be used together")
    if sum(
        (
            args.ieee_webvpn,
            args.springer_webvpn,
            args.nature_vpn,
            args.elsevier_webvpn,
            args.acs_webvpn,
            args.aps_webvpn,
            args.aaa_webvpn,
            args.cambridge_webvpn,
            args.rsc_webvpn,
        )
    ) > 1:
        parser.error(
            "institution-specific browser modes are mutually exclusive"
        )
    if args.ieee_webvpn:
        args.campus_vpn = True
        args.browser_only = not args.session_http
        if not args.only_campus_platform:
            args.only_campus_platform = ["ieee"]
        elif set(args.only_campus_platform) != {"ieee"}:
            parser.error("--ieee-webvpn can only be used with the IEEE platform")
    if args.session_http:
        if not args.ieee_webvpn:
            parser.error("--session-http currently requires --ieee-webvpn")
        args.campus_vpn = True
        args.browser_only = False
        if args.chrome_profile is None:
            spec = get_platform_spec("ieee")
            profile = spec.profile_dir if spec is not None else ".drission-workflow/profiles/doi-ieee-campus"
            args.chrome_profile = Path(profile)
        if not args.chrome_profile.is_absolute():
            args.chrome_profile = (repo_root() / args.chrome_profile).resolve()
    if args.springer_webvpn:
        args.campus_vpn = True
        args.browser_only = True
        if not args.only_campus_platform:
            args.only_campus_platform = ["springer"]
        elif set(args.only_campus_platform) != {"springer"}:
            parser.error(
                "--springer-webvpn can only be used with the Springer platform"
            )
    if args.nature_vpn:
        args.campus_vpn = True
        args.browser_only = True
        if not args.only_campus_platform:
            args.only_campus_platform = ["nature"]
        elif set(args.only_campus_platform) != {"nature"}:
            parser.error("--nature-vpn can only be used with the Nature platform")
    if args.elsevier_webvpn:
        args.campus_vpn = True
        args.browser_only = True
        if not args.only_campus_platform:
            args.only_campus_platform = ["elsevier_cell"]
        elif set(args.only_campus_platform) != {"elsevier_cell"}:
            parser.error(
                "--elsevier-webvpn can only be used with the Elsevier/Cell platform"
            )
    for enabled, platform, option in (
        (args.acs_webvpn, "acs", "--acs-webvpn"),
        (args.aps_webvpn, "aps", "--aps-webvpn"),
        (args.aaa_webvpn, "aaa", "--aaa-webvpn"),
        (args.cambridge_webvpn, "cambridge", "--cambridge-webvpn"),
        (args.rsc_webvpn, "rsc", "--rsc-webvpn"),
    ):
        if enabled:
            args.campus_vpn = True
            args.browser_only = True
            if not args.only_campus_platform:
                args.only_campus_platform = [platform]
            elif set(args.only_campus_platform) != {platform}:
                parser.error(f"{option} can only be used with the {platform.upper()} platform")
    if len(set(args.exclude_campus_platform)) == len(CAMPUS_PLATFORM_CHOICES):
        parser.error("at least one campus platform must remain enabled")
    overlap = set(args.exclude_campus_platform) & set(args.only_campus_platform)
    if overlap:
        parser.error(
            "a platform cannot be both included and excluded: "
            + ", ".join(sorted(overlap))
        )
    if not 1 <= args.metadata_workers <= 16:
        parser.error("--metadata-workers must be between 1 and 16")
    if not 1 <= args.download_workers <= 32:
        parser.error("--download-workers must be between 1 and 32")
    if not 1 <= args.per_host <= 4:
        parser.error("--per-host must be between 1 and 4")
    if not 5 <= args.direct_timeout <= 120:
        parser.error("--direct-timeout must be between 5 and 120")
    if not 1 <= args.direct_attempts <= 3:
        parser.error("--direct-attempts must be between 1 and 3")
    if not 0 <= args.publish_every <= 500:
        parser.error("--publish-every must be between 0 and 500")
    return args


def main() -> None:
    args = parse_args()
    if args.scan_only:
        excluded = set(args.exclude_campus_platform)
        only = set(args.only_campus_platform)
        count = 0
        non_article_count = 0
        for w in iter_oa_rows(args.input, args.oa_mode):
            plat = campus_platform(w)
            if only and plat not in only:
                continue
            if plat in excluded:
                continue
            if args.acs_webvpn and acs_non_article_reason(w):
                non_article_count += 1
                continue
            count += 1
        print(
            f"oa_mode={args.oa_mode} (filtered): {count}; "
            f"non_article_skipped={non_article_count}"
        )
        return

    root = repo_root()
    if args.workflow:
        workflow = args.workflow.expanduser()
        if not workflow.is_absolute():
            workflow = root / workflow
        workflow = workflow.resolve()
    else:
        workflow_name = (
            "doi-campus-browser-downloader.yaml"
            if args.campus_vpn
            else "doi-oa-real-browser-downloader.yaml"
        )
        workflow = root / "examples" / "templates" / workflow_name
    if not workflow.is_file():
        raise SystemExit(f"workflow not found: {workflow}")
    args.output.mkdir(parents=True, exist_ok=True)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    runtime_platform = (
        (args.only_campus_platform[0] if len(args.only_campus_platform) == 1 else "")
        or args.state_prefix
        or "doi_hybrid"
    )
    runtime = PlatformRuntimeState(
        platform_id=runtime_platform,
        state="running",
        reason=f"oa_mode={args.oa_mode}, limit={args.limit or 'all'}",
    )
    save_runtime_state(args.input, runtime)
    direct_output = args.output
    if args.parallel_safe:
        staging_label = args.state_prefix or "parallel_direct"
        direct_output = args.input / f".{staging_label}_staging"
        direct_output.mkdir(parents=True, exist_ok=True)
    state_prefix = f"{args.state_prefix}_" if args.state_prefix else ""
    manifest = args.state_dir / f"{state_prefix}manifest.jsonl"
    cache_path = args.state_dir / f"{state_prefix}openalex_oa_cache.jsonl"
    completed = load_completed(
        manifest,
        args.output,
        include_validation_terminal=not args.campus_vpn,
    )
    completed.update(load_manifest_dois_by_status(manifest, {"non_article"}))
    if args.nature_vpn and args.nature_route == "buaa":
        completed.update(load_manifest_dois_by_status(manifest, {"institution_access_unavailable"}))
    deferred = load_deferred_dois(manifest, statuses={"publication_pending"}) if args.retry_failed else load_deferred_dois(manifest)
    print(f"Resume: completed={len(completed)}, deferred_failures={len(deferred)}")
    oa_cache = load_all_oa_caches(args.state_dir, cache_path)
    seen_dois: set[str] = set()
    chrome_session = None
    session_headers: dict[str, str] | None = None
    session_ssl: ssl.SSLContext | None = None
    if args.session_http:
        try:
            from platforms.chrome_session import ChromeSession, unverified_ssl_context
        except ImportError:
            from examples.academic.platforms.chrome_session import (
                ChromeSession,
                unverified_ssl_context,
            )
        chrome_session = ChromeSession(args.chrome_profile)
        chrome_session.ensure_session(
            f"{BUAA_IEEE_WEBVPN_PREFIX}/Xplore/home.jsp"
        )
        session_headers = {
            "Cookie": chrome_session.cookie_header_for("d.buaa.edu.cn")
        }
        session_ssl = unverified_ssl_context()
        print(
            "IEEE session HTTP enabled: Chrome stays on the login profile, "
            "PDFs are fetched over HTTP"
        )

    excluded_platforms = set(args.exclude_campus_platform)
    only_platforms = set(args.only_campus_platform)
    if only_platforms:
        excluded_platforms.update(
            platform
            for platform in CAMPUS_PLATFORM_CHOICES
            if platform not in only_platforms
        )
    source_rows: Iterable[Work]
    if args.campus_trial:
        source_rows = campus_trial_rows(
            args.input,
            args.campus_trial,
            completed | deferred,
            excluded_platforms,
            args.oa_mode,
        )
    else:
        source_rows = iter_oa_rows(args.input, args.oa_mode)

    source_rows = prioritize_retry_rows(source_rows, load_manifest_dois_by_status(manifest, {"retry_pending"}))

    def pending_rows() -> Iterable[Work]:
        for work in source_rows:
            key = work.doi.lower()
            # Check BEFORE publisher prefilters, which themselves append rows.
            if key in completed or key in deferred or key in seen_dois:
                continue
            if '...' in key or '…' in key or any(c.isspace() for c in key):
                seen_dois.add(key)
                append_manifest(manifest, {"doi": work.doi, "title": work.title,
                    "status": "invalid_doi", "error": "Truncated DOI or embedded whitespace; source correction required",
                    "recorded_at": datetime.now(timezone.utc).isoformat()})
                continue
            if args.ieee_webvpn and not key.startswith('10.1109/') and not any(
                ieee_pdf_candidates(url) for url in (work.url, work.landing_url, work.openalex_landing_url, work.openalex_pdf_url, *work.oa_urls) if url
            ):
                continue
            # Include EMBO articles hosted by SpringerLink; publisher labels alone
            # are too broad to choose a deterministic PDF route.
            if args.springer_webvpn and not key.startswith(("10.1007/", "10.1186/", "10.1057/") + SPRINGER_EMBO_PREFIXES):
                continue
            # Nature Portfolio DOI records use the 10.1038 registrant. Keep
            # broad or stale publisher labels from routing unrelated sites.
            if args.nature_vpn and not key.startswith("10.1038/"):
                continue
            if args.elsevier_webvpn and not key.startswith(("10.1016/", "10.1053/")):
                continue
            if args.acs_webvpn and not key.startswith("10.1021/"):
                continue
            if args.cambridge_webvpn and not key.startswith("10.1017/"):
                continue
            if args.cambridge_webvpn:
                non_article_reason = cambridge_non_article_reason(work)
                if non_article_reason:
                    print(f"[Cambridge non-article skip] {work.doi}: {work.title}")
                    append_manifest(
                        manifest,
                        {
                            "doi": work.doi,
                            "title": work.title,
                            "publisher": work.publisher,
                            "landing_url": work.url,
                            "status": "non_article",
                            "file": None,
                            "bytes": 0,
                            "engine": "cambridge-prefilter",
                            "error": non_article_reason,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    completed.add(key)
                    continue
            if args.acs_webvpn:
                non_article_reason = acs_non_article_reason(work)
                if non_article_reason:
                    print(f"[ACS non-article skip] {work.doi}: {work.title}")
                    append_manifest(
                        manifest,
                        {
                            "doi": work.doi,
                            "title": work.title,
                            "publisher": work.publisher,
                            "landing_url": work.url,
                            "status": "non_article",
                            "file": None,
                            "bytes": 0,
                            "engine": "acs-prefilter",
                            "error": non_article_reason,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    completed.add(key)
                    continue
            if args.aps_webvpn and not key.startswith("10.1103/"):
                continue
            if args.aaa_webvpn and not key.startswith("10.2308/"):
                continue
            if args.cambridge_webvpn and not key.startswith("10.1017/"):
                continue
            if args.rsc_webvpn and not key.startswith("10.1039/"):
                continue
            platform = campus_platform(work)
            if only_platforms and platform not in only_platforms:
                continue
            if platform in excluded_platforms:
                continue
            if key in completed or key in seen_dois:
                continue
            # Filter out non-article placeholder titles (e.g. Blank page, Front Cover)
            if is_placeholder_work(work):
                append_manifest(
                    manifest,
                    {
                        "doi": work.doi,
                        "title": work.title,
                        "publisher": work.publisher,
                        "landing_url": work.url,
                        "status": "non_article",
                        "file": None,
                        "bytes": 0,
                        "engine": "prefilter-placeholder-title",
                        "error": f"Placeholder title skipped: {work.title}",
                        "recorded_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                completed.add(key)
                continue
            if looks_like_pdf(args.output / safe_filename(work)):
                completed.add(key)
                continue
            seen_dois.add(key)
            yield work

    pending = iter(pending_rows())

    runner = runner_prefix(root, args.runner)
    total_ok = total_failed = total_not_oa = total_metadata_failed = 0
    ingest_engine = (
        "ieee-session-http" if args.session_http else "concurrent-direct-http"
    )
    if args.parallel_safe:
        ingested = ingest_existing_staging(
            direct_output,
            args.output,
            manifest,
            completed,
            True,
            ingest_engine,
            args.publish_every,
        )
        if ingested:
            total_ok += ingested
            print(
                f"Ingested {ingested} already-staged PDF(s) into {args.output}"
            )
    selected_oa = 0
    exhausted = False
    metadata_batch_size = min(
        200, max(args.batch_size, args.metadata_workers * 4)
    )
    if args.skip_oa_metadata or args.ieee_webvpn:
        metadata_batch_size = max(args.batch_size, metadata_batch_size)
    while True:
        candidates: list[Work] = []
        while len(candidates) < metadata_batch_size:
            try:
                candidates.append(next(pending))
            except StopIteration:
                exhausted = True
                break
        if not candidates:
            break

        if (
            args.ieee_webvpn
            or args.springer_webvpn
            or args.nature_vpn
            or args.elsevier_webvpn
            or args.acs_webvpn
            or args.aps_webvpn
            or args.aaa_webvpn
            or args.cambridge_webvpn
            or args.rsc_webvpn
        ):
            if args.ieee_webvpn:
                platform_label = "BUAA WebVPN IEEE"
            elif args.springer_webvpn:
                platform_label = "BUAA WebVPN Springer"
            elif args.elsevier_webvpn:
                platform_label = "BUAA WebVPN Elsevier/Cell"
            elif args.acs_webvpn:
                platform_label = "BUAA WebVPN ACS"
            elif args.aps_webvpn:
                platform_label = "BUAA WebVPN APS"
            elif args.aaa_webvpn:
                platform_label = "BUAA WebVPN AAA"
            elif args.cambridge_webvpn:
                platform_label = "BUAA WebVPN Cambridge"
            elif args.rsc_webvpn:
                platform_label = "BUAA WebVPN RSC"
            else:
                platform_label = "BUAA WebVPN Nature" if args.nature_route == "buaa" else "Fudan EasyConnect Nature"
            print(
                f"Preparing {platform_label} batch: {len(candidates)} DOI(s); "
                "skipping unnecessary OpenAlex validation"
            )
            metadata_results = [
                (
                    candidate,
                    {
                        "status": "not_oa",
                        "title": candidate.title,
                        "publisher": candidate.publisher,
                        "landing_url": candidate.openalex_landing_url or candidate.landing_url or candidate.url,
                    },
                )
                for candidate in candidates
            ]
        elif args.skip_oa_metadata:
            print(
                f"Using CSV OA flag and local OpenAlex caches for "
                f"{len(candidates)} DOI(s); skipping live metadata APIs"
            )
            metadata_results = [
                (candidate, metadata_from_cache_or_csv(candidate, oa_cache))
                for candidate in candidates
            ]
        else:
            print(
                f"Validating {'campus access' if args.campus_vpn else 'OA'} metadata: "
                f"{len(candidates)} candidate(s), "
                f"openalex_batch={OPENALEX_BATCH_SIZE}, "
                f"unpaywall={'off' if (args.skip_unpaywall or (args.campus_vpn and args.oa_mode == 'non_oa')) else 'gap-fill'}"
            )
            metadata_results = resolve_metadata_batch(
                candidates,
                oa_cache,
                cache_path,
                args.metadata_workers,
                skip_unpaywall=args.skip_unpaywall
                or (args.campus_vpn and args.oa_mode == "non_oa"),
            )
        verified: list[Work] = []
        for candidate, metadata in metadata_results:
            if args.limit and selected_oa >= args.limit:
                break
            if metadata.get("status") == "metadata_failed":
                total_metadata_failed += 1
                record_non_oa(manifest, candidate, metadata)
                print(f"[metadata failed] {candidate.doi}: {metadata.get('error')}")
                continue
            resolved = resolved_work(candidate, metadata, args.campus_vpn)
            if resolved is None:
                total_not_oa += 1
                record_non_oa(manifest, candidate, metadata)
                print(
                    f"[skip {metadata.get('status')}] {candidate.doi}: "
                    f"{metadata.get('title') or candidate.title}"
                )
                continue
            verified.append(resolved)
            selected_oa += 1

        if verified and args.campus_vpn:
            verified, public_results = public_direct_first(verified, args.state_dir.parent, args.output)
            for result in public_results:
                append_manifest(manifest, {**result, 'status': 'downloaded',
                    'engine': 'public-direct-first', 'recorded_at': datetime.now(timezone.utc).isoformat()})
                completed.add(result['doi'].lower())
            total_ok += sum(result['status'] == 'downloaded' for result in public_results)
            print(f"Public direct first: available={len(public_results)}, browser/session fallback={len(verified)}")

        if verified:
            if args.ieee_webvpn:
                print(
                    f"Resolving IEEE article numbers for BUAA WebVPN: "
                    f"{len(verified)} DOI(s), workers={args.download_workers}"
                )
                routed_works: list[Work] = []
                for original, routed, error in resolve_ieee_webvpn_batch(
                    verified, args.download_workers, session_headers
                ):
                    if routed is not None:
                        routed_works.append(routed)
                    else:
                        total_failed += 1
                        print(f"[IEEE resolve failed] {original.doi}: {error}")
                        append_manifest(
                            manifest,
                            {
                                "doi": original.doi,
                                "title": original.title,
                                "publisher": original.publisher,
                                "landing_url": original.landing_url or original.url,
                                "status": "resolve_failed",
                                "file": None,
                                "bytes": 0,
                                "engine": "ieee-webvpn-resolver",
                                "error": error,
                                "recorded_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                verified = routed_works
            elif args.springer_webvpn:
                print(
                    f"Routing {len(verified)} Springer DOI(s) through BUAA WebVPN"
                )
                try:
                    from platforms.springer_doi import resolve_aliases
                except ImportError:
                    from examples.academic.platforms.springer_doi import resolve_aliases
                routed = []
                canonical_seen = set()
                for work, error in resolve_aliases(verified, args.state_dir / "springer_doi_aliases.jsonl"):
                    if error:
                        append_manifest(manifest, {"doi": work.doi, "status": "resolve_failed",
                            "error": f"Springer DOI resolution failed: {error}",
                            "recorded_at": datetime.now(timezone.utc).isoformat()})
                        total_failed += 1
                        continue
                    key = work.doi.lower()
                    if key in canonical_seen or key in completed or key in deferred or looks_like_pdf(args.output / doi_filename(work.doi)):
                        continue
                    canonical_seen.add(key)
                    routed.append(resolve_springer_webvpn_work(work))
                verified = routed
            elif args.nature_vpn:
                print(
                    f"Routing {len(verified)} Nature DOI(s) via {args.nature_route}"
                )
                verified = [resolve_nature_vpn_work(work, args.nature_route) for work in verified]
            elif args.elsevier_webvpn:
                print(
                    f"Resolving Elsevier PII values for BUAA WebVPN: "
                    f"{len(verified)} DOI(s), workers={args.download_workers}"
                )
                routed_works = []
                for original, routed, error in resolve_elsevier_webvpn_batch(
                    verified, args.download_workers, session_headers
                ):
                    if routed is not None:
                        routed_works.append(routed)
                    else:
                        total_failed += 1
                        print(f"[Elsevier resolve failed] {original.doi}: {error}")
                        append_manifest(
                            manifest,
                            {
                                "doi": original.doi,
                                "title": original.title,
                                "publisher": original.publisher,
                                "landing_url": original.landing_url or original.url,
                                "status": "resolve_failed",
                                "file": None,
                                "bytes": 0,
                                "engine": "elsevier-webvpn-resolver",
                                "error": error,
                                "recorded_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                verified = routed_works
            elif args.acs_webvpn:
                print(f"Routing {len(verified)} ACS DOI(s) through BUAA WebVPN")
                verified = [resolve_acs_webvpn_work(work) for work in verified]
            elif args.aps_webvpn:
                print(
                    f"Resolving canonical APS article routes for BUAA WebVPN: "
                    f"{len(verified)} DOI(s), workers={args.download_workers}"
                )
                routed_works = []
                for original, routed, error in resolve_aps_webvpn_batch(
                    verified, args.download_workers, session_headers
                ):
                    if routed is not None:
                        routed_works.append(routed)
                    else:
                        total_failed += 1
                        print(f"[APS resolve failed] {original.doi}: {error}")
                        append_manifest(
                            manifest,
                            {
                                "doi": original.doi,
                                "title": original.title,
                                "publisher": original.publisher,
                                "landing_url": original.landing_url or original.url,
                                "status": "resolve_failed",
                                "file": None,
                                "bytes": 0,
                                "engine": "aps-webvpn-resolver",
                                "error": error,
                                "recorded_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                verified = routed_works
            elif args.aaa_webvpn:
                print(
                    f"Resolving {len(verified)} AAA article pages for BUAA WebVPN, "
                    f"workers={args.download_workers}"
                )
                routed_works = []
                for original, routed, error in resolve_aaa_webvpn_batch(
                    verified, args.download_workers, session_headers
                ):
                    if routed is not None:
                        routed_works.append(routed)
                    else:
                        total_failed += 1
                        print(f"[AAA resolve failed] {original.doi}: {error}")
                        append_manifest(
                            manifest,
                            {
                                "doi": original.doi,
                                "title": original.title,
                                "publisher": original.publisher,
                                "landing_url": original.landing_url or original.url,
                                "status": "resolve_failed",
                                "file": None,
                                "bytes": 0,
                                "engine": "aaa-webvpn-resolver",
                                "error": error,
                                "recorded_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                verified = routed_works
            elif args.cambridge_webvpn:
                print(
                    f"Routing {len(verified)} Cambridge DOI(s) through BUAA WebVPN"
                )
                verified = [
                    resolve_cambridge_webvpn_work(work) for work in verified
                ]
            elif args.rsc_webvpn:
                print(f"Routing {len(verified)} RSC DOI(s) through BUAA WebVPN")
                verified = [resolve_rsc_webvpn_work(work) for work in verified]

            if args.browser_only:
                print(
                    f"Browser-only phase: {len(verified)} DOI(s); "
                    "skipping unauthenticated direct HTTP"
                )
                browser_fallback = list(verified)
            else:
                extra_headers = session_headers
                ssl_context = session_ssl
                if args.session_http and chrome_session is not None:
                    extra_headers = {
                        "Cookie": chrome_session.cookie_header_for("d.buaa.edu.cn")
                    }
                    print(
                        f"IEEE session HTTP download: {len(verified)} DOI(s), "
                        f"workers={args.download_workers}, per_host={args.per_host}, "
                        f"publish_every={args.publish_every or 'batch'}"
                    )
                else:
                    print(
                        f"Concurrent direct download: {len(verified)} DOI(s), "
                        f"workers={args.download_workers}"
                    )
                engine = (
                    "ieee-session-http"
                    if args.session_http
                    else "concurrent-direct-http"
                )
                pending_ok: list[tuple[Work, Path]] = []
                browser_fallback: list[Work] = []
                login_expired: list[Work] = []
                direct_ok = 0
                skip_label = (
                    "direct skip"
                    if args.direct_only or args.session_http
                    else "browser fallback"
                )
                host_interval = 1.5 if args.session_http else 0.0

                def consume_direct_result(
                    work: Work, path: Path | None, error: str | None
                ) -> None:
                    nonlocal direct_ok, total_ok
                    if error and error.startswith('non_article:'):
                        append_manifest(manifest, {'doi':work.doi, 'title':work.title, 'status':'non_article', 'file':None, 'error':error, 'recorded_at':datetime.now(timezone.utc).isoformat()})
                        return
                    if path is not None:
                        pending_ok.append((work, path))
                        published = flush_direct_successes(
                            pending_ok,
                            args.output,
                            manifest,
                            args.parallel_safe,
                            engine,
                            args.publish_every,
                            completed,
                            force=False,
                        )
                        if published:
                            direct_ok += published
                            total_ok += published
                            print(
                                f"Published {published} PDF(s) to {args.output} "
                                f"(batch total {direct_ok})"
                            )
                        return
                    if (
                        args.session_http
                        and error
                        and "login_expired" in error
                    ):
                        login_expired.append(work)
                        return
                    browser_fallback.append(
                        Work(
                            doi=work.doi,
                            title=work.title,
                            publisher=work.publisher,
                            url=work.url,
                            oa_urls=work.oa_urls,
                            landing_url=work.landing_url,
                        )
                    )
                    print(f"[{skip_label}] {work.doi}: {error}")

                for work, path, error in direct_download_iter(
                    verified,
                    direct_output,
                    args.download_workers,
                    args.per_host,
                    args.direct_timeout,
                    args.direct_attempts,
                    extra_headers=extra_headers,
                    ssl_context=ssl_context,
                    min_interval=host_interval,
                ):
                    consume_direct_result(work, path, error)

                if login_expired and chrome_session is not None:
                    print(
                        f"Refreshing Chrome cookies and retrying "
                        f"{len(login_expired)} login-expired IEEE DOI(s)"
                    )
                    chrome_session.ensure_session(
                        f"{BUAA_IEEE_WEBVPN_PREFIX}/Xplore/home.jsp",
                        timeout=60.0,
                    )
                    retry_headers = {
                        "Cookie": chrome_session.cookie_header_for("d.buaa.edu.cn")
                    }
                    still_expired: list[Work] = []
                    current_expired = login_expired
                    login_expired = still_expired
                    for work, path, error in direct_download_iter(
                        current_expired,
                        direct_output,
                        args.download_workers,
                        args.per_host,
                        args.direct_timeout,
                        args.direct_attempts,
                        extra_headers=retry_headers,
                        ssl_context=ssl_context,
                        min_interval=host_interval,
                    ):
                        consume_direct_result(work, path, error)
                    browser_fallback.extend(login_expired)
                    for work in login_expired:
                        print(f"[{skip_label}] {work.doi}: login_expired after retry")

                leftover = flush_direct_successes(
                    pending_ok,
                    args.output,
                    manifest,
                    args.parallel_safe,
                    engine,
                    args.publish_every,
                    completed,
                    force=True,
                )
                if leftover:
                    direct_ok += leftover
                    total_ok += leftover
                    print(
                        f"Published leftover {leftover} PDF(s) to {args.output}"
                    )
                print(
                    f"Direct phase finished: downloaded={direct_ok}, "
                    f"{'direct_failed' if args.direct_only else 'browser_fallback'}="
                    f"{len(browser_fallback)}"
                )

            if args.direct_only or args.session_http:
                now = datetime.now(timezone.utc).isoformat()
                engine = (
                    "ieee-session-http" if args.session_http else "concurrent-direct-http"
                )
                for work in browser_fallback:
                    append_manifest(
                        manifest,
                        {
                            "doi": work.doi,
                            "title": work.title,
                            "publisher": work.publisher,
                            "landing_url": work.landing_url or work.url,
                            "status": "direct_failed",
                            "file": None,
                            "bytes": 0,
                            "engine": engine,
                            "error": "all direct PDF candidates failed",
                            "recorded_at": now,
                        },
                    )
                total_failed += len(browser_fallback)
                browser_fallback = []

            browser_fallback.sort(
                key=lambda work: (
                    urllib.parse.urlparse(work.landing_url or work.url).hostname
                    or ""
                ).lower()
            )
            for browser_batch in browser_batches(browser_fallback, args.batch_size):
                if not browser_batch:
                    continue
                browser_stage_root = args.state_dir / "browser_batches"
                browser_stage_root.mkdir(parents=True, exist_ok=True)
                with nullcontext(tempfile.mkdtemp(
                    prefix="batch_", dir=browser_stage_root
                )) as browser_stage:
                    artifact_dir = Path(browser_stage)
                    returncode = run_batch(
                        root, workflow, runner, artifact_dir, browser_batch
                    )
                    ok, failed = record_batch(
                        manifest,
                        artifact_dir,
                        args.output,
                        browser_batch,
                        returncode,
                    )
                details_path = artifact_dir / "workflow_failures.json"
                if details_path.exists() and "RATE_LIMITED:" in details_path.read_text():
                    from platforms.runtime_state import trip_cooldown
                    trip_cooldown(args.input, runtime_platform, 1800,
                                  "站点明确限流；停止连续请求，至少冷却 30 分钟后再试")
                    print("Publisher rate limit confirmed; stopped further batches for cooldown")
                    return
                if not failed and not returncode and not any(artifact_dir.glob("*.access.json")):
                    shutil.rmtree(artifact_dir, ignore_errors=True)
                else:
                    print(f"Failure artifacts retained: {artifact_dir}")
                total_ok += ok
                total_failed += failed
                print(f"Browser batch finished: downloaded={ok}, failed={failed}")
                terminal_dois = load_manifest_dois_by_status(manifest, {"no_pdf_available"})
                terminal_only = all(w.doi.lower() in terminal_dois for w in browser_batch)
                if returncode or (failed and not ok and not terminal_only):
                    runtime.state = "running"
                    runtime.reason = (
                        f"Browser batch produced {ok} valid PDFs, {failed} failures; "
                        f"workflow exit={returncode}. Continuing with the next batch."
                    )
                    runtime.recent_attempts = total_ok + total_failed
                    runtime.recent_successes = total_ok
                    save_runtime_state(args.input, runtime)
                    print(runtime.reason)

        if exhausted or (args.limit and selected_oa >= args.limit):
            break

    print(
        f"Finished: verified_{'campus' if args.campus_vpn else 'oa'}={selected_oa}, downloaded={total_ok}, "
        f"browser_failed={total_failed}, skipped_not_oa={total_not_oa}, "
        f"metadata_failed={total_metadata_failed}, "
        f"previously_completed={len(completed)}, output={args.output}"
    )
    runtime.state = "failed" if total_failed and not total_ok else "stopped"
    runtime.reason = f"downloaded={total_ok}, failed={total_failed}"
    runtime.processed_records = selected_oa
    runtime.recent_attempts = total_ok + total_failed
    runtime.recent_successes = total_ok
    runtime.last_success_at = datetime.now(timezone.utc).isoformat() if total_ok else None
    save_runtime_state(args.input, runtime)
    if total_failed and not total_ok:
        raise SystemExit("No valid PDF downloaded; see manifest failure details")


if __name__ == "__main__":
    main()
