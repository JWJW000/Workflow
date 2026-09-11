"""Browser download completion watcher.

Monitors staging directory for browser downloads:
- Waits for temporary extensions (.crdownload, .tmp) to disappear
- Verifies file size stability across 3 consecutive checks (each 2s apart)
- Provides timeout protection (default 180s)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set


@dataclass
class DownloadWatchResult:
    completed: bool
    final_file: Optional[Path] = None
    initial_size: int = 0
    final_size: int = 0
    elapsed_seconds: float = 0.0
    error: str = ""


def wait_for_browser_download(
    download_dir: Path,
    expected_filename: Optional[str] = None,
    timeout_seconds: float = 180.0,
    check_interval: float = 2.0,
    stability_checks: int = 3,
    existing_files: Optional[Set[str]] = None,
) -> DownloadWatchResult:
    """Watch download_dir for a browser download to finish.

    A download is finished when:
    1. No .crdownload or .tmp files remain for the target file.
    2. File exists and has size > 0.
    3. File size is unchanged for `stability_checks` consecutive checks spaced by `check_interval`.
    """
    start_time = time.monotonic()
    initial_existing = existing_files if existing_files is not None else {p.name for p in download_dir.glob("*")}

    last_size = -1
    consecutive_stable = 0
    candidate: Optional[Path] = None

    while True:
        now = time.monotonic()
        elapsed = now - start_time
        if elapsed > timeout_seconds:
            return DownloadWatchResult(
                completed=False,
                final_file=candidate,
                final_size=last_size if last_size > 0 else 0,
                elapsed_seconds=elapsed,
                error=f"Timeout of {timeout_seconds}s exceeded",
            )

        # Inspect current files in download directory
        all_current = [p for p in download_dir.glob("*") if not p.name.startswith("._")]

        # Look for target or new files
        target_path: Optional[Path] = None
        if expected_filename:
            target = download_dir / expected_filename
            crdownload_target = download_dir / f"{expected_filename}.crdownload"
            if crdownload_target.exists():
                # Still downloading
                consecutive_stable = 0
                time.sleep(check_interval)
                continue
            if target.is_file():
                target_path = target
        else:
            # Look for any newly created file that is not a temporary download
            new_files = [p for p in all_current if p.name not in initial_existing]
            crdownloads = [p for p in new_files if p.name.endswith(".crdownload") or p.name.endswith(".tmp")]
            if crdownloads:
                consecutive_stable = 0
                time.sleep(check_interval)
                continue
            pdfs = [p for p in new_files if p.suffix.lower() == ".pdf" and not p.name.endswith(".crdownload")]
            if pdfs:
                # Pick the latest modified
                target_path = max(pdfs, key=lambda p: p.stat().st_mtime)

        if target_path and target_path.is_file():
            candidate = target_path
            current_size = target_path.stat().st_size
            if current_size > 0 and current_size == last_size:
                consecutive_stable += 1
                if consecutive_stable >= stability_checks:
                    return DownloadWatchResult(
                        completed=True,
                        final_file=target_path,
                        initial_size=current_size,
                        final_size=current_size,
                        elapsed_seconds=time.monotonic() - start_time,
                    )
            else:
                consecutive_stable = 1
                last_size = current_size
        else:
            consecutive_stable = 0

        time.sleep(check_interval)
