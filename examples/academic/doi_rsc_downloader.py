#!/usr/bin/env python3
"""RSC DOI downloader through BUAA WebVPN with completion watcher."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from doi_oa_hybrid_downloader import main


def add_default(name: str, *values: str) -> None:
    if name not in sys.argv[1:]:
        sys.argv.extend((name, *values))


if __name__ == "__main__":
    add_default("--oa-mode", "all")
    add_default("--campus-vpn")
    add_default("--rsc-webvpn")
    add_default("--state-prefix", "rsc_buaa_webvpn")
    add_default("--parallel-safe")
    add_default("--only-campus-platform", "rsc")
    add_default("--metadata-workers", "8")
    add_default("--download-workers", "4")
    add_default("--batch-size", "5")
    add_default("--per-host", "1")
    add_default("--direct-timeout", "30")
    add_default("--direct-attempts", "1")
    add_default(
        "--workflow",
        "examples/templates/doi-rsc-browser-downloader.yaml",
    )
    main()
