#!/usr/bin/env python3
"""Download American Accounting Association PDFs through BUAA WebVPN."""

from __future__ import annotations

import sys

from doi_oa_hybrid_downloader import main


def add_default(name: str, *values: str) -> None:
    if name not in sys.argv[1:]:
        sys.argv.extend((name, *values))


if __name__ == "__main__":
    add_default("--oa-mode", "all")
    add_default("--aaa-webvpn")
    add_default("--state-prefix", "aaa_buaa_webvpn")
    add_default("--parallel-safe")
    add_default("--batch-size", "20")
    add_default("--metadata-workers", "16")
    add_default("--download-workers", "12")
    add_default("--per-host", "1")
    add_default("--workflow", "examples/templates/doi-aaa-browser-downloader.yaml")
    main()
