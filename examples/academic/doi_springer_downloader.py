#!/usr/bin/env python3
"""Download OA and non-OA SpringerLink PDFs through BUAA WebVPN."""

from __future__ import annotations

import sys

from doi_oa_hybrid_downloader import main


def add_default(name: str, *values: str) -> None:
    if name not in sys.argv[1:]:
        sys.argv.extend((name, *values))


if __name__ == "__main__":
    add_default("--oa-mode", "all")
    add_default("--campus-vpn")
    add_default("--springer-webvpn")
    add_default("--state-prefix", "springer_buaa_webvpn")
    add_default("--parallel-safe")
    add_default("--only-campus-platform", "springer")
    add_default("--metadata-workers", "16")
    add_default("--download-workers", "12")
    # Recycle the real browser regularly.  Very large WebVPN batches retain
    # publisher renderers for too long and eventually produce Chrome's
    # "Aw, Snap / error code 5" page.
    add_default("--batch-size", "20")
    add_default("--per-host", "1")
    add_default("--direct-timeout", "15")
    add_default("--direct-attempts", "1")
    add_default(
        "--workflow",
        "examples/templates/doi-springer-browser-downloader.yaml",
    )
    main()
