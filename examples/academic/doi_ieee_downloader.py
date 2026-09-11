#!/usr/bin/env python3
"""BUAA WebVPN IEEE Xplore downloader for DOI CSV datasets.

Chrome is kept on the logged-in campus profile only to supply session
cookies. PDF bytes are fetched over HTTP. The default mode still covers
both OA and non-OA IEEE records.
"""

from __future__ import annotations

import sys

from doi_oa_hybrid_downloader import main


def add_default(name: str, *values: str) -> None:
    if name not in sys.argv[1:]:
        sys.argv.extend((name, *values))


if __name__ == "__main__":
    add_default("--oa-mode", "all")
    add_default("--campus-vpn")
    add_default("--ieee-webvpn")
    add_default("--session-http")
    add_default("--state-prefix", "ieee_buaa_webvpn")
    add_default("--parallel-safe")
    add_default("--only-campus-platform", "ieee")
    add_default("--metadata-workers", "16")
    add_default("--download-workers", "8")
    add_default("--batch-size", "200")
    add_default("--publish-every", "20")
    add_default("--per-host", "2")
    add_default("--direct-timeout", "40")
    add_default("--direct-attempts", "2")
    add_default(
        "--workflow",
        "examples/templates/doi-ieee-browser-downloader.yaml",
    )
    main()
