#!/usr/bin/env python3
"""Download OA and subscribed Nature PDFs using campus VPN and Chrome."""

from __future__ import annotations

import sys
import argparse
import os

from doi_oa_hybrid_downloader import main
from platforms.registry import get_platform_spec


def add_default(name: str, *values: str) -> None:
    if name not in sys.argv[1:]:
        sys.argv.extend((name, *values))


if __name__ == "__main__":
    spec = get_platform_spec('nature')
    default_route = 'buaa' if spec and spec.access_mode == 'buaa_webvpn' else 'fudan'
    route_parser = argparse.ArgumentParser(add_help=False)
    route_parser.add_argument('--nature-route', choices=['fudan', 'buaa'], default=default_route)
    buaa = route_parser.parse_known_args()[0].nature_route == 'buaa'
    add_default("--oa-mode", "all")
    add_default("--campus-vpn")
    add_default("--nature-vpn")
    add_default("--nature-route", "buaa" if buaa else "fudan")
    add_default("--state-prefix", spec.state_prefix if spec else ("nature_buaa_vpn" if buaa else "nature_fudan_vpn"))
    add_default("--parallel-safe")
    add_default("--only-campus-platform", "nature")
    add_default("--metadata-workers", "16")
    add_default("--download-workers", "12")
    add_default("--batch-size", "10")
    add_default("--per-host", "1")
    add_default("--direct-timeout", "15")
    add_default("--direct-attempts", "1")
    add_default(
        "--workflow",
        "examples/templates/doi-nature-buaa-browser-downloader.yaml" if buaa else "examples/templates/doi-nature-fudan-browser-downloader.yaml",
    )
    # Show the site before CSV/metadata work, even when no candidate is found.
    if os.environ.get("DRISSION_SHARED_BROWSER_ENDPOINT") and "--help" not in sys.argv:
        from platforms.chrome_session import shared_debug_port, open_start_page
        port = shared_debug_port()
        if not port:
            raise RuntimeError("Nature shared browser is unavailable")
        from platforms.webvpn import webvpn_https
        open_start_page(port, webvpn_https('www.nature.com') if buaa else "https://www.nature.com/", clean_empty_tabs=True)
    main()
