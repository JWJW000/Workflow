"""Open Access repository fast-lane adapter.

Uses direct HTTP stream downloading for works with known OA PDF URLs.
Strictly isolated from Campus WebVPN/proxy queues.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

from .base import FailureClass, PlatformAdapter, ResolvedWork, Work
from .routing import normalize_doi


class OARepositoryAdapter:
    id: str = "oa_repository"

    def matches(self, work: Work) -> bool:
        # Matches if work is flagged as OA and has an explicit direct PDF URL
        return bool(work.is_oa and work.openalex_pdf_url and work.openalex_pdf_url.startswith("http"))

    def resolve(self, work: Work) -> ResolvedWork:
        doi = normalize_doi(work.doi)
        pdf_url = work.openalex_pdf_url
        landing_url = work.openalex_landing_url or f"https://doi.org/{doi}"

        return ResolvedWork(
            doi=doi,
            platform=self.id,
            landing_url=landing_url,
            pdf_url=pdf_url,
            access_route="direct_http",
        )

    def classify_page(self, url: str, title: str, html: str) -> Optional[str]:
        content = f"{url} {title} {html}".lower()
        if "403 forbidden" in content or "access denied" in content:
            return FailureClass.ACCESS_DENIED.value
        if "429 too many requests" in content or "rate limit" in content:
            return FailureClass.RATE_LIMITED.value
        if "<!doctype html" in content or "<html" in content:
            return FailureClass.HTML_PLACEHOLDER.value
        return None
