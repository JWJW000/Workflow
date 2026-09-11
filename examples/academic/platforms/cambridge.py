"""Cambridge Core platform adapter.

Matches 10.1017 DOIs.
Constructs canonical landing URL:
https://www.cambridge.org/core/product/identifier/{S-number}/type/journal_article
or falls back to DOI landing page.
"""

from __future__ import annotations

import re
from typing import Optional

from urllib.parse import quote

from .base import FailureClass, PlatformAdapter, ResolvedWork, Work
from .routing import normalize_doi
from .webvpn import webvpn_https

BUAA_CAMBRIDGE_CORE_WEBVPN_PREFIX = webvpn_https("www.cambridge.org", "/").rstrip("/")


class CambridgeAdapter:
    id: str = "cambridge"

    def matches(self, work: Work) -> bool:
        doi = normalize_doi(work.doi)
        if not doi.startswith("10.1017/"):
            return False
        return True

    def extract_s_number(self, doi: str) -> Optional[str]:
        """Extract S-number (e.g. S0033291722003750) from DOI if present."""
        clean = normalize_doi(doi)
        match = re.search(r"/(s[0-9x]{16,17})", clean, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        match2 = re.search(r"/(s[0-9a-z]{10,20})", clean, re.IGNORECASE)
        if match2:
            return match2.group(1).upper()
        return None

    def resolve(self, work: Work) -> ResolvedWork:
        doi = normalize_doi(work.doi)
        s_num = self.extract_s_number(doi)
        if s_num:
            webvpn_landing = webvpn_https(
                "www.cambridge.org",
                f"/core/product/identifier/{s_num}/type/journal_article",
            )
        else:
            webvpn_landing = webvpn_https(
                "www.cambridge.org",
                "/core/search",
                query=f"q={quote(doi, safe='')}",
            )

        return ResolvedWork(
            doi=doi,
            platform=self.id,
            landing_url=webvpn_landing,
            pdf_url=None,
            access_route="buaa_webvpn",
        )

    def classify_page(self, url: str, title: str, html: str) -> Optional[str]:
        content = f"{url} {title} {html}".lower()
        if "rate limit" in content or "429 too many requests" in content:
            return FailureClass.RATE_LIMITED.value
        if (
            "recaptcha" in content
            or "cf-turnstile" in content
            or "challenge-platform" in content
            or "正在进行安全验证" in content
            or "浏览器不支持" in content
        ):
            return FailureClass.CAPTCHA.value
        if "access denied" in content or "403 forbidden" in content:
            return FailureClass.ACCESS_DENIED.value
        if "purchase access" in content or "buy this article" in content or "view subscription options" in content:
            return FailureClass.SUBSCRIPTION_MISSING.value
        if "login required" in content or "sign in via your institution" in content:
            return FailureClass.LOGIN_EXPIRED.value
        return None
