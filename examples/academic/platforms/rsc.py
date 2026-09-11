"""RSC platform adapter.

Matches 10.1039 DOIs.
Constructs canonical landing URL:
https://pubs.rsc.org/en/content/articlelanding/{year}/{letter}/{id}
or fallback https://doi.org/10.1039/{id}
"""

from __future__ import annotations

import re
from typing import Optional

from .base import FailureClass, PlatformAdapter, ResolvedWork, Work
from .routing import normalize_doi
from .webvpn import webvpn_https

BUAA_RSC_WEBVPN_PREFIX = webvpn_https("pubs.rsc.org", "/").rstrip("/")


class RSCAdapter:
    id: str = "rsc"

    def matches(self, work: Work) -> bool:
        doi = normalize_doi(work.doi)
        return doi.startswith("10.1039/")

    def article_paths(self, doi: str) -> tuple[str, str]:
        """Return (landing_path, pdf_path) under pubs.rsc.org.

        Typical DOI 10.1039/d3mh01692g maps to
        /en/content/articlelanding/2023/mh/d3mh01692g
        """
        suffix = normalize_doi(doi).split("/", 1)[1] if "/" in normalize_doi(doi) else normalize_doi(doi)
        match = re.match(r"^([cd])(\d)([a-z]{2})([0-9a-z]+)$", suffix.lower())
        if match:
            era, year_digit, journal, _rest = match.groups()
            year = (2020 if era == "d" else 2010) + int(year_digit)
            landing = f"/en/content/articlelanding/{year}/{journal}/{suffix}"
            pdf = f"/en/content/articlepdf/{year}/{journal}/{suffix}"
            return landing, pdf
        return (
            f"/en/content/articlelanding/{suffix}",
            f"/en/content/articlepdf/{suffix}",
        )

    def resolve(self, work: Work) -> ResolvedWork:
        doi = normalize_doi(work.doi)
        landing_path, pdf_path = self.article_paths(doi)
        landing_url = webvpn_https("pubs.rsc.org", landing_path)
        pdf_url = webvpn_https("pubs.rsc.org", pdf_path)

        return ResolvedWork(
            doi=doi,
            platform=self.id,
            landing_url=landing_url,
            pdf_url=pdf_url,
            access_route="buaa_webvpn",
        )

    def classify_page(self, url: str, title: str, html: str) -> Optional[str]:
        content = f"{url} {title} {html}".lower()
        if "rate limit" in content or "429" in content:
            return FailureClass.RATE_LIMITED.value
        if "cf-turnstile" in content or "recaptcha" in content or "challenge-platform" in content:
            return FailureClass.CAPTCHA.value
        if "access denied" in content or "403 forbidden" in content:
            return FailureClass.ACCESS_DENIED.value
        if "buy this article" in content or "purchase this article" in content:
            return FailureClass.SUBSCRIPTION_MISSING.value
        if "login" in content and "sign in" in content:
            return FailureClass.LOGIN_EXPIRED.value
        return None
