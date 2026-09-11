"""Tianjin University Resource Access 2 Audit Adapter.

Audits 15 key platforms:
ASCO/JCO, ASH/Blood, Neurology, LWW/Ovid, ACP, RSNA, BMJ, OUP,
AACR, AHA, JAMA, ERS, APA, ASN, JBJS.

Samples 3 works per platform (1 OA, 2 non-OA).
Auditor records access, institution identification, entitlements, PDF trigger,
elapsed duration, and screenshots.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .base import FailureClass, PlatformAdapter, ResolvedWork, Work
from .routing import normalize_doi

TJU_AUDIT_PLATFORMS: List[Tuple[str, str, str, str]] = [
    ("asco", "ASCO / JCO", "10.1200", "American Society of Clinical Oncology"),
    ("ash", "ASH / Blood", "10.1182", "American Society of Hematology"),
    ("neurology", "Neurology", "10.1212", "American Academy of Neurology"),
    ("lww_ovid", "LWW / Ovid", "10.14309", "Wolters Kluwer"),
    ("acp", "ACP", "10.7326", "American College of Physicians"),
    ("rsna", "RSNA", "10.1148", "Radiological Society of North America"),
    ("bmj", "BMJ", "10.1136", "BMJ"),
    ("oup", "OUP", "10.1093", "Oxford University Press"),
    ("aacr", "AACR", "10.1158", "American Association for Cancer Research"),
    ("aha", "AHA", "10.1161", "American Heart Association"),
    ("jama", "JAMA", "10.1001", "American Medical Association"),
    ("ers", "ERS", "10.1183", "European Respiratory Society"),
    ("apa", "APA", "10.1037", "American Psychological Association"),
    ("asn", "ASN", "10.1681", "American Society of Nephrology"),
    ("jbjs", "JBJS", "10.2106", "The Journal of Bone and Joint Surgery"),
]


class TJUAuditAdapter:
    id: str = "tju_audit"

    def matches(self, work: Work) -> bool:
        doi = normalize_doi(work.doi)
        return any(doi.startswith(prefix + "/") for _, _, prefix, _ in TJU_AUDIT_PLATFORMS)

    def resolve(self, work: Work) -> ResolvedWork:
        doi = normalize_doi(work.doi)
        landing_url = f"https://doi.org/{doi}"
        return ResolvedWork(
            doi=doi,
            platform=self.id,
            landing_url=landing_url,
            pdf_url=None,
            access_route="tju_resource_2",
        )

    def classify_page(self, url: str, title: str, html: str) -> Optional[str]:
        content = f"{url} {title} {html}".lower()
        if "429" in content or "rate limit" in content or "error 1015" in content:
            return FailureClass.RATE_LIMITED.value
        if "captcha" in content or "cf-turnstile" in content or "challenge-platform" in content:
            return FailureClass.CAPTCHA.value
        if "access denied" in content or "403 forbidden" in content or "cpe00001" in content:
            return FailureClass.ACCESS_DENIED.value
        if "purchase instant access" in content or "subscribe to this journal" in content:
            return FailureClass.SUBSCRIPTION_MISSING.value
        if "login required" in content or "sign in via shibboleth" in content:
            return FailureClass.LOGIN_EXPIRED.value
        return None
