"""Base data structures and protocol for academic platforms."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Protocol, runtime_checkable


class FailureClass(str, Enum):
    RATE_LIMITED = "rate_limited"
    CAPTCHA = "captcha"
    ACCESS_DENIED = "access_denied"
    SUBSCRIPTION_MISSING = "subscription_missing"
    LOGIN_EXPIRED = "login_expired"
    HTML_PLACEHOLDER = "html_placeholder"
    NON_ARTICLE = "non_article"
    NO_PDF_AVAILABLE = "no_pdf_available"
    DOWNLOAD_NOT_TRIGGERED = "download_not_triggered"
    DOWNLOAD_TIMEOUT = "download_timeout"
    NAVIGATION_FAILED = "navigation_failed"
    CONTENT_MISMATCH = "content_mismatch"
    CORRUPTED_PDF = "corrupted_pdf"
    TRUNCATED_PDF = "truncated_pdf"
    SIZE_TOO_SMALL = "size_too_small"


@dataclass(frozen=True)
class Work:
    doi: str
    title: str = ""
    publisher: str = ""
    journal: str = ""
    issn: str = ""
    is_oa: bool = False
    url: str = ""
    oa_urls: tuple[str, ...] = ()
    landing_url: str = ""
    openalex_pdf_url: str = ""
    openalex_landing_url: str = ""
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PlatformSpec:
    id: str
    label: str
    access_mode: str
    profile_dir: str
    workflow: str
    state_prefix: str
    maturity: str          # production | calibrating | experimental | blocked
    enabled: bool
    batch_size: int
    delay_min_seconds: float
    delay_max_seconds: float
    cooldown_seconds: int
    note: str = ""
    priority: int = 100


@dataclass(frozen=True)
class ResolvedWork:
    doi: str
    platform: str
    landing_url: str
    pdf_url: Optional[str]
    access_route: str


@runtime_checkable
class PlatformAdapter(Protocol):
    id: str

    def matches(self, work: Work) -> bool:
        """Return True if this adapter handles the given work."""
        ...

    def resolve(self, work: Work) -> ResolvedWork:
        """Resolve work into a platform-specific landing/pdf URL and access route."""
        ...

    def classify_page(self, url: str, title: str, html: str) -> Optional[str]:
        """Classify page response. Returns a FailureClass value if blocked/error, else None."""
        ...
