"""Platforms package for DOI multi-campus academic downloaders."""

from __future__ import annotations

from .base import (
    FailureClass,
    PlatformAdapter,
    PlatformSpec,
    ResolvedWork,
    Work,
)
from .registry import load_registry, get_platform_spec
from .routing import resolve_platform_for_work
from .validation import validate_pdf, ValidationResult
from .download_watcher import wait_for_browser_download

__all__ = [
    "FailureClass",
    "PlatformAdapter",
    "PlatformSpec",
    "ResolvedWork",
    "Work",
    "load_registry",
    "get_platform_spec",
    "resolve_platform_for_work",
    "validate_pdf",
    "ValidationResult",
    "wait_for_browser_download",
]
