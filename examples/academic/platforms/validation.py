"""Validation module for downloaded PDFs and landing/error pages.

Strict enforcement of PDF structure, min size, EOF markers, text extraction,
and rejection of HTML error pages (CPE00001, Error 1015, Access Denied, getpdf.jsp).
"""

from __future__ import annotations

import os
import re
from html import unescape
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .base import FailureClass
from .content_filter import non_article_reason, pdf_notice_reason

MIN_PDF_BYTES = 16 * 1024  # 16 KiB
ERROR_SNIPPET_BYTES = 256 * 1024  # 256 KiB
TRAILER_EOF_BYTES = 8 * 1024  # 8 KiB


def text_reports_no_pdf_available(text: str) -> bool:
    """Recognize publisher-generated PDF containers that deny PDF availability."""
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    return any(
        marker in normalized
        for marker in (
            "sorry, we don't have this article in pdf format",
            "sorry, we don’t have this article in pdf format",
            "this article is not available in pdf format",
        )
    )

# Known error patterns that should NEVER pass as valid PDF
KNOWN_ERROR_MARKERS = [
    (re.compile(rb"CPE00001", re.IGNORECASE), FailureClass.ACCESS_DENIED),
    (re.compile(rb"Error\s+1015", re.IGNORECASE), FailureClass.RATE_LIMITED),
    (re.compile(rb"Access\s+Denied", re.IGNORECASE), FailureClass.ACCESS_DENIED),
    (re.compile(rb"getpdf\.jsp", re.IGNORECASE), FailureClass.HTML_PLACEHOLDER),
    (re.compile(rb"<!DOCTYPE\s+html", re.IGNORECASE), FailureClass.HTML_PLACEHOLDER),
    (re.compile(rb"<html[\s>]", re.IGNORECASE), FailureClass.HTML_PLACEHOLDER),
    (re.compile(rb"Cloudflare\s+Ray\s+ID", re.IGNORECASE), FailureClass.RATE_LIMITED),
    (re.compile(rb"cf-browser-verification", re.IGNORECASE), FailureClass.CAPTCHA),
    (re.compile(rb"cf-turnstile", re.IGNORECASE), FailureClass.CAPTCHA),
    (re.compile(rb"challenge-platform", re.IGNORECASE), FailureClass.CAPTCHA),
    (re.compile(rb"Please\s+verify\s+you\s+are\s+a\s+human", re.IGNORECASE), FailureClass.CAPTCHA),
    (re.compile(rb"Login\s+Required|Please\s+Sign\s+In", re.IGNORECASE), FailureClass.LOGIN_EXPIRED),
    (re.compile(rb"Purchase\s+Instant\s+Access|Subscribe\s+to\s+this\s+journal", re.IGNORECASE), FailureClass.SUBSCRIPTION_MISSING),
]


@dataclass
class ValidationResult:
    is_valid: bool
    size_bytes: int = 0
    pages: int = 0
    failure_class: Optional[FailureClass] = None
    error_message: str = ""
    content_match: str = "unverified"  # "matched", "unverified", "mismatched"


def sanitize_filename_doi(doi: str) -> str:
    """Normalize DOI to standard file stem: 10.1017/s0033291722003750 -> 10.1017_s0033291722003750.pdf"""
    clean = doi.strip()
    match = re.search(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", clean)
    if match:
        clean = match.group(0)
    clean = clean.lower().replace("/", "_")
    # Replace other forbidden filesystem characters
    clean = re.sub(r'[\:*?"<>|]', "_", clean)
    return f"{clean}.pdf"


def validate_pdf(
    file_path: Path,
    expected_doi: str = "",
    expected_title: str = "",
) -> ValidationResult:
    """Validate a downloaded PDF file according to the strict specification."""
    if not file_path.is_file():
        return ValidationResult(
            is_valid=False,
            failure_class=FailureClass.DOWNLOAD_TIMEOUT,
            error_message="File does not exist",
        )

    # Reject macOS metadata / resource fork files (._*)
    if file_path.name.startswith("._"):
        return ValidationResult(
            is_valid=False,
            failure_class=FailureClass.HTML_PLACEHOLDER,
            error_message="MacOS resource fork file rejected",
        )

    size = file_path.stat().st_size
    if size < MIN_PDF_BYTES:
        return ValidationResult(
            is_valid=False,
            size_bytes=size,
            failure_class=FailureClass.SIZE_TOO_SMALL,
            error_message=f"File size {size} bytes is under threshold {MIN_PDF_BYTES} bytes",
        )

    # Check for placeholder titles (e.g. Blank page, Back cover, Table of contents)
    if expected_title:
        title_lower = expected_title.lower().strip()
        if any(p in title_lower for p in ["blank page", "front cover", "back cover", "table of contents"]):
            return ValidationResult(
                is_valid=False,
                size_bytes=size,
                failure_class=FailureClass.HTML_PLACEHOLDER,
                error_message=f"Rejected placeholder title: '{expected_title}'",
            )

    notice = non_article_reason(expected_title)
    if notice:
        return ValidationResult(is_valid=False, size_bytes=size,
                                failure_class=FailureClass.NON_ARTICLE, error_message=notice)

    # Check header (%PDF-)
    try:
        with file_path.open("rb") as f:
            header = f.read(1024)
            if not header.startswith(b"%PDF-"):
                # Inspect for HTML error markers
                for pattern, failure in KNOWN_ERROR_MARKERS:
                    if pattern.search(header):
                        return ValidationResult(
                            is_valid=False,
                            size_bytes=size,
                            failure_class=failure,
                            error_message=f"Header contains error marker: {failure.value}",
                        )
                return ValidationResult(
                    is_valid=False,
                    size_bytes=size,
                    failure_class=FailureClass.CORRUPTED_PDF,
                    error_message="File does not start with %PDF- header",
                )

            # Check for error page markers in the first 256 KiB
            f.seek(0)
            prefix_data = f.read(ERROR_SNIPPET_BYTES)
            for pattern, failure in KNOWN_ERROR_MARKERS:
                if pattern.search(prefix_data):
                    return ValidationResult(
                        is_valid=False,
                        size_bytes=size,
                        failure_class=failure,
                        error_message=f"File contains error marker in first 256KiB: {failure.value}",
                    )

            # Check EOF marker in the last 8 KiB
            f.seek(max(0, size - TRAILER_EOF_BYTES))
            trailer = f.read(TRAILER_EOF_BYTES)
            if b"%%EOF" not in trailer:
                return ValidationResult(
                    is_valid=False,
                    size_bytes=size,
                    failure_class=FailureClass.TRUNCATED_PDF,
                    error_message="Missing %%EOF in last 8KiB of file",
                )
    except OSError as e:
        return ValidationResult(
            is_valid=False,
            size_bytes=size,
            failure_class=FailureClass.CORRUPTED_PDF,
            error_message=f"File read error: {e}",
        )

    # Attempt PDF parsing using pypdf
    num_pages = 0
    extracted_text = ""
    page_text = ""
    try:
        import pypdf

        reader = pypdf.PdfReader(str(file_path))
        num_pages = len(reader.pages)
        if num_pages < 1:
            return ValidationResult(
                is_valid=False,
                size_bytes=size,
                failure_class=FailureClass.CORRUPTED_PDF,
                error_message="PDF contains zero pages",
            )

        # Extract text from first page for content matching
        if reader.pages:
            try:
                page_text = reader.pages[0].extract_text() or ""
                extracted_text = page_text[:2000]
            except Exception:
                extracted_text = ""
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            size_bytes=size,
            failure_class=FailureClass.CORRUPTED_PDF,
            error_message=f"pypdf parsing failed: {e}",
        )

    if text_reports_no_pdf_available(extracted_text):
        return ValidationResult(
            is_valid=False,
            size_bytes=size,
            pages=num_pages,
            failure_class=FailureClass.NO_PDF_AVAILABLE,
            error_message="Publisher reports that this article has no PDF format",
            content_match="mismatched",
        )

    notice = pdf_notice_reason(extracted_text)
    if notice:
        return ValidationResult(is_valid=False, size_bytes=size, pages=num_pages,
                                failure_class=FailureClass.NON_ARTICLE, error_message=notice)

    # Content verification:
    # If text is extractable, either DOI or title must match. If text is extractable but NEITHER matches,
    # it is a content_mismatch defect (e.g. wrong article downloaded or placeholder).
    content_match = "unverified"
    if extracted_text and (expected_doi or expected_title):
        norm_text = re.sub(r"\s+", "", extracted_text.lower())
        doi_clean = re.sub(r"\s+", "", expected_doi.lower())
        plain_title = unescape(re.sub(r"<[^>]+>", " ", expected_title))
        title_tokens = [t.lower() for t in re.findall(r"\w{4,}", plain_title) if t.isalnum()]

        # APS places its DOI below long abstracts; keep title matching limited
        # to the header, but check the DOI across the complete first page.
        doi_hit = doi_clean and doi_clean in re.sub(r"\s+", "", page_text.lower())
        title_hits = sum(1 for token in title_tokens if token in norm_text)
        title_hit = bool(title_tokens and (title_hits / len(title_tokens) >= 0.4))

        if doi_hit or title_hit:
            content_match = "matched"
        else:
            return ValidationResult(
                is_valid=False,
                size_bytes=size,
                pages=num_pages,
                failure_class=FailureClass.CONTENT_MISMATCH,
                error_message="Extractable text did not match expected DOI or title",
                content_match="mismatched",
            )

    return ValidationResult(
        is_valid=True,
        size_bytes=size,
        pages=num_pages,
        failure_class=None,
        error_message="",
        content_match=content_match,
    )
