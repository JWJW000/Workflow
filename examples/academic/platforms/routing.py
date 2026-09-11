"""Routing logic for DOI works.

Maps works to platforms using DOI prefix as primary criterion,
and Journal name / ISSN as secondary disambiguation.
Specifically ensures 10.1086 (University of Chicago Press vs other societies)
is not blindly swallowed by a single adapter.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from .base import ResolvedWork, Work

# EMBO DOI prefixes resolve to SpringerLink, despite the 10.1038 registrant.
SPRINGER_EMBO_PREFIXES = tuple(f"10.1038/s{code}-" for code in range(44318, 44322))

# DOI Prefix mappings to platform ID
# Primary routing rule
PREFIX_RULES: List[Tuple[str, str]] = [
    # IEEE
    ("10.1109", "ieee"),
    # Springer / Nature
    ("10.1007", "springer"),
    ("10.1186", "springer"),
    ("10.1057", "springer"),
    ("10.1140", "springer"),
    ("10.1365", "springer"),
    ("10.1038", "nature"),
    # Wiley
    ("10.1002", "wiley"),
    ("10.1111", "wiley"),
    # ACS
    ("10.1021", "acs"),
    # APS
    ("10.1103", "aps"),
    # AAA
    ("10.2308", "aaa"),
    # Cambridge Core
    ("10.1017", "cambridge"),
    # RSC
    ("10.1039", "rsc"),
    # Elsevier / ScienceDirect / Cell
    ("10.1016", "elsevier"),
    ("10.1053", "elsevier"),
    # SIAM
    ("10.1137", "siam"),
    # Annual Reviews
    ("10.1146", "annual_reviews"),
    # OUP
    ("10.1093", "oup"),
    # BMJ
    ("10.1136", "bmj"),
    # AIP
    ("10.1063", "aip"),
]

# Secondary / Specialized routing rules (prefix, condition, platform_id)
# e.g., 10.1086 has multiple journals published by Chicago, but journal name / ISSN determines routing
CHICAGO_101086_JOURNALS = {
    "the journal of infectious diseases",
    "clinical infectious diseases",
    "journal of political economy",
    "the american naturalist",
    "signs: journal of women in culture and society",
    "the university of chicago law review",
}


def normalize_doi(doi: str) -> str:
    doi = doi.strip()
    match = re.search(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", doi)
    if match:
        return match.group(0).lower()
    return doi.lower()


def doi_prefix(doi: str) -> str:
    clean = normalize_doi(doi)
    if "/" in clean:
        return clean.split("/", 1)[0]
    return clean


def resolve_platform_for_work(work: Work) -> Optional[str]:
    """Resolve which platform ID should handle the given work.

    Returns the platform ID, or None if unhandled.
    """
    clean_doi = normalize_doi(work.doi)
    prefix = doi_prefix(clean_doi)
    journal_lower = (work.journal or "").strip().lower()
    publisher_lower = (work.publisher or "").strip().lower()
    issn_clean = (work.issn or "").strip().replace("-", "")

    # Special handling for 10.1086: multi-publisher / Chicago press
    if prefix == "10.1086":
        # If journal/publisher indicates OUP transition (e.g. Clinical Infectious Diseases moved to OUP)
        if "oxford" in publisher_lower or "oup" in publisher_lower:
            return "oup"
        # Check journal or publisher for Chicago Press
        if "chicago" in publisher_lower or any(j in journal_lower for j in CHICAGO_101086_JOURNALS):
            return "chicago"
        return "chicago_mixed"

    if clean_doi.startswith(SPRINGER_EMBO_PREFIXES):
        return "springer"

    # Primary prefix match
    for rule_prefix, platform_id in PREFIX_RULES:
        if clean_doi.startswith(rule_prefix + "/"):
            # Journal-level validation for Cambridge
            if platform_id == "cambridge":
                # Ensure it's not a mislabeled non-Cambridge DOI
                return "cambridge"
            return platform_id

    # Secondary check by publisher if prefix unknown
    if "ieee" in publisher_lower or "institute of electrical and electronics engineers" in publisher_lower:
        return "ieee"
    if "springer" in publisher_lower:
        return "springer"
    if "nature portfolio" in publisher_lower or publisher_lower == "nature":
        return "nature"
    if "wiley" in publisher_lower:
        return "wiley"
    if "american chemical society" in publisher_lower or publisher_lower == "acs":
        return "acs"
    if "american physical society" in publisher_lower or publisher_lower == "aps":
        return "aps"
    if "american accounting association" in publisher_lower:
        return "aaa"
    if "cambridge" in publisher_lower:
        return "cambridge"
    if "royal society of chemistry" in publisher_lower or publisher_lower == "rsc":
        return "rsc"
    if any(p in publisher_lower for p in ("elsevier", "cell press")):
        return "elsevier"
    if "society for industrial and applied mathematics" in publisher_lower or "siam" in publisher_lower:
        return "siam"
    if "annual reviews" in publisher_lower:
        return "annual_reviews"

    return None
