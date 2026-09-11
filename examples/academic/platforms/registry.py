"""Registry loader and validator for academic platform configurations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from .base import PlatformSpec

DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parent.parent / "platform_registry.json"

REQUIRED_FIELDS = {
    "id",
    "label",
    "access_mode",
    "profile_dir",
    "workflow",
    "state_prefix",
    "maturity",
    "enabled",
    "batch_size",
    "delay_min_seconds",
    "delay_max_seconds",
    "cooldown_seconds",
}

VALID_MATURITIES = {"production", "calibrating", "experimental", "blocked"}


def load_registry(registry_path: Optional[Path] = None) -> Dict[str, PlatformSpec]:
    """Load and validate the platform registry JSON file.

    Returns a dict mapping platform ID to PlatformSpec.
    """
    path = registry_path or DEFAULT_REGISTRY_PATH
    if not path.is_file():
        raise FileNotFoundError(f"Platform registry not found: {path}")

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "platforms" not in data:
        raise ValueError("Invalid registry structure: top-level object must contain 'platforms' list")

    platforms_list = data["platforms"]
    if not isinstance(platforms_list, list):
        raise ValueError("'platforms' must be a list")

    specs: Dict[str, PlatformSpec] = {}
    for idx, item in enumerate(platforms_list):
        if not isinstance(item, dict):
            raise ValueError(f"Platform at index {idx} must be a dict")
        missing = REQUIRED_FIELDS - set(item.keys())
        if missing:
            raise ValueError(f"Platform {item.get('id', idx)} missing required fields: {missing}")

        maturity = str(item["maturity"])
        if maturity not in VALID_MATURITIES:
            raise ValueError(
                f"Platform {item['id']} invalid maturity '{maturity}'. Must be one of {VALID_MATURITIES}"
            )

        spec = PlatformSpec(
            id=str(item["id"]),
            label=str(item["label"]),
            access_mode=str(item["access_mode"]),
            profile_dir=str(item["profile_dir"]),
            workflow=str(item["workflow"]),
            state_prefix=str(item["state_prefix"]),
            maturity=maturity,
            enabled=bool(item["enabled"]),
            batch_size=int(item["batch_size"]),
            delay_min_seconds=float(item["delay_min_seconds"]),
            delay_max_seconds=float(item["delay_max_seconds"]),
            cooldown_seconds=int(item["cooldown_seconds"]),
            note=str(item.get("note", "")),
            priority=int(item.get("priority", 100)),
        )
        if spec.id in specs:
            raise ValueError(f"Duplicate platform ID in registry: {spec.id}")
        specs[spec.id] = spec

    return specs


def get_platform_spec(platform_id: str, registry_path: Optional[Path] = None) -> Optional[PlatformSpec]:
    """Look up a platform spec by ID."""
    specs = load_registry(registry_path)
    return specs.get(platform_id)
