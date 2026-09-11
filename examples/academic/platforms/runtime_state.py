"""Runtime State and Circuit Breaking management.

Persists each platform's dynamic operational state to:
.doi_download_state/runtime/<platform>.json

Fields tracked:
- state: stopped | starting | running | stopping | cooldown | needs_login | failed
- reason: descriptive text for current state
- cooldown_until: ISO timestamp or None
- needs_login: bool
- updated_at: ISO timestamp
- recent_attempts: int
- recent_successes: int
- last_success_at: ISO timestamp or None
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class PlatformRuntimeState:
    platform_id: str
    state: str = "stopped"  # stopped | starting | running | stopping | cooldown | needs_login | failed
    reason: str = ""
    cooldown_until: Optional[str] = None
    needs_login: bool = False
    recent_attempts: int = 0
    recent_successes: int = 0
    processed_records: int = 0
    last_success_at: Optional[str] = None
    updated_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_runtime_state_path(target_dir: Path, platform_id: str) -> Path:
    return target_dir / ".doi_download_state" / "runtime" / f"{platform_id}.json"


def load_runtime_state(target_dir: Path, platform_id: str) -> PlatformRuntimeState:
    path = get_runtime_state_path(target_dir, platform_id)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return PlatformRuntimeState(
                platform_id=platform_id,
                state=data.get("state", "stopped"),
                reason=data.get("reason", ""),
                cooldown_until=data.get("cooldown_until"),
                needs_login=bool(data.get("needs_login", False)),
                recent_attempts=int(data.get("recent_attempts", 0)),
                recent_successes=int(data.get("recent_successes", 0)),
                processed_records=int(data.get("processed_records", 0)),
                last_success_at=data.get("last_success_at"),
                updated_at=data.get("updated_at", ""),
            )
        except Exception:
            pass
    return PlatformRuntimeState(platform_id=platform_id, updated_at=datetime.now(timezone.utc).isoformat())


def save_runtime_state(target_dir: Path, state: PlatformRuntimeState) -> None:
    path = get_runtime_state_path(target_dir, state.platform_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    state.updated_at = datetime.now(timezone.utc).isoformat()
    # Write atomically
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def trip_cooldown(target_dir: Path, platform_id: str, cooldown_seconds: int, reason: str) -> None:
    """Trip a circuit breaker, setting platform into cooldown state."""
    state = load_runtime_state(target_dir, platform_id)
    cooldown_target = datetime.now(timezone.utc).timestamp() + cooldown_seconds
    state.state = "cooldown"
    state.reason = reason
    state.cooldown_until = datetime.fromtimestamp(cooldown_target, timezone.utc).isoformat()
    save_runtime_state(target_dir, state)


def trip_needs_login(target_dir: Path, platform_id: str, reason: str) -> None:
    """Mark platform as requiring interactive campus/institution login."""
    state = load_runtime_state(target_dir, platform_id)
    state.state = "needs_login"
    state.reason = reason
    state.needs_login = True
    save_runtime_state(target_dir, state)
