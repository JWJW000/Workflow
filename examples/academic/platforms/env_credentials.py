"""Read campus credentials from process environment or the project .env file.

Values are literals: never evaluate shell expressions or expand variables.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile

DEFAULT_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"
KEYS = {"BUAA_ACCOUNT", "BUAA_PASSWORD", "TJU_ACCOUNT", "TJU_PASSWORD"}


def env_file() -> Path:
    return Path(os.environ.get("DRISSION_ENV_FILE", str(DEFAULT_ENV_FILE))).expanduser()


def read_values() -> dict[str, str]:
    try:
        lines = env_file().read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    except OSError:
        raise ValueError("Cannot read campus .env file") from None
    values = {}
    for line in lines:
        line = line.strip().removeprefix("export ").strip()
        key, sep, raw = line.partition("=")
        key = key.strip()
        if not sep or key not in KEYS:
            continue
        raw = raw.strip()
        if raw.startswith('"'):
            try:
                value = json.loads(raw)
            except ValueError:
                raise ValueError("Invalid quoted campus credential in .env") from None
            if not isinstance(value, str):
                raise ValueError("Campus credential must be a string")
        elif raw.startswith("'"):
            if not raw.endswith("'") or len(raw) < 2:
                raise ValueError("Invalid quoted campus credential in .env")
            value = raw[1:-1]
        else:
            value = raw.split(" #", 1)[0].rstrip()
        values[key] = value
    return values


def load_pair(prefix: str) -> dict[str, str]:
    if prefix not in {"TJU", "BUAA"}:
        raise ValueError("Unsupported campus")
    account_key, password_key = f"{prefix}_ACCOUNT", f"{prefix}_PASSWORD"
    source = os.environ if account_key in os.environ or password_key in os.environ else read_values()
    account, password = source.get(account_key, "").strip(), source.get(password_key, "")
    if not account or not password:
        raise ValueError(f"Set both {account_key} and {password_key} in the project .env")
    return {"account": account, "password": password}


def store_pair(prefix: str, account: str, password: str) -> None:
    if prefix not in {"TJU", "BUAA"} or not account.strip() or not password:
        raise ValueError("Campus, account and password are required")
    path = env_file()
    if path.is_symlink():
        raise ValueError("Refusing to replace a symlinked .env file")
    keys = {f"{prefix}_ACCOUNT": account.strip(), f"{prefix}_PASSWORD": password}
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    lines = [line for line in lines if line.strip().removeprefix("export ").split("=", 1)[0].strip() not in keys]
    lines.extend(f"{key}={json.dumps(value, ensure_ascii=False)}" for key, value in keys.items())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            os.chmod(temporary, 0o600)
            handle.write("\n".join(lines) + "\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
