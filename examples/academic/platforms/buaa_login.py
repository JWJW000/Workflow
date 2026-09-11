"""BUAA WebVPN login; credentials never enter workflow inputs, argv or logs."""
from __future__ import annotations

import argparse
import getpass
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

try:
    from .env_credentials import load_pair, store_pair
except ImportError:
    from env_credentials import load_pair, store_pair

SCRIPT = Path(__file__).resolve().parents[3] / "crates/driver-drission/src/buaa_login.js"


class BuaaLoginError(RuntimeError):
    """Only fixed, non-sensitive messages may be exposed to the caller."""


def load_credentials() -> dict[str, str]:
    try:
        return load_pair("BUAA")
    except ValueError as error:
        raise BuaaLoginError(str(error)) from None


def store_credentials(account: str, password: str) -> None:
    try:
        store_pair("BUAA", account, password)
    except (ValueError, OSError):
        raise BuaaLoginError("Could not save BUAA credentials to .env") from None


def trusted_url(raw: str) -> bool:
    try:
        url = urlsplit(raw)
        return (url.scheme == "https" and url.hostname == "d.buaa.edu.cn"
                and url.port in (None, 443) and not url.username and not url.password)
    except ValueError:
        return False


def _probe(client, credentials=None) -> str:
    object_id = None
    try:
        result = client.call("Runtime.evaluate", {
            "expression": "location.origin === 'https://d.buaa.edu.cn' ? globalThis : null",
            "returnByValue": False,
        })
        object_id = result.get("result", {}).get("objectId")
        if not object_id:
            raise BuaaLoginError("Login left the trusted portal")
        result = client.call("Runtime.callFunctionOn", {
            "objectId": object_id, "functionDeclaration": SCRIPT.read_text(),
            "arguments": [{"value": credentials}], "returnByValue": True,
        })
        return result.get("result", {}).get("value", {}).get("state", "loading")
    except Exception:
        # CDP errors can contain request data; never propagate their payloads.
        raise BuaaLoginError("Cannot inspect BUAA login page; check the browser") from None
    finally:
        if object_id:
            try:
                client.call("Runtime.releaseObject", {"objectId": object_id})
            except Exception:
                pass


def ensure_login(client, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    submitted = False
    stable = 0
    state = "loading"
    while time.monotonic() < deadline:
        state = _probe(client)
        if state == "ready":
            stable += 1
            if stable >= 3:
                return
        elif state == "credentials_required" and not submitted:
            submitted = True
            stable = 0
            if _probe(client, load_credentials()) != "submitted":
                raise BuaaLoginError("Login form changed or requires manual verification")
        elif state in {"manual_required", "rejected", "untrusted"}:
            messages = {
                "manual_required": "Complete BUAA CAPTCHA or second-factor verification in the browser",
                "rejected": "BUAA credentials rejected; automatic retry stopped",
                "untrusted": "Untrusted BUAA login form destination",
            }
            raise BuaaLoginError(messages[state])
        else:
            stable = 0
        time.sleep(1)
    phase = "credentials submitted, awaiting confirmation" if submitted else "login form not ready; credentials not submitted"
    # Only fixed state labels enter diagnostics; never include DOM text or credentials.
    safe_state = state if state in {"loading", "ready", "credentials_required"} else "unknown"
    raise BuaaLoginError(f"BUAA login timeout: {phase}; state={safe_state}; no repeat submission")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["setup", "status"])
    args = parser.parse_args()
    try:
        if args.command == "setup":
            account = input("BUAA account: ")
            password = getpass.getpass("BUAA password (hidden): ")
            store_credentials(account, password)
            print("BUAA credentials saved in project .env")
        else:
            load_credentials()
            print("BUAA credentials configured")
    except BuaaLoginError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
