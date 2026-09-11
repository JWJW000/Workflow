"""Reuse a live Chrome login for authenticated HTTP downloads.

Cookie values stay in memory and must never be written to logs, manifests,
or exception messages. Callers may print cookie counts only.
"""

from __future__ import annotations

import base64
import atexit
import hashlib
import json
import os
import re
import socket
import ssl
import subprocess
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Optional


GUIDE = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
SESSION_COOKIE_HINTS = ("wengine", "vpn", "cas", "jsessionid", "session")
EGO_LITE_BIN = "/Applications/ego lite.app/Contents/MacOS/ego lite"
GOOGLE_CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def resolve_chrome_binary() -> str:
    """Resolve an explicitly configured browser, then common local binaries."""
    configured = os.environ.get("DRISSION_CHROME_BIN") or os.environ.get("CHROME_BIN")
    if configured:
        return configured
    candidates = [
        GOOGLE_CHROME_BIN,
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    for candidate in candidates:
        if Path(candidate).is_file():
            return candidate
    return shutil.which("google-chrome") or shutil.which("chromium") or GOOGLE_CHROME_BIN


DEFAULT_CHROME_BIN = resolve_chrome_binary()


def cookie_summary(cookies: Iterable[dict]) -> str:
    return f"{sum(1 for _ in cookies)} cookies"


def cookies_for_host(cookies: Iterable[dict], host: str) -> list[dict]:
    host = (host or "").lower().split(":")[0]
    matched: list[dict] = []
    for cookie in cookies:
        domain = str(cookie.get("domain") or "").lstrip(".").lower()
        if not domain:
            continue
        if host == domain or host.endswith("." + domain) or domain.endswith("." + host):
            matched.append(cookie)
    return matched


def format_cookie_header(cookies: Iterable[dict]) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for cookie in cookies:
        name = str(cookie.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        value = str(cookie.get("value") or "")
        parts.append(f"{name}={value}")
    return "; ".join(parts)


def has_webvpn_session(cookies: Iterable[dict]) -> bool:
    for cookie in cookies:
        name = str(cookie.get("name") or "").lower()
        domain = str(cookie.get("domain") or "").lower()
        if any(hint in name for hint in SESSION_COOKIE_HINTS):
            return True
        if "buaa.edu.cn" in domain and str(cookie.get("value") or ""):
            return True
    return False


def body_looks_like_login(first: bytes, content_type: str = "") -> bool:
    if first.startswith(b"%PDF"):
        return False
    ctype = (content_type or "").lower()
    if "pdf" in ctype:
        return False
    head = first[:800].lstrip().lower()
    if "html" in ctype or "text/" in ctype:
        return True
    return (
        head.startswith(b"<!doctype")
        or head.startswith(b"<html")
        or b"<form" in head
        or b"unified identity" in head
        or "统一身份认证".encode("utf-8") in first[:2000]
    )


def parse_chrome_debug_port(command: str, profile_dir: Path) -> Optional[int]:
    profile = str(profile_dir)
    if not re.search(r"--user-data-dir=[\"\']?" + re.escape(profile) + r"(?:[\"\']?(?:\s|$))", command):
        return None
    match = re.search(r"--remote-debugging-port=(\d+)", command)
    if not match:
        return None
    return int(match.group(1))


def read_devtools_active_port(profile_dir: Path) -> Optional[int]:
    path = profile_dir / "DevToolsActivePort"
    try:
        first = path.read_text(encoding="utf-8").splitlines()[0].strip()
        port = int(first)
    except (OSError, IndexError, ValueError):
        return None
    return port if port > 0 else None


def encode_ws_text(payload: bytes, mask: Optional[bytes] = None) -> bytes:
    mask_bytes = mask if mask is not None else os.urandom(4)
    if len(mask_bytes) != 4:
        raise ValueError("WebSocket mask must be 4 bytes")
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(length.to_bytes(2, "big"))
    else:
        header.append(0x80 | 127)
        header.extend(length.to_bytes(8, "big"))
    header.extend(mask_bytes)
    masked = bytes(b ^ mask_bytes[i % 4] for i, b in enumerate(payload))
    return bytes(header) + masked


def decode_ws_frame(frame: bytes) -> tuple[int, bytes, bytes]:
    if len(frame) < 2:
        raise ValueError("truncated WebSocket frame")
    opcode = frame[0] & 0x0F
    masked = bool(frame[1] & 0x80)
    length = frame[1] & 0x7F
    offset = 2
    if length == 126:
        length = int.from_bytes(frame[offset : offset + 2], "big")
        offset += 2
    elif length == 127:
        length = int.from_bytes(frame[offset : offset + 8], "big")
        offset += 8
    mask = b""
    if masked:
        mask = frame[offset : offset + 4]
        offset += 4
    payload = frame[offset : offset + length]
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    leftover = frame[offset + length :]
    return opcode, payload, leftover


class CdpClient:
    def __init__(self, ws_url: str, timeout: float = 15.0) -> None:
        parsed = urllib.parse.urlparse(ws_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        self._buffer = bytearray()
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        self.sock.sendall(request.encode("ascii"))
        header = self._recv_http_headers()
        if "101" not in header.split("\r\n", 1)[0]:
            self.close()
            raise RuntimeError("Chrome DevTools websocket upgrade failed")
        accept = ""
        for line in header.split("\r\n"):
            if line.lower().startswith("sec-websocket-accept:"):
                accept = line.split(":", 1)[1].strip()
                break
        expected = base64.b64encode(hashlib.sha1((key + GUIDE).encode("ascii")).digest()).decode(
            "ascii"
        )
        if accept and accept != expected:
            self.close()
            raise RuntimeError("Chrome DevTools websocket accept mismatch")
        self._next_id = 1

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self) -> "CdpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def call(self, method: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        message_id = self._next_id
        self._next_id += 1
        payload = {"id": message_id, "method": method, "params": params or {}}
        self.sock.sendall(encode_ws_text(json.dumps(payload, separators=(",", ":")).encode("utf-8")))
        original_timeout = self.sock.gettimeout()
        self._deadline = time.monotonic() + max(0.1, original_timeout or 15.0)
        try:
            while time.monotonic() < self._deadline:
                message = self._recv_json()
                if message.get("id") == message_id:
                    if "error" in message:
                        error = message.get("error") or {}
                        code = error.get("code") if isinstance(error, dict) else None
                        text = error.get("message") if isinstance(error, dict) else type(error).__name__
                        raise RuntimeError(f"CDP {method} failed: {code} {text}")
                    result = message.get("result")
                    return result if isinstance(result, dict) else {}
            raise TimeoutError(f"CDP {method} timed out")
        finally:
            self._deadline = None
            self.sock.settimeout(original_timeout)

    def _recv_http_headers(self) -> str:
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            data.extend(chunk)
        headers, _, remainder = data.partition(b"\r\n\r\n")
        self._buffer.extend(remainder)
        return headers.decode("iso-8859-1", errors="replace")

    def _recv_exact(self, size: int) -> bytes:
        deadline = getattr(self, "_deadline", None)
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("CDP response deadline exceeded")
        while len(self._buffer) < size:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("CDP response deadline exceeded")
                self.sock.settimeout(remaining)
            chunk = self.sock.recv(max(4096, size - len(self._buffer)))
            if not chunk:
                raise ConnectionError("Chrome DevTools websocket closed")
            self._buffer.extend(chunk)
        out = bytes(self._buffer[:size])
        del self._buffer[:size]
        return out

    def _recv_json(self) -> dict[str, Any]:
        while True:
            header = self._recv_exact(2)
            opcode = header[0] & 0x0F
            masked = bool(header[1] & 0x80)
            length = header[1] & 0x7F
            if length == 126:
                length = int.from_bytes(self._recv_exact(2), "big")
            elif length == 127:
                length = int.from_bytes(self._recv_exact(8), "big")
            mask = self._recv_exact(4) if masked else b""
            payload = self._recv_exact(length)
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:
                raise ConnectionError("Chrome DevTools websocket closed")
            if opcode == 0x9:
                # Ping: reply with pong, keep waiting for the RPC result.
                pong = bytearray([0x8A, 0x80 | len(payload)])
                ping_mask = os.urandom(4)
                pong.extend(ping_mask)
                pong.extend(b ^ ping_mask[i % 4] for i, b in enumerate(payload))
                self.sock.sendall(pong)
                continue
            if opcode in {0x1, 0x2}:
                return json.loads(payload.decode("utf-8"))


def probe_devtools(port: int, timeout: float = 2.0) -> Optional[dict[str, Any]]:
    if port <= 0:
        return None
    url = f"http://127.0.0.1:{port}/json/version"
    request = urllib.request.Request(url, headers={"Host": f"127.0.0.1:{port}"})
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=timeout) as response:
            data = json.load(response)
    except (OSError, ValueError, urllib.error.URLError):
        return None
    return data if isinstance(data, dict) else None


def shared_debug_port() -> Optional[int]:
    """Return the dashboard broker port when this task uses a shared browser."""
    endpoint = os.environ.get("DRISSION_SHARED_BROWSER_ENDPOINT", "").strip()
    if not endpoint:
        return None
    try:
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            return None
        port = parsed.port
    except ValueError:
        return None
    return port if port and probe_devtools(port) else None


def list_page_targets(port: int, timeout: float = 2.0) -> list[dict[str, Any]]:
    request = urllib.request.Request(f"http://127.0.0.1:{port}/json/list")
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=timeout) as response:
            data = json.load(response)
    except (OSError, ValueError, urllib.error.URLError):
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict) and item.get("type") == "page"]


def browser_page_health(port: int) -> str:
    """Read-only, bounded probe; a live DevTools port does not imply a live renderer."""
    if not probe_devtools(port):
        return "browser_unavailable"
    pages = list_page_targets(port)
    if not pages:
        return "page_unavailable"
    # Ignore an idle blank tab when a task has an article page.
    articles = [p for p in pages if str(p.get("url", "")).startswith(("http://", "https://"))]
    # A verification document may temporarily reject CDP evaluation. Leave it alone.
    if any(str(p.get("title", "")).strip().lower() in {"just a moment...", "just a moment…", "请稍候…", "请稍候..."} for p in (articles or pages)):
        return "healthy"
    for page in (articles or pages)[:3]:
        try:
            with CdpClient(page["webSocketDebuggerUrl"], timeout=5) as client:
                response = client.call("Runtime.evaluate", {
                    "expression": "({alive:true, crash:/Aw,\\s*Snap!|喔唷，崩溃啦|错误代码[：:]\\s*5|Error code:\\s*5/.test(document.title+' '+(document.body?.textContent||'').slice(0,4000))})",
                    "returnByValue": True,
                })
            value = response.get("result", {}).get("value", {})
            if not isinstance(value, dict) or not value.get("alive"):
                return "page_unresponsive"
            if value.get("crash"):
                return "renderer_crashed"
        except (KeyError, OSError, RuntimeError, ConnectionError, ValueError):
            return "page_unresponsive"
    return "healthy"


def ensure_usable_page(port: int, preferred_hosts: tuple[str, ...] = (), *, close_unresponsive: bool = False) -> dict[str, Any]:
    """Bounded read-only readiness check; one new tab if existing tabs are stuck.

    Only the dashboard's dedicated profiles opt in to closing unresponsive tabs.
    Credentials and login submissions are never retried by this transport layer.
    """
    pages = list_page_targets(port)
    # Chromium may expose DevTools before its startup tab is registered.
    # Let that tab appear before creating a second target.
    if not pages:
        deadline = time.monotonic() + 2
        while not pages and time.monotonic() < deadline:
            time.sleep(0.1)
            pages = list_page_targets(port)
    def page_priority(page):
        url = urllib.parse.urlsplit(str(page.get("url", "")))
        return (url.scheme not in {"http", "https"}, bool(preferred_hosts) and url.hostname not in preferred_hosts)
    pages.sort(key=page_priority)
    version = probe_devtools(port) or {}
    browser_ws = str(version.get("webSocketDebuggerUrl") or "")
    def ready(page):
        ws = str(page.get("webSocketDebuggerUrl") or "")
        if not ws:
            return False
        try:
            with CdpClient(ws, timeout=2) as client:
                client.call("Page.enable")
                return client.call("Runtime.evaluate", {"expression": "1", "returnByValue": True}).get("result", {}).get("value") == 1
        except (OSError, RuntimeError, ConnectionError, ValueError):
            return False
    for page in pages[:3]:
        if ready(page):
            return page
        if close_unresponsive and browser_ws and page.get("id"):
            with CdpClient(browser_ws, timeout=2) as client:
                client.call("Target.closeTarget", {"targetId": page["id"]})
    if not browser_ws:
        raise RuntimeError("Shared browser endpoint unavailable; no login was submitted")
    with CdpClient(browser_ws, timeout=3) as client:
        target = client.call("Target.createTarget", {"url": "about:blank", "background": True}).get("targetId")
        if not target:
            raise RuntimeError("Shared browser could not create a usable task page")
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        page = next((p for p in list_page_targets(port) if p.get("id") == target), None)
        if page and ready(page):
            return page
        time.sleep(0.2)
    raise RuntimeError("Shared browser page did not respond; task stopped before login")


def close_empty_task_tabs(port: int, keep_id: str) -> int:
    """Remove only empty, fully loaded about:blank tabs in a dedicated task browser."""
    version = probe_devtools(port) or {}
    browser_ws = version.get("webSocketDebuggerUrl")
    if not browser_ws:
        return 0
    closed = 0
    with CdpClient(browser_ws, timeout=3) as browser:
        for candidate in list_page_targets(port):
            if candidate.get("id") == keep_id or candidate.get("url") != "about:blank":
                continue
            try:
                with CdpClient(candidate["webSocketDebuggerUrl"], timeout=2) as client:
                    empty = client.call("Runtime.evaluate", {
                        "expression": "location.href === 'about:blank' && document.readyState === 'complete' && (!document.body || document.body.childNodes.length === 0)",
                        "returnByValue": True,
                    }).get("result", {}).get("value") is True
                if empty:
                    result = browser.call("Target.closeTarget", {"targetId": candidate["id"]})
                    closed += int(result.get("success") is True)
            except (OSError, RuntimeError, ValueError, KeyError):
                # A tab may navigate/disappear between enumeration and inspection.
                continue
    return closed


def open_start_page(port: int, url: str, *, clean_empty_tabs: bool = False, foreground: bool = False) -> dict[str, Any]:
    """Show a task's website immediately; preserve existing article pages."""
    host = urllib.parse.urlsplit(url).hostname
    page = ensure_usable_page(port, (host,))
    current = str(page.get("url") or "")
    if current in {"", "about:blank", "chrome://newtab/"}:
        with CdpClient(page["webSocketDebuggerUrl"], timeout=10) as client:
            result = client.call("Page.navigate", {"url": url})
            if result.get("errorText"):
                raise RuntimeError("Task start page could not load")
    version = probe_devtools(port) or {}
    if foreground and version.get("webSocketDebuggerUrl"):
        with CdpClient(version["webSocketDebuggerUrl"], timeout=3) as client:
            client.call("Target.activateTarget", {"targetId": page["id"]})
    if clean_empty_tabs:
        close_empty_task_tabs(port, page["id"])
    return page


def discover_debug_port(profile_dir: Path) -> Optional[int]:
    profile_dir = profile_dir.expanduser().resolve()
    try:
        result = subprocess.run(
            ["ps", "-ax", "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        result = None
    if result and result.stdout:
        for line in result.stdout.splitlines():
            port = parse_chrome_debug_port(line, profile_dir)
            if port and probe_devtools(port):
                return port
    file_port = read_devtools_active_port(profile_dir)
    if file_port and probe_devtools(file_port):
        return file_port
    return None


def prepare_task_browser_profile(profile_dir: Path) -> None:
    """Suppress Ego welcome UI before launching an inactive task profile.

    Do not call while the browser is running: it owns Local State then.
    Preserve all unrelated profile data, especially login and workspace state.
    """
    profile_dir.mkdir(parents=True, exist_ok=True)
    state_path = profile_dir / "Local State"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        state = {}
    ego = state.setdefault("ego", {})
    if ego.get("welcome_page_shown") is True:
        return
    ego["welcome_page_shown"] = True
    temporary = state_path.with_name("Local State.task-start.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(state_path)


def prepare_pdf_download_profile(profile_dir: Path) -> None:
    """Configure an inactive task profile to save PDFs instead of opening a viewer."""
    default = profile_dir / 'Default'
    default.mkdir(parents=True, exist_ok=True)
    path = default / 'Preferences'
    prefs = json.loads(path.read_text()) if path.exists() else {}
    prefs.setdefault('plugins', {})['always_open_pdf_externally'] = True
    prefs.setdefault('download', {}).update(prompt_for_download=False, directory_upgrade=True)
    temporary = path.with_suffix('.task-start.tmp')
    temporary.write_text(json.dumps(prefs, ensure_ascii=False))
    temporary.chmod(0o600)
    temporary.replace(path)


def launch_chrome(profile_dir: Path, chrome_bin: str = DEFAULT_CHROME_BIN) -> subprocess.Popen:
    profile_dir.mkdir(parents=True, exist_ok=True)
    prepare_pdf_download_profile(profile_dir)
    command = [
        chrome_bin,
        "--remote-debugging-port=0",
        f"--user-data-dir={profile_dir}",
        "--window-size=1280,900",
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--dns-over-https-mode=off",
        "--disable-features=DnsOverHttps,Translate,OptimizationHints,MediaRouter",
        "--no-proxy-server",
        "--start-minimized",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        "--hide-crash-restore-bubble",
    ]
    headless = os.environ.get("DRISSION_HEADLESS", os.environ.get("DRISSION_CHROME_HEADLESS", "1"))
    if sys.platform != "darwin" and headless.lower() not in {"0", "false", "no"}:
        command.append("--headless=new")
    return subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def unverified_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


class ChromeSession:
    def __init__(self, profile_dir: Path, chrome_bin: str = DEFAULT_CHROME_BIN) -> None:
        self.profile_dir = profile_dir.expanduser().resolve()
        self.chrome_bin = chrome_bin if Path(chrome_bin).is_file() else resolve_chrome_binary()
        self.port: Optional[int] = None
        self._proc: Optional[subprocess.Popen] = None

    def close(self) -> None:
        """Close only Chrome launched here; borrowed dashboard sessions stay open."""
        proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def ensure_debug_port(self, timeout: float = 30.0) -> int:
        # IEEE's session-HTTP mode needs browser cookies before it invokes a
        # workflow.  Reuse the dashboard broker here as well, otherwise this
        # preflight would silently launch a second, per-platform Ego instance.
        port = shared_debug_port()
        if os.environ.get("DRISSION_SHARED_BROWSER_ENDPOINT") and not port:
            raise RuntimeError("Configured shared browser is unavailable; refusing to launch a second session")
        if port:
            self.port = port
            return port
        port = discover_debug_port(self.profile_dir)
        if port:
            self.port = port
            return port
        if not Path(self.chrome_bin).is_file():
            raise RuntimeError(f"Chrome binary not found: {self.chrome_bin}")
        self._proc = launch_chrome(self.profile_dir, self.chrome_bin)
        atexit.register(self.close)
        deadline = time.time() + timeout
        while time.time() < deadline:
            port = discover_debug_port(self.profile_dir)
            if port:
                self.port = port
                return port
            time.sleep(0.4)
        raise RuntimeError("Chrome started but DevTools port was not reachable")

    def get_cookies(self) -> list[dict[str, Any]]:
        port = self.ensure_debug_port()
        pages = list_page_targets(port)
        ws_urls: list[str] = []
        for page in pages:
            ws_url = str(page.get("webSocketDebuggerUrl") or "")
            if ws_url:
                ws_urls.append(ws_url)
        version = probe_devtools(port) or {}
        browser_ws = str(version.get("webSocketDebuggerUrl") or "")
        if browser_ws:
            ws_urls.insert(0, browser_ws)
        if not ws_urls:
            raise RuntimeError("Chrome DevTools websocket URL missing")
        errors: list[str] = []
        for ws_url in ws_urls:
            try:
                with CdpClient(ws_url) as client:
                    try:
                        if ws_url != browser_ws:
                            client.call("Network.enable")
                    except RuntimeError:
                        pass
                    for method in ("Storage.getCookies", "Network.getCookies", "Network.getAllCookies"):
                        try:
                            result = client.call(method)
                        except RuntimeError as error:
                            errors.append(str(error))
                            continue
                        cookies = result.get("cookies")
                        if isinstance(cookies, list):
                            return [item for item in cookies if isinstance(item, dict)]
            except (OSError, RuntimeError, TimeoutError, ConnectionError) as error:
                errors.append(f"{type(error).__name__}")
                continue
        raise RuntimeError("Chrome cookie export failed")

    def cookie_header_for(self, host: str) -> str:
        return format_cookie_header(cookies_for_host(self.get_cookies(), host))

    def navigate(self, url: str) -> None:
        port = self.ensure_debug_port()
        pages = list_page_targets(port)
        if not pages:
            raise RuntimeError("Chrome has no page target to keep the login session")
        ws_url = str(pages[0].get("webSocketDebuggerUrl") or "")
        if not ws_url:
            raise RuntimeError("Chrome page websocket URL missing")
        with CdpClient(ws_url) as client:
            client.call("Page.enable")
            client.call("Page.navigate", {"url": url})

    def ensure_session(self, warmup_url: str, timeout: float = 180.0) -> int:
        from .buaa_login import BuaaLoginError, ensure_login, trusted_url
        if not trusted_url(warmup_url):
            raise BuaaLoginError("BUAA warmup URL must use the trusted HTTPS portal")
        port = self.ensure_debug_port()
        page = ensure_usable_page(port, ("d.buaa.edu.cn",))
        if os.environ.get("DRISSION_SHARED_BROWSER_ENDPOINT"):
            close_empty_task_tabs(port, page["id"])
        ws_url = str(page.get("webSocketDebuggerUrl") or "")
        if not ws_url:
            raise BuaaLoginError("Chrome has no usable page for BUAA login")
        with CdpClient(ws_url) as client:
            client.call("Page.enable")
            client.call("Page.navigate", {"url": warmup_url})
            # The gateway can redirect to CAS after its first document loads.
            time.sleep(3)
            ensure_login(client, timeout=min(timeout, 60))
        cookies = self.get_cookies()
        if not has_webvpn_session(cookies_for_host(cookies, "d.buaa.edu.cn")):
            raise BuaaLoginError("BUAA login completed without a session cookie")
        print(f"BUAA session verified on port {port} ({cookie_summary(cookies)})")
        return port
