"""Automatic login for Tianjin University resource access in shared Chrome.

Credentials are read from the process environment or project .env.  They
are never included in workflow inputs, manifests, or logs.
CAPTCHAs are deliberately not automated: the shared visible browser is left on
the challenge page so the operator can complete it and restart the task.
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from platforms.chrome_session import CdpClient, list_page_targets, shared_debug_port, ensure_usable_page


TjuAccessMode = Literal["eds", "resource2"]

from platforms.env_credentials import load_pair, store_pair
LOGIN_URLS: dict[TjuAccessMode, str] = {
    "eds": "https://eds.tju.edu.cn/",
    "resource2": "https://p.lib.tju.edu.cn/login",
}
EDS_PROBE_URL = "https://cfbfh253cb3a601b84ef2sbwnowbbpncov6xw6fgac.eds.tju.edu.cn/"


class TjuLoginError(RuntimeError):
    """Base error safe to expose in the task log."""


class TjuCredentialsMissing(TjuLoginError):
    pass


class TjuManualLoginRequired(TjuLoginError):
    pass


@dataclass(frozen=True, repr=False)
class TjuCredentials:
    account: str
    password: str


def load_tju_credentials() -> TjuCredentials:
    """Read .env or process environment without exposing either field."""
    try:
        values = load_pair("TJU")
    except ValueError as error:
        raise TjuCredentialsMissing(str(error)) from None
    return TjuCredentials(**values)


def store_tju_credentials(account: str, password: str) -> None:
    try:
        store_pair("TJU", account, password)
    except (ValueError, OSError):
        raise TjuLoginError("Could not save Tianjin credentials to .env") from None


def _select_page(port: int, mode: TjuAccessMode) -> dict[str, Any]:
    host = "eds.tju.edu.cn" if mode == "eds" else "p.lib.tju.edu.cn"
    return ensure_usable_page(port, (host, "user.lib.tju.edu.cn"))



# This function is passed to Runtime.callFunctionOn. Credentials are supplied as
# CDP arguments, rather than interpolated into JavaScript source.
LOGIN_BROWSER_FUNCTION = r"""
function(credentials) {
  const trusted = url => url.protocol === 'https:'
    && ['eds.tju.edu.cn', 'p.lib.tju.edu.cn', 'user.lib.tju.edu.cn'].includes(url.hostname)
    && (!url.port || url.port === '443') && !url.username && !url.password;
  if (!trusted(new URL(location.href))) return {state: 'untrusted'};
  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || el.disabled) return false;
    const style = getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
  };
  const all = (selector) => Array.from(document.querySelectorAll(selector));
  const pageText = clean(document.body && document.body.innerText).slice(0, 12000);
  const currentUrl = location.href;
  const lowerUrl = currentUrl.toLowerCase();
  if (/(用户名或密码错误|账号或密码错误|密码不正确|账号.{0,8}锁定|invalid credentials|incorrect password)/i.test(pageText)) {
    return {state: 'rejected'};
  }

  const challengeInput = all('input').find((el) => {
    const hint = clean([el.id, el.name, el.placeholder, el.getAttribute('aria-label')].join(' ')).toLowerCase();
    return visible(el) && /(captcha|verifycode|verification|验证码|校验码)/i.test(hint);
  });
  if (challengeInput || /(请输入.{0,4}验证码|图形验证码|滑动验证|captcha)/i.test(pageText)) {
    return {state: 'captcha', url: currentUrl, title: document.title};
  }

  if (/(已经在其他地方登录|已在其他位置登录|already logged in elsewhere|重复登录)/i.test(pageText)) {
    const controls = all('button, input[type=button], input[type=submit], a').filter(visible);
    const confirmation = controls.find((el) => {
      const label = clean(el.innerText || el.value || el.title || el.getAttribute('aria-label'));
      return !/(取消|返回|否|退出|cancel|\bno\b)/i.test(label)
        && /(继续.{0,4}登录|^登录$|继续|确认|确定|重新登录|仍然登录|continue|\byes\b)/i.test(label);
    }) || controls.find((el) => el.matches('button[type=submit], input[type=submit]'));
    if (confirmation) {
      confirmation.click();
      return {state: 'continue_clicked', url: currentUrl, title: document.title};
    }
    return {
      state: 'conflict_unhandled',
      url: currentUrl,
      title: document.title,
      controls: controls.slice(0, 8).map((el) => clean(el.innerText || el.value || el.title)).filter(Boolean)
    };
  }

  const password = all('input[type=password]').find(visible);
  const usernameCandidates = all('input').filter((el) => {
    if (!visible(el) || el === password) return false;
    const type = clean(el.type).toLowerCase();
    if (!['', 'text', 'email', 'tel'].includes(type)) return false;
    const hint = clean([el.id, el.name, el.placeholder, el.getAttribute('aria-label')].join(' ')).toLowerCase();
    return /(user|account|login|student|card|学号|账号|用户名|工号)/i.test(hint);
  });
  const username = usernameCandidates[0] || all('input[type=text], input:not([type])').find(visible);

  if (password && username) {
    const form = password.form;
    // The library SPA submits POST /login/access-token through its button handler.
    const spa = location.hostname === 'user.lib.tju.edu.cn' && form
      && form.matches('form.uni-form') && form.querySelector('button[type=button]');
    if (form && (!trusted(new URL(form.action || location.href, location.href))
      || (!spa && String(form.method).toLowerCase() !== 'post'))) return {state: 'untrusted'};
    if (!credentials.allowSubmit) return {state: 'waiting'};
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
    const assign = (el, value) => {
      setter.call(el, value);
      el.dispatchEvent(new Event('input', {bubbles: true}));
      el.dispatchEvent(new Event('change', {bubbles: true}));
      el.dispatchEvent(new Event('blur', {bubbles: true}));
    };
    assign(username, credentials.account);
    assign(password, credentials.password);
    const submit = (spa ? [...form.querySelectorAll('button[type=button]')] : all('button[type=submit], input[type=submit], button, a')).find((el) => {
      const label = clean(el.innerText || el.value || el.title || el.getAttribute('aria-label'));
      return visible(el) && (el.matches('button[type=submit], input[type=submit]') || /^(登录|登 录|login|sign in)$/i.test(label));
    });
    if (!submit) return {state: 'submit_missing', url: currentUrl, title: document.title};
    submit.click();
    return {state: 'submitted', url: currentUrl, title: document.title};
  }

  const hasLogout = all("#logout, a[href*='logout' i], a[href*='logoff' i], a[href*='exit' i]").some(visible)
    || /(退出登录|注销登录|安全退出)/i.test(pageText);
  const onLoginUrl = /(ermslogin|\/login(?:[/?#]|$)|cas\/login)/i.test(lowerUrl);
  return {
    state: hasLogout ? 'authenticated' : (onLoginUrl ? 'login_form_missing' : 'no_login_form'),
    url: currentUrl,
    title: document.title
  };
}
"""


def _call_login_probe(client: CdpClient, credentials: TjuCredentials, allow_submit: bool = True) -> dict[str, Any]:
    global_result = client.call(
        "Runtime.evaluate",
        {"expression": """(() => {
            if (['https://eds.tju.edu.cn', 'https://p.lib.tju.edu.cn', 'https://user.lib.tju.edu.cn'].includes(location.origin)) return globalThis;
            if (location.protocol === 'https:' && location.hostname.endsWith('.eds.tju.edu.cn')
                && (!location.port || location.port === '443') && document.readyState === 'complete'
                && !document.querySelector('input[type=password]'))
                return JSON.stringify({state:'no_login_form', url:location.href});
            return null;
        })()""", "returnByValue": False},
    )
    object_id = global_result.get("result", {}).get("objectId")
    if not object_id:
        value = global_result.get("result", {}).get("value")
        if isinstance(value, str):
            return json.loads(value)
        raise TjuLoginError("Could not access the Tianjin login page")
    result = client.call(
        "Runtime.callFunctionOn",
        {
            "objectId": object_id,
            "functionDeclaration": LOGIN_BROWSER_FUNCTION,
            "arguments": [
                {
                    "value": {
                        "account": credentials.account,
                        "password": credentials.password,
                        "allowSubmit": allow_submit,
                    }
                }
            ],
            "returnByValue": True,
            "awaitPromise": True,
        },
    )
    value = result.get("result", {}).get("value")
    return value if isinstance(value, dict) else {}


def _mode_authenticated(mode: TjuAccessMode, state: dict[str, Any]) -> bool:
    try:
        url = urlsplit(str(state.get("url") or ""))
        valid = (url.scheme == "https" and url.port in (None, 443)
                 and not url.username and not url.password)
    except ValueError:
        return False
    host = url.hostname or ""
    campus = (host == "eds.tju.edu.cn" or host.endswith(".eds.tju.edu.cn")) if mode == "eds" else host == "p.lib.tju.edu.cn"
    if not valid or not campus:
        return False
    probe = state.get("state")
    if probe == "authenticated":
        return True
    return probe == "no_login_form" and not any(
        marker in url.path.lower() for marker in ("/login", "ermslogin", "cas/login")
    )


def ensure_tju_login(mode: TjuAccessMode, timeout: float = 90.0) -> None:
    """Log in through the current shared visible Chrome session."""
    if mode not in LOGIN_URLS:
        raise ValueError(f"Unsupported Tianjin access mode: {mode}")
    credentials = load_tju_credentials()
    port = shared_debug_port()
    if not port:
        raise TjuLoginError("Tianjin shared browser is not available")
    page = _select_page(port, mode)
    ws_url = str(page.get("webSocketDebuggerUrl") or "")
    if not ws_url:
        raise TjuLoginError("Shared Chrome page has no DevTools endpoint")

    deadline = time.monotonic() + timeout
    submitted = False
    stable_successes = 0
    resource_probed = False
    with CdpClient(ws_url, timeout=min(15.0, max(5.0, timeout))) as client:
        client.call("Page.enable")
        client.call("Runtime.enable")
        client.call("Page.navigate", {"url": LOGIN_URLS[mode]})
        time.sleep(1.0)
        while time.monotonic() < deadline:
            try:
                state = _call_login_probe(client, credentials, allow_submit=not submitted)
            except Exception:
                raise TjuLoginError("Could not inspect the trusted Tianjin login page") from None
            status = str(state.get("state") or "")
            if status in {"untrusted", "rejected"}:
                raise TjuManualLoginRequired(
                    "Tianjin login rejected credentials or left the trusted login destination; automatic retry stopped"
                )
            if status == "captcha":
                raise TjuManualLoginRequired(
                    "Tianjin login requires a CAPTCHA; complete it in the shared browser, then restart the task"
                )
            if status == "conflict_unhandled":
                controls = state.get("controls")
                detail = f" (controls: {controls})" if isinstance(controls, list) and controls else ""
                raise TjuManualLoginRequired(
                    "Tianjin account conflict confirmation could not be clicked automatically" + detail
                )
            if status in {"submitted", "continue_clicked"}:
                submitted = True
                stable_successes = 0
                time.sleep(1.5)
                continue
            if status in {"submit_missing", "login_form_missing"}:
                stable_successes = 0
            elif _mode_authenticated(mode, state):
                stable_successes += 1
                if stable_successes >= 2:
                    if mode == "eds" and not resource_probed:
                        # A session can look valid on the public EDS landing page
                        # while the resource proxy has expired. Probe it once.
                        client.call("Page.navigate", {"url": EDS_PROBE_URL})
                        resource_probed = True
                        stable_successes = 0
                        time.sleep(1.5)
                        continue
                    print(f"Tianjin {mode} login ready in shared Chrome", flush=True)
                    return
            else:
                stable_successes = 0
            time.sleep(1.0)

    raise TjuLoginError(f"Tianjin {mode} login did not become ready within {int(timeout)} seconds")


def _setup() -> None:
    account = input("Tianjin University account: ").strip()
    password = getpass.getpass("Tianjin University password: ")
    store_tju_credentials(account, password)
    print("Tianjin credentials saved in project .env")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tianjin University shared-browser login")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("setup", help="store credentials in project .env")
    login = subparsers.add_parser("login", help="perform an automatic login")
    login.add_argument("--mode", choices=sorted(LOGIN_URLS), required=True)
    login.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args()
    if args.command == "setup":
        _setup()
    else:
        ensure_tju_login(args.mode, timeout=args.timeout)


if __name__ == "__main__":
    main()
