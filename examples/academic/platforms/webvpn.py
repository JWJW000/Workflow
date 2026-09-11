"""BUAA WebVPN hostname wrapping.

``d.buaa.edu.cn`` exposes ``https://HOST/PATH`` as::

    https://d.buaa.edu.cn/https/{blob}{path}

``blob`` is hex(``wrdvpnisthebest!``) plus AES-128-CFB of the hostname
(key = IV = ``wrdvpnisthebest!``, 128-bit segments). A wrong blob decrypts
to garbage such as ``pubs.`svgaqh`` and WebVPN renders PARSE_FAILED.
"""

from __future__ import annotations

from urllib.parse import urlparse

WEBVPN_ORIGIN = "https://d.buaa.edu.cn"

# Verified with `openssl enc -aes-128-cfb` (key=IV=wrdvpnisthebest!).
HOST_BLOBS: dict[str, str] = {
    "www.nature.com": "77726476706e69737468656265737421e7e056d229317c456c0dc7af9758",
    "pubs.acs.org": "77726476706e69737468656265737421e0e2438f69316b4330079bab",
    "pubs.rsc.org": "77726476706e69737468656265737421e0e2438f69227b5330079bab",
    "www.cambridge.org": "77726476706e69737468656265737421e7e056d2243165526c018dab9d1b2c2702",
}

# Ciphertexts that previously reached Chrome and must never be reused.
FORBIDDEN_BLOBS: dict[str, str] = {
    "77726476706e69737468656265737421e0e2438f69307b46790998a4": "pubs.`svgaqh",
    "77726476706e69737468656265737421fcfe4f976932784430079bab": "link.bpt.org",
}


def host_blob(hostname: str) -> str:
    host = (hostname or "").strip().lower()
    try:
        return HOST_BLOBS[host]
    except KeyError as exc:
        raise KeyError(f"no verified BUAA WebVPN blob for {hostname!r}") from exc


def assert_not_forbidden_webvpn_url(url: str) -> str:
    for blob, decoded in FORBIDDEN_BLOBS.items():
        if blob in url:
            raise ValueError(
                f"refusing WebVPN URL that decrypts to {decoded!r}: {url}"
            )
    return url


def webvpn_https(hostname: str, path: str = "/", *, query: str = "") -> str:
    blob = host_blob(hostname)
    if blob in FORBIDDEN_BLOBS:
        raise ValueError(f"refusing known-bad WebVPN blob for {hostname}")
    if not path.startswith("/"):
        path = f"/{path}"
    suffix = f"?{query.lstrip('?')}" if query else ""
    return assert_not_forbidden_webvpn_url(
        f"{WEBVPN_ORIGIN}/https/{blob}{path}{suffix}"
    )


def webvpn_host_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "d.buaa.edu.cn":
        return None
    parts = [item for item in parsed.path.split("/") if item]
    if len(parts) < 2:
        return None
    blob = parts[1].split("-", 1)[0]
    inverse = {value: key for key, value in HOST_BLOBS.items()}
    return inverse.get(blob)
