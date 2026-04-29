from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests as curl_requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from listo.config import settings
from listo.fetch.cookies import have_kasada_token, load_cookies_for

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    url: str
    status: int
    body: str
    headers: dict[str, str]


class BlockedError(RuntimeError):
    """403/blocked or no usable Kasada token."""


class _RetryableHTTPError(RuntimeError):
    """5xx or 429 — worth retrying via tenacity."""


# curl_cffi impersonation profile. chrome146 is the closest match to user's
# Chromium 147 — TLS handshake + HTTP/2 frame ordering must match the browser
# that minted the Kasada token.
_IMPERSONATE = "chrome146"


class Fetcher:
    """HTTP fetcher using curl_cffi (Chrome TLS impersonation) + browser cookies."""

    def __init__(self) -> None:
        self._session = curl_requests.Session(impersonate=_IMPERSONATE)
        # Cache cookies per host so we don't hit the SQLite jar on every request.
        self._cookies_cache: dict[str, dict[str, str]] = {}

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "Fetcher":
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    def _cookies_for(self, url: str) -> dict[str, str]:
        host = urlparse(url).hostname or ""
        # Strip leading 'www.' so the lookup matches the apex domain Chromium
        # stores cookies under.
        domain = host.removeprefix("www.")
        if domain in self._cookies_cache:
            return self._cookies_cache[domain]
        cookies = load_cookies_for(domain)
        if not have_kasada_token(cookies):
            logger.warning(
                "No Kasada token (KP_UIDz cookie) found for %s — fetch likely to be blocked. "
                "Open the site in Chromium and let it fully load.",
                domain,
            )
        self._cookies_cache[domain] = cookies
        return cookies

    def _sleep_jitter(self) -> None:
        delay = random.uniform(settings.request_min_delay, settings.request_max_delay)
        time.sleep(delay)

    @retry(
        retry=retry_if_exception_type(_RetryableHTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=4, max=30),
        reraise=True,
    )
    def _request(self, url: str, cookies: dict[str, str]):
        resp = self._session.get(url, cookies=cookies, timeout=settings.fetch_timeout)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            wait = max(60.0, float(ra)) if (ra and ra.isdigit()) else random.uniform(60, 120)
            logger.warning("429 from %s, sleeping %.1fs", url, wait)
            time.sleep(wait)
            raise _RetryableHTTPError("429 Too Many Requests")
        if 500 <= resp.status_code < 600:
            raise _RetryableHTTPError(f"{resp.status_code} server error")
        return resp

    def get(self, url: str) -> FetchResult:
        cookies = self._cookies_for(url)
        self._sleep_jitter()
        resp = self._request(url, cookies)
        if resp.status_code == 403:
            raise BlockedError(f"403 Forbidden from {url}")
        return FetchResult(
            url=url,
            status=resp.status_code,
            body=resp.text,
            headers=dict(resp.headers),
        )
