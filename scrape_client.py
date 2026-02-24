from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import requests

try:
    from scrapling.fetchers import DynamicSession, FetcherSession, StealthySession
except Exception:  # pragma: no cover - optional dependency at runtime
    DynamicSession = None
    FetcherSession = None
    StealthySession = None

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    blocked: bool = False
    mode: str = "fast"


class ScrapeClient:
    def __init__(self, per_domain_min_delay: float = 2.0) -> None:
        self.mode_default = os.getenv("SCRAPE_MODE_DEFAULT", "fast")
        self.mode_fallback = os.getenv("SCRAPE_MODE_FALLBACK", "stealth")
        self.mode_hard = os.getenv("SCRAPE_MODE_HARD", "dynamic")
        self.per_domain_min_delay = per_domain_min_delay
        self._domain_last_call: dict[str, float] = {}
        self._blocked_until: dict[str, float] = {}

    def fetch(self, url: str, timeout: int = 20, max_retries: int = 2, mode: str | None = None) -> FetchResult:
        requested_mode = mode or self.mode_default
        result = self._fetch_once(url, timeout=timeout, max_retries=max_retries, mode=requested_mode)
        if not result.blocked:
            return result
        if requested_mode != self.mode_fallback:
            fallback = self._fetch_once(url, timeout=timeout, max_retries=max_retries, mode=self.mode_fallback)
            if fallback.text and fallback.status_code == 200 and not fallback.blocked:
                return fallback
            result = fallback
        if result.blocked and self.mode_hard not in {requested_mode, self.mode_fallback}:
            hard = self._fetch_once(url, timeout=timeout, max_retries=max_retries, mode=self.mode_hard)
            if hard.text and hard.status_code == 200 and not hard.blocked:
                return hard
            result = hard
        return result

    def _fetch_once(self, url: str, timeout: int, max_retries: int, mode: str) -> FetchResult:
        host = urlparse(url).netloc.lower()
        self._throttle(host)
        if host in self._blocked_until and self._blocked_until[host] > time.time() and mode == "fast":
            return FetchResult(url=url, status_code=429, text="", blocked=True, mode=mode)

        last_result = FetchResult(url=url, status_code=599, text="", blocked=False, mode=mode)
        for attempt in range(max(1, max_retries)):
            status_code, text = self._perform_request(url, timeout=timeout, mode=mode)
            decoded = self._normalize_encoding(text)
            blocked = self._is_block_page(decoded)
            last_result = FetchResult(url=url, status_code=status_code, text=decoded, blocked=blocked, mode=mode)
            if blocked:
                return last_result
            if status_code == 200:
                return last_result
            if status_code in {202, 403, 429, 503}:
                time.sleep((2 ** attempt) + random.uniform(0.1, 0.6))
                continue
            return last_result
        return last_result

    def _perform_request(self, url: str, timeout: int, mode: str) -> tuple[int, str]:
        if mode == "dynamic" and DynamicSession:
            with DynamicSession(headless=True) as session:  # pragma: no cover
                response = session.get(url, timeout=timeout)
                return int(getattr(response, "status_code", 200)), str(getattr(response, "text", ""))
        if mode == "stealth" and StealthySession:
            with StealthySession(headless=True, solve_cloudflare=True) as session:  # pragma: no cover
                response = session.get(url, timeout=timeout)
                return int(getattr(response, "status_code", 200)), str(getattr(response, "text", ""))
        if mode == "fast" and FetcherSession:
            with FetcherSession(impersonate="chrome") as session:  # pragma: no cover
                response = session.get(url, timeout=timeout)
                return int(getattr(response, "status_code", 200)), str(getattr(response, "text", ""))

        response = requests.get(url, timeout=timeout, allow_redirects=True)
        status_code = int(getattr(response, "status_code", 200 if getattr(response, "ok", False) else 500))
        return status_code, str(getattr(response, "text", ""))

    def _normalize_encoding(self, text: str) -> str:
        if not text:
            return ""
        if "Ð" in text and "Ñ" in text:
            try:
                return text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            except Exception:
                return text
        return text

    def _is_block_page(self, text: str) -> bool:
        body = text.lower()
        markers = [
            "captcha",
            "cloudflare",
            "just a moment",
            "access denied",
            "проверка браузера",
            "подтвердите, что вы человек",
            "браузер не подходит",
        ]
        return any(marker in body for marker in markers)

    def _mark_blocked(self, url: str, ttl_seconds: int) -> None:
        host = urlparse(url).netloc.lower()
        if not host:
            return
        self._blocked_until[host] = time.time() + ttl_seconds

    def _throttle(self, host: str) -> None:
        if not host:
            return
        last = self._domain_last_call.get(host, 0.0)
        delay = self.per_domain_min_delay - (time.time() - last)
        if delay > 0:
            time.sleep(delay)
        self._domain_last_call[host] = time.time()
