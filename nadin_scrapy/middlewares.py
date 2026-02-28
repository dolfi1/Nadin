from __future__ import annotations

import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
]


class RotateUserAgentMiddleware:
    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(USER_AGENTS)


class BlockDetectionMiddleware:
    MARKERS = ("captcha", "access denied", "проверка браузера", "just a moment")

    def process_response(self, request, response, spider):
        text = response.text.lower() if hasattr(response, "text") else ""
        if response.status in {403, 429} or any(marker in text for marker in self.MARKERS):
            request.meta["blocked"] = True
        return response
