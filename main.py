from __future__ import annotations

import os
import threading
import time
import webbrowser

from app_paths import configure_runtime_env
from web_app import run_server


def _open_browser_later(url: str, delay_seconds: float = 1.0) -> None:
    def _open() -> None:
        time.sleep(delay_seconds)
        webbrowser.open(url)

    threading.Thread(target=_open, daemon=True).start()


def main() -> None:
    configure_runtime_env()
    host = os.getenv("APP_HOST", "127.0.0.1")
    port = int(os.getenv("APP_PORT", "8000"))
    db_path = os.getenv("NADIN_DB_PATH", "cards.db")

    if os.getenv("OPEN_BROWSER_ON_START", "1").lower() in {"1", "true", "yes"}:
        _open_browser_later(f"http://127.0.0.1:{port}")

    run_server(db_path=db_path, host=host, port=port)


if __name__ == "__main__":
    main()
