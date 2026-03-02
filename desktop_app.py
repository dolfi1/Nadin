from __future__ import annotations

import os
import socket
import sys
import threading
import time
from wsgiref.simple_server import make_server

from app_paths import configure_runtime_env


def _wait_for_server(host: str, port: int, timeout_seconds: float = 15.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def main() -> None:
    paths = configure_runtime_env()

    host = os.getenv("APP_HOST", "127.0.0.1")
    port = int(os.getenv("APP_PORT", "8000"))
    db_path = os.getenv("NADIN_DB_PATH", str(paths["db_dir"] / "cards.db"))

    from web_app import CompanyWebApp
    from web_app import ThreadingWSGIServer

    app = CompanyWebApp(db_path=db_path)
    httpd = make_server(host, port, app, server_class=ThreadingWSGIServer)

    def _server_target() -> None:
        httpd.serve_forever()

    server_thread = threading.Thread(target=_server_target, daemon=True, name="nadin-server")
    server_thread.start()

    if not _wait_for_server(host=host, port=port):
        raise RuntimeError(f"Nadin server did not start on {host}:{port}")

    try:
        import webview
    except ImportError as exc:  # pragma: no cover - runtime packaging dependency
        raise RuntimeError("PyWebView is required for desktop mode. Install: pip install pywebview pyside6") from exc

    def _on_window_closed() -> None:
        httpd.shutdown()
        httpd.server_close()
        server_thread.join(timeout=5)
        sys.exit(0)

    window = webview.create_window("Nadin", f"http://{host}:{port}", width=420, height=720)
    window.events.closed += _on_window_closed
    webview.start(gui="qt")


if __name__ == "__main__":
    main()
