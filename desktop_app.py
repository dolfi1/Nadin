from __future__ import annotations

import ctypes
import logging
import os
import socket
import threading
import time
from pathlib import Path
from wsgiref.simple_server import make_server

from app_paths import configure_runtime_env
from logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        probe.listen(1)
        return int(probe.getsockname()[1])


def _wait_for_server(host: str, port: int, timeout_seconds: float = 15.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _show_error_dialog(title: str, message: str) -> None:
    try:
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x10)
    except Exception:
        logger.error("%s: %s", title, message)


def main() -> None:
    paths = configure_runtime_env()
    log_path = setup_logging()

    host = "127.0.0.1"
    port = _pick_free_port(host)
    db_path = os.getenv("NADIN_DB_PATH", str(paths["db_dir"] / "cards.db"))

    from web_app import CompanyWebApp
    from web_app import ThreadingWSGIServer

    app = CompanyWebApp(db_path=db_path)
    httpd = make_server(host, port, app, server_class=ThreadingWSGIServer)
    stop_event = threading.Event()

    def _server_target() -> None:
        logger.info("Starting embedded server on http://%s:%s", host, port)
        httpd.serve_forever()

    server_thread = threading.Thread(target=_server_target, daemon=True, name="nadin-server")
    server_thread.start()

    if not _wait_for_server(host=host, port=port):
        raise RuntimeError(f"Nadin server did not start on {host}:{port}")

    try:
        import webview
    except ImportError as exc:  # pragma: no cover - runtime packaging dependency
        raise RuntimeError("PyWebView is required for desktop mode. Install: pip install pywebview pythonnet") from exc

    def _shutdown_and_exit(force: bool = False) -> None:
        if stop_event.is_set():
            return
        stop_event.set()
        logger.info("Shutting down Nadin desktop app")
        try:
            httpd.shutdown()
            httpd.server_close()
            server_thread.join(timeout=1.5)
        except Exception:
            logger.exception("Failed to stop embedded server cleanly")

        if force or server_thread.is_alive():
            os._exit(0)

    def _on_window_closed() -> None:
        _shutdown_and_exit(force=True)

    window = webview.create_window("Nadin", f"http://{host}:{port}", width=420, height=720, resizable=True)
    window.events.closed += _on_window_closed

    logger.info("Desktop app initialized. Logs: %s", Path(log_path))
    try:
        webview.start(gui="edgechromium")
    except Exception as exc:
        logger.exception("Unable to launch Edge WebView runtime")
        _show_error_dialog(
            "Nadin startup error",
            "Не удалось запустить встроенное окно WebView.\n"
            "Установите Microsoft Edge WebView2 Runtime и повторите запуск.\n\n"
            f"Техническая информация: {exc}",
        )
        _shutdown_and_exit(force=True)

    _shutdown_and_exit(force=False)


if __name__ == "__main__":
    main()
