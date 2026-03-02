from __future__ import annotations

import logging
import os
import socket
import threading
import webbrowser
from wsgiref.simple_server import make_server

from app_paths import configure_runtime_env
from logging_setup import setup_logging
from web_app import CompanyWebApp, ThreadingWSGIServer

logger = logging.getLogger(__name__)


def _pick_port(host: str, preferred_port: int = 8000) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if probe.connect_ex((host, preferred_port)) != 0:
            return preferred_port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])


def main() -> None:
    paths = configure_runtime_env()
    log_path = setup_logging()

    host = "127.0.0.1"
    port = _pick_port(host)
    db_path = os.getenv("NADIN_DB_PATH", str(paths["db_dir"] / "cards.db"))

    shutdown_event = threading.Event()
    httpd = None

    def _shutdown_server() -> None:
        if shutdown_event.is_set():
            return
        shutdown_event.set()
        logger.info("Shutdown requested, stopping HTTP server")

        def _shutdown_worker() -> None:
            if httpd is not None:
                httpd.shutdown()

        threading.Thread(target=_shutdown_worker, daemon=True, name="nadin-shutdown").start()

    app = CompanyWebApp(db_path=db_path, shutdown_callback=_shutdown_server)
    httpd = make_server(host, port, app, server_class=ThreadingWSGIServer)

    url = f"http://{host}:{port}/"
    logger.info("Starting local server at %s", url)
    logger.info("Logs path: %s", log_path)
    webbrowser.open(url)

    try:
        httpd.serve_forever()
    finally:
        logger.info("Server stopped")
        httpd.server_close()


if __name__ == "__main__":
    main()
