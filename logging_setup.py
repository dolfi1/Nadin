from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def _default_log_path() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / "Nadin" / "logs" / "nadin.log"
    return Path.home() / ".nadin" / "logs" / "nadin.log"


def setup_logging() -> Path:
    env_path = os.getenv("NADIN_LOG_PATH")
    log_path = Path(env_path) if env_path else _default_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    if any(getattr(handler, "baseFilename", None) == str(log_path) for handler in root_logger.handlers):
        return log_path

    handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    return log_path
