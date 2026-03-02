from __future__ import annotations

import os
import sys
from pathlib import Path


APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent


def _bundle_dir() -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    return APP_DIR


def get_runtime_base_dir() -> Path:
    """Return a writable runtime base dir next to app executable (or project dir in dev)."""
    explicit_base = os.getenv("APP_BASE_DIR")
    if explicit_base:
        return Path(explicit_base)

    return APP_DIR


def resource_path(relative_path: str) -> str:
    """Resolve resource path from bundle dir for source and frozen runtime."""
    return str(_bundle_dir() / relative_path)


def ensure_runtime_dirs(base_dir: Path | None = None) -> dict[str, Path]:
    base = base_dir or get_runtime_base_dir()
    data_dir = base / "data"
    cache_dir = data_dir / "cache"
    db_dir = data_dir / "db"
    logs_dir = base / "logs"

    for path in (data_dir, cache_dir, db_dir, logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    return {
        "base_dir": base,
        "data_dir": data_dir,
        "cache_dir": cache_dir,
        "db_dir": db_dir,
        "logs_dir": logs_dir,
    }


def configure_runtime_env(base_dir: Path | None = None) -> dict[str, Path]:
    paths = ensure_runtime_dirs(base_dir)
    os.environ.setdefault("APP_BASE_DIR", str(paths["base_dir"]))
    os.environ.setdefault("APP_DATA_DIR", str(paths["data_dir"]))
    os.environ.setdefault("APP_CACHE_DIR", str(paths["cache_dir"]))
    os.environ.setdefault("APP_DB_DIR", str(paths["db_dir"]))
    os.environ.setdefault("APP_LOG_DIR", str(paths["logs_dir"]))
    os.environ.setdefault("NADIN_DB_PATH", str(paths["db_dir"] / "cards.db"))
    os.environ.setdefault("NADIN_LOG_PATH", str(paths["logs_dir"] / "nadin.log"))
    os.environ.setdefault("SCRAPE_MODE_DEFAULT", "fast")
    os.environ.setdefault("SCRAPE_MODE_FALLBACK", "fast")
    os.environ.setdefault("SCRAPE_MODE_HARD", "fast")
    return paths
