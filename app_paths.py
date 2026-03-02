from __future__ import annotations

import os
import sys
from pathlib import Path


def get_runtime_base_dir() -> Path:
    """Return portable base dir (next to executable in frozen mode)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def resource_path(relative_path: str) -> str:
    """Resolve bundled resource path for normal and PyInstaller runtime."""
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    else:
        base = Path(__file__).resolve().parent
    return str(base / relative_path)


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
    os.environ.setdefault("NADIN_LOG_PATH", str(paths["logs_dir"] / "app.log"))
    os.environ.setdefault("SCRAPE_MODE_DEFAULT", "fast")
    os.environ.setdefault("SCRAPE_MODE_FALLBACK", "fast")
    os.environ.setdefault("SCRAPE_MODE_HARD", "fast")
    return paths
