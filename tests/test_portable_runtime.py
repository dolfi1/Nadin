from __future__ import annotations

import os
from pathlib import Path

from app_paths import configure_runtime_env
from web_app import CompanyWebApp


def test_configure_runtime_env_sets_db_path(tmp_path):
    managed_keys = {
        "APP_BASE_DIR",
        "APP_DATA_DIR",
        "APP_CACHE_DIR",
        "APP_DB_DIR",
        "APP_LOG_DIR",
        "NADIN_DB_PATH",
        "NADIN_LOG_PATH",
        "SCRAPE_MODE_DEFAULT",
        "SCRAPE_MODE_FALLBACK",
        "SCRAPE_MODE_HARD",
    }
    backup = {key: os.environ.get(key) for key in managed_keys}
    try:
        paths = configure_runtime_env(tmp_path)
        assert Path(paths["db_dir"]).exists()
        assert (tmp_path / "logs").exists()
    finally:
        for key, value in backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_base_mode_keeps_only_fns_provider(tmp_path, monkeypatch):
    monkeypatch.setenv("NADIN_PROVIDERS_MODE", "base")
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    assert [provider["kind"] for provider in app.SOURCE_PROVIDERS] == ["egrul"]


def test_extended_mode_has_additional_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("NADIN_PROVIDERS_MODE", "extended")
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    provider_kinds = {provider["kind"] for provider in app.SOURCE_PROVIDERS}
    assert "egrul" in provider_kinds
    assert "rusprofile" in provider_kinds
