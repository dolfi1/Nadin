from __future__ import annotations

import csv
import hashlib
import logging
import io
import json
import os
import random
import re
import socket
import sqlite3
import time
import threading
import unicodedata
import uuid
import webbrowser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
from difflib import SequenceMatcher
from urllib.request import Request
from urllib.request import urlopen
from datetime import datetime, timezone
from html import escape, unescape
from pathlib import Path
from typing import Any, Callable
import shutil
import subprocess
import sys
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from urllib.parse import parse_qs
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIServer, make_server

from card_bot import Card
from app_paths import resource_path
from logging_setup import setup_logging
from constants import (
    BASE_MODE_PROVIDER_KINDS,
    EXTENDED_MODE_PROVIDER_KINDS,
    PASSPORT_MAP,
    POSITION_TRANSLATIONS,
    RU_TO_EN_OPF,
)
from scrape_client import ScrapeClient
try:
    from nadin_scrapy.service import merge_provider_payloads as scrapy_merge_provider_payloads
except Exception:  # pragma: no cover - optional runtime dependency
    scrapy_merge_provider_payloads = None



setup_logging()

logger = logging.getLogger(__name__)

# === Предкомпилированные регулярные выражения ===
_MAX_MEMORY_CACHE = 500
_RE_SPACES = re.compile(r"\s+")
_RE_NOISE_CHARS = re.compile(r"[\"'\u201c\u201d«»()\[\]{}.,;:!?]")
_RE_INN = re.compile(r"\d{10,12}")
_RE_HTTP = re.compile(r"https?://", re.IGNORECASE)
_RE_FIO_CANDIDATE = re.compile(r"\b[А-ЯЁа-яё][а-яё-]+\s+[А-ЯЁа-яё][а-яё-]+\s+[А-ЯЁа-яё][а-яё-]+\b")
_RE_TRAILING_PUNCT = re.compile(r"\s*[,;:]+\s*$")
_RE_POSITION_NOISE = re.compile(r"(Факторы риска|Дисквалификация|Нахождение под)")


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True

EN_TO_RU_OPF = {v: k for k, v in RU_TO_EN_OPF.items()}
COMPANY_SEARCH_REQUIRED_FIELDS = [
    "surname_ru",
    "name_ru",
    "family_name",
    "first_name",
    "gender",
    "appeal",
    "ru_org",
    "en_org",
    "inn",
    "ru_position",
    "en_position",
]
LEADER_LABEL_RE = re.compile(
    r"(руководитель(?:\s+организации)?|генеральный\s+директор|президент|ректор|rector)",
    flags=re.IGNORECASE,
)
FIO_STOP_TOKENS = {
    "юридического",
    "лица",
    "лиц",
    "организации",
    "руководитель",
    "директор",
    "генеральный",
    "президент",
    "председатель",
    "правления",
    "лицо",
    "история",
    "физлиц",
    "проверка",
    "инвестиции",
    "мероприятия",
}
LEADER_FIO_STOP = {
    "действует",
    "обновлено",
    "актуально",
    "статус",
    "карточка",
    "организация",
    "инн",
    "огрн",
    "кпп",
    "пао",
    "ооо",
    "ао",
    "банк",
    "россии",
    "история",
    "проверить",
    "юридического",
    "лица",
    "лиц",
    "физлиц",
}
FIO_FALSE_SURNAME_TOKENS = {
    "россии",
    "инн",
    "банк",
    "пао",
    "ооо",
    "ао",
    "огрн",
}
FIO_FORBIDDEN_TOKEN_RE = re.compile(r"\d|\b(?:инн|огрн|кпп|пао|ооо|ао|оао|зао|банк)\b", flags=re.IGNORECASE)
LEADER_FIO_SOURCE_PRIORITY = {
    "zachestnyibiznes.ru": 1,
    "companies.rbc.ru": 2,
    "ФНС ЕГРЮЛ": 3,
    "rusprofile.ru": 4,
    "focus.kontur.ru": 5,
}
SPECIAL_EN_ORG_NAMES = {"газпром": "Gazprom PJSC", "лукойл": "Lukoil PJSC"}
BLOCK_PAGE_MARKERS = (
    "браузер не подходит",
    "captcha",
    "доступ ограничен",
    "подтвердите",
    "робот",
    "429",
    "access denied",
)
REVIEW_MARKERS = (
    "автотранслит",
    "нужно уточнить",
    "не найден",
    "заполните",
    "ошибка",
)
KNOWN_RU_TO_EN_ORG = {
    "тюменский государственный университет": "Tyumen State University",
}
RU_LEGAL_TO_EN = {
    "ооо": "LLC",
    "пао": "PJSC",
    "ао": "JSC",
    "ип": "IE",
}
RU_TO_EN_OPF_EXTENDED = {
    **RU_TO_EN_OPF,
    "ФГАОУ ВО": "FSAEI HE",
}
FULL_RU_OPF = {
    "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ": "ООО",
    "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ПАО",
    "АКЦИОНЕРНОЕ ОБЩЕСТВО": "АО",
    "ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ОАО",
}
SOURCE_DOMAINS = {
    "egrul.nalog.ru": "ЕГРЮЛ",
    "www.rusprofile.ru": "rusprofile.ru",
    "rusprofile.ru": "rusprofile.ru",
    "zachestnyibiznes.ru": "zachestnyibiznes.ru",
    "focus.kontur.ru": "focus.kontur.ru",
    "checko.ru": "checko.ru",
    "www.checko.ru": "checko.ru",
    "opencorporates.com": "OpenCorporates",
    "www.opencorporates.com": "OpenCorporates",
    "offshoreleaks.icij.org": "OffshoreLeaks",
    "www.corporationwiki.com": "Corporation Wiki",
    "corporationwiki.com": "Corporation Wiki",
    "www.globalbrownbook.net": "Global Brownbook",
    "globalbrownbook.net": "Global Brownbook",
    "data.occrp.org": "OCCRP Aleph",
    "www.faros.ai": "FAROS OSINT",
}

SOURCE_PROVIDERS: list[dict[str, Any]] = [
    {"name": "ФНС ЕГРЮЛ", "kind": "egrul", "supports_inn": True, "supports_name": False, "supports_url": False, "is_person_source": True, "priority": 1},
    {"name": "Wikipedia", "kind": "wikipedia_html", "supports_inn": False, "supports_name": True, "supports_url": True, "is_person_source": False, "priority": 3},
    {"name": "DuckDuckGo HTML", "kind": "duckduckgo_html", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": False, "priority": 4},
    {"name": "rusprofile.ru", "kind": "rusprofile", "supports_inn": True, "supports_name": True, "supports_url": True, "is_person_source": True, "priority": 5},
    {"name": "zachestnyibiznes.ru", "kind": "zachestnyibiznes_scrape", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": False, "priority": 6},
    {"name": "checko.ru", "kind": "checko", "url_template": "https://checko.ru/search/quick?query={query}", "supports_inn": True, "supports_name": True, "supports_url": True, "is_person_source": False, "priority": 6},
    {"name": "focus.kontur.ru", "kind": "kontur", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": False, "priority": 6},
    {"name": "companies.rbc.ru", "kind": "rbc_companies_scrape", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": False, "priority": 7},
    {"name": "tbank/tinkoff", "kind": "tbank_leadership_scrape", "supports_inn": False, "supports_name": True, "supports_url": False, "is_person_source": False, "priority": 8},
]

RUSPROFILE_NOISE_RE = re.compile(
    r"(Факторы риска|Дисквалификация|Нахождение под|Общие сведения|Связи|Регион регистрации|Показать)",
    flags=re.IGNORECASE,
)
GARBAGE_ORG_TITLES = {"результаты поиска", "поиск", "search results"}


def is_person_query(raw: str) -> bool:
    """Определяет, является ли запрос по человеку (ФИО, ИНН 12 цифр, URL /person/)."""
    value = raw.strip().lower()
    company_markers = {
        "ооо", "пао", "ао", "оао", "зао", "ип", "фгбу", "фгуп", "муп", "ltd", "llc", "inc", "corp", "company",
    }
    company_keywords = ("банк", "холдинг", "group")
    if any(keyword in value for keyword in company_keywords):
        return False
    if any(re.search(rf"\b{marker}\b", value) for marker in company_markers):
        return False
    if re.fullmatch(r"\d{12}", value):
        return True
    if re.match(r"^[а-яё\s-]{3,}$", value):
        return True
    if "http" in value and ("/person/" in value or "/ip/" in value):
        return True
    return False


def normalize_gender(patronymic: str) -> str:
    """Автоопределение пола по отчеству."""
    token = patronymic.lower().strip()
    if token.endswith("вич") or token.endswith("ич"):
        return "М"
    if token.endswith("вна"):
        return "Ж"
    return ""

INPUT_TYPE_INN = "INN"
INPUT_TYPE_URL = "URL"
INPUT_TYPE_ORG_TEXT = "ORG_TEXT"
INPUT_TYPE_PERSON_TEXT = "PERSON_TEXT"

CARD_FIELDS: list[tuple[str, str]] = [
    ("title", "Титул"),
    ("appeal", "Обращение"),
    ("family_name", "Family name"),
    ("first_name", "First name"),
    ("middle_name_en", "Middle name (EN)"),
    ("surname_ru", "Фамилия"),
    ("name_ru", "Имя"),
    ("middle_name_ru", "Отчество"),
    ("gender", "Пол"),
    ("inn", "ИНН"),
    ("ru_org", "Организация"),
    ("en_org", "Organization"),
    ("ru_position", "Должность"),
    ("en_position", "Position"),
]

PERSON_REQUIRED_FIELDS = ["surname_ru", "name_ru"]
COMPANY_REQUIRED_FIELDS = ["ru_org", "inn"]
PERSON_IN_COMPANY_REQUIRED_FIELDS = [
    "surname_ru",
    "name_ru",
    "family_name",
    "first_name",
    "ru_org",
    "en_org",
    "inn",
    "ru_position",
    "en_position",
    "gender",
    "appeal",
]
PROBLEMATIC_PROVIDERS = {"rusprofile", "rusprofile_enhanced", "zachestnyibiznes"}
NO_NEGATIVE_CACHE_KINDS = {"rusprofile", "rusprofile_enhanced", "zachestnyibiznes"}

FETCH_STATUS_EMPTY_OK = "EMPTY_OK"
FETCH_STATUS_BLOCKED_403 = "BLOCKED_403"
FETCH_STATUS_RATE_LIMIT_202 = "RATE_LIMIT_202"
FETCH_STATUS_NETWORK_ERROR = "NETWORK_ERROR"

FIELD_PRIORITIES: dict[str, list[str]] = {
    "surname_ru": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "name_ru": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "middle_name_ru": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "gender": ["rusprofile.ru", "focus.kontur.ru", "zachestnyibiznes.ru"],
    "ru_position": ["ФНС ЕГРЮЛ", "rusprofile.ru", "focus.kontur.ru", "zachestnyibiznes.ru"],
    "position": ["ФНС ЕГРЮЛ"],
    "ru_org": ["ФНС ЕГРЮЛ", "rusprofile.ru", "focus.kontur.ru"],
    "en_org": ["ФНС ЕГРЮЛ"],
}

POSITION_NOISE_MARKERS = {
    "история",
    "проверить",
    "юридического",
    "юридического лица",
    "физлицо",
    "сведения",
    "поиск",
    "результат",
    "подробнее",
}

POSITION_NOISE_PHRASES_RE = re.compile(
    r"\b(?:история|проверить|юридического\s+лица|физлиц|действует|обновлено|сведения|инн|огрн)\b",
    flags=re.IGNORECASE,
)


def _clean_fio_part(s: str) -> str:
    if not s:
        return ""
    tokens = re.findall(r"[А-Яа-яЁё-]+", s)
    return tokens[0] if tokens else ""


class CompanyWebApp:
    def __init__(self, db_path: str = "cards.db", shutdown_callback: Callable[[], None] | None = None) -> None:
        self.db_path = Path(db_path)
        self.shutdown_callback = shutdown_callback
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.scrape_client: ScrapeClient | None = None
        self._ensure_scrape_client()
        self._providers_mode = self._resolve_provider_mode()
        self.SOURCE_PROVIDERS = self._build_provider_list()
        self._source_cache: dict[str, dict[str, Any]] = {}
        self._positive_cache_ttl = 30 * 24 * 60 * 60
        self._negative_cache_ttl_reliable = 4 * 60 * 60
        self._negative_cache_ttl_problematic = 30 * 60
        self._provider_error_streak: dict[str, int] = defaultdict(int)
        self._provider_disabled_until: dict[str, float] = {}
        self._domain_last_call: dict[str, float] = {}
        self._domain_last_call_lock = threading.Lock()
        self._domain_throttle_seconds = 2
        self._rusprofile_throttle_range = (3, 7)
        self._active_searches: dict[str, float] = {}
        self._autofill_result_cache: dict[str, dict[str, Any]] = {}
        self._last_search_time: dict[str, float] = {}
        self._endpoint_rate_limit: dict[str, list[float]] = defaultdict(list)
        self._ddg_query_cache: dict[str, tuple[float, list[str]]] = {}
        self._thread_state = threading.local()
        self._http_session = requests.Session()
        _adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0,
        )
        self._http_session.mount("https://", _adapter)
        self._http_session.mount("http://", _adapter)
        self._source_cache_lock = threading.Lock()
        self._provider_state_lock = threading.Lock()
        self._active_searches_lock = threading.Lock()
        self._strict_scraping_mode = False
        self._init_thread_state()
        self._init_db()
        threading.Thread(target=self._cache_cleanup_loop, daemon=True).start()
        self._clear_provider_cache_pattern("list-org.com")
        self._clear_provider_cache_pattern("list_org")

    def _resolve_provider_mode(self) -> str:
        mode = os.getenv("NADIN_PROVIDERS_MODE", "extended").strip().lower()
        return mode if mode in {"base", "extended"} else "extended"

    def _build_provider_list(self) -> list[dict[str, Any]]:
        providers = [dict(provider) for provider in SOURCE_PROVIDERS]
        if self._providers_mode == "extended":
            allowed_kinds = set(EXTENDED_MODE_PROVIDER_KINDS)
        else:
            allowed_kinds = set(BASE_MODE_PROVIDER_KINDS)
        return [provider for provider in providers if provider.get("kind") in allowed_kinds]

    def _ensure_scrape_client(self) -> ScrapeClient | None:
        client = getattr(self, "scrape_client", None)
        if client is not None:
            return client
        try:
            client = ScrapeClient()
            self.scrape_client = client
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to initialize ScrapeClient, fallback to requests: %s", exc)
            self.scrape_client = None
            client = None
        logger.info("ScrapeClient initialized: %s", bool(client))
        return client

    def _clear_provider_cache_pattern(self, pattern: str) -> None:
        with self._source_cache_lock:
            dropped = [k for k in self._source_cache if pattern.lower() in k.lower()]
            for key in dropped:
                self._source_cache.pop(key, None)
        with self._connect() as db:
            db.execute("DELETE FROM source_cache WHERE lower(cache_key) LIKE ?", (f"%{pattern.lower()}%",))
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-32000")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    def _init_thread_state(self) -> None:
        if not hasattr(self._thread_state, "last_fetch_status"):
            self._thread_state.last_fetch_status = FETCH_STATUS_EMPTY_OK
        if not hasattr(self._thread_state, "blocked_fetch"):
            self._thread_state.blocked_fetch = False

    def _profile_value(self, profile: dict[str, Any], field: str) -> str:
        if field == "en_position":
            return str(profile.get("en_position") or profile.get("position") or "")
        return str(profile.get(field, ""))

    def _detect_profile_type(self, profile: dict[str, Any]) -> str:
        explicit_type = self._normalize_spaces(str(profile.get("type", ""))).lower()
        if explicit_type in {"person", "company", "person_in_company"}:
            return explicit_type
        has_person_name = bool(self._normalize_spaces(str(profile.get("surname_ru", ""))) and self._normalize_spaces(str(profile.get("name_ru", ""))))
        has_org_name = bool(self._normalize_spaces(str(profile.get("ru_org", ""))) or self._normalize_spaces(str(profile.get("en_org", ""))))
        if has_person_name and has_org_name:
            return "person_in_company"
        if has_person_name:
            return "person"
        return "company"

    def _required_fields_for_profile(self, profile: dict[str, Any]) -> list[str]:
        profile_type = self._detect_profile_type(profile)
        forced_search_type = self._normalize_spaces(str(profile.get("search_type", ""))).lower()
        has_org = bool(self._normalize_spaces(str(profile.get("ru_org", ""))) or self._normalize_spaces(str(profile.get("en_org", ""))))
        if forced_search_type == "company":
            return COMPANY_REQUIRED_FIELDS
        if profile_type == "person_in_company":
            return PERSON_IN_COMPANY_REQUIRED_FIELDS
        if profile_type == "person" and has_org:
            return PERSON_IN_COMPANY_REQUIRED_FIELDS
        if profile_type == "person":
            return PERSON_REQUIRED_FIELDS
        return COMPANY_REQUIRED_FIELDS

    def _missing_required_fields(self, profile: dict[str, Any]) -> list[str]:
        required_fields = self._required_fields_for_profile(profile)
        missing: list[str] = []
        for field in required_fields:
            if field == "inn_or_ogrn":
                field = "inn"
            if not self._normalize_spaces(str(profile.get(field, ""))):
                missing.append(field)
        return missing

    def _is_profile_complete(self, profile: dict[str, Any]) -> bool:
        required = [
            "surname_ru",
            "name_ru",
            "family_name",
            "first_name",
            "ru_org",
            "en_org",
            "inn",
            "ru_position",
            "en_position",
            "gender",
            "appeal",
        ]
        return all(self._normalize_spaces(str(profile.get(field, ""))) for field in required)

    def _init_db(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS cards (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ru_org TEXT NOT NULL,
                  en_org TEXT NOT NULL,
                  status TEXT NOT NULL,
                  source TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  data_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS audits (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  card_id INTEGER,
                  action TEXT NOT NULL,
                  actor TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  details TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_cache (
                  cache_key TEXT PRIMARY KEY,
                  payload_json TEXT NOT NULL,
                  expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS provider_errors (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  provider_name TEXT NOT NULL,
                  error_type TEXT NOT NULL,
                  error_details TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cards_ru_org ON cards(ru_org);
                CREATE INDEX IF NOT EXISTS idx_cards_inn ON cards(json_extract(data_json, '$.profile.inn'));
                CREATE INDEX IF NOT EXISTS idx_source_cache_expires ON source_cache(expires_at);
                """
            )
            db.commit()


    def _cache_cleanup_loop(self) -> None:
        while True:
            time.sleep(3600)
            try:
                with self._connect() as db:
                    db.execute("DELETE FROM source_cache WHERE expires_at < ?", (time.time(),))
                    db.commit()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Cache cleanup error: %s", exc)

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        query = parse_qs(environ.get("QUERY_STRING", ""))
        form = self._parse_form(environ) if method == "POST" else {}

        try:
            if path == "/" and method == "GET":
                body, status, headers = self.search_page(query)
            elif path == "/autofill/review" and method == "POST":
                if self._rate_limited(environ, "autofill_review", limit=1, window_seconds=10):
                    body, status, headers = "Rate limit exceeded", "429 Too Many Requests", [("Content-Type", "text/plain; charset=utf-8")]
                else:
                    wants_json = self._request_wants_json(environ)
                    body, status, headers = self.autofill_review(form, wants_json=wants_json)
            elif path == "/autofill/confirm" and method == "POST":
                body, status, headers = self.autofill_confirm(form)
            elif path == "/create/manual" and method == "GET":
                body, status, headers = self.manual_get(query)
            elif path == "/create/manual" and method == "POST":
                wants_json = self._request_wants_json(environ)
                body, status, headers = self.manual_post(form, wants_json=wants_json)
            elif re.fullmatch(r"/card/\d+", path) and method == "GET":
                card_id = int(path.split("/")[-1])
                body, status, headers = self.card_view(card_id)
            elif re.fullmatch(r"/card/\d+/edit", path) and method == "GET":
                card_id = int(path.split("/")[-2])
                body, status, headers = self.card_edit_get(card_id)
            elif re.fullmatch(r"/card/\d+/edit", path) and method == "POST":
                card_id = int(path.split("/")[-2])
                body, status, headers = self.card_edit_post(card_id, form)
            elif re.fullmatch(r"/card/\d+/export.csv", path) and method == "GET":
                card_id = int(path.split("/")[-2])
                body, status, headers = self.export_csv(card_id)
            elif re.fullmatch(r"/card/\d+/export/xlsx", path) and method == "GET":
                card_id = int(path.split("/")[-3])
                body, status, headers = self.export_xlsx(card_id)
            elif re.fullmatch(r"/card/\d+/export", path) and method == "GET":
                card_id = int(path.split("/")[-2])
                body, status, headers = self.export_preview(card_id)
            elif path == "/shutdown" and method == "POST":
                body, status, headers = self.shutdown()
            else:
                body, status, headers = "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]
        except Exception as exc:  # noqa: BLE001
            body, status, headers = f"Internal error: {escape(str(exc))}", "500 Internal Server Error", [("Content-Type", "text/plain; charset=utf-8")]

        start_response(status, headers)
        return [body.encode("utf-8")]

    def _parse_form(self, environ: dict[str, Any]) -> dict[str, list[str]]:
        length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(length).decode("utf-8") if length else ""
        return parse_qs(body)

    def _request_wants_json(self, environ: dict[str, Any]) -> bool:
        accept = (environ.get("HTTP_ACCEPT") or "").lower()
        requested_with = (environ.get("HTTP_X_REQUESTED_WITH") or "").lower()
        content_type = (environ.get("CONTENT_TYPE") or "").lower()
        return "application/json" in accept or requested_with == "xmlhttprequest" or "application/json" in content_type

    def _autofill_redirect_response(self, location: str, *, wants_json: bool) -> tuple[str, str, list[tuple[str, str]]]:
        if not wants_json:
            return "", "302 Found", [("Location", location)]
        card_match = re.fullmatch(r"/card/(\d+)", location)
        payload: dict[str, Any] = {"ok": bool(card_match), "redirect": location}
        if card_match:
            payload["card_id"] = int(card_match.group(1))
        return json.dumps(payload, ensure_ascii=False), "200 OK", [("Content-Type", "application/json; charset=utf-8")]

    def _get_one(self, data: dict[str, list[str]], key: str) -> str:
        return (data.get(key) or [""])[0]

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _page(self, title: str, content: str, back_href: str = "/") -> str:
        nav = (
            "<nav style='margin-bottom: 16px'>"
            "<a href='/' style='margin-right: 12px'>🏠 Главная</a>"
            f"<a href='{escape(back_href)}'>← Назад</a>"
            "<form method='post' action='/shutdown' style='display:inline; margin-left: 12px;'>"
            "<button type='submit' style='cursor:pointer;'>Выход</button>"
            "</form>"
            "</nav>"
        )
        return f"<html><head><meta charset='utf-8'><title>{escape(title)}</title></head><body>{nav}{content}</body></html>"

    def shutdown(self) -> tuple[str, str, list[tuple[str, str]]]:
        logger.info("Shutdown requested from UI")
        if self.shutdown_callback:
            self.shutdown_callback()
        body = self._page("Выход", "<p>Сервер завершает работу…</p>")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def _normalize_spaces(self, text: str) -> str:
        return _RE_SPACES.sub(" ", text).strip()

    def _strip_noise(self, text: str) -> str:
        return _RE_NOISE_CHARS.sub(" ", text)

    def _strip_punct(self, text: str, russian: bool = False) -> str:
        allowed = "A-Za-z0-9А-Яа-яЁё -" if russian else "A-Za-z0-9 -"
        return re.sub(rf"[^{allowed}]", "", text)

    def detect_input_type(self, raw: str) -> str:
        value = self._normalize_spaces(raw)
        if _RE_INN.fullmatch(value):
            return INPUT_TYPE_INN
        if _RE_HTTP.match(value):
            return INPUT_TYPE_URL
        if self._looks_like_person_text(value):
            return INPUT_TYPE_PERSON_TEXT
        return INPUT_TYPE_ORG_TEXT

    def _contains_org_form(self, value: str) -> bool:
        upper = value.upper()
        short_forms = "|".join(re.escape(opf) for opf in RU_TO_EN_OPF)
        if re.search(rf"\b({short_forms})\b", upper):
            return True
        return any(full in upper for full in FULL_RU_OPF)

    def _looks_like_person_text(self, value: str) -> bool:
        norm = re.sub(r"[^\w\sЁёА-Яа-я]", "", self._normalize_spaces(value)).strip()
        if not norm or self._contains_org_form(norm):
            return False
        parts = norm.split()
        if not (1 <= len(parts) <= 3):
            return False
        if len(parts) == 1:
            token = parts[0]
            # Однословные запросы вроде "ВТБ" обычно означают организацию.
            if token.isupper():
                return False
            if not re.fullmatch(r"[А-Яа-яЁё-]+", token):
                return False
        return all(re.fullmatch(r"[А-Яа-яЁё]+", part) for part in parts)

    def _clean_ru_org_name(self, value: str) -> str:
        return re.sub(r"^Организация\s+", "", self._normalize_spaces(value), flags=re.IGNORECASE).strip()

    def _dict_get_path(self, payload: Any, *path: str) -> Any:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current

    def _pick_fns_ru_org_full(self, fns_svyl: dict[str, Any], fallback_name: str = "") -> str:
        if not isinstance(fns_svyl, dict):
            return self._normalize_spaces(fallback_name)

        naim = fns_svyl.get("СвНаимЮЛ") or {}
        if not isinstance(naim, dict):
            naim = {}

        full_name = (
            self._dict_get_path(naim, "СвНаимЮЛПолн", "@attributes", "НаимПолн")
            or self._dict_get_path(naim, "СвНаимЮЛПолн", "НаимПолн")
            or self._dict_get_path(naim, "@attributes", "НаимПолн")
            or naim.get("НаимПолн")
            or fns_svyl.get("НаимПолн")
        )
        short_name = (
            self._dict_get_path(naim, "СвНаимЮЛСокр", "@attributes", "НаимСокр")
            or self._dict_get_path(naim, "СвНаимЮЛСокр", "НаимСокр")
            or self._dict_get_path(naim, "@attributes", "НаимСокр")
            or naim.get("НаимСокр")
            or fns_svyl.get("НаимСокр")
        )

        name = full_name or short_name or fallback_name
        if not self._normalize_spaces(str(name)):
            return ""

        normalized_name = str(name).strip().strip('"').strip("«»„“”").strip()

        opf_container = fns_svyl.get("СвОбрЮЛ") or {}
        if not isinstance(opf_container, dict):
            opf_container = {}
        opf_value = (
            self._dict_get_path(opf_container, "@attributes", "НаимОПФ")
            or self._dict_get_path(opf_container, "ОПФ", "@attributes", "НаимОПФ")
            or opf_container.get("НаимОПФ")
        )
        if opf_value:
            opf_text = str(opf_value).strip()
            opf_map = {
                "АКЦИОНЕРНОЕ ОБЩЕСТВО": "АО",
                "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ПАО",
                "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ": "ООО",
            }
            opf_short = opf_map.get(opf_text.upper(), opf_text)
            if not normalized_name.upper().endswith((" ООО", " АО", " ПАО", " ЗАО", " НКО", " ИП")):
                normalized_name = f"{normalized_name} {opf_short}".strip()

        return self._normalize_spaces(normalized_name)

    def _is_garbage_org_title(self, title: str, raw_query: str = "") -> bool:
        normalized_title = self._normalize_spaces(title).lower()
        normalized_query = self._normalize_spaces(raw_query).lower()
        if not normalized_title:
            return True
        if normalized_title in GARBAGE_ORG_TITLES:
            return True
        if len(normalized_title) <= 3:
            return True
        if normalized_query and normalized_title == normalized_query:
            return True
        return False

    def _extract_inn(self, raw: str) -> str:
        value = self._normalize_spaces(raw)
        if re.fullmatch(r"\d{10}|\d{12}", value):
            return value
        if self.detect_input_type(value) != INPUT_TYPE_URL:
            return ""

        parsed = urlparse(value)
        for candidate in re.findall(r"\d{10}|\d{12}", parsed.path):
            return candidate
        return ""

    def _extract_checko_company_id(self, raw: str) -> str:
        if self.detect_input_type(raw) != INPUT_TYPE_URL:
            return ""
        parsed = urlparse(raw)
        if parsed.netloc.lower() not in {"checko.ru", "www.checko.ru"}:
            return ""
        match = re.search(r"-(\d{13})/?$", parsed.path)
        return match.group(1) if match else ""

    def _split_fio_ru(self, fio: str) -> tuple[str, str, str]:
        if not fio or not fio.strip():
            return "", "", ""
        parts = self._normalize_spaces(fio).split()
        if len(parts) >= 3:
            return parts[0].capitalize(), parts[1].capitalize(), parts[2].capitalize()
        if len(parts) == 2:
            return parts[0].capitalize(), parts[1].capitalize(), ""
        if len(parts) == 1:
            return parts[0].capitalize(), "", ""
        return "", "", ""

    def _valid_fio_part(self, value: str, *, min_len: int = 2) -> bool:
        token = self._normalize_spaces(value).lower()
        if not token or len(token) < min_len:
            return False
        if token in FIO_STOP_TOKENS:
            return False
        if not re.fullmatch(r"[А-Яа-яЁё\-]+", token):
            return False
        letters_count = len(re.findall(r"[А-Яа-яЁё]", token))
        return letters_count >= min_len

    def is_valid_person_name_tokens(self, tokens: list[str]) -> bool:
        cleaned_tokens = [self._clean_fio_part(token) for token in tokens if self._clean_fio_part(token)]
        if len(cleaned_tokens) < 2:
            return False
        for token in cleaned_tokens:
            lowered = token.lower()
            if len(token) < 2:
                return False
            if lowered in LEADER_FIO_STOP:
                return False
            if not re.fullmatch(r"[А-Яа-яЁё\-]+", token):
                return False
            if any(ch.isdigit() for ch in token):
                return False
        return True

    def is_valid_ru_fio(self, surname_ru: str, name_ru: str, middle_name_ru: str = "") -> bool:
        cleaned = [self._clean_fio_part(surname_ru), self._clean_fio_part(name_ru), self._clean_fio_part(middle_name_ru)]
        tokens = [token for token in cleaned if token]
        if not self.is_valid_person_name_tokens(tokens):
            return False
        return bool(cleaned[0] and cleaned[1])

    def _clean_fio_part(self, value: str) -> str:
        return _clean_fio_part(self._normalize_spaces(str(value or "")))

    def _sanitize_profile_fio(self, profile: dict[str, Any]) -> None:
        for field in ("surname_ru", "name_ru", "middle_name_ru"):
            profile[field] = self._clean_fio_part(str(profile.get(field, "")))

    def _is_valid_leader_fio(self, surname_ru: str, name_ru: str, middle_name_ru: str = "") -> bool:
        accepted, _ = self._validate_leader_fio_candidate(surname_ru, name_ru, middle_name_ru)
        return accepted

    def _validate_leader_fio_candidate(self, surname_ru: str, name_ru: str, middle_name_ru: str = "") -> tuple[bool, str]:
        surname = self._clean_fio_part(surname_ru)
        name = self._clean_fio_part(name_ru)
        middle = self._clean_fio_part(middle_name_ru)
        words = [x for x in (surname, name, middle) if x]
        if not self.is_valid_person_name_tokens(words):
            return False, "stop_or_invalid_tokens"
        if len(words) < 2:
            return False, "too_few_words"
        if surname.lower() in FIO_FALSE_SURNAME_TOKENS:
            return False, "surname_in_false_stoplist"
        for idx, token in enumerate(words):
            lowered = token.lower()
            if lowered in FIO_STOP_TOKENS:
                return False, f"token_in_stoplist:{lowered}"
            if FIO_FORBIDDEN_TOKEN_RE.search(lowered):
                return False, f"token_contains_forbidden_marker:{lowered}"
            min_len = 2 if idx == 1 else 3
            if not self._valid_fio_part(token, min_len=min_len):
                return False, f"invalid_token:{lowered}"
        return True, "ok"

    def _extract_fio_from_leader_obj(self, obj: dict[str, Any]) -> tuple[str, str, str]:
        if not isinstance(obj, dict):
            return "", "", ""

        sv_fl = obj.get("СвФЛ")
        if isinstance(sv_fl, dict):
            surname = self._clean_fio_part(str(sv_fl.get("Фамилия") or ""))
            name = self._clean_fio_part(str(sv_fl.get("Имя") or ""))
            middle = self._clean_fio_part(str(sv_fl.get("Отчество") or ""))
            if surname and name:
                return surname, name, middle
            fio_full = self._normalize_spaces(str(sv_fl.get("ФИОПолн") or ""))
            if fio_full:
                return self._split_fio_ru(fio_full)

        for key in ("ФИОПолн", "ФИО"):
            fio_str = self._normalize_spaces(str(obj.get(key) or ""))
            if fio_str:
                return self._split_fio_ru(fio_str)

        surname = self._clean_fio_part(str(obj.get("Фамилия") or ""))
        name = self._clean_fio_part(str(obj.get("Имя") or ""))
        middle = self._clean_fio_part(str(obj.get("Отчество") or ""))
        if surname and name:
            return surname, name, middle
        return "", "", ""

    def _leader_source_rank(self, source_name: str) -> int:
        return LEADER_FIO_SOURCE_PRIORITY.get(source_name, 100)

    def _pick_best_leader_fio(self, source_hits: list[dict[str, Any]]) -> tuple[str, str, str, str]:
        ranked_hits = sorted(source_hits, key=lambda item: self._leader_source_rank(str(item.get("source", ""))))
        for hit in ranked_hits:
            source_name = str(hit.get("source", ""))
            raw_data = hit.get("data", {})
            if not isinstance(raw_data, dict):
                continue
            data = self._enrich_alternative_person_fields(dict(raw_data))
            surname_ru = self._clean_fio_part(str(data.get("surname_ru", "")))
            name_ru = self._clean_fio_part(str(data.get("name_ru", "")))
            middle_name_ru = self._clean_fio_part(str(data.get("middle_name_ru", "")))
            if self._is_valid_leader_fio(surname_ru, name_ru, middle_name_ru):
                return surname_ru, name_ru, middle_name_ru, source_name
        return "", "", "", ""

    def _extract_leader_from_labeled_text(self, text: str) -> tuple[str, str]:
        normalized = self._normalize_spaces(text)
        if not normalized:
            return "", ""
        for sentence in re.split(r"[\n\r;]+", normalized):
            candidate = self._normalize_spaces(sentence)
            if not LEADER_LABEL_RE.search(candidate):
                continue
            fio_match = re.search(r"([А-ЯЁ][а-яё\-]+\s+[А-ЯЁ][а-яё\-]+(?:\s+[А-ЯЁ][а-яё\-]+)?)", candidate)
            if not fio_match:
                continue
            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_match.group(1))
            if self._is_valid_leader_fio(surname_ru, name_ru, middle_name_ru):
                before_fio = candidate.split(fio_match.group(1))[0]
                position_match = re.search(
                    r"(генеральный\s+директор|президент|председатель\s+правления|председатель|директор|руководитель|ректор)",
                    before_fio,
                    flags=re.IGNORECASE,
                )
                ru_position = self._normalize_position_ru(position_match.group(1)) if position_match else ""
                return fio_match.group(1), ru_position
        return "", ""

    def _extract_ru_org_from_keywords(self, keywords_text: str) -> str:
        text = self._normalize_spaces(unescape(keywords_text))
        if not text:
            return ""

        short_match = re.search(
            r"\b(ПАО|АО|ООО|ОАО|ЗАО|ФГУП|ФГБУ|АНО|МУП|НКО|ИП)\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-]+(?:\s+[А-ЯЁа-яёA-Za-z0-9\-]+){0,7})[»\"]?",
            text,
        )
        if short_match:
            return self._clean_ru_org_name(f"{short_match.group(1)} {short_match.group(2)}")

        full_match = re.search(
            r"(ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО|АКЦИОНЕРНОЕ ОБЩЕСТВО|ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ)\s+[\"«]([^\"»]+)[\"»]",
            text,
            flags=re.IGNORECASE,
        )
        if full_match:
            full_opf = full_match.group(1).upper()
            short_opf = FULL_RU_OPF.get(full_opf, "")
            if short_opf:
                return self._clean_ru_org_name(f"{short_opf} {full_match.group(2)}")
        return ""

    def _extract_fio_from_text(self, text: str) -> tuple[str, str, str]:
        patterns = [
            r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
            r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(match.group(1))
                accepted, _ = self._validate_leader_fio_candidate(surname_ru, name_ru, middle_name_ru)
                if accepted:
                    return surname_ru, name_ru, middle_name_ru
        return "", "", ""

    def extract_fio_from_noise(self, text: str) -> dict[str, str] | None:
        sample = self._normalize_spaces(str(text))[:200]
        if not sample:
            return None
        candidates: list[tuple[str, str, str]] = []
        stop_tokens = {"юридического", "лица", "история", "проверить", "сведения", "поиск", "результат", "подробнее"}
        for match in re.finditer(r"\b([А-ЯЁа-яё][а-яё-]+)\s+([А-ЯЁа-яё][а-яё-]+)\s+([А-ЯЁа-яё][а-яё-]+)\b", sample):
            parts = (match.group(1), match.group(2), match.group(3))
            if any(part.lower() in stop_tokens for part in parts):
                continue
            candidates.append(parts)
        if len(candidates) != 1:
            return None
        surname, name, middle_name = candidates[0]
        return {
            "surname_ru": surname.title(),
            "name_ru": name.title(),
            "middle_name_ru": middle_name.title(),
        }

    def _extract_fio_from_position_text(self, text: str) -> tuple[str, str, str, str]:
        normalized = self._normalize_spaces(text)
        if not normalized:
            return "", "", "", ""
        for match in re.finditer(r"\b([А-ЯЁа-яё][а-яё-]+(?:\s+[А-ЯЁа-яё][а-яё-]+){1,2})\b", normalized):
            fio_raw = self._normalize_spaces(match.group(1))
            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_raw.title())
            accepted, _ = self._validate_leader_fio_candidate(surname_ru, name_ru, middle_name_ru)
            if accepted:
                return surname_ru, name_ru, middle_name_ru, fio_raw
        return "", "", "", ""

    def _clean_position_and_extract_fio(self, raw_position: str) -> tuple[str, str, str, str, str]:
        position = self._normalize_spaces(raw_position)
        if not position:
            return "", "", "", "", ""
        position = POSITION_NOISE_PHRASES_RE.sub(" ", position)
        position = self._normalize_spaces(position)
        surname_ru, name_ru, middle_name_ru, fio_raw = self._extract_fio_from_position_text(position)
        if fio_raw:
            position = self._normalize_spaces(position.replace(fio_raw, " "))
        position = _RE_TRAILING_PUNCT.sub("", position)
        position = self._normalize_spaces(position)

        role_matches = re.findall(
            r"(президент|председатель\s+правления|председатель|генеральный\s+директор|директор|руководитель|ректор|управляющий)",
            position,
            flags=re.IGNORECASE,
        )
        if role_matches:
            unique_roles: list[str] = []
            for role in role_matches:
                normalized_role = self._normalize_position_ru(role)
                if normalized_role and normalized_role not in unique_roles:
                    unique_roles.append(normalized_role)
            cleaned_position = ", ".join(unique_roles)
        else:
            cleaned_position = self.sanitize_ru_position(position) or ""
            if cleaned_position:
                cleaned_position = self._normalize_position_ru(cleaned_position)

        return cleaned_position, surname_ru, name_ru, middle_name_ru, fio_raw

    def sanitize_ru_position(self, text: str) -> str | None:
        cleaned = self._normalize_spaces(str(text or ""))
        if not cleaned:
            return None
        if len(cleaned) > 100:
            return None
        lowered = cleaned.lower()
        if any(marker in lowered for marker in POSITION_NOISE_MARKERS):
            return None
        if _RE_FIO_CANDIDATE.search(cleaned):
            return None

        whitelist = (
            ("председатель правления", "Председатель правления"),
            ("генеральный директор", "Генеральный директор"),
            ("директор", "Директор"),
            ("ректор", "Ректор"),
            ("президент", "Президент"),
            ("управляющий", "Управляющий"),
        )
        for needle, canonical in whitelist:
            if re.search(rf"\b{re.escape(needle)}\b", lowered):
                return canonical

        if re.search(r"\d", cleaned):
            return None
        if not re.fullmatch(r"[А-Яа-яЁё\-\s,]+", cleaned):
            return None
        return cleaned

    def _sanitize_profile_position(self, profile: dict[str, str], source_hits: list[dict[str, Any]], field_sources: dict[str, str]) -> None:
        def _clean_candidate(value: str) -> str:
            cleaned = self._normalize_spaces(str(value or ""))
            if not cleaned or len(cleaned) > 100:
                return ""
            lowered = cleaned.lower()
            if any(marker in lowered for marker in POSITION_NOISE_MARKERS):
                return ""
            if re.search(r"\b[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\b", cleaned):
                return ""
            if re.search(r"\d", cleaned):
                return ""
            if not re.fullmatch(r"[А-Яа-яЁё\-\s,]+", cleaned):
                return ""
            return cleaned

        current_position = _clean_candidate(profile.get("ru_position", ""))
        if current_position:
            profile["ru_position"] = current_position
            return

        source_priority = ["ФНС ЕГРЮЛ", "zachestnyibiznes.ru"]
        for source_name in source_priority:
            for hit in source_hits:
                if hit.get("source") != source_name:
                    continue
                data = hit.get("data", {})
                if not isinstance(data, dict):
                    continue
                candidate = _clean_candidate(str(data.get("ru_position", "") or data.get("position", "")))
                if candidate:
                    profile["ru_position"] = candidate
                    field_sources["ru_position"] = source_name
                    return

        profile["ru_position"] = ""

    def _normalize_positions_ru(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        items = [self._normalize_spaces(x.replace(" и ", ", ")) for x in raw.split(",") if self._normalize_spaces(x)]
        normalized: list[str] = []
        for item in items:
            cleaned = item.replace("ИО", "Исполняющий обязанности")
            if cleaned != item:
                notes.append("Должность RU: сокращения раскрыты")
            normalized.append(cleaned[:1].upper() + cleaned[1:].lower() if cleaned else "")
        return ", ".join(normalized), notes

    def _normalize_positions_en(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        raw = raw.replace(" and ", ", ").replace(" & ", ", ")
        items = [self._normalize_spaces(x) for x in raw.split(",") if self._normalize_spaces(x)]
        normalized: list[str] = []
        abbreviations = {"CEO", "CFO", "COO", "CTO", "CIO", "CMO", "CPO", "CSO", "CISO", "CHRO", "CCO", "CLO", "CDO", "EVP", "SVP", "VP", "GM"}
        small_words = {"and", "of", "the", "a", "an", "to", "for", "in", "on", "at", "by", "or"}
        for item in items:
            words: list[str] = []
            for idx, word in enumerate(item.split()):
                if word.upper() in abbreviations:
                    words.append(word.upper())
                elif idx > 0 and word.lower() in small_words:
                    words.append(word.lower())
                else:
                    words.append(word[:1].upper() + word[1:].lower())
            normalized.append(" ".join(words))
        if " and " in raw.lower() or " & " in raw:
            notes.append("Position EN: разделители приведены к запятым")
        return ", ".join(normalized), notes

    def _derive_salutation(self, gender: str) -> str:
        if gender == "М":
            return "Г-н"
        if gender == "Ж":
            return "Г-жа"
        return ""

    def _field_statuses(self, profile: dict[str, str], notes: list[str]) -> dict[str, str]:
        statuses: dict[str, str] = {}
        for field, _ in CARD_FIELDS:
            value = self._normalize_spaces(profile.get(field, ""))
            if value:
                statuses[field] = "Заполнено"
            else:
                statuses[field] = "—Нужно заполнить" if field == "title" else "Нужно заполнить"
        return statuses

    def _render_profile(self, profile: dict[str, str], field_sources: dict[str, str], notes: list[str]) -> str:
        rows = ""
        for field, label in CARD_FIELDS:
            value = profile.get(field, "")
            normalized_value = self._normalize_spaces(str(value))
            if normalized_value:
                source = field_sources.get(field, "—")
                status = "Заполнено"
            else:
                source = "—"
                status = "Нужно заполнить"
            if field == "revenue_mln":
                source = field_sources.get("revenue_mln", field_sources.get("revenue", "Источник"))
                status = "Справочно"
            if field == "gender" and normalized_value:
                source = field_sources.get(field, "Автоопределение")
            if field == "en_org" and normalized_value:
                source = field_sources.get(field, "Транслитерация из RU")
            if not normalized_value and field in {"surname_ru", "name_ru", "ru_org"}:
                logger.warning("Поле %s пустое в профиле", field)
            rows += f"<tr><td>{label}</td><td>{escape(str(value))}</td><td>{source}</td><td>{status}</td></tr>"
        return f"<table border='1' cellpadding='6' cellspacing='0'><tr><th>Поле</th><th>Значение</th><th>Источник</th><th>Статус</th></tr>{rows}</table>"

    def normalize_ru_org(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(str(raw))
        cleaned = re.sub(r"[«»\"'()]", " ", cleaned)
        cleaned = re.sub(r"\bИНН\s*\d+\b", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\b\d{6,}\b", " ", cleaned)
        if "," in cleaned:
            parts = [self._normalize_spaces(part) for part in cleaned.split(",")]
            cleaned = next((part for part in parts if re.search(r"[А-Яа-яЁёA-Za-z]", part)), cleaned)
            notes.append("RU организация: оставлена первая часть до запятой")
        cleaned = self._strip_punct(cleaned, russian=True)
        cleaned = re.sub(r"\b(НЕ|НЕТ)\s+(ООО|АО|ПАО|ИП|ЗАО|ОАО)\b", r"\2", cleaned, flags=re.IGNORECASE)
        cleaned = self._normalize_spaces(self._strip_noise(cleaned))
        cleaned_upper = cleaned.upper()
        if "ТЮМЕНСКИЙ ГОСУДАРСТВЕННЫЙ УНИВЕРСИТЕТ" in cleaned_upper:
            cleaned = re.sub(r"\bТЮМГУ\b", "", cleaned, flags=re.IGNORECASE)
            notes.append("RU организация: удалена хвостовая аббревиатура")
        for full, short in FULL_RU_OPF.items():
            if full in cleaned_upper:
                cleaned = re.sub(rf"\b{re.escape(full)}\b", short, cleaned, flags=re.IGNORECASE)
                cleaned_upper = cleaned.upper()
                notes.append("RU организация: полная ОПФ сокращена")
        cleaned = re.sub(r"\b(УНИВЕРСИТЕТ)(\s+\1)+\b", r"\1", cleaned, flags=re.IGNORECASE)
        tokens = cleaned.split()
        tokens_upper = [token.upper() for token in tokens]

        # --- NEW: detect OPF written in words at the beginning ---
        # Examples:
        #   "АКЦИОНЕРНОЕ ОБЩЕСТВО УРАЛЬСКИЙ ЗАВОД ..." -> "... АО"
        #   "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО ..."       -> "... ПАО"
        #   "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ ..." -> "... ООО"
        def _starts_with(seq: list[str], prefix: list[str]) -> bool:
            return len(seq) >= len(prefix) and seq[: len(prefix)] == prefix

        opf_from_words: str | None = None
        if _starts_with(tokens_upper, ["ПУБЛИЧНОЕ", "АКЦИОНЕРНОЕ", "ОБЩЕСТВО"]):
            opf_from_words = "ПАО"
            tokens = tokens[3:]
            tokens_upper = tokens_upper[3:]
        elif _starts_with(tokens_upper, ["АКЦИОНЕРНОЕ", "ОБЩЕСТВО"]):
            opf_from_words = "АО"
            tokens = tokens[2:]
            tokens_upper = tokens_upper[2:]
        elif _starts_with(tokens_upper, ["ОБЩЕСТВО", "С", "ОГРАНИЧЕННОЙ", "ОТВЕТСТВЕННОСТЬЮ"]):
            opf_from_words = "ООО"
            tokens = tokens[5:]
            tokens_upper = tokens_upper[5:]

        opf = ""
        # if we detected OPF-in-words, force it as if it were the leading abbreviation
        if opf_from_words:
            opf = opf_from_words
        if not opf:
            if len(tokens_upper) >= 2 and tokens_upper[0] == "ФГАОУ" and tokens_upper[1] == "ВО":
                opf, tokens = "ФГАОУ ВО", tokens[2:]
                tokens_upper = tokens_upper[2:]
                notes.append("RU организация: ОПФ перенесена в конец")
            elif tokens_upper and tokens_upper[0] in RU_TO_EN_OPF:
                opf, tokens = tokens_upper[0], tokens[1:]
                tokens_upper = tokens_upper[1:]
            elif len(tokens_upper) >= 2 and tokens_upper[-2] == "ФГАОУ" and tokens_upper[-1] == "ВО":
                opf, tokens = "ФГАОУ ВО", tokens[:-2]
                tokens_upper = tokens_upper[:-2]
            elif tokens_upper and tokens_upper[-1] in RU_TO_EN_OPF:
                opf, tokens = tokens_upper[-1], tokens[:-1]
                tokens_upper = tokens_upper[:-1]
            else:
                notes.append("RU организация: ОПФ должна быть в конце")

        def _smart_title_ru(name: str) -> str:
            result_tokens: list[str] = []
            for token in name.split():
                token_upper = token.upper()
                if token_upper in RU_TO_EN_OPF:
                    result_tokens.append(token_upper)
                elif len(token) <= 6 and token.isupper() and (
                    re.search(r"[A-Z0-9]", token_upper) or not re.search(r"[АЕЁИОУЫЭЮЯ]", token_upper)
                ):
                    result_tokens.append(token_upper)
                else:
                    result_tokens.append(token.capitalize())
            return " ".join(result_tokens)

        name = " ".join(tokens)
        result = self._normalize_spaces(f"{name} {opf}" if opf else name)
        result = _smart_title_ru(result)
        if re.search(r"тюменский\s+государственный\s+университет", result, flags=re.IGNORECASE):
            suffix = " ФГАОУ ВО" if "ФГАОУ ВО" in result.upper() else ""
            result = f"Тюменский государственный университет{suffix}"
        if "Сбербанк" in result and "ПАО" not in result:
            result = self._normalize_spaces(f"{result} ПАО")
            notes.append("RU организация: добавлено ПАО для Сбербанка")
        return result, notes

    def _transliterate_ru_to_en(self, text: str) -> str:
        words = [self._translit(tok) for tok in re.split(r"\s+", self._normalize_spaces(text)) if tok]
        return self._normalize_spaces(" ".join(w for w in words if w))

    def _generate_en_position(self, ru_position: str) -> str:
        ru_position = self._normalize_spaces(ru_position)
        if not ru_position:
            return ""

        parts = [p for p in (self._normalize_spaces(chunk) for chunk in ru_position.split(",")) if p]
        en_positions: list[str] = []
        for part in parts:
            key = self._normalize_ru_position_key(part)
            translated = POSITION_TRANSLATIONS.get(key, "")
            if translated:
                en_positions.append(translated)
        return ", ".join(en_positions)

    def _normalize_ru_position_key(self, value: str) -> str:
        return " ".join((value or "").replace("ё", "е").strip().lower().split())

    def _detect_org_type(self, ru_org: str) -> str:
        value = self._normalize_spaces(ru_org).upper()
        if not value:
            return "other"
        school_markers = (
            "СОШ", "ШКОЛ", "ЛИЦЕЙ", "ГИМНАЗ", "ДЕТСК", "МАОУ", "МБОУ", "МКОУ", "ДОУ",
        )
        university_markers = (
            "УНИВЕРСИТЕТ", "ИНСТИТУТ", "АКАДЕМИ", "ФГАОУ ВО", "ФГБОУ ВО", "НИТУ", "ВПО",
        )
        if any(marker in value for marker in school_markers):
            return "school"
        if any(marker in value for marker in university_markers):
            return "university"
        return "company"

    def _infer_ru_position_from_en(self, en_position: str, org_type: str) -> str:
        value = self._normalize_spaces(en_position)
        if not value:
            return ""
        mapping = {
            "ceo": "Генеральный директор",
            "general director": "Генеральный директор",
            "director": "Директор",
            "rector": "Ректор",
        }
        ru_positions: list[str] = []
        for part in [self._normalize_spaces(x) for x in value.split(",") if self._normalize_spaces(x)]:
            lowered = part.lower()
            if lowered == "director":
                ru_positions.append("Директор")
            elif lowered == "rector":
                ru_positions.append("Ректор" if org_type == "university" else "Директор")
            elif lowered in mapping:
                ru_positions.append(mapping[lowered])
            else:
                ru_positions.append(part)
        return ", ".join(ru_positions)

    def _generate_middle_name_en(self, middle_name_ru: str) -> str:
        value = self._normalize_spaces(middle_name_ru)
        if not value or value == "—":
            return ""
        return self._transliterate_ru_to_en(value)

    def _translit(self, token: str) -> str:
        if not re.search(r"[A-Za-zА-Яа-яЁё]", token):
            return ""
        out = "".join(PASSPORT_MAP.get(ch, PASSPORT_MAP.get(ch.upper(), ch)) for ch in token)
        result = out[:1].upper() + out[1:].lower() if out else ""
        if result.startswith("Ayr"):
            result = "Air" + result[3:]
        return result

    def normalize_en_org(self, raw: str, fallback_ru: str, is_media: bool = False, is_ru_registered: bool = False) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(self._strip_noise(raw))
        ru_key = self._normalize_spaces(fallback_ru.lower())
        ru_org_normalized, _ = self.normalize_ru_org(fallback_ru)
        ru_exact_key = self._normalize_spaces(ru_org_normalized.lower())
        opf_ru = "ФГАОУ ВО" if fallback_ru.upper().endswith("ФГАОУ ВО") else ""
        opf_en = RU_TO_EN_OPF_EXTENDED.get(opf_ru, "")

        if ru_exact_key and ru_exact_key in KNOWN_RU_TO_EN_ORG:
            return self._normalize_spaces(f"{KNOWN_RU_TO_EN_ORG[ru_exact_key]} {opf_en}"), notes

        for key, value in KNOWN_RU_TO_EN_ORG.items():
            if key in ru_key:
                return self._normalize_spaces(f"{value} {opf_en}"), notes

        if not cleaned and re.search(r"[A-Za-zА-Яа-яЁё]", fallback_ru):
            ru_parts = fallback_ru.split()
            if len(ru_parts) >= 2 and ru_parts[-2:] == ["ФГАОУ", "ВО"]:
                opf_ru = "ФГАОУ ВО"
                name_tokens = ru_parts[:-2]
            else:
                opf_ru = ru_parts[-1] if ru_parts and ru_parts[-1] in RU_TO_EN_OPF_EXTENDED else ""
                name_tokens = ru_parts[:-1] if opf_ru else ru_parts
            name = " ".join(self._translit(tok) for tok in name_tokens)
            cleaned = self._normalize_spaces(f"{name} {RU_TO_EN_OPF_EXTENDED.get(opf_ru, '')}")
            if cleaned:
                notes.append("Organization EN: автотранслит — требует перевода или подтверждения" if not is_ru_registered else "Транслит допустим (зарегистрировано в РФ)")

        if not cleaned:
            return "", notes

        for key, value in SPECIAL_EN_ORG_NAMES.items():
            if key in ru_key:
                return value, notes

        cleaned = unicodedata.normalize("NFKD", cleaned)
        cleaned = "".join(ch for ch in cleaned if ord(ch) < 128)
        parts = cleaned.split()
        opf = ""
        if parts and parts[0].upper() in EN_TO_RU_OPF:
            opf, parts = parts[0].upper(), parts[1:]
            notes.append("Organization EN: OPF moved to suffix")
        elif parts and parts[-1].upper() in EN_TO_RU_OPF:
            opf, parts = parts[-1].upper(), parts[:-1]
        else:
            notes.append("Organization EN: OPF should be at the end")

        name = " ".join(p.capitalize() for p in parts)
        if name.startswith("The ") and not is_media:
            notes.append("Organization EN: The в начале запрещен")
        result = self._normalize_spaces(f"{name} {opf}" if opf else name)
        result = re.sub(r"^The\s+", "", result, flags=re.IGNORECASE)
        if not opf and ru_org_normalized:
            ru_tokens = ru_org_normalized.split()
            ru_opf = ""
            if len(ru_tokens) >= 2 and ru_tokens[-2:] == ["ФГАОУ", "ВО"]:
                ru_opf = "фгаоу во"
            elif ru_tokens:
                ru_opf = ru_tokens[-1].lower()
            legal_en = RU_LEGAL_TO_EN.get(ru_opf)
            if legal_en:
                result = self._normalize_spaces(f"{result} {legal_en}")
        return result, notes

    def is_block_page_value(self, text: str) -> bool:
        value = self._normalize_spaces(str(text)).lower()
        if not value:
            return False
        return any(marker in value for marker in BLOCK_PAGE_MARKERS)

    def _status(self, notes: list[str], required_ok: bool) -> str:
        if not required_ok:
            return "Черновик / Нужно дополнить"
        fatal_markers = ("пол должен", "forbidden", "only english")
        if any(any(marker in n.lower() for marker in fatal_markers) for n in notes):
            return "Ошибка формата"
        if any(any(marker in n.lower() for marker in REVIEW_MARKERS) for n in notes):
            return "Нужно проверить"
        return "Найдено"

    def _is_foreign_query(self, query: str) -> bool:
        cleaned = self._normalize_spaces(query)
        input_type = self.detect_input_type(cleaned)
        if not cleaned or input_type in {INPUT_TYPE_INN, INPUT_TYPE_URL}:
            return False
        return bool(re.search(r"[A-Za-z]", cleaned)) and not self._contains_org_form(cleaned)

    def _provider_chain(self, input_type: str, raw: str) -> list[dict[str, Any]]:
        if input_type == INPUT_TYPE_PERSON_TEXT:
            names = ["rusprofile.ru", "Wikipedia", "tbank/tinkoff"]
        elif input_type == INPUT_TYPE_INN:
            names = ["ФНС ЕГРЮЛ", "zachestnyibiznes.ru", "checko.ru", "rusprofile.ru", "focus.kontur.ru", "companies.rbc.ru"]
        elif input_type == INPUT_TYPE_ORG_TEXT:
            names = ["ФНС ЕГРЮЛ", "zachestnyibiznes.ru", "checko.ru", "rusprofile.ru", "focus.kontur.ru", "companies.rbc.ru", "DuckDuckGo HTML"]
        elif self._is_foreign_query(raw):
            names = ["rusprofile.ru"]
        elif input_type == INPUT_TYPE_URL:
            names = ["ФНС ЕГРЮЛ", "Wikipedia", "DuckDuckGo HTML", "rusprofile.ru", "zachestnyibiznes.ru", "checko.ru", "companies.rbc.ru"]
        else:
            names = ["rusprofile.ru"]
        provider_map = {provider["name"]: provider for provider in self.SOURCE_PROVIDERS}
        selected = [provider_map[name] for name in names if name in provider_map]
        return selected

    def _rate_limited(self, environ: dict[str, Any], endpoint: str, limit: int = 8, window_seconds: int = 60) -> bool:
        client = environ.get("REMOTE_ADDR", "unknown")
        key = f"{endpoint}:{client}"
        now = time.time()
        samples = [stamp for stamp in self._endpoint_rate_limit[key] if now - stamp < window_seconds]
        if len(samples) >= limit:
            self._endpoint_rate_limit[key] = samples
            return True
        samples.append(now)
        self._endpoint_rate_limit[key] = samples
        return False

    def _get_cache(self, cache_key: str) -> list[dict[str, Any]] | None:
        with self._source_cache_lock:
            cached = self._source_cache.get(cache_key)
        now = time.time()
        if cached and now < float(cached.get("expires_at", 0)):
            return list(cached.get("hits", []))

        with self._connect() as db:
            row = db.execute("SELECT payload_json, expires_at FROM source_cache WHERE cache_key=?", (cache_key,)).fetchone()
            if not row:
                return None
            if now >= float(row["expires_at"]):
                db.execute("DELETE FROM source_cache WHERE cache_key=?", (cache_key,))
                db.commit()
                return None
            hits = json.loads(row["payload_json"])

        with self._source_cache_lock:
            self._source_cache[cache_key] = {"hits": hits, "expires_at": float(row["expires_at"])}
        return hits

    def _set_cache(self, cache_key: str, hits: list[dict[str, Any]], ttl: int = 3600) -> None:
        expires_at = time.time() + ttl
        payload = json.dumps(hits, ensure_ascii=False)
        with self._source_cache_lock:
            if len(self._source_cache) >= _MAX_MEMORY_CACHE:
                oldest_key = min(
                    self._source_cache,
                    key=lambda k: self._source_cache[k].get("expires_at", 0),
                )
                self._source_cache.pop(oldest_key, None)
            self._source_cache[cache_key] = {"hits": hits, "expires_at": expires_at}
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO source_cache(cache_key, payload_json, expires_at) VALUES (?, ?, ?)",
                (cache_key, payload, expires_at),
            )
            db.commit()

    def _clear_cache_for_inn(self, inn: str) -> int:
        key_fragment = f":{inn}"
        with self._source_cache_lock:
            dropped = [k for k in self._source_cache if key_fragment in k]
            for key in dropped:
                self._source_cache.pop(key, None)
        with self._connect() as db:
            cur = db.execute("DELETE FROM source_cache WHERE cache_key LIKE ?", (f"%:{inn}%",))
            db.commit()
            return len(dropped) + int(cur.rowcount)

    def _clear_cache_for_person(self, query: str) -> int:
        normalized = _RE_SPACES.sub("", self._normalize_spaces(query).lower())
        if not normalized:
            return 0
        key_fragment = f"person:{normalized}"
        with self._source_cache_lock:
            dropped = [k for k in self._source_cache if key_fragment in k.lower()]
            for key in dropped:
                self._source_cache.pop(key, None)
        with self._connect() as db:
            cur = db.execute("DELETE FROM source_cache WHERE lower(cache_key) LIKE ?", (f"%{key_fragment}%",))
            db.commit()
            return len(dropped) + int(cur.rowcount)

    def _should_call_provider(self, provider: dict[str, Any], input_type: str) -> bool:
        if self._is_provider_temporarily_disabled(provider):
            return False
        if input_type == INPUT_TYPE_INN:
            return bool(provider.get("supports_inn"))
        if input_type == INPUT_TYPE_URL:
            return bool(provider.get("supports_url"))
        return bool(provider.get("supports_name"))

    def _is_provider_temporarily_disabled(self, provider: dict[str, Any]) -> bool:
        with self._provider_state_lock:
            disabled_until = self._provider_disabled_until.get(provider.get("name", ""), 0)
        return time.time() < disabled_until

    def _mark_provider_success(self, provider_name: str) -> None:
        with self._provider_state_lock:
            self._provider_error_streak[provider_name] = 0
            self._provider_disabled_until.pop(provider_name, None)

    def _mark_provider_failure(self, provider_name: str) -> None:
        with self._provider_state_lock:
            streak = self._provider_error_streak.get(provider_name, 0) + 1
            self._provider_error_streak[provider_name] = streak
            if streak > 3:
                self._provider_disabled_until[provider_name] = time.time() + random.randint(10 * 60, 15 * 60)

    def _negative_ttl_for_provider(self, provider: dict[str, Any]) -> int:
        if provider.get("kind") in NO_NEGATIVE_CACHE_KINDS:
            return self._negative_cache_ttl_problematic
        if provider.get("kind") == "egrul":
            return self._negative_cache_ttl_reliable
        return 10 * 60

    def _negative_cache_policy(self, provider: dict[str, Any]) -> tuple[bool, int]:
        self._init_thread_state()
        status = getattr(self._thread_state, "last_fetch_status", FETCH_STATUS_EMPTY_OK)
        if status == FETCH_STATUS_EMPTY_OK:
            return True, self._negative_ttl_for_provider(provider)
        if status in {FETCH_STATUS_BLOCKED_403, FETCH_STATUS_RATE_LIMIT_202}:
            return True, 5 * 60
        if status == FETCH_STATUS_NETWORK_ERROR:
            return False, 0
        return True, self._negative_ttl_for_provider(provider)

    def _call_provider(
        self,
        provider: dict[str, Any],
        raw: str,
        input_type: str,
        no_cache: bool = False,
        search_type: str = "",
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        self._init_thread_state()
        self._thread_state.blocked_fetch = False
        self._thread_state.last_fetch_status = FETCH_STATUS_EMPTY_OK
        normalized = self._normalize_spaces(raw)
        inn = self._extract_inn(raw) if input_type == INPUT_TYPE_INN else None
        cache_key = f"provider:{provider.get('name', '')}:{input_type}:{normalized.lower()}:{search_type}"
        if not no_cache:
            cached_hits = self._get_cache(cache_key)
            if cached_hits is not None:
                logger.info("Кэш найден для %s", cache_key)
                return cached_hits

        try:
            hits = self._fetch_from_provider(provider, normalized, input_type, inn, search_type=search_type)
            normalized_hits = hits if isinstance(hits, list) else ([hits] if isinstance(hits, dict) else [])
            valid_hits = [
                item for item in normalized_hits
                if self._is_valid_provider_payload(item if isinstance(item, dict) else {}, str((item or {}).get("type", "") if isinstance(item, dict) else ""))
            ]
            if valid_hits:
                self._mark_provider_success(provider.get("name", ""))
                if not no_cache:
                    self._set_cache(cache_key, valid_hits, ttl=self._positive_cache_ttl)
                    logger.info("Кэш сохранен для %s", cache_key)
                if isinstance(hits, list):
                    return valid_hits
                return valid_hits[0]
        except Exception as exc:  # noqa: BLE001
            reason = "provider_unavailable" if self._is_blocking_error(exc) else "code_error"
            logger.warning("Provider %s failed for %s [%s]: %s", provider.get("name"), raw, reason, str(exc))
            self._mark_provider_failure(provider.get("name", ""))
            self._handle_provider_error(provider.get("name", "unknown"), exc)

        fallback_hits = self._try_fallback_providers(provider, normalized, input_type, inn, search_type=search_type)
        if provider.get("kind") == "duckduckgo_html":
            if not no_cache:
                should_cache_negative, negative_ttl = self._negative_cache_policy(provider)
                if should_cache_negative and negative_ttl > 0:
                    self._set_cache(cache_key, [], ttl=negative_ttl)
            return []
        if not fallback_hits:
            if getattr(self._thread_state, "blocked_fetch", False):
                blocked_ttl = 12 * 60 * 60
                if not no_cache:
                    self._set_cache(cache_key, [], ttl=blocked_ttl)
                with self._provider_state_lock:
                    self._provider_disabled_until[provider.get("name", "")] = time.time() + blocked_ttl
                logger.warning("Provider %s returned captcha/block for '%s'; cached as blocked", provider.get("name", "unknown"), raw)
                return None
            if not no_cache:
                should_cache_negative, negative_ttl = self._negative_cache_policy(provider)
                if should_cache_negative and negative_ttl > 0:
                    self._set_cache(cache_key, [], ttl=negative_ttl)
                    logger.info("Кэш сохранен для %s", cache_key)
            return []
        if provider.get("kind") == "egrul":
            result = fallback_hits[0].get("data", {})
            if not no_cache:
                self._set_cache(cache_key, [result], ttl=self._positive_cache_ttl)
                logger.info("Кэш сохранен для %s", cache_key)
            return result
        result_list = [hit.get("data", {}) for hit in fallback_hits]
        if not no_cache:
            self._set_cache(cache_key, result_list, ttl=self._positive_cache_ttl)
            logger.info("Кэш сохранен для %s", cache_key)
        return result_list

    def _fetch_from_provider(
        self,
        provider: dict[str, Any],
        raw: str,
        input_type: str,
        inn: str | None,
        search_type: str = "",
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        kind = provider.get("kind")
        person_query = input_type == INPUT_TYPE_PERSON_TEXT or is_person_query(raw)
        if kind == "egrul":
            parsed = self._parse_egrul(inn or raw)
            return parsed
        if kind in {"rusprofile", "rusprofile_enhanced"}:
            return self._collect_rusprofile_profiles(raw, input_type, is_person=person_query, search_type=search_type)
        if kind in {"wikipedia_html", "duckduckgo_html"}:
            url = provider.get("url_template", "")
            if not url:
                if kind == "wikipedia_html":
                    if " " in raw.strip():
                        url = f"https://ru.wikipedia.org/wiki/{quote(raw.replace(' ', '_'))}"
                    else:
                        url = f"https://ru.wikipedia.org/w/index.php?search={quote(raw)}"
                else:
                    url = f"https://duckduckgo.com/html/?q={quote(raw)}"
            else:
                url = url.format(inn=inn or "", query=quote(raw))
            if not self._normalize_spaces(url):
                logger.error("Provider %s generated empty URL", provider.get("name", kind))
                return []
            return self._parse_generic_osint(url, provider.get("name", kind))
        if kind == "zachestnyibiznes_scrape":
            return self._parse_zachestnyibiznes_direct(raw)
        if kind == "rbc_companies_scrape":
            return self._parse_rbc_companies_direct(raw)
        if kind == "tbank_leadership_scrape":
            leadership_query = f"{raw} руководство"
            return self._parse_from_ddg_site(leadership_query, ["tbank.ru", "tinkoff.ru"], provider.get("name", kind))
        if kind == "kontur":
            return self._parse_kontur(raw)
        if kind in {"bank_russia", "rbc"}:
            if not inn and not raw:
                logger.warning("Provider %s skipped: no INN or query", provider.get("name"))
                return None
            url = provider.get("url_template", "").format(inn=inn or quote(raw), query=quote(raw))
            if not url.startswith(("http://", "https://")):
                logger.error("Invalid URL generated for %s: %s", provider.get("name"), url)
                return None
            return self._parse_generic_osint(url, provider.get("name", kind))
        if kind in {"google_search", "yandex_search", "linkedin_search", "facebook_search", "offshoreleaks", "checko", "zachestnyibiznes", "sherlock", "maigret", "holehe", "theharvester"}:
            if kind == "checko":
                template = provider.get("url_template", "") or "https://checko.ru/search/quick?query={query}"
                url = template.format(inn=inn or "", query=quote(raw))
            else:
                url = provider.get("url_template", "").format(inn=inn or "", query=quote(raw))
            if not self._normalize_spaces(url):
                logger.error("Provider %s generated empty URL", provider.get("name", kind))
                return []
            return self._parse_generic_osint(url, provider.get("name", kind))
        if kind == "open_corporates" and inn:
            url = provider.get("url_template", "").format(inn=inn)
            return self._parse_open_corporates(url)
        if kind == "sparks" and inn:
            url = provider.get("url_template", "").format(inn=inn)
            return self._parse_sparks(url)
        if kind in {"sbis", "kontur_focus", "banki_ru"} and inn:
            url = provider.get("url_template", "").format(inn=inn)
            return self._parse_generic_osint(url, provider.get("name", kind))
        return None

    def _is_blocking_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        return any(
            [
                "429" in error_str,
                "rate limit" in error_str,
                "blocked" in error_str,
                "timeout" in error_str and "10060" in error_str,
                "connection refused" in error_str,
            ]
        )

    def _try_fallback_providers(
        self,
        provider: dict[str, Any],
        query: str,
        input_type: str,
        inn: str | None,
        search_type: str = "",
    ) -> list[dict[str, Any]]:
        fallback_providers = self._get_fallback_providers(provider, query, input_type)
        if provider.get("kind") in {"rusprofile", "rusprofile_enhanced"}:
            fallback_providers = [
                fallback
                for fallback in fallback_providers
                if fallback.get("kind") not in {"rusprofile", "rusprofile_enhanced"}
            ]
        hits: list[dict[str, Any]] = []
        seen_fallbacks: set[str] = set()
        normalized_query = self._normalize_spaces(query).lower()
        for fallback in fallback_providers:
            fallback_id = f"{fallback.get('name', '')}:{fallback.get('kind', '')}"
            if fallback_id in seen_fallbacks:
                continue
            seen_fallbacks.add(fallback_id)
            fallback_cache_key = f"provider:{fallback.get('name', '')}:{input_type}:{normalized_query}:{search_type}"
            try:
                cached_hits = self._get_cache(fallback_cache_key)
                if cached_hits is not None:
                    logger.info("Кэш найден для fallback %s", fallback_cache_key)
                    fallback_hits = [{"source": fallback["name"], "url": item.get("url", ""), "data": item} for item in cached_hits if item]
                    if fallback_hits:
                        hits.extend(fallback_hits)
                        break
                    continue

                logger.info("Trying fallback provider %s for %s", fallback["name"], query)
                fallback_result = self._fetch_from_provider(fallback, query, input_type, inn, search_type=search_type)
                fallback_hits: list[dict[str, Any]] = []
                if isinstance(fallback_result, list):
                    fallback_hits = [{"source": fallback["name"], "url": item.get("url", ""), "data": item} for item in fallback_result if item]
                elif isinstance(fallback_result, dict) and fallback_result:
                    fallback_hits = [{"source": fallback["name"], "url": fallback_result.get("url", ""), "data": fallback_result}]
                if fallback_hits:
                    self._set_cache(fallback_cache_key, [hit.get("data", {}) for hit in fallback_hits], ttl=self._positive_cache_ttl)
                    hits.extend(fallback_hits)
                    logger.info("Successfully retrieved %d results from %s", len(fallback_hits), fallback["name"])
                    break
                should_cache_negative, negative_ttl = self._negative_cache_policy(fallback)
                if should_cache_negative and negative_ttl > 0:
                    self._set_cache(fallback_cache_key, [], ttl=negative_ttl)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Fallback provider %s failed: %s", fallback["name"], str(exc))
        return hits

    def _supports_input_type(self, provider: dict[str, Any], input_type: str, _query: str) -> bool:
        if input_type == INPUT_TYPE_INN:
            return bool(provider.get("supports_inn", False))
        if input_type == INPUT_TYPE_PERSON_TEXT:
            return bool(provider.get("supports_name", False) and provider.get("is_person_source", False))
        if input_type == INPUT_TYPE_ORG_TEXT:
            return bool(provider.get("supports_name", False))
        if input_type == INPUT_TYPE_URL:
            return bool(provider.get("supports_url", False) or provider.get("supports_inn", False))
        return bool(provider.get("supports_name", False))

    def _get_fallback_providers(self, provider: dict[str, Any], query: str, input_type: str) -> list[dict[str, Any]]:
        return sorted(
            [
                p
                for p in self.SOURCE_PROVIDERS
                if p.get("name") != provider.get("name") and self._supports_input_type(p, input_type, query)
            ],
            key=lambda item: int(item.get("priority", 10)),
        )

    def _get_provider_by_name(self, name: str) -> dict[str, Any] | None:
        for provider in self.SOURCE_PROVIDERS:
            if provider["name"] == name:
                return provider
        return None

    def _add_enhanced_providers(self) -> None:
        if any(p["name"] == "enhanced_rusprofile" for p in self.SOURCE_PROVIDERS):
            return
        enhanced_providers = [
            {
                "name": "enhanced_rusprofile",
                "kind": "rusprofile_enhanced",
                "supports_inn": True,
                "supports_name": True,
                "supports_url": True,
                "is_person_source": True,
                "priority": 95,
            },
            {
                "name": "bank_of_russia",
                "kind": "bank_russia",
                "supports_inn": True,
                "supports_name": False,
                "supports_url": False,
                "is_person_source": False,
                "priority": 75,
            },
        ]
        for provider in sorted(enhanced_providers, key=lambda p: int(p["priority"])):
            if not any(p["name"] == provider["name"] for p in self.SOURCE_PROVIDERS):
                self.SOURCE_PROVIDERS.insert(0, provider)

    def _add_osint_providers(self) -> None:
        osint_providers = [
            {"name": "open-corporates", "kind": "open_corporates", "url_template": "https://opencorporates.com/companies/ru_{inn}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "sparks", "kind": "sparks", "url_template": "https://sparks.ru/company/{inn}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "sbis", "kind": "sbis", "url_template": "https://sbis.ru/contragents/{inn}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "kontur_focus", "kind": "kontur_focus", "url_template": "https://focus.kontur.ru/entity/{inn}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "banki_ru", "kind": "banki_ru", "url_template": "https://www.banki.ru/company/{inn}/", "supports_inn": True, "supports_name": False, "priority": 50},
            {"name": "Google Search", "kind": "google_search", "url_template": "https://www.google.com/search?q={query}", "supports_inn": True, "supports_name": True, "priority": 7},
            {"name": "Yandex Search", "kind": "yandex_search", "url_template": "https://yandex.ru/search/?text={query}", "supports_inn": True, "supports_name": True, "priority": 7},
            {"name": "LinkedIn", "kind": "linkedin_search", "url_template": "https://www.google.com/search?q=site%3Alinkedin.com%2Fin+{query}", "supports_inn": False, "supports_name": True, "priority": 9},
            {"name": "Facebook", "kind": "facebook_search", "url_template": "https://www.google.com/search?q=site%3Afacebook.com+{query}", "supports_inn": False, "supports_name": True, "priority": 9},
            {"name": "OffshoreLeaks", "kind": "offshoreleaks", "url_template": "https://offshoreleaks.icij.org/search?q={query}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "checko.ru", "kind": "checko", "url_template": "https://checko.ru/search/quick?query={query}", "supports_inn": True, "supports_name": True, "priority": 50},
            {"name": "zachestnyibiznes.ru", "kind": "zachestnyibiznes", "url_template": "https://zachestnyibiznes.ru/search?query={query}", "supports_inn": True, "supports_name": True, "priority": 50},
                        {"name": "sherlock", "kind": "sherlock", "url_template": "https://github.com/sherlock-project/sherlock/search?q={query}", "supports_inn": False, "supports_name": True, "priority": 50},
            {"name": "maigret", "kind": "maigret", "url_template": "https://github.com/soxoj/maigret/search?q={query}", "supports_inn": False, "supports_name": True, "priority": 50},
            {"name": "holehe", "kind": "holehe", "url_template": "https://github.com/megadose/holehe/search?q={query}", "supports_inn": False, "supports_name": True, "priority": 50},
            {"name": "theHarvester", "kind": "theharvester", "url_template": "https://github.com/laramies/theHarvester/search?q={query}", "supports_inn": False, "supports_name": True, "priority": 50},
        ]
        for provider in osint_providers:
            provider.setdefault("supports_url", False)
            provider.setdefault("is_person_source", True)
            if not any(p.get("name") == provider["name"] for p in self.SOURCE_PROVIDERS):
                self.SOURCE_PROVIDERS.append(provider)
        self.SOURCE_PROVIDERS.sort(key=lambda p: int(p.get("priority", 10)))

    def _extract_revenue(self, text: str) -> int:
        digits = re.sub(r"[^\d]", "", text or "")
        return int(digits) if digits else 0

    def _deep_values_for_keys(self, payload: Any, keys: set[str]) -> list[Any]:
        matches: list[Any] = []
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key in keys:
                    matches.append(value)
                matches.extend(self._deep_values_for_keys(value, keys))
        elif isinstance(payload, list):
            for item in payload:
                matches.extend(self._deep_values_for_keys(item, keys))
        return matches

    def _first_non_empty_deep_value(self, payload: Any, keys: set[str]) -> str:
        for value in self._deep_values_for_keys(payload, keys):
            text = self._normalize_spaces(str(value))
            if text:
                return text
        return ""

    def _is_inn_profile_complete(self, hits: list[dict[str, Any]]) -> bool:
        merged: dict[str, str] = {}
        for hit in hits:
            data = hit.get("data", {})
            if not isinstance(data, dict):
                continue
            for field in ("ru_org", "inn", "surname_ru", "name_ru"):
                if not merged.get(field):
                    merged[field] = self._normalize_spaces(str(data.get(field, "")))
        return bool(merged.get("ru_org") and merged.get("inn"))

    def _is_mojibake_text(self, value: str) -> bool:
        text = self._normalize_spaces(value)
        return "Ð" in text or "Ñ" in text

    def _is_valid_provider_payload(self, payload: dict[str, Any], hit_type: str) -> bool:
        if not isinstance(payload, dict):
            return False
        normalized_type = (hit_type or payload.get("type") or "").strip().lower()
        ru_org = self._normalize_spaces(str(payload.get("ru_org", "")))
        surname_ru = self._normalize_spaces(str(payload.get("surname_ru", "")))
        name_ru = self._normalize_spaces(str(payload.get("name_ru", "")))
        if self._is_mojibake_text(ru_org):
            return False
        if normalized_type == "person":
            return bool(surname_ru and name_ru)
        if normalized_type == "company":
            return bool(ru_org)
        if surname_ru and name_ru:
            return True
        return bool(ru_org)

    def _can_stop_provider_search(self, hits: list[dict[str, Any]], search_type: str, input_type: str) -> bool:
        merged: dict[str, str] = {}
        for hit in hits:
            data = hit.get("data", {})
            if not isinstance(data, dict):
                continue
            for field in ("inn", "ru_org", "surname_ru", "name_ru", "middle_name_ru", "ru_position", "family_name", "first_name"):
                if not merged.get(field):
                    merged[field] = self._normalize_spaces(str(data.get(field, "")))

        if merged.get("surname_ru") and merged.get("name_ru") and not self._is_valid_leader_fio(
            merged.get("surname_ru", ""), merged.get("name_ru", ""), merged.get("middle_name_ru", "")
        ):
            merged["surname_ru"] = ""
            merged["name_ru"] = ""

        if search_type == "person":
            return bool(merged.get("surname_ru") and merged.get("name_ru"))
        if search_type == "company" or input_type == INPUT_TYPE_INN:
            if bool(
                merged.get("inn")
                and merged.get("ru_org")
                and merged.get("surname_ru")
                and merged.get("name_ru")
                and merged.get("ru_position")
                and (merged.get("family_name") or merged.get("surname_ru"))
                and (merged.get("first_name") or merged.get("name_ru"))
            ):
                return True

        fns_hits = [h for h in hits if h.get("source") == "ФНС ЕГРЮЛ"]
        if fns_hits:
            fns_data = fns_hits[0].get("data", {})
            if (
                fns_data.get("inn")
                and fns_data.get("ru_org")
                and fns_data.get("ru_position")
                and fns_data.get("surname_ru")
                and fns_data.get("name_ru")
            ):
                return True
        return False

    def _merge_hits_with_scrapy_pipeline(self, hits: list[dict[str, Any]]) -> dict[str, Any]:
        payloads: list[dict[str, Any]] = []
        for hit in hits:
            data = hit.get("data", {})
            if not isinstance(data, dict):
                continue
            payloads.append(
                {
                    "source_name": hit.get("source", ""),
                    "ru_org": data.get("ru_org", ""),
                    "company_inn": data.get("inn", ""),
                    "company_ogrn": data.get("ogrn", ""),
                    "leader_surname_ru": data.get("surname_ru", ""),
                    "leader_name_ru": data.get("name_ru", ""),
                    "leader_middle_ru": data.get("middle_name_ru", ""),
                    "leader_position_ru": data.get("ru_position", ""),
                    "gender": data.get("gender", ""),
                }
            )
        if not payloads or scrapy_merge_provider_payloads is None:
            return {}
        return scrapy_merge_provider_payloads(payloads)

    def _extract_revenue_from_soup(self, soup: BeautifulSoup) -> int:
        rev_tag = soup.find("td", string=re.compile(r"Выручка|Доход|Revenue", re.IGNORECASE))
        if isinstance(rev_tag, Tag):
            next_td = rev_tag.find_next("td")
            if isinstance(next_td, Tag):
                return self._extract_revenue(next_td.get_text(" ", strip=True))
        text = soup.get_text(" ", strip=True)
        rev_match = re.search(r"(?:Выручка|Доход|Revenue)[^\d]{0,30}([\d\s.,]+)", text, flags=re.IGNORECASE)
        return self._extract_revenue(rev_match.group(1) if rev_match else "")

    def _score_hit(self, hit: dict[str, Any], query: str) -> float:
        data = hit.get("data", hit)
        q_lower = self._normalize_spaces(query.lower())
        fio = " ".join(x for x in [data.get("surname_ru", ""), data.get("name_ru", ""), data.get("middle_name_ru", "")] if x)
        fio_lower = self._normalize_spaces(fio.lower())

        score = 0.0
        if self.detect_input_type(query) == INPUT_TYPE_PERSON_TEXT and q_lower and fio_lower:
            q_words = sorted(q_lower.split())
            fio_words = sorted(fio_lower.split())
            if q_words == fio_words:
                score += 60
            if SequenceMatcher(None, q_lower, fio_lower).ratio() > 0.8:
                score += 60

        revenue = int(data.get("revenue", 0) or 0)
        if revenue > 1_000_000_000_000:
            score += 100
        elif revenue > 100_000_000_000:
            score += 80
        else:
            score += min(revenue / 1e5, 50)

        pos = self._normalize_spaces(str(data.get("ru_position", "")).lower())
        if "президент" in pos or "председатель" in pos or "директор" in pos:
            score += 50

        if hit.get("source") == "ФНС ЕГРЮЛ":
            score += 25

        if self.detect_input_type(query) == INPUT_TYPE_INN:
            inn = self._extract_inn(query)
            if inn and str(data.get("inn", "")) == inn:
                score += 100
        return score

    def _search_external_sources(
        self,
        raw: str,
        no_cache: bool = False,
        search_type: str = "",
        provider_names: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        self._init_thread_state()
        input_type = self.detect_input_type(raw)
        request_fingerprint = hashlib.sha1(f"{input_type}|{self._normalize_spaces(raw).lower()}|{search_type}|{int(no_cache)}".encode("utf-8")).hexdigest()
        active_key = f"external:{request_fingerprint}"
        with self._active_searches_lock:
            if active_key in self._active_searches:
                logger.warning("Пропущен дублирующийся активный запрос external:%s", request_fingerprint[:8])
                return [], ["Источник: дублирующийся активный запрос пропущен"]
            self._active_searches[active_key] = time.time()
        request_key = f"{input_type}:{raw.lower()}"
        last_started = self._last_search_time.get(request_key)
        if last_started is not None:
            time_diff = time.time() - last_started
            if time_diff < 10:
                logger.warning("Дубликат запроса %s через %.1f сек", request_key, time_diff)
        self._last_search_time[request_key] = time.time()
        logger.info("🔍 НАЧАЛО ПОИСКА: '%s' (Тип: %s, Режим: %s)", raw, input_type, search_type or "auto")
        hits: list[dict[str, Any]] = []
        trace: list[str] = [f"1. Тип ввода: {input_type}", f"2. Ключ поиска: {raw}"]
        hits_by_provider: dict[str, int] = {}
        providers = self._provider_chain(input_type, raw)
        if provider_names is not None:
            allowed = {name.strip() for name in provider_names if self._normalize_spaces(name)}
            providers = [provider for provider in providers if provider.get("name") in allowed]

        def load_provider(provider: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str]:
            started = time.perf_counter()
            try:
                data = self._call_provider(provider, raw, input_type, no_cache=no_cache, search_type=search_type)
            except (requests.Timeout, TimeoutError) as exc:
                logger.warning("Provider %s timeout for %s: %s", provider.get("name"), raw, exc)
                return provider["name"], [], "provider_timeout_skipped"
            provider_hits: list[dict[str, Any]] = []
            if isinstance(data, list):
                provider_hits = [
                    {
                        "source": provider["name"],
                        "url": item.get("url", ""),
                        "data": item,
                        "type": item.get("type", "unknown"),
                    }
                    for item in data
                    if item
                ]
            elif data:
                provider_hits = [{
                    "source": provider["name"],
                    "url": data.get("url", ""),
                    "data": data,
                    "type": data.get("type", "unknown"),
                }]

            provider_hits = [
                hit
                for hit in provider_hits
                if self._is_valid_provider_payload(hit.get("data", {}), str(hit.get("type", "")))
            ]

            for hit in provider_hits:
                hit_data = hit.get("data", {})
                logger.debug(
                    "Хит от %s: data keys=%s, ru_org=%s",
                    provider["name"],
                    list(hit_data.keys()) if isinstance(hit_data, dict) else "N/A",
                    hit_data.get("ru_org") if isinstance(hit_data, dict) else "N/A",
                )

            elapsed = time.perf_counter() - started
            logger.info("✅ %s нашел %d записей (%.2f сек)", provider["name"], len(provider_hits), elapsed)

            if provider_hits:
                return provider["name"], provider_hits, "provider_called_ok"
            fetch_status = getattr(self._thread_state, "last_fetch_status", FETCH_STATUS_EMPTY_OK)
            if fetch_status == FETCH_STATUS_BLOCKED_403:
                return provider["name"], [], "provider_blocked_403"
            if fetch_status == FETCH_STATUS_RATE_LIMIT_202:
                return provider["name"], [], "provider_rate_limited_202"
            if fetch_status == FETCH_STATUS_NETWORK_ERROR:
                return provider["name"], [], "provider_network_error"
            return provider["name"], [], "provider_called_empty"

        try:
            active_providers = [
                provider
                for provider in providers
                if self._should_call_provider(provider, input_type)
                and not (search_type == "person" and not provider.get("is_person_source", False))
            ]
            if active_providers:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {executor.submit(load_provider, provider): provider for provider in active_providers}
                    try:
                        for future in as_completed(futures, timeout=30):
                            try:
                                provider_name, provider_hits, state = future.result()
                            except Exception as exc:  # noqa: BLE001
                                logger.warning("Provider future error: %s", exc)
                                continue
                            if provider_hits:
                                hits.extend(provider_hits)
                            hits_by_provider[provider_name] = len(provider_hits)
                            icon = "✅" if provider_hits else "❌"
                            trace.append(f"{icon} Источник: {provider_name} — {state}")
                            if self._can_stop_provider_search(hits, search_type=search_type, input_type=input_type):
                                trace.append("⏹️ Ранняя остановка")
                                for pending in futures:
                                    pending.cancel()
                                break
                    except FuturesTimeoutError:
                        trace.append("⏱️ Источники: timeout ожидания результатов")
            for provider in providers:
                if not self._should_call_provider(provider, input_type):
                    continue
                hits_by_provider.setdefault(provider["name"], 0)

            trace.append("hits_by_provider: " + ", ".join(f"{k}={v}" for k, v in hits_by_provider.items()))
            if os.getenv("SCRAPY_PIPELINE_MERGE", "1").lower() in {"1", "true", "yes"} and hits:
                merged = self._merge_hits_with_scrapy_pipeline(hits)
                if merged:
                    trace.append("🧪 Scrapy pipeline: merged profile assembled")
            hits.sort(key=lambda item: self._score_hit(item, raw), reverse=True)
            if not hits:
                trace.append("Источники: не получено")
            logger.info("🏁 ПОИСК ЗАВЕРШЕН: Всего найдено %d записей (режим: %s)", len(hits), search_type or "auto")
            return hits, trace
        finally:
            with self._active_searches_lock:
                self._active_searches.pop(active_key, None)

    def _domain_throttle(self, url: str) -> None:
        host = urlparse(url).netloc.lower()
        if not host:
            return
        throttle_seconds = self._domain_throttle_seconds
        if host in {"rusprofile.ru", "www.rusprofile.ru"}:
            throttle_seconds = random.randint(*self._rusprofile_throttle_range)
        with self._domain_last_call_lock:
            last_call = self._domain_last_call.get(host, 0)
            wait_for = throttle_seconds - (time.time() - last_call)
            if wait_for > 0:
                logger.info("Throttle for %s: %.1f sec", host, wait_for)
                time.sleep(wait_for)
            self._domain_last_call[host] = time.time()

    def _is_captcha_or_block(self, response_text: str, url: str = "") -> bool:
        text_lower = response_text.lower()
        host = urlparse(url).netloc.lower()

        if host in {"ru.wikipedia.org", "wikipedia.org", "duckduckgo.com", "html.duckduckgo.com", "www.duckduckgo.com"}:
            strict_markers = ["cf-chl", "g-recaptcha", "hcaptcha", "just a moment", "access denied"]
            return any(marker in text_lower for marker in strict_markers)

        block_markers = [
            "captcha", "проверка браузера", "cloudflare", "ddos-guard",
            "access denied", "just a moment", "введите код", "подтвердите, что вы человек",
            "браузер не подходит", "включите javascript", "разрешите куки",
        ]
        if any(marker in text_lower for marker in block_markers):
            return True

        if "rusprofile.ru" in url.lower() and len(response_text) > 50000:
            has_data = any(token in text_lower for token in [
                "инн", "огрн", "наименование", "адрес", "директор",
                "выручка", "учредитель", "вид деятельности",
            ])
            if not has_data:
                return True

        if len(response_text) < 3000:
            has_structure = any(token in text_lower for token in [
                "<h1", "<table", "search-result", "/person/", "/id/",
                "инн", "огрн", "наименование",
            ])
            if not has_structure:
                return True

        return False

    def _fetch_page(self, url: str, timeout: int = 15, max_retries: int = 5) -> str | None:
        self._init_thread_state()
        if not self._normalize_spaces(url):
            logger.error("Provider generated empty URL, fetch skipped")
            return None
        timeout = max(15, timeout)
        self._thread_state.last_fetch_status = FETCH_STATUS_EMPTY_OK
        scrape_client = self._ensure_scrape_client()
        if scrape_client is None:
            try:
                response = self._http_session.get(url, timeout=timeout, allow_redirects=True)
                result_text = str(getattr(response, "text", ""))
                status_code = int(getattr(response, "status_code", 500))
                if status_code == 200 and not self._is_captcha_or_block(result_text, url=url):
                    return result_text
                if status_code == 403:
                    self._thread_state.last_fetch_status = FETCH_STATUS_BLOCKED_403
                elif status_code == 202:
                    self._thread_state.last_fetch_status = FETCH_STATUS_RATE_LIMIT_202
                else:
                    self._thread_state.last_fetch_status = FETCH_STATUS_NETWORK_ERROR
                logger.warning("Fallback fetch failed for %s: status=%d", url, status_code)
                return None
            except requests.RequestException as exc:
                self._thread_state.last_fetch_status = FETCH_STATUS_NETWORK_ERROR
                logger.warning("Fallback fetch request error for %s: %s", url, exc)
                return None

        result = scrape_client.fetch(url, timeout=timeout, max_retries=max(2, max_retries))
        logger.info(
            "Fetched %s mode=%s ok=%s status=%d error=%s len=%d",
            url,
            result.mode,
            result.ok,
            result.status_code,
            result.error_code or "-",
            len(result.text),
        )
        if result.status_code == 403:
            self._thread_state.last_fetch_status = FETCH_STATUS_BLOCKED_403
        elif result.status_code == 202:
            self._thread_state.last_fetch_status = FETCH_STATUS_RATE_LIMIT_202
        elif result.status_code >= 500 or result.error_code in {"network_error", "timeout", "request_error"}:
            self._thread_state.last_fetch_status = FETCH_STATUS_NETWORK_ERROR

        if result.blocked or self._is_captcha_or_block(result.text, url=url):
            host = urlparse(url).netloc.lower()
            self._thread_state.blocked_fetch = True
            self._thread_state.last_fetch_status = FETCH_STATUS_BLOCKED_403
            block_ttl = 12 * 60 * 60
            self._save_rate_limited(host or "source", f"blocked:{url}", retry_seconds=block_ttl)
            with self._provider_state_lock:
                self._provider_disabled_until[host or "source"] = time.time() + block_ttl
            logger.warning("%s returned captcha/block page", url)
            return None
        if not result.ok:
            logger.warning(
                "Failed to fetch %s: status=%d error_code=%s error=%s",
                url,
                result.status_code,
                result.error_code or "unknown",
                result.error or "",
            )
            return None
        if result.status_code != 200:
            logger.error("Failed to fetch %s, status code: %d", url, result.status_code)
            return None
        return result.text



    def _is_localhost(self, url: str) -> bool:
        host = urlparse(url).netloc.lower()
        return host.startswith("localhost") or host.startswith("127.0.0.1")

    def _get_random_user_agent(self) -> str:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/68.0",
        ]
        return random.choice(user_agents)

    def _get_random_headers(self, user_agent: str) -> dict[str, str]:
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(["ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "en-US,en;q=0.9,ru;q=0.8"]),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "DNT": "1",
        }
        if random.random() > 0.3:
            headers["Referer"] = random.choice(["https://www.google.com/", "https://yandex.ru/", "https://www.rusprofile.ru/"])
        return headers

    def _get_stealth_headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            ]),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "TE": "Trailers",
            "DNT": "1",
            "Referer": random.choice(["https://www.google.com/", "https://yandex.ru/"]),
        }
        return headers

    def _get_random_proxy(self) -> dict[str, str] | None:
        free_proxies: list[str] = []
        if free_proxies and random.random() > 0.3:
            proxy = random.choice(free_proxies)
            return {"http": proxy, "https": proxy}
        return None

    def _request(self, url: str, timeout: int = 20) -> requests.Response:
        self._domain_throttle(url)
        attempts = 3
        if not self._is_localhost(url):
            time.sleep(random.uniform(0.2, 0.6))
        last_exc: Exception | None = None
        for attempt in range(attempts):
            try:
                headers = self._get_random_headers(self._get_random_user_agent())
                response = self._http_session.get(url, timeout=timeout, headers=headers, verify=True, allow_redirects=True)
                if getattr(response, "apparent_encoding", None):
                    response.encoding = response.apparent_encoding
                return response
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as exc:
                last_exc = exc
                logger.warning("Request retry for %s (attempt %d/%d): %s", url, attempt + 1, attempts, exc)
                if attempt < attempts - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed request for {url}")

    def _parse_egrul(self, query: str) -> dict[str, Any] | None:
        if not re.fullmatch(r"\d{10,12}", query):
            return None
        url = f"https://egrul.itsoft.ru/{query}.json"
        try:
            resp = self._request(url, timeout=20)
            if not resp.ok:
                return None
            content_type = resp.headers.get("content-type", "")
            if "json" not in content_type and "javascript" not in content_type:
                return None
            data = resp.json()
            logger.debug("=== ФНС ЕГРЮЛ RAW DATA ===")
            logger.debug("TOP KEYS: %s", list(data.keys()))
            if data.get("СвЮЛ"):
                logger.debug("СвЮЛ KEYS: %s", list(data["СвЮЛ"].keys()) if isinstance(data["СвЮЛ"], dict) else "NOT DICT")
            if data.get("СведДолжнФЛ"):
                logger.debug("СведДолжнФЛ: %s", data["СведДолжнФЛ"][:2] if isinstance(data["СведДолжнФЛ"], list) else data["СведДолжнФЛ"])

            if not any([data.get("СвЮЛ"), data.get("company"), data.get("name"), data.get("НаимСокр")]):
                logger.warning("ФНС ЕГРЮЛ вернул пустую структуру для INN=%s", query)
                return None

            sv_yul = data.get("СвЮЛ") or data.get("company") or data.get("ЮЛ") or {}
            if not isinstance(sv_yul, dict):
                sv_yul = {}

            dol_candidates = []
            sv_dolzhn_fl = sv_yul.get("СведДолжнФЛ")
            if sv_dolzhn_fl is not None:
                if isinstance(sv_dolzhn_fl, list):
                    first_item = sv_dolzhn_fl[0] if sv_dolzhn_fl else None
                    first_keys = sorted(list(first_item.keys()))[:30] if isinstance(first_item, dict) else []
                    logger.info(
                        "FNS СвЮЛ.СведДолжнФЛ diag: type=list len=%d first_item_type=%s first_item_keys=%s",
                        len(sv_dolzhn_fl),
                        type(first_item).__name__ if first_item is not None else "None",
                        first_keys,
                    )
                elif isinstance(sv_dolzhn_fl, dict):
                    logger.info("FNS СвЮЛ.СведДолжнФЛ diag: type=dict keys=%s", sorted(list(sv_dolzhn_fl.keys()))[:30])
                else:
                    logger.info("FNS СвЮЛ.СведДолжнФЛ diag: type=%s", type(sv_dolzhn_fl).__name__)
            for k in ("СведДолжнФЛ", "СвДолжнФЛ", "СведДолжнФЛЮЛ", "СвРуководитель", "СведРуководитель"):
                v = sv_yul.get(k)
                if v is not None:
                    dol_candidates.append(v)
            dol_candidates += self._deep_values_for_keys(
                data,
                {"СведДолжнФЛ", "СвДолжнФЛ", "СведДолжнФЛЮЛ", "СвРуководитель", "СведРуководитель", "СвРукЮЛ", "Руководитель", "РукЮЛ"},
            )

            ru_org_raw = (
                data.get("НаимПолн")
                or data.get("НаимСокр")
                or sv_yul.get("НаимПолн")
                or data.get("name")
                or sv_yul.get("НаимСокр")
                or (sv_yul.get("НаимЮЛ") if isinstance(sv_yul.get("НаимЮЛ"), dict) else {}).get("ПолнНаим")
                or data.get("ru_org")
                or ""
            )
            if not self._normalize_spaces(str(ru_org_raw)):
                ru_org_raw = self._first_non_empty_deep_value(
                    data,
                    {"НаимПолн", "НаимСокр", "ПолнНаим", "КраткНаим", "Name", "name", "ru_org"},
                )
            ru_org_raw = self._pick_fns_ru_org_full(sv_yul, str(ru_org_raw or "")) or ru_org_raw
            ru_org = self._clean_ru_org_name(str(ru_org_raw).replace("ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО", "ПАО"))
            if ru_org:
                ru_org, _ = self.normalize_ru_org(ru_org)
            if not ru_org:
                logger.warning("ФНС: ru_org пустой для INN=%s", query)

            logger.info("FNS data keys: %s", sorted(list(data.keys()))[:200])
            for nested_key in ("СвЮЛ", "СвДолжнФЛ", "СведДолжнФЛ", "СведДолжнФЛЮЛ", "СвРуководитель", "СведРуководитель"):
                nested_value = data.get(nested_key)
                if isinstance(nested_value, dict):
                    logger.info("FNS nested %s: type=dict keys=%s", nested_key, sorted(list(nested_value.keys()))[:50])
                elif isinstance(nested_value, list):
                    first_item = nested_value[0] if nested_value else None
                    if isinstance(first_item, dict):
                        first_item_keys = sorted(list(first_item.keys()))[:50]
                    else:
                        first_item_keys = []
                    logger.info(
                        "FNS nested %s: type=list len=%d first_item_type=%s first_item_keys=%s",
                        nested_key,
                        len(nested_value),
                        type(first_item).__name__ if first_item is not None else "None",
                        first_item_keys,
                    )
                elif nested_value is not None:
                    logger.info("FNS nested %s: type=%s", nested_key, type(nested_value).__name__)

            director = data.get("director") or {}
            surname_ru = str(director.get("Фамилия") or director.get("surname") or director.get("surname_ru") or "")
            name_ru = str(director.get("Имя") or director.get("name") or director.get("name_ru") or "")
            middle_name_ru = str(director.get("Отчество") or director.get("patronymic") or director.get("middle_name_ru") or "")
            position = str(director.get("Должность") or director.get("position") or data.get("ru_position") or "")

            if not surname_ru or not name_ru or not middle_name_ru or not position:
                dol_list: list[dict[str, Any]] = []
                for candidate in dol_candidates:
                    if isinstance(candidate, list):
                        dol_list.extend([item for item in candidate if isinstance(item, dict)])
                    elif isinstance(candidate, dict):
                        dol_list.append(candidate)
                if dol_list:
                    def _is_leader_position(pos: str) -> bool:
                        p = (pos or "").lower()
                        return any(
                            x in p
                            for x in (
                                "генеральн",
                                "директор",
                                "президент",
                                "председатель правления",
                                "руководител",
                                "управляющ",
                                "chief executive",
                                "ceo",
                            )
                        )

                    head = None
                    for item in dol_list:
                        pos = self._first_non_empty_deep_value(item, {"НаимДолжн", "Должность", "НаимДолжнРук", "ru_position", "position"})
                        if _is_leader_position(str(pos or "")):
                            head = item
                            break
                    if head is None:
                        head = dol_list[0]
                    extracted_surname, extracted_name, extracted_middle = self._extract_fio_from_leader_obj(head)
                    if extracted_surname or extracted_name or extracted_middle:
                        surname_ru, name_ru, middle_name_ru = extracted_surname, extracted_name, extracted_middle
                    if not (surname_ru and name_ru):
                        fio_str = self._first_non_empty_deep_value(head, {"ФИО", "ФИОПолн", "head_ru"})
                        if fio_str:
                            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_str)
                    if not position:
                        dolzhn = head.get("СвДолжн") or {}
                        position = str((dolzhn.get("НаимДолжн") if isinstance(dolzhn, dict) else "") or head.get("Должность") or "")
                    if not position:
                        position = self._first_non_empty_deep_value(head, {"НаимДолжн", "Должность", "ru_position", "position", "НаимДолжнРук"})
                    if not (surname_ru and name_ru):
                        deep_head_fio = self._first_non_empty_deep_value(head, {"ФИО", "ФИОПолн", "НаимРук", "Руководитель"})
                        if deep_head_fio:
                            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(deep_head_fio)

            if not surname_ru:
                deep_fio = self._first_non_empty_deep_value(data, {"head_ru", "ФИО", "ФИОПолн"})
                if deep_fio:
                    surname_ru, name_ru, middle_name_ru = self._split_fio_ru(deep_fio)

            if not position:
                position = self._first_non_empty_deep_value(data, {"НаимДолжн", "Должность", "НаимДолжнРук", "ru_position", "position"})

            if surname_ru or name_ru or middle_name_ru or position:
                logger.info("ФНС leader parsed: %s %s %s / %s", surname_ru, name_ru, middle_name_ru, position)

            if not surname_ru:
                keywords = str(data.get("keywords") or data.get("meta_keywords") or "")
                if keywords:
                    surname_ru, name_ru, middle_name_ru = self._extract_fio_from_text(keywords)

            inn = str(
                data.get("inn")
                or data.get("ИННЮЛ")
                or (sv_yul.get("ИННЮЛ") if isinstance(sv_yul, dict) else "")
                or query
            ).replace(" ", "")
            ogrn = str(data.get("ogrn") or data.get("ОГРН") or sv_yul.get("ОГРН") or "")
            rev_raw = (
                data.get("revenue")
                or (data.get("ФинПоказ") or {}).get("Выручка")
                or (sv_yul.get("ФинПоказ") or {}).get("Выручка")
                or 0
            )
            gender = self._infer_gender(middle_name_ru, position)

            return {
                "url": url,
                "inn": inn or "",
                "ogrn": ogrn or "",
                "ru_org": ru_org or "",
                "en_org": "",
                "surname_ru": surname_ru or "",
                "name_ru": name_ru or "",
                "middle_name_ru": middle_name_ru or "",
                "gender": gender,
                "ru_position": (position or ""),
                "en_position": "",
                "revenue": self._extract_revenue(str(rev_raw)),
            }
        except Exception as exc:  # noqa: BLE001
            logger.error("EGRUL request failed for %s: %s", query, exc)
            return None

    def _search_rusprofile(self, query: str, is_person: bool = False, search_type: str = "") -> list[dict[str, str]]:
        search_url = f"https://www.rusprofile.ru/search?query={quote(query)}"

        if search_type == "company":
            is_person = False
            logger.debug("rusprofile search (ORG ONLY): %s", search_url)
        elif search_type == "person":
            is_person = True
            logger.debug("rusprofile search (PERSON ONLY): %s", search_url)
        else:
            logger.debug("rusprofile search (AUTO): %s", search_url)

        html = self._fetch_page(search_url, timeout=15, max_retries=2)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        hits: list[dict[str, str]] = []

        if search_type == "company":
            for a_tag in soup.find_all("a", href=re.compile(r"^/id/")):
                href = str(a_tag.get("href", ""))
                name = self._normalize_spaces(a_tag.get_text(" ", strip=True))
                if not name or not href:
                    continue
                parent = a_tag.find_parent()
                block_text = self._normalize_spaces(parent.get_text(" ", strip=True)) if isinstance(parent, Tag) else ""
                inn_match = re.search(r"\b(\d{10})\b", block_text)
                hits.append({
                    "source": "rusprofile.ru",
                    "type": "company",
                    "name": name,
                    "org": name,
                    "inn": inn_match.group(1) if inn_match else "",
                    "url": "https://www.rusprofile.ru" + href,
                })
        elif search_type == "person":
            for a_tag in soup.find_all("a", href=re.compile(r"^/person/")):
                href = str(a_tag.get("href", ""))
                name = self._normalize_spaces(a_tag.get_text(" ", strip=True))
                if not name or not href:
                    continue
                parent = a_tag.find_parent()
                block_text = self._normalize_spaces(parent.get_text(" ", strip=True)) if isinstance(parent, Tag) else ""
                inn_match = re.search(r"\b(\d{10}|\d{12})\b", block_text)
                org_link = parent.find("a", href=re.compile(r"^/id/")) if isinstance(parent, Tag) else None
                org_name = self._normalize_spaces(org_link.get_text(" ", strip=True)) if isinstance(org_link, Tag) else ""
                hits.append({
                    "source": "rusprofile.ru",
                    "type": "person",
                    "name": name,
                    "org": org_name,
                    "inn": inn_match.group(1) if inn_match else "",
                    "url": "https://www.rusprofile.ru" + href,
                })
        else:
            for a_tag in soup.find_all("a", href=re.compile(r"^/person/")):
                href = str(a_tag.get("href", ""))
                name = self._normalize_spaces(a_tag.get_text(" ", strip=True))
                if not name or not href:
                    continue
                parent = a_tag.find_parent()
                block_text = self._normalize_spaces(parent.get_text(" ", strip=True)) if isinstance(parent, Tag) else ""
                inn_match = re.search(r"\b(\d{10}|\d{12})\b", block_text)
                org_link = parent.find("a", href=re.compile(r"^/id/")) if isinstance(parent, Tag) else None
                org_name = self._normalize_spaces(org_link.get_text(" ", strip=True)) if isinstance(org_link, Tag) else ""
                hits.append({
                    "source": "rusprofile.ru",
                    "type": "person",
                    "name": name,
                    "org": org_name,
                    "inn": inn_match.group(1) if inn_match else "",
                    "url": "https://www.rusprofile.ru" + href,
                })
                if len(hits) >= 20:
                    break

            if not is_person:
                for a_tag in soup.find_all("a", href=re.compile(r"^/id/")):
                    href = str(a_tag.get("href", ""))
                    name = self._normalize_spaces(a_tag.get_text(" ", strip=True))
                    if not name or not href:
                        continue
                    parent = a_tag.find_parent()
                    block_text = self._normalize_spaces(parent.get_text(" ", strip=True)) if isinstance(parent, Tag) else ""
                    inn_match = re.search(r"\b(\d{10})\b", block_text)
                    hits.append({
                        "source": "rusprofile.ru",
                        "type": "company",
                        "name": name,
                        "org": name,
                        "inn": inn_match.group(1) if inn_match else "",
                        "url": "https://www.rusprofile.ru" + href,
                    })
                    if len(hits) >= 20:
                        break

            if is_person:
                hits = [h for h in hits if h.get("type") == "person"]

        logger.info("rusprofile hits: найдено %d записей (тип: %s)", len(hits), search_type or "auto")
        return hits[:20]

    def _parse_rusprofile(self, url: str) -> dict[str, Any]:
        if not re.match(r"https?://", url, flags=re.IGNORECASE):
            search_hits = self._search_rusprofile(url, is_person=is_person_query(url), search_type="")
            if search_hits and search_hits[0].get("url"):
                url = search_hits[0]["url"]
        html = self._fetch_page(url, timeout=20, max_retries=2)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        profile: dict[str, Any] = {"url": url, "source": "rusprofile.ru"}
        page_text = soup.get_text(" ", strip=True)
        jsonld_data: dict[str, Any] = {}
        keywords_node = soup.find("meta", attrs={"name": re.compile(r"^keywords$", re.IGNORECASE)})
        keywords_raw = str(keywords_node.get("content", "")) if isinstance(keywords_node, Tag) else ""
        keywords_text = self._normalize_spaces(unescape(keywords_raw))
        if keywords_text:
            profile["ru_org"] = self._extract_ru_org_from_keywords(keywords_text)
            logger.debug("RusProfile keywords: %s", keywords_text[:200])
        for script in soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.IGNORECASE)}):
            if not isinstance(script, Tag):
                continue
            raw_json = script.string or script.get_text(" ", strip=True)
            if not raw_json:
                continue
            try:
                payload = json.loads(raw_json)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, list):
                payload = next((item for item in payload if isinstance(item, dict)), {})
            if isinstance(payload, dict):
                jsonld_data = payload
                if isinstance(payload.get("name"), str):
                    profile["ru_org"] = profile.get("ru_org") or self._clean_ru_org_name(payload.get("name", ""))
                identifier = payload.get("identifier")
                if isinstance(identifier, dict) and isinstance(identifier.get("value"), str):
                    profile["inn"] = profile.get("inn") or re.sub(r"\D", "", identifier["value"])
                elif isinstance(payload.get("taxID"), str):
                    profile["inn"] = profile.get("inn") or re.sub(r"\D", "", payload["taxID"])
        is_person = "/person/" in url or "/ip/" in url

        if is_person:
            structure = self._detect_page_structure(soup)
            if structure == "old":
                self._parse_rusprofile_old(soup, profile)
            elif structure == "new":
                self._parse_rusprofile_new(soup, profile)
            else:
                self._parse_rusprofile_fallback(soup, profile)
            patronymic = profile["middle_name_ru"]
            profile["gender"] = "М" if patronymic.lower().endswith(("вич", "ич")) else "Ж" if patronymic.lower().endswith("вна") else ""
            inn_match = re.search(r"ИНН[:\s]*(\d{10,12})", page_text)
            if not inn_match:
                inn_match = re.search(r"Идентификационный номер налогоплательщика[:\s]*(\d{10,12})", page_text)
            profile["inn"] = inn_match.group(1) if inn_match else ""

            org_text = self._select_first_text(
                soup,
                [
                    "a[href^='/id/']",
                    ".company-links a[href^='/id/']",
                    ".company-card__title a[href^='/id/']",
                ],
            )
            org_match = re.search(r"\b(ПАО|АО|ООО|ОАО|ЗАО|ФГУП|ФГБУ|АНО|МУП|НКО|ИП)\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-]+(?:\s+[А-ЯЁа-яёA-Za-z0-9\-]+)*)[»\"]?", org_text or page_text)
            if org_match:
                raw_org = f"{org_match.group(1)} {org_match.group(2).strip()}"
                if not re.search(r"^(НЕ|НЕТ|ЛИКВИДИРОВАНО)", raw_org, flags=re.IGNORECASE) and not RUSPROFILE_NOISE_RE.search(raw_org):
                    profile["ru_org"] = self._clean_ru_org_name(raw_org)
            else:
                org_element = soup.find("div", class_=re.compile(r"(company|organization)", re.IGNORECASE))
                if org_element:
                    org_text = self._normalize_spaces(org_element.get_text(" ", strip=True))
                    org_match = re.search(r"\b(ПАО|АО|ООО|ОАО|ЗАО)\s+([^\(]+)", org_text)
                    if org_match:
                        profile["ru_org"] = self._clean_ru_org_name(f"{org_match.group(1)} {org_match.group(2).strip()}")

            position_text = self._select_first_text(
                soup,
                [
                    ".person-main-info__position",
                    ".company-main-info__position",
                    "[data-test='position']",
                ],
            )
            pos_match = re.search(
                r"(Генеральный директор|Президент|Председатель правления|Председатель|Директор|Руководитель|Ректор|Заместитель)\s*([А-ЯЁа-яё\s,]{0,40}?)",
                position_text or page_text,
                flags=re.IGNORECASE,
            )
            if pos_match:
                pos_text = pos_match.group(0).strip()
                if not RUSPROFILE_NOISE_RE.search(pos_text):
                    pos_text = re.sub(r"(Факторы риска|Дисквалификация|Нахождение под|Общие сведения|Связи|Регион регистрации)", "", pos_text, flags=re.IGNORECASE).strip()
                    pos_text = re.sub(r"[.,]+$", "", pos_text)
                    profile["ru_position"] = pos_text
            else:
                position_element = soup.find("div", class_=re.compile(r"(position|post)", re.IGNORECASE))
                if position_element:
                    position_text = self._normalize_spaces(position_element.get_text(" ", strip=True))
                    pos_match = re.search(
                        r"(Генеральный директор|Президент|Председатель правления|Председатель|Директор|Руководитель|Ректор|Заместитель)\s*([А-ЯЁа-яё\s,]{0,40}?)",
                        position_text,
                        flags=re.IGNORECASE,
                    )
                    if pos_match:
                        pos_text = pos_match.group(0).strip().split(".")[0].split(",")[0]
                        if not RUSPROFILE_NOISE_RE.search(pos_text):
                            profile["ru_position"] = pos_text

            if not (profile.get("surname_ru") and profile.get("ru_org")):
                logger.warning("rusprofile structure drift suspected for %s", url)

            rev_match = re.search(r"([\d\s]+(?:[.,]\d+)?)\s*млн\s*руб", page_text)
            if rev_match:
                try:
                    profile["revenue"] = int(float(rev_match.group(1).replace(" ", "").replace(",", ".")) * 1_000_000)
                except ValueError:
                    profile["revenue"] = 0
            else:
                profile["revenue"] = self._extract_revenue_from_soup(soup)
        else:
            profile["ru_org"] = self._extract_ru_org_from_keywords(keywords_text)
            if not profile.get("ru_org"):
                profile["ru_org"] = self._clean_ru_org_name(str(jsonld_data.get("name") or ""))
            if not profile.get("ru_org"):
                title = soup.find("h1", class_=re.compile(r"(company|org)", re.IGNORECASE))
                if not title:
                    title = soup.find("div", class_=re.compile(r"(company|org)", re.IGNORECASE))
                if not title:
                    title = soup.find("h1")
                profile["ru_org"] = self._clean_ru_org_name(title.get_text(strip=True) if isinstance(title, Tag) else "")
            inn_match = (
                re.search(r"ИНН[:\s]*(\d{10,12})", page_text)
                or re.search(r"Идентификационный номер налогоплательщика[:\s]*(\d{10,12})", page_text)
                or re.search(r"ИНН[:\s]*(\d{10,12})", keywords_text)
                or re.search(r"\bИНН\s*(\d{10,12})\b", page_text, flags=re.IGNORECASE)
            )
            profile["inn"] = inn_match.group(1) if inn_match else ""
            if not profile.get("inn") and isinstance(jsonld_data.get("taxID"), str):
                profile["inn"] = re.sub(r"\D", "", jsonld_data["taxID"])

            ogrn_match = re.search(r"ОГРН[:\s]*(\d{13,15})", page_text) or re.search(r"ОГРН[:\s]*(\d{13,15})", keywords_text)
            if ogrn_match:
                profile["ogrn"] = ogrn_match.group(1)
            okpo_match = re.search(r"ОКПО[:\s]*(\d{8,10})", page_text) or re.search(r"ОКПО[:\s]*(\d{8,10})", keywords_text)
            if okpo_match:
                profile["okpo"] = okpo_match.group(1)

            profile["revenue"] = self._extract_revenue_from_soup(soup)
            head_block = re.search(r"Руководитель\s+([А-ЯЁа-яё\-,\s]{3,120}?)\s+([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", page_text)
            fio_match = None
            if head_block:
                ru_position = self._normalize_position_ru(head_block.group(1))
                if ru_position:
                    profile["ru_position"] = ru_position
                fio_match = re.search(r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", head_block.group(2))
            if not fio_match:
                for pattern in [
                    r"Руководитель[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
                    r"Генеральный директор[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
                    r"Директор[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
                    r"Ректор[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
                ]:
                    fio_match = re.search(pattern, page_text)
                    if fio_match:
                        break
            if not fio_match:
                director_element = soup.find("div", class_=re.compile(r"(director|rukovoditel)", re.IGNORECASE))
                if director_element:
                    director_text = self._normalize_spaces(director_element.get_text(" ", strip=True))
                    fio_match = re.search(r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", director_text)
                    if not profile.get("ru_position"):
                        before_fio = director_text.split(fio_match.group(1))[0] if fio_match else ""
                        profile["ru_position"] = self._normalize_position_ru(before_fio)
            if not fio_match and keywords_text:
                surname_ru, name_ru, middle_name_ru = self._extract_fio_from_text(keywords_text)
                if surname_ru and name_ru:
                    profile.update({"surname_ru": surname_ru, "name_ru": name_ru, "middle_name_ru": middle_name_ru})

            chief_title = self._select_first_text(soup, [".chief-title", ".company-info__item .chief-title"])
            if chief_title:
                profile["ru_position"] = self._normalize_position_ru(chief_title)
            if not profile.get("ru_position"):
                position_match = re.search(
                    r"(Президент|Председатель правления|Генеральный директор|Директор|Руководитель|Ректор)[^\w]{0,30}",
                    page_text,
                )
                if position_match:
                    profile["ru_position"] = self._normalize_position_ru(position_match.group(1))
            if fio_match:
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_match.group(1))
                profile.update({"surname_ru": surname_ru, "name_ru": name_ru, "middle_name_ru": middle_name_ru})
            if profile.get("middle_name_ru") and not profile.get("gender"):
                profile["gender"] = self._infer_gender(profile["middle_name_ru"], profile.get("ru_position", ""))
        return profile

    def _detect_page_structure(self, soup: BeautifulSoup) -> str:
        if soup.find("h1", class_="fio-element") or soup.find("div", class_="fio-element"):
            return "old"
        if soup.find("div", class_="person-main-info-position") or soup.find("div", class_="person-main-info__position"):
            return "new"
        return "unknown"

    def _parse_rusprofile_old(self, soup: BeautifulSoup, profile: dict[str, Any]) -> None:
        h1 = soup.find("h1", class_=re.compile(r"(fio|person-name)", re.IGNORECASE))
        if not h1:
            h1 = soup.find("div", class_=re.compile(r"(fio|person-name)", re.IGNORECASE))
        full_name = self._normalize_spaces(h1.get_text(" ", strip=True)) if isinstance(h1, Tag) else ""
        parts = full_name.split()
        profile["surname_ru"] = parts[0] if parts else ""
        profile["name_ru"] = parts[1] if len(parts) > 1 else ""
        profile["middle_name_ru"] = parts[2] if len(parts) > 2 else ""

    def _parse_rusprofile_new(self, soup: BeautifulSoup, profile: dict[str, Any]) -> None:
        name_element = soup.find("h1")
        if not isinstance(name_element, Tag):
            name_element = soup.select_one(".person-main-info__name, .person-main-info-name, .person-main-info")
        full_name = self._normalize_spaces(name_element.get_text(" ", strip=True)) if isinstance(name_element, Tag) else ""
        parts = full_name.split()
        profile["surname_ru"] = parts[0] if parts else ""
        profile["name_ru"] = parts[1] if len(parts) > 1 else ""
        profile["middle_name_ru"] = parts[2] if len(parts) > 2 else ""
        position_element = soup.find("div", class_=re.compile(r"position", re.IGNORECASE))
        if isinstance(position_element, Tag):
            profile["ru_position"] = self._normalize_spaces(position_element.get_text(" ", strip=True))

    def _parse_rusprofile_fallback(self, soup: BeautifulSoup, profile: dict[str, Any]) -> None:
        possible_name_selectors = ["h1.fio-element", "div.person-main-info-position", "div.fio", "div.person-name", "h1"]
        for selector in possible_name_selectors:
            name_element = soup.select_one(selector)
            if not isinstance(name_element, Tag):
                continue
            full_name = self._normalize_spaces(name_element.get_text(" ", strip=True))
            parts = full_name.split()
            if len(parts) >= 2:
                profile["surname_ru"] = parts[0]
                profile["name_ru"] = parts[1]
                profile["middle_name_ru"] = parts[2] if len(parts) > 2 else ""
                break

    def _normalize_for_comparison(self, text: str) -> str:
        normalized = text.lower()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        normalized = re.sub(r"\bи\b", "", normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    def _company_name_matches(self, org_name: str, search_query: str) -> bool:
        if not org_name or not search_query:
            return False

        normalized_org = self._normalize_for_comparison(org_name)
        normalized_query = self._normalize_for_comparison(search_query)
        if normalized_query in normalized_org:
            return True

        if "сбер" in normalized_query:
            return "сбербанк" in normalized_org or "сбер" in normalized_org

        opf_map = {
            "пао": ["пао", "публичное акционерное общество"],
            "ао": ["ао", "акционерное общество"],
            "ооо": ["ооо", "общество с ограниченной ответственностью"],
            "ип": ["ип", "индивидуальный предприниматель"],
        }

        for opf_variants in opf_map.values():
            if any(variant in normalized_query for variant in opf_variants):
                return any(variant in normalized_org for variant in opf_variants)
        return False

    def _call_provider_with_retry(
        self,
        provider: dict[str, Any],
        query: str,
        input_type: str,
        max_retries: int = 0,
        search_type: str = "",
    ) -> list[dict[str, Any]]:
        for attempt in range(max_retries + 1):
            try:
                result = self._call_provider(provider, query, input_type, search_type=search_type)
                if isinstance(result, list):
                    return result
                if isinstance(result, dict):
                    return [result]
                return []
            except Exception as exc:  # noqa: BLE001
                if attempt < max_retries:
                    wait_time = (attempt + 1) * 2
                    logger.warning(
                        "Provider %s failed (attempt %d/%d), retrying in %d sec: %s",
                        provider.get("name"),
                        attempt + 1,
                        max_retries + 1,
                        wait_time,
                        str(exc),
                    )
                    time.sleep(wait_time)
                    continue
                logger.error("Provider %s failed after %d attempts: %s", provider.get("name"), max_retries + 1, str(exc))
        return []

    def _generate_company_name_variants(self, company_name: str) -> list[str]:
        variants = [company_name]
        opf_patterns = [r"\bПАО\b", r"\bАО\b", r"\bООО\b", r"\bОАО\b", r"\bЗАО\b", r"\bИП\b", r"\bФГУП\b", r"\bФГБУ\b"]
        for pattern in opf_patterns:
            if re.search(pattern, company_name, re.IGNORECASE):
                variant = re.sub(pattern, "", company_name, flags=re.IGNORECASE).strip()
                if variant and variant != company_name:
                    variants.append(variant)

        if "сбер" in company_name.lower():
            variants.extend(["Сбербанк ПАО", "ПАО Сбербанк"])

        return list(dict.fromkeys(variants))

    def _select_first_text(self, soup: BeautifulSoup, selectors: list[str]) -> str:
        for selector in selectors:
            node = soup.select_one(selector)
            if isinstance(node, Tag):
                text = self._normalize_spaces(node.get_text(" ", strip=True))
                if text:
                    return text
        return ""

    def _handle_provider_error(self, provider_name: str, error: Exception) -> str:
        error_id = str(uuid.uuid4())
        logger.error("Provider %s failed (%s): %s", provider_name, error_id, error, exc_info=True)
        with self._connect() as db:
            db.execute(
                "INSERT INTO provider_errors(provider_name,error_type,error_details,created_at) VALUES(?,?,?,?)",
                (provider_name, type(error).__name__, str(error), self._now()),
            )
            db.commit()
        if isinstance(error, requests.HTTPError) and getattr(error.response, "status_code", None) == 429:
            return f"provider_error:{error_id}:rate_limited"
        if isinstance(error, requests.ConnectionError):
            return f"provider_error:{error_id}:connection_error"
        return f"provider_error:{error_id}:{type(error).__name__}"

    def _collect_rusprofile_profiles(
        self,
        query: str,
        input_type: str,
        is_person: bool = False,
        search_type: str = "",
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        person_mode = is_person or input_type == INPUT_TYPE_PERSON_TEXT or is_person_query(query)
        if input_type == INPUT_TYPE_URL and "rusprofile.ru" in query and ("/person/" in query or "/ip/" in query):
            person_mode = True

        if input_type == INPUT_TYPE_URL and "rusprofile.ru" in query:
            profile = self._parse_rusprofile(query)
            return profile or None

        hits = self._search_rusprofile(query, is_person=person_mode, search_type=search_type)
        profiles: list[dict[str, Any]] = []
        if not hits and input_type == INPUT_TYPE_INN:
            inn = self._extract_inn(query)
            direct_urls = [f"https://www.rusprofile.ru/id/{inn}", f"https://www.rusprofile.ru/ip/{inn}"] if inn else []
            hits = [{"url": url} for url in direct_urls]

        def parse_single_hit(hit: dict[str, Any]) -> dict[str, Any] | None:
            if not hit.get("url"):
                return None
            try:
                profile = self._parse_rusprofile(hit["url"])
                if not profile:
                    return None
                if hit.get("org") and not profile.get("ru_org"):
                    profile["ru_org"] = hit.get("org", "")
                if hit.get("position") and not profile.get("ru_position"):
                    profile["ru_position"] = hit.get("position", "")
                if not profile.get("surname_ru") and hit.get("name"):
                    sur, nam, patr = self._split_fio_ru(str(hit.get("name", "")))
                    profile["surname_ru"] = sur
                    profile["name_ru"] = nam
                    profile["middle_name_ru"] = patr
                return profile
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse %s: %s", hit.get("url", ""), exc)
                return None

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(parse_single_hit, hit): hit for hit in hits[:10]}
            try:
                for future in as_completed(futures, timeout=60):
                    result = future.result()
                    if result:
                        profiles.append(result)
            except FuturesTimeoutError:
                logger.warning("RusProfile parse timeout for query: %s", query)

        logger.info("RusProfile: найдено %d профилей из %d хитов", len(profiles), len(hits))

        if person_mode:
            return profiles
        return profiles[0] if profiles else None

    def _parse_kontur(self, query: str) -> dict[str, Any] | None:
        if self.detect_input_type(query) != INPUT_TYPE_INN:
            return None
        url = f"https://focus.kontur.ru/entity?query={quote(query)}"
        time.sleep(0.2)
        resp = self._request(url)
        if not resp.ok:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        h1 = soup.find("h1")
        ru_org = self._clean_ru_org_name(h1.get_text(strip=True) if isinstance(h1, Tag) else "")
        director_block = soup.select_one(".director-block")
        director_text = director_block.get_text(" ", strip=True) if isinstance(director_block, Tag) else ""
        surname_ru, name_ru, middle_name_ru = self._split_fio_ru(director_text)
        return {
            "url": url,
            "ru_org": ru_org,
            "surname_ru": surname_ru,
            "name_ru": name_ru,
            "middle_name_ru": middle_name_ru,
            "inn": self._extract_inn(query),
            "revenue": self._extract_revenue_from_soup(soup),
        }

    def _parse_open_corporates(self, url: str) -> dict[str, Any]:
        html = self._fetch_page(url, timeout=20, max_retries=2)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        org_name = self._select_first_text(soup, [".company-name", ".company-header h1", "h1"])
        director = self._select_first_text(soup, [".officer-name", ".director-name"])
        position = self._select_first_text(soup, [".officer-role", ".director-position"])
        return self._build_osint_profile(url, "open-corporates", org_name, director, position, soup.get_text(" ", strip=True))

    def _parse_sparks(self, url: str) -> dict[str, Any]:
        html = self._fetch_page(url, timeout=20, max_retries=2)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        org_name = self._select_first_text(soup, [".company-title", ".company-name h1", "h1"])
        director = self._select_first_text(soup, [".director-name", ".company-director", ".company-head"])
        position = self._select_first_text(soup, [".director-position", ".company-position"]) or "Генеральный директор"
        return self._build_osint_profile(url, "sparks", org_name, director, position, soup.get_text(" ", strip=True))

    def _parse_generic_osint(self, url: str, source: str) -> dict[str, Any]:
        html = self._fetch_page(url, timeout=20, max_retries=2)
        if not html:
            return {}
        final_url = url
        if "wikipedia" in final_url.lower():
            html_lower = html.lower()
            if any(marker in html_lower for marker in [
                "результаты поиска",
                "страница значений",
                "в википедии есть статьи о других людях",
            ]):
                return {}
            if "index.php?search=" in final_url.lower():
                return {}
        soup = BeautifulSoup(html, "lxml")
        org_name = self._select_first_text(soup, ["h1", ".company-name", ".org-name", ".entity-title"])
        director = self._select_first_text(
            soup,
            [
                ".director-name",
                ".company-director",
                ".manager-name",
                ".company-info__director",
                ".company-card__manager-name",
                "[itemprop='employee']",
            ],
        )
        position = self._select_first_text(
            soup,
            [
                ".director-position",
                ".manager-position",
                ".position",
                ".company-card__manager-position",
                "[itemprop='jobTitle']",
            ],
        )
        return self._build_osint_profile(url, source, org_name, director, position, soup.get_text(" ", strip=True))

    def _parse_from_ddg_site(self, raw_query: str, domains: list[str], source: str) -> dict[str, Any]:
        target_urls = self._ddg_site_search(raw_query, domains, max_urls=3)
        for target_url in target_urls:
            profile = self._parse_generic_osint(target_url, source)
            if profile and (profile.get("ru_org") or profile.get("inn") or profile.get("surname_ru")):
                profile["url"] = target_url
                return profile
        return {}

    def _parse_zachestnyibiznes_direct(self, raw_query: str) -> dict[str, Any]:
        query = self._normalize_spaces(raw_query)
        if not query:
            return {}
        search_url = f"https://zachestnyibiznes.ru/search?query={quote(query)}"
        html = self._fetch_page(search_url, timeout=20, max_retries=2)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        links = [a.get("href") for a in soup.select("a[href*='/company/ul/']") if a.get("href")]
        if not links and "/company/ul/" in html:
            raw_match = re.search(r"(/company/ul/[\w\-_/]+)", html)
            if raw_match:
                links = [raw_match.group(1)]
        if not links:
            return {}
        card_url = str(links[0])
        if card_url.startswith("/"):
            card_url = f"https://zachestnyibiznes.ru{card_url}"
        profile = self._parse_generic_osint(card_url, "zachestnyibiznes.ru")
        ru_position = self._normalize_spaces(str(profile.get("ru_position", "")))
        if ru_position:
            tokens = [token.lower() for token in re.findall(r"[А-Яа-яЁёA-Za-z-]+", ru_position)]
            if any(token in FIO_STOP_TOKENS for token in tokens):
                profile["ru_position"] = ""
        if self._is_garbage_org_title(str(profile.get("ru_org", "")), query):
            return {}
        return profile

    def _parse_rbc_companies_direct(self, raw_query: str) -> dict[str, Any]:
        query = self._normalize_spaces(raw_query)
        if not query:
            return {}
        search_url = f"https://companies.rbc.ru/search/?query={quote(query)}"
        html = self._fetch_page(search_url, timeout=20, max_retries=2)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        link = soup.select_one("a[href*='/id/'], a[href*='/company/']")
        if not isinstance(link, Tag):
            return {}
        href = str(link.get("href") or "")
        if href.startswith("/"):
            href = f"https://companies.rbc.ru{href}"
        profile = self._parse_generic_osint(href, "companies.rbc.ru")
        if self._is_garbage_org_title(str(profile.get("ru_org", "")), query):
            return {}
        return profile

    def _ddg_site_search(self, raw_query: str, domains: list[str], max_urls: int = 3) -> list[str]:
        query_text = self._normalize_spaces(raw_query)
        if not query_text:
            return []
        cache_key = f"{query_text.lower()}|{'|'.join(sorted(domains))}|{max_urls}"
        cached = self._ddg_query_cache.get(cache_key)
        if cached and time.time() < cached[0]:
            return list(cached[1])
        site_query = " OR ".join(f"site:{domain}" for domain in domains)
        search_url = f"https://duckduckgo.com/html/?q={quote(f'{site_query} {query_text}') }"
        html = self._fetch_page(search_url, timeout=20, max_retries=2)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []
        for anchor in soup.select("a.result__a, a[href^='http']"):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            parsed = urlparse(href)
            host = parsed.netloc.lower()
            if not host:
                continue
            if any(domain in host for domain in domains):
                cleaned = href.split("&rut=", 1)[0]
                if cleaned not in urls:
                    urls.append(cleaned)
            if len(urls) >= max_urls:
                break
        self._ddg_query_cache[cache_key] = (time.time() + 120, list(urls))
        return urls

    def _build_osint_profile(self, url: str, source: str, org_name: str, director: str, position: str, page_text: str) -> dict[str, Any]:
        surname_ru, name_ru, middle_name_ru = "", "", ""
        if director:
            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(director)

        cleaned_position, pos_surname, pos_name, pos_middle, _ = self._clean_position_and_extract_fio(position)
        if cleaned_position:
            position = cleaned_position
        if not (surname_ru and name_ru) and pos_surname and pos_name:
            surname_ru, name_ru, middle_name_ru = pos_surname, pos_name, pos_middle

        text = self._normalize_spaces(page_text)
        if not self._is_valid_leader_fio(surname_ru, name_ru, middle_name_ru):
            surname_ru = name_ru = middle_name_ru = ""
        if not surname_ru:
            leader_fio, leader_position = self._extract_leader_from_labeled_text(text)
            if leader_fio:
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(leader_fio)
                if leader_position and not position:
                    position = leader_position
        if not surname_ru:
            leader_match = re.search(
                r"(?:Руководитель|Генеральный директор|Президент|Председатель правления|Ректор)\s*[:—\-]\s*(?!юридического\s+лица|история)([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+(?:\s+[А-ЯЁ][а-яё-]+)?)",
                text,
                flags=re.IGNORECASE,
            )
            if leader_match:
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(leader_match.group(1))
                if not self._is_valid_leader_fio(surname_ru, name_ru, middle_name_ru):
                    surname_ru = name_ru = middle_name_ru = ""
        if not position:
            pos_match = re.search(
                r"(?:Руководитель|Ректор|Должность)[:\s-]*([А-ЯЁа-яё\s,\-]{5,120})",
                text,
            )
            if pos_match:
                position = self._normalize_spaces(pos_match.group(1).split("ИНН")[0])

        cleaned_position, pos_surname, pos_name, pos_middle, _ = self._clean_position_and_extract_fio(position)
        if cleaned_position:
            position = cleaned_position
        if not (surname_ru and name_ru) and pos_surname and pos_name:
            surname_ru, name_ru, middle_name_ru = pos_surname, pos_name, pos_middle

        inn = ""
        inn_match = re.search(r"ИНН[:\s]*(\d{10,12})", text)
        if inn_match:
            inn = inn_match.group(1)
        return {
            "url": url,
            "source": source,
            "ru_org": self._clean_ru_org_name(org_name),
            "surname_ru": surname_ru,
            "name_ru": name_ru,
            "middle_name_ru": middle_name_ru,
            "ru_position": self._normalize_positions_ru(position)[0] if position else "",
            "inn": inn,
        }

    def _parse_url_detail(self, raw_url: str) -> dict[str, Any] | None:
        if self.detect_input_type(raw_url) != INPUT_TYPE_URL:
            return None
        try:
            resp = self._request(raw_url)
            if not resp.ok:
                return None
            soup = BeautifulSoup(resp.text, "lxml")
            title = soup.find("h1")
            page_text = soup.get_text(" ", strip=True)
            ru_org = self._clean_ru_org_name(title.get_text(" ", strip=True) if isinstance(title, Tag) else "")
            surname_ru = name_ru = middle_name_ru = ""
            fio_match = re.search(r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", page_text)
            if fio_match:
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_match.group(1))
            return {
                "url": raw_url,
                "ru_org": ru_org,
                "surname_ru": surname_ru,
                "name_ru": name_ru,
                "middle_name_ru": middle_name_ru,
                "inn": self._extract_inn(page_text),
                "revenue": self._extract_revenue_from_soup(soup),
            }
        except Exception as exc:  # noqa: BLE001
            logger.error("URL parse failed for %s: %s", raw_url, exc)
            return None

    def _retry_reason(self, provider_name: str) -> str:
        retry_after_minutes = 10
        retry_at = datetime.now(timezone.utc).timestamp() + retry_after_minutes * 60
        retry_at_iso = datetime.fromtimestamp(retry_at, tz=timezone.utc).isoformat()
        return f"retry_at={retry_at_iso}; provider={provider_name}"

    def _provider_fallback_from_catalog(self, provider_name: str, normalized: str, inn: str) -> tuple[dict[str, Any] | None, str, str]:
        return None, "empty", "no catalog fallback"

    def _fetch_inn_fixture(self, provider_name: str, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if not inn:
            return self._provider_fallback_from_catalog(provider_name, normalized, inn)
        return self._provider_fallback_from_catalog(provider_name, normalized, inn)

    def _infer_gender(self, middle_name_ru: str, ru_position: str) -> str:
        token = self._normalize_spaces(middle_name_ru).lower()
        position = self._normalize_spaces(ru_position).lower()
        if token.endswith(("ич", "оглы")):
            return "М"
        if token.endswith(("вна", "кызы")):
            return "Ж"
        if "директор" in position or "председатель" in position:
            return "М"
        return ""

    def _enrich_provider_payload(self, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if not payload:
            return payload
        data = payload.setdefault("data", {})
        fio_ru = str(data.get("head_ru", "")).strip()
        if fio_ru and not data.get("surname_ru") and not data.get("name_ru"):
            sur, nam, patr = self._split_fio_ru(fio_ru)
            data["surname_ru"] = sur
            data["name_ru"] = nam
            data["middle_name_ru"] = patr
        if not data.get("gender"):
            data["gender"] = self._infer_gender(str(data.get("middle_name_ru", "")), str(data.get("ru_position", "")))
        return payload

    def _throttle_acquire(self, domain: str) -> bool:
        with self._domain_last_call_lock:
            now = time.time()
            last = self._domain_last_call.get(domain, 0)
            if now - last < self._domain_throttle_seconds:
                time.sleep(self._domain_throttle_seconds)
            self._domain_last_call[domain] = now
        return True

    def _save_rate_limited(self, provider_name: str, key: str, retry_seconds: int = 300) -> None:
        cache_key = f"{provider_name}:{key}"
        with self._source_cache_lock:
            self._source_cache[cache_key] = {
                "ts": time.time(),
                "state": "rate_limited",
                "retry_at": time.time() + retry_seconds,
                "reason": f"429 → retry after {retry_seconds}s",
            }

    def _fetch_from_egrul(self, inn: str) -> tuple[dict[str, Any] | None, str, str]:
        # deprecated: kept for backward compatibility; use _parse_egrul in provider flow.
        if not re.fullmatch(r"\d{10,12}", inn):
            return None, "empty", "invalid inn"
        data = self._parse_egrul(inn)
        if not data:
            fallback, state, reason = self._provider_fallback_from_catalog("ФНС ЕГРЮЛ", inn, inn)
            if fallback:
                return fallback, state, reason
            return None, "empty", "not found"
        return {"source": "ФНС ЕГРЮЛ", "url": data.get("url", ""), "data": data}, "ok", ""

    def _fetch_html_page(self, url: str) -> tuple[str | None, str, str]:
        self._domain_throttle(url)
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as response:
                return response.read().decode("utf-8", errors="ignore"), "ok", ""
        except TimeoutError:
            return None, "error", "timeout"
        except Exception as exc:  # noqa: BLE001
            reason = str(exc).strip() or exc.__class__.__name__
            if "429" in reason:
                return None, "rate_limited", self._retry_reason(urlparse(url).netloc or "source")
            return None, "error", reason

    def _extract_director_from_html(self, html: str) -> tuple[str, str, str]:
        soup = BeautifulSoup(html, "lxml")
        labeled_chunks: list[str] = []
        for node in soup.find_all(["tr", "li", "p", "div", "section", "dt", "dd"]):
            text = self._normalize_spaces(node.get_text(" ", strip=True))
            if not text or not LEADER_LABEL_RE.search(text):
                continue
            lowered = text.lower()
            if any(marker in lowered for marker in ("история", "проверка физлиц", "юридического лица")):
                continue
            labeled_chunks.append(text)

        if not labeled_chunks:
            full_text = self._normalize_spaces(soup.get_text(" ", strip=True))
            labeled_chunks = [line for line in re.split(r"[\n\r;]", full_text) if LEADER_LABEL_RE.search(line)]

        patterns = [
            r"(?:генеральный\s+директор|президент|председатель\s+правления|руководитель(?:\s+организации)?)\s*[:—\-]\s*(?!юридического\s+лица|история)([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+(?:\s+[А-ЯЁ][а-яё-]+)?)",
        ]
        for chunk in labeled_chunks:
            for pattern in patterns:
                match = re.search(pattern, chunk, flags=re.IGNORECASE)
                if not match:
                    continue
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(match.group(1))
                accepted, _ = self._validate_leader_fio_candidate(surname_ru, name_ru, middle_name_ru)
                if accepted:
                    return surname_ru, name_ru, middle_name_ru
        return "", "", ""

    def _extract_org_from_html(self, html: str) -> str:
        title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        if title_match:
            title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", title_match.group(1))).strip()
            title = title.split("—", 1)[0].strip()
            if title:
                return title
        h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
        if h1_match:
            h1_text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", h1_match.group(1))).strip()
            return h1_text
        return ""

    def _fetch_from_rusprofile(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        # deprecated: kept for backward compatibility; use _search_rusprofile + _parse_rusprofile.
        hits = self._search_rusprofile(inn, is_person=False)
        for hit in hits:
            url = hit.get("url", "")
            if not url:
                continue
            data = self._parse_rusprofile(url)
            if data:
                if hit.get("org") and not data.get("ru_org"):
                    data["ru_org"] = hit.get("org", "")
                return {"source": "rusprofile.ru", "url": url, "data": data}, "ok", ""
        fallback, state, reason = self._provider_fallback_from_catalog("rusprofile.ru", normalized, inn)
        if fallback:
            return fallback, state, reason
        return None, "empty", "not found"

    def _fetch_from_open_corporates(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("OpenCorporates", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_offshore_leaks(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("OffshoreLeaks", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_companies_cse(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("Companies & Orgs Search Engine", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_corporation_wiki(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("Corporation Wiki", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_global_brownbook(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("Global Brownbook", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_faros(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("FAROS OSINT", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_occrp(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        hit, state, reason = self._fetch_inn_fixture("OCCRP Aleph", inn, normalized)
        return self._enrich_provider_payload(hit), state, reason

    def _fetch_from_zachestnyibiznes(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if not inn:
            return self._provider_fallback_from_catalog("zachestnyibiznes.ru", normalized, inn)
        slug = normalized.replace(" ", "-") if normalized else ""
        url = f"https://zachestnyibiznes.ru/company/ul/{inn}_{slug}" if slug else f"https://zachestnyibiznes.ru/search?query={inn}"
        if not self._throttle_acquire("zachestnyibiznes.ru"):
            return None, "rate_limited", "throttle"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as response:
                html = response.read().decode("utf-8", errors="ignore")
            surname_ru, name_ru, middle_name_ru = self._extract_director_from_html(html)
            org_name = self._extract_org_from_html(html)
            if not org_name and not surname_ru:
                title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
                if title_match:
                    org_name = re.sub(r"\s+", " ", title_match.group(1)).strip().split("—", 1)[0]
            if not org_name and not surname_ru:
                fallback, state, reason = self._provider_fallback_from_catalog("rusprofile.ru", normalized, inn)
                if fallback:
                    return fallback, state, reason
                return None, "empty", "not found"
            return {
                "source": "zachestnyibiznes.ru",
                "url": url,
                "data": {
                    "inn": inn,
                    "ru_org": org_name,
                    "surname_ru": surname_ru,
                    "name_ru": name_ru,
                    "middle_name_ru": middle_name_ru,
                    "ru_position": "Генеральный директор" if surname_ru else "",
                },
            }, "ok", ""
        except Exception as exc:
            if "429" in str(exc):
                self._save_rate_limited("zachestnyibiznes.ru", f"zb:{inn}", 180)
                return None, "rate_limited", "429"
            return None, "error", str(exc)

    def _fetch_from_kontur(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if not inn:
            return self._provider_fallback_from_catalog("focus.kontur.ru", normalized, inn)
        url = f"https://focus.kontur.ru/entity?query={inn}"
        if not self._throttle_acquire("focus.kontur.ru"):
            return None, "rate_limited", "throttle"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as response:
                body = response.read().decode("utf-8", errors="ignore")
            org_name = ""
            surname_ru, name_ru, middle_name_ru = "", "", ""
            match = re.search(r'"name"\s*:\s*"([^\"]+)"', body)
            if match:
                org_name = match.group(1).strip()
            director = re.search(r'"director"\s*:\s*"([^\"]+)"', body)
            if director:
                parts = director.group(1).split()
                if parts:
                    surname_ru = parts[0]
                    name_ru = parts[1] if len(parts) > 1 else ""
                    middle_name_ru = " ".join(parts[2:]) if len(parts) > 2 else ""
            if not org_name:
                org_name = self._extract_org_from_html(body)
            if not surname_ru:
                surname_ru, name_ru, middle_name_ru = self._extract_director_from_html(body)
            if not org_name and not surname_ru:
                fallback, state, reason = self._provider_fallback_from_catalog("rusprofile.ru", normalized, inn)
                if fallback:
                    return fallback, state, reason
                return None, "empty", "not found"
            return {
                "source": "focus.kontur.ru",
                "url": url,
                "data": {
                    "inn": inn,
                    "ru_org": org_name,
                    "surname_ru": surname_ru,
                    "name_ru": name_ru,
                    "middle_name_ru": middle_name_ru,
                    "ru_position": "Генеральный директор" if surname_ru else "",
                },
            }, "ok", ""
        except Exception as exc:
            if "429" in str(exc):
                self._save_rate_limited("focus.kontur.ru", f"kontur:{inn}", 180)
                return None, "rate_limited", "429"
            return None, "error", str(exc)

    def _fetch_from_checko(self, raw_input: str, inn: str = "") -> tuple[dict[str, Any] | None, str, str]:
        parsed = urlparse(raw_input) if self.detect_input_type(raw_input) == INPUT_TYPE_URL else None
        host = (parsed.netloc.lower() if parsed else "")

        if host and host not in {"checko.ru", "www.checko.ru"}:
            return None, "empty", "домен не checko.ru"
        if not inn and host:
            path_match = re.search(r"-(\d{10}|\d{12})/?$", parsed.path)
            if path_match:
                inn = path_match.group(1)
        if not inn and not host:
            return None, "empty", "ИНН не найден во входе"

        url = raw_input if host in {"checko.ru", "www.checko.ru"} else f"https://checko.ru/company/by-inn/{inn}"
        self._domain_throttle(url)
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=8) as response:
                html = response.read().decode("utf-8", errors="ignore")
        except TimeoutError:
            return None, "error", "timeout"
        except Exception as exc:  # noqa: BLE001
            reason = str(exc).strip() or exc.__class__.__name__
            if "429" in reason:
                return None, "rate_limited", self._retry_reason("checko.ru")
            return None, "error", reason

        org_name = ""
        title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        if title_match:
            title = re.sub(r"\s+", " ", title_match.group(1)).strip()
            title = title.split("—", 1)[0].strip()
            org_name = re.sub(r"\s*\(ИНН.*$", "", title, flags=re.IGNORECASE).strip()

        if not org_name:
            h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
            if h1_match:
                org_name = re.sub(r"<[^>]+>", "", h1_match.group(1)).strip()

        if not org_name:
            return None, "error", "parse error"

        ru_org, _ = self.normalize_ru_org(org_name)
        en_org, _ = self.normalize_en_org("", ru_org)
        return {
            "source": "checko.ru",
            "url": url,
            "data": {
                "ru_org": ru_org,
                "en_org": en_org,
            },
        }, "ok", ""

    def _source_is_person_eligible(self, source_name: str, source_data: dict[str, Any]) -> bool:
        providers = {item["name"]: item for item in self.SOURCE_PROVIDERS}
        provider = providers.get(source_name, {})
        if provider.get("is_person_source"):
            return True
        ru_org = self._normalize_spaces(str(source_data.get("ru_org", ""))).lower()
        title = self._normalize_spaces(str(source_data.get("title", ""))).lower()
        noise_markers = ("список организаций", "реквизиты компании")
        if any(marker in ru_org or marker in title for marker in noise_markers):
            return False
        if any(source_data.get(key) for key in ("surname_ru", "name_ru", "middle_name_ru", "director", "ceo")):
            return True
        return False

    def _enrich_alternative_person_fields(self, source_data: dict[str, Any]) -> dict[str, Any]:
        if source_data.get("surname_ru") and source_data.get("name_ru"):
            return source_data
        person_raw = self._normalize_spaces(str(source_data.get("director") or source_data.get("ceo") or ""))
        if not person_raw:
            return source_data
        sur, nam, patr = self._split_fio_ru(person_raw)
        if sur:
            source_data["surname_ru"] = source_data.get("surname_ru") or sur
        if nam:
            source_data["name_ru"] = source_data.get("name_ru") or nam
        if patr:
            source_data["middle_name_ru"] = source_data.get("middle_name_ru") or patr
        return source_data

    def _pick_field_by_priority(
        self,
        field: str,
        source_hits: list[dict[str, Any]],
        skip_person_noise: bool = False,
    ) -> tuple[str, str]:
        priority = FIELD_PRIORITIES.get(field, [])
        ordered_hits = sorted(
            source_hits,
            key=lambda item: (priority.index(item.get("source")) if item.get("source") in priority else len(priority)),
        )
        for item in ordered_hits:
            source_name = item.get("source", "unknown")
            data = self._enrich_alternative_person_fields(dict(item.get("data", {})))
            value = self._normalize_spaces(str(data.get(field, "")))
            if field in {"ru_org", "en_org", "ru_position"} and self.is_block_page_value(value):
                continue
            if not value:
                continue
            if field in {"surname_ru", "name_ru", "middle_name_ru"}:
                surname = self._normalize_spaces(str(data.get("surname_ru", "")))
                name = self._normalize_spaces(str(data.get("name_ru", "")))
                middle = self._normalize_spaces(str(data.get("middle_name_ru", "")))
                accepted, _ = self._validate_leader_fio_candidate(surname, name, middle)
                if not accepted:
                    continue
            if skip_person_noise and not self._source_is_person_eligible(source_name, data):
                continue
            return value, source_name
        return "", ""

    def _merge_person_hits(self, hits: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
        if not hits:
            return {}, {}
        ordered = list(hits)
        base = dict(ordered[0].get("data", {}))
        merged_sources: dict[str, str] = {k: str(ordered[0].get("source", "")) for k in base if self._normalize_spaces(str(base.get(k, "")))}
        fields = ["surname_ru", "name_ru", "middle_name_ru", "middle_name_en", "gender", "ru_position", "position", "ru_org", "en_org", "inn"]
        for hit in ordered[1:]:
            source = str(hit.get("source", ""))
            data = hit.get("data", {})
            for field in fields:
                if self._normalize_spaces(str(base.get(field, ""))):
                    continue
                candidate = self._normalize_spaces(str(data.get(field, "")))
                if candidate:
                    base[field] = candidate
                    merged_sources[field] = source
        merged = {k: self._normalize_spaces(str(v)) for k, v in base.items() if self._normalize_spaces(str(v))}
        if "ru_org" in merged:
            merged["ru_org"] = self._clean_ru_org_name(merged["ru_org"])
        return merged, merged_sources

    def _merge_profiles(self, hits: list[dict[str, Any]], query: str) -> dict[str, Any]:
        """Объединяет данные из нескольких источников в один профиль."""
        if not hits:
            return {}
        ranked_hits = sorted(
            hits,
            key=lambda item: self._get_provider_priority(str(item.get("source", ""))),
            reverse=True,
        )
        best_hit = ranked_hits[0]
        merged_data = dict(best_hit.get("data", best_hit))
        for hit in ranked_hits[1:]:
            data = hit.get("data", {})
            for field in ["ru_org", "en_org", "ru_position", "en_position", "middle_name_ru", "middle_name_en"]:
                if not merged_data.get(field) and data.get(field):
                    merged_data[field] = data[field]
            if merged_data.get("inn") and data.get("inn") and merged_data["inn"] == data["inn"]:
                for field in ["surname_ru", "name_ru", "middle_name_ru", "gender"]:
                    if not merged_data.get(field) and data.get(field):
                        merged_data[field] = data[field]
        self._enrich_merged_data(merged_data)
        return merged_data

    def _get_provider_priority(self, provider_name: str) -> int:
        priority_map = {
            "ФНС ЕГРЮЛ": 100,
            "enhanced_rusprofile": 95,
            "rusprofile.ru": 90,
            "bank_of_russia": 75,
            "zachestnyibiznes.ru": 70,
            "focus.kontur.ru": 65,
            "checko.ru": 60,
        }
        return priority_map.get(provider_name, 50)

    def _enrich_merged_data(self, data: dict[str, Any]) -> None:
        if data.get("ru_position") and not data.get("position") and not data.get("en_position"):
            data["position"] = self._generate_en_position(str(data["ru_position"]))
        if data.get("middle_name_ru") and not data.get("middle_name_en"):
            data["middle_name_en"] = self._generate_middle_name_en(str(data["middle_name_ru"]))
        if data.get("gender") and not data.get("appeal"):
            data["appeal"] = "Г-н" if data["gender"] == "М" else "Г-жа"
        if data.get("ru_org") and "Сбербанк" in str(data["ru_org"]) and "ПАО" not in str(data["ru_org"]):
            data["ru_org"] = f"{data['ru_org']} ПАО"

    def _normalize_card_data(self, profile: dict[str, str], field_sources: dict[str, str]) -> dict[str, str]:
        """Универсальная нормализация данных карточки."""
        if profile.get("ru_position") and not profile.get("en_position"):
            profile["en_position"], _ = self._normalize_positions_en(profile["ru_position"])
            if profile.get("en_position"):
                field_sources.setdefault("en_position", "Автогенерация из RU")
        if not profile.get("middle_name_en"):
            if profile.get("middle_name"):
                profile["middle_name_en"] = self._generate_middle_name_en(profile["middle_name"])
            elif profile.get("middle_name_ru"):
                profile["middle_name_en"] = self._generate_middle_name_en(profile["middle_name_ru"])
        if not profile.get("appeal") and profile.get("gender"):
            profile["appeal"] = "Г-н" if profile["gender"] == "М" else "Г-жа"
            field_sources.setdefault("appeal", "Автоопределение")
        return profile

    def apply_card_rules(self, profile: dict[str, str], card_type: str = "") -> tuple[dict[str, str], list[str]]:
        """Применяет финальную нормализацию карточки перед сохранением."""
        normalized = dict(profile)
        notes: list[str] = []

        pos_raw = self._normalize_spaces(normalized.get("ru_position", ""))
        cleaned_position, pos_surname, pos_name, pos_middle, _ = self._clean_position_and_extract_fio(pos_raw)
        if cleaned_position:
            normalized["ru_position"] = cleaned_position
            pos_raw = cleaned_position
        if pos_surname and pos_name and not (normalized.get("surname_ru") and normalized.get("name_ru")):
            normalized["surname_ru"] = normalized.get("surname_ru") or pos_surname
            normalized["name_ru"] = normalized.get("name_ru") or pos_name
            normalized["middle_name_ru"] = normalized.get("middle_name_ru") or pos_middle

        org_type = self._detect_org_type(normalized.get("ru_org", ""))

        if pos_raw:
            normalized["ru_position"] = self.sanitize_ru_position(pos_raw) or ""
        else:
            normalized["ru_position"] = self._infer_ru_position_from_en(
                normalized.get("en_position") or normalized.get("position", ""),
                org_type,
            )

        ru_org, ru_notes = self.normalize_ru_org(normalized.get("ru_org", ""))
        en_org, en_notes = self.normalize_en_org(normalized.get("en_org", ""), ru_org)
        ru_pos, ru_pos_notes = self._normalize_positions_ru(normalized.get("ru_position", ""))
        en_pos_raw = normalized.get("en_position") or normalized.get("position", "")
        if ru_pos:
            en_pos_raw = en_pos_raw or self._generate_en_position(ru_pos)
        en_pos, en_pos_notes = self._normalize_positions_en(en_pos_raw)

        normalized["ru_org"] = ru_org
        normalized["en_org"] = en_org
        normalized["ru_position"] = ru_pos
        normalized["position"] = en_pos
        normalized["en_position"] = en_pos

        profile_type = card_type or self._detect_profile_type(normalized)
        forced_search_type = self._normalize_spaces(str(normalized.get("search_type", ""))).lower()
        has_leader = bool(
            self._normalize_spaces(str(normalized.get("surname_ru", "")))
            and self._normalize_spaces(str(normalized.get("name_ru", "")))
        )
        if forced_search_type == "company" and has_leader:
            profile_type = "person_in_company"

        is_company = profile_type == "company"
        if is_company:
            for field in (
                "surname_ru", "name_ru", "middle_name_ru", "family_name", "first_name", "middle_name_en",
                "gender", "salutation", "title", "appeal",
            ):
                normalized.pop(field, None)
        else:
            if normalized.get("middle_name_ru"):
                middle_en = self._normalize_spaces(str(normalized.get("middle_name_en", "")))
                if not middle_en or re.search(r"[А-Яа-яЁё]", middle_en):
                    normalized["middle_name_en"] = self._generate_middle_name_en(normalized["middle_name_ru"])
            if not normalized.get("appeal"):
                normalized["appeal"] = self._derive_salutation(normalized.get("gender", ""))
            normalized["salutation"] = normalized.get("appeal", "")

        notes.extend(ru_notes)
        notes.extend(en_notes)
        notes.extend(ru_pos_notes)
        notes.extend(en_pos_notes)
        return normalized, notes

    def _build_profile_from_sources(
        self,
        source_hits: list[dict[str, Any]],
        raw_name: str,
        input_type: str,
        forced_type: str = "",
    ) -> tuple[dict[str, str], dict[str, str]]:
        profile = {field: "" for field, _ in CARD_FIELDS}
        query = self._extract_inn(raw_name) if input_type == INPUT_TYPE_INN else self._normalize_spaces(raw_name)
        field_sources: dict[str, str] = {}
        logger.info("Построение профиля из %d хитов", len(source_hits))
        profile_type = forced_type if forced_type in {"person", "company"} else ""

        for hit in source_hits:
            if hit.get("source") != "special_case":
                continue
            data = hit.get("data", {})
            if not isinstance(data, dict):
                continue
            for noisy_field in ("ru_org", "en_org", "ru_position"):
                if self.is_block_page_value(data.get(noisy_field, "")):
                    data[noisy_field] = None
            fio_surname = self._clean_fio_part(str(data.get("surname_ru", "")))
            fio_name = self._clean_fio_part(str(data.get("name_ru", "")))
            fio_middle = self._clean_fio_part(str(data.get("middle_name_ru", "")))
            if fio_surname or fio_name or fio_middle:
                accepted, reason = self._validate_leader_fio_candidate(fio_surname, fio_name, fio_middle)
                logger.info(
                    "leader_fio_candidate source=%s fio='%s' accepted=%s reason=%s",
                    "special_case",
                    " ".join(part for part in (fio_surname, fio_name, fio_middle) if part),
                    accepted,
                    reason,
                )
                if not accepted:
                    data["surname_ru"] = ""
                    data["name_ru"] = ""
                    data["middle_name_ru"] = ""
            for field in [
                "surname_ru",
                "name_ru",
                "middle_name_ru",
                "family_name",
                "first_name",
                "middle_name_en",
                "ru_org",
                "en_org",
                "ru_position",
                "en_position",
                "gender",
                "inn",
                "appeal",
            ]:
                if data.get(field):
                    profile[field] = str(data[field])
                    field_sources[field] = "special_case"
            logger.info("Данные special_case применены с приоритетом")
            break

        fns_fio_locked = self.is_valid_ru_fio(profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")) and (
            field_sources.get("surname_ru") == "ФНС ЕГРЮЛ" or field_sources.get("name_ru") == "ФНС ЕГРЮЛ"
        )

        for hit_idx, hit in enumerate(source_hits):
            if hit.get("source") == "special_case":
                continue
            data = hit.get("data", {})
            source_name = hit.get("source", "unknown")
            logger.debug(
                "Хит %d от %s: data keys=%s, ru_org=%s, surname=%s",
                hit_idx,
                source_name,
                list(data.keys()) if isinstance(data, dict) else "N/A",
                data.get("ru_org", "") if isinstance(data, dict) else "N/A",
                data.get("surname_ru", "") if isinstance(data, dict) else "N/A",
            )
            if not isinstance(data, dict):
                continue
            fio_surname = self._clean_fio_part(str(data.get("surname_ru", "")))
            fio_name = self._clean_fio_part(str(data.get("name_ru", "")))
            fio_middle = self._clean_fio_part(str(data.get("middle_name_ru", "")))
            if fio_surname or fio_name or fio_middle:
                accepted, reason = self._validate_leader_fio_candidate(fio_surname, fio_name, fio_middle)
                logger.info(
                    "leader_fio_candidate source=%s fio='%s' accepted=%s reason=%s",
                    source_name,
                    " ".join(part for part in (fio_surname, fio_name, fio_middle) if part),
                    accepted,
                    reason,
                )
                if not accepted:
                    data = dict(data)
                    data["surname_ru"] = ""
                    data["name_ru"] = ""
                    data["middle_name_ru"] = ""
            for field in [
                "surname_ru",
                "name_ru",
                "middle_name_ru",
                "gender",
                "ru_org",
                "inn",
                "ru_position",
                "en_org",
                "en_position",
            ]:
                if fns_fio_locked and field in {"surname_ru", "name_ru", "middle_name_ru"} and source_name != "ФНС ЕГРЮЛ":
                    continue
                if data.get(field) and not profile.get(field):
                    profile[field] = str(data[field])
                    field_sources[field] = source_name
                    logger.info("Поле %s заполнено из %s", field, source_name)
                    if field in {"surname_ru", "name_ru", "middle_name_ru"}:
                        fns_fio_locked = self.is_valid_ru_fio(profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")) and (
                            field_sources.get("surname_ru") == "ФНС ЕГРЮЛ" or field_sources.get("name_ru") == "ФНС ЕГРЮЛ"
                        )

        for field, _ in CARD_FIELDS:
            if field_sources.get(field) == "special_case":
                continue
            if field == "ru_position" and field_sources.get("ru_position") == "ФНС ЕГРЮЛ":
                continue
            skip_person_noise = field in {"surname_ru", "name_ru", "middle_name_ru", "gender", "ru_position", "position"}
            value, source_name = self._pick_field_by_priority(field, source_hits, skip_person_noise=skip_person_noise)
            if value:
                if fns_fio_locked and field in {"surname_ru", "name_ru", "middle_name_ru"} and source_name != "ФНС ЕГРЮЛ":
                    continue
                if (
                    field in {"surname_ru", "name_ru", "middle_name_ru"}
                    and profile.get(field)
                    and field_sources.get(field) == "ФНС ЕГРЮЛ"
                    and source_name != "ФНС ЕГРЮЛ"
                ):
                    continue
                profile[field] = value
                field_sources[field] = source_name
                if field in {"surname_ru", "name_ru", "middle_name_ru"}:
                    fns_fio_locked = self.is_valid_ru_fio(profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")) and (
                        field_sources.get("surname_ru") == "ФНС ЕГРЮЛ" or field_sources.get("name_ru") == "ФНС ЕГРЮЛ"
                    )

        required_fallback_fields = ["surname_ru", "name_ru", "ru_org", "en_org", "inn"]
        for field in required_fallback_fields:
            if profile.get(field):
                continue
            for hit in source_hits:
                data = hit.get("data", {})
                candidate = str(data.get(field) or "").strip()
                if field in {"ru_org", "en_org", "ru_position"} and self.is_block_page_value(candidate):
                    continue
                if candidate and candidate not in {"", " ", query}:
                    if field in {"surname_ru", "name_ru"}:
                        if fns_fio_locked and hit.get("source") != "ФНС ЕГРЮЛ":
                            continue
                        source_data = hit.get("data", {}) if isinstance(hit.get("data", {}), dict) else {}
                        surname = self._clean_fio_part(str(source_data.get("surname_ru", "") or (candidate if field == "surname_ru" else "")))
                        name = self._clean_fio_part(str(source_data.get("name_ru", "") or (candidate if field == "name_ru" else "")))
                        middle = self._clean_fio_part(str(source_data.get("middle_name_ru", "")))
                        accepted, _ = self._validate_leader_fio_candidate(surname, name, middle)
                        if not accepted:
                            continue
                    profile[field] = candidate
                    field_sources[field] = f"fallback:{hit.get('source', 'unknown')}"
                    logger.info("FALLBACK: %s = '%s' из %s", field, candidate[:50], field_sources[field])
                    break

        if profile.get("ru_org") == query and input_type == INPUT_TYPE_INN:
            profile["ru_org"] = raw_name
            field_sources["ru_org"] = "raw_query_fallback"
            logger.info("ru_org был равен ИНН, заменено на raw_name: %s", raw_name)

        if input_type == INPUT_TYPE_PERSON_TEXT and profile_type != "company":
            merged_person, merged_sources = self._merge_person_hits(source_hits)
            for key, value in merged_person.items():
                if not value:
                    continue
                if field_sources.get(key) == "special_case":
                    continue
                if not profile.get(key):
                    profile[key] = value
                    if merged_sources.get(key):
                        field_sources[key] = merged_sources[key]
            merged_profile = self._merge_profiles(source_hits, raw_name)
            for key in ["ru_org", "en_org", "ru_position", "en_position", "middle_name_ru", "middle_name_en"]:
                if not profile.get(key) and merged_profile.get(key):
                    profile[key] = str(merged_profile[key])

        if source_hits:
            best_revenue_hit = max(source_hits, key=lambda item: int(item.get("data", {}).get("revenue", 0) or 0))
            revenue_value = int(best_revenue_hit.get("data", {}).get("revenue", 0) or 0)
            if revenue_value:
                profile["revenue"] = str(revenue_value)
                profile["revenue_mln"] = f"{(revenue_value / 1_000_000):.2f}"
                field_sources["revenue"] = best_revenue_hit.get("source", "")
                field_sources["revenue_mln"] = best_revenue_hit.get("source", "")

        if not profile["ru_org"] and source_hits:
            for item in source_hits:
                candidate = self._normalize_spaces(str(item.get("data", {}).get("ru_org", "")))
                if self.is_block_page_value(candidate):
                    continue
                if candidate:
                    profile["ru_org"] = candidate
                    field_sources["ru_org"] = item.get("source", "unknown")
                    break

        if not profile["ru_org"] and input_type != INPUT_TYPE_INN and profile_type != "person":
            candidate_raw_name = raw_name.strip()
            if not re.fullmatch(r"\d{10,12}", candidate_raw_name):
                profile["ru_org"] = raw_name
                field_sources["ru_org"] = "Нормализация запроса"
            else:
                logger.warning("ru_org пустой, но raw_name='%s' похож на ИНН — не подставляем", raw_name)

        profile["ru_org"], _ = self.normalize_ru_org(profile["ru_org"])
        if profile["ru_org"]:
            field_sources["ru_org"] = field_sources.get("ru_org", "Нормализация/источник")

        if not profile["en_org"]:
            profile["en_org"], _ = self.normalize_en_org("", profile["ru_org"])
            if profile["en_org"]:
                field_sources["en_org"] = "Транслитерация из RU"
        else:
            profile["en_org"], _ = self.normalize_en_org(profile["en_org"], profile["ru_org"])
            if profile["en_org"] and not field_sources.get("en_org"):
                field_sources["en_org"] = "Нормализация/источник"

        self._sanitize_profile_fio(profile)
        if profile.get("surname_ru") and not profile.get("surname"):
            profile["surname"] = profile["surname_ru"]
        if profile.get("name_ru") and not profile.get("name"):
            profile["name"] = profile["name_ru"]

        if profile.get("surname") or profile.get("name"):
            profile["family_name"] = profile.get("family_name") or self._translit(profile.get("surname", ""))
            profile["first_name"] = profile.get("first_name") or self._translit(profile.get("name", ""))
            if profile.get("family_name"):
                field_sources.setdefault("family_name", "Транслитерация из Фамилия")
            if profile.get("first_name"):
                field_sources.setdefault("first_name", "Транслитерация из Имя")

        if input_type == INPUT_TYPE_PERSON_TEXT and profile_type != "company" and not profile.get("surname_ru") and not profile.get("name_ru"):
            raw_tokens = [tok for tok in self._normalize_spaces(raw_name).split() if tok]
            if len(raw_tokens) >= 2:
                sur, nam, patr = self._split_fio_ru(raw_name)
                profile["surname_ru"] = self._clean_fio_part(sur)
                profile["name_ru"] = self._clean_fio_part(nam)
                profile["middle_name_ru"] = self._clean_fio_part(patr)
                if profile["surname_ru"]:
                    profile["surname"] = profile["surname_ru"]
                    profile["family_name"] = self._translit(profile["surname_ru"])
                if profile["name_ru"]:
                    profile["name"] = profile["name_ru"]
                    profile["first_name"] = self._translit(profile["name_ru"])

        profile["surname_ru"] = _clean_fio_part(profile.get("surname_ru", ""))
        profile["name_ru"] = _clean_fio_part(profile.get("name_ru", ""))
        profile["middle_name_ru"] = _clean_fio_part(profile.get("middle_name_ru", ""))
        candidate_source = field_sources.get("surname_ru", field_sources.get("name_ru", ""))
        candidate_fio = " ".join(part for part in (profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")) if part)
        accepted, reason = self._validate_leader_fio_candidate(profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", ""))
        logger.info("leader_fio_candidate source=%s fio='%s' accepted=%s reason=%s", candidate_source or "", candidate_fio, accepted, reason)
        if candidate_fio and not accepted:
            logger.warning("invalid_fio_rejected: surname=%s name=%s source=%s", profile.get("surname_ru", ""), profile.get("name_ru", ""), candidate_source)
            for field in ("surname_ru", "name_ru", "middle_name_ru", "surname", "name", "family_name", "first_name"):
                profile[field] = ""
                field_sources.pop(field, None)

        input_inn = self._extract_inn(raw_name) if input_type == INPUT_TYPE_INN else ""
        if input_inn and not profile.get("inn"):
            profile["inn"] = input_inn
        if profile.get("inn"):
            field_sources["inn"] = "Ввод пользователя/ФНС" if input_inn else field_sources.get("inn", "ФНС")

        if not profile.get("gender"):
            inferred_gender = self._infer_gender(profile.get("middle_name_ru", ""), profile.get("ru_position", ""))
            if inferred_gender:
                profile["gender"] = inferred_gender
                field_sources["gender"] = field_sources.get("gender", "Автоопределение")

        if not profile.get("appeal"):
            profile["appeal"] = self._derive_salutation(profile.get("gender", ""))
        self._sanitize_profile_position(profile, source_hits, field_sources)
        profile["ru_position"], _ = self._normalize_positions_ru(profile.get("ru_position", ""))

        ru_pos = profile.get("ru_position", "").strip()
        noise_markers = ["юридического лица", "история", "проверить"]
        if any(marker in ru_pos.lower() for marker in noise_markers):
            ru_pos = ""
        profile["ru_position"] = ru_pos

        en_position_source = self._normalize_spaces(profile.get("en_position", "") or profile.get("position", ""))
        if en_position_source:
            profile["en_position"] = en_position_source
        else:
            profile["en_position"] = self._generate_en_position(ru_pos) if ru_pos else ""
        profile["position"] = profile["en_position"]
        profile["position"], _ = self._normalize_positions_en(profile.get("position", ""))
        profile["en_position"] = profile.get("position", "")
        profile["salutation"] = profile.get("appeal", "")


        profile = self._normalize_card_data(profile, field_sources)

        for field, _ in CARD_FIELDS:
            if profile.get(field) or field_sources.get(field) == "special_case":
                continue
            value, source_name = self._pick_field_by_priority(field, source_hits)
            if value:
                if field == "ru_position" and not self.sanitize_ru_position(value):
                    continue
                profile[field] = value
                field_sources[field] = source_name

        if not profile.get("appeal") and profile.get("gender"):
            profile["appeal"] = self._derive_salutation(profile["gender"])
            if profile["appeal"]:
                field_sources.setdefault("appeal", "Автоопределение")

        if profile.get("surname_ru") and not profile.get("surname"):
            profile["surname"] = profile["surname_ru"]
        if profile.get("name_ru") and not profile.get("name"):
            profile["name"] = profile["name_ru"]
        if profile.get("middle_name_ru") and not profile.get("middle_name"):
            profile["middle_name"] = profile["middle_name_ru"]

        if not profile.get("middle_name_en"):
            if profile.get("middle_name"):
                profile["middle_name_en"] = self._generate_middle_name_en(profile["middle_name"])
            elif profile.get("middle_name_ru"):
                profile["middle_name_en"] = self._generate_middle_name_en(profile["middle_name_ru"])

        if not profile.get("family_name") and profile.get("surname"):
            profile["family_name"] = self._translit(profile["surname"])
            field_sources.setdefault("family_name", "Транслитерация из Фамилия")
        if not profile.get("first_name") and profile.get("name"):
            profile["first_name"] = self._translit(profile["name"])
            field_sources.setdefault("first_name", "Транслитерация из Имя")
        profile["inn_or_ogrn"] = profile.get("inn") or profile.get("ogrn") or ""

        filled = [key for key, value in profile.items() if value and key not in {"title", "appeal"}]
        logger.info("Заполненные поля: %s", filled)
        missing_required = self._missing_required_fields(profile)
        if missing_required:
            logger.error("❌ НЕ ЗАПОЛНЕНЫ ОБЯЗАТЕЛЬНЫЕ ПОЛЯ: %s", missing_required)
            if not profile.get("ru_org") and raw_name:
                candidate_raw_name = raw_name.strip()
                if not re.fullmatch(r"\d{10,12}", candidate_raw_name):
                    profile["ru_org"] = raw_name
                    field_sources["ru_org"] = "user_input_fallback"
                else:
                    logger.warning("ru_org пустой, raw_name='%s' похож на ИНН — fallback пропущен", raw_name)
            logger.warning("Не заполнены обязательные поля: %s", missing_required)

        return profile, field_sources

    def _create_autofill_card(
        self,
        profile_data: dict[str, Any],
        notes: list[str],
        source_hits: list[dict[str, Any]],
        search_trace: list[str],
        field_provenance: dict[str, str],
    ) -> int:
        profile_data["middle_name_en"] = (profile_data.get("middle_name_en") or "").strip()
        ru_org = str(profile_data.get("ru_org", ""))
        en_org = str(profile_data.get("en_org", ""))
        status = self._status(notes, bool(ru_org and en_org))
        with self._connect() as db:
            cur = db.execute(
                "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
                (
                    ru_org,
                    en_org,
                    status,
                    "autofill",
                    self._now(),
                    self._now(),
                    json.dumps(
                        {
                            "notes": notes,
                            "source_hits": source_hits,
                            "search_trace": search_trace,
                            "field_provenance": field_provenance,
                            "profile": profile_data,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            card_id = cur.lastrowid
            db.commit()
        self._write_audit("create_autofill", card_id, {"ru_org": ru_org, "en_org": en_org, "status": status})
        return int(card_id)

    def _write_audit(self, action: str, card_id: int | None, details: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO audits(card_id, action, actor, created_at, details) VALUES(?,?,?,?,?)",
                (card_id, action, "web-user", self._now(), json.dumps(details, ensure_ascii=False)),
            )
            db.commit()

    def _build_person_candidates(self, hits: list[dict[str, Any]], query: str = "", search_type: str = "") -> list[dict[str, str]]:
        candidates: list[dict[str, Any]] = []
        query_words = [w for w in self._normalize_spaces(query.lower()).split() if w]
        logger.debug("Построение кандидатов: %d хитов, search_type=%s", len(hits), search_type)

        for idx, hit in enumerate(hits):
            data = hit.get("data", {})
            normalized_data = dict(data)
            hit_type = str(hit.get("type") or normalized_data.get("type") or "unknown")

            source_is_person = bool(
                hit.get("person_source")
                or normalized_data.get("person_source")
                or hit_type == "person"
            )
            source_is_company = hit_type == "company"
            if not source_is_company:
                has_person_names = bool(normalized_data.get("surname_ru") and normalized_data.get("name_ru"))
                has_company_markers = bool(normalized_data.get("ru_org") or normalized_data.get("inn") or normalized_data.get("ogrn"))
                if not has_person_names and has_company_markers:
                    source_is_company = True

            if search_type == "person" and source_is_company:
                continue
            if search_type == "company" and source_is_person:
                continue

            if normalized_data.get("ru_org"):
                normalized_data["ru_org"] = self._clean_ru_org_name(str(normalized_data["ru_org"]))
            if normalized_data.get("ru_org") and not normalized_data.get("en_org"):
                normalized_data["en_org"], _ = self.normalize_en_org(str(normalized_data["ru_org"]), str(normalized_data["ru_org"]))
            elif normalized_data.get("en_org"):
                normalized_data["en_org"], _ = self.normalize_en_org(str(normalized_data["en_org"]), str(normalized_data.get("ru_org", "")))

            if normalized_data.get("ru_position"):
                cleaned_position, pos_surname, pos_name, pos_middle, fio_raw = self._clean_position_and_extract_fio(str(normalized_data["ru_position"]))
                normalized_data["ru_position"] = cleaned_position
                if fio_raw:
                    normalized_data["leader_fio_raw"] = fio_raw
                if not normalized_data.get("surname_ru") and pos_surname:
                    normalized_data["surname_ru"] = pos_surname
                if not normalized_data.get("name_ru") and pos_name:
                    normalized_data["name_ru"] = pos_name
                if not normalized_data.get("middle_name_ru") and pos_middle:
                    normalized_data["middle_name_ru"] = pos_middle
            if normalized_data.get("ru_position") and not normalized_data.get("en_position"):
                normalized_data["en_position"] = self._generate_en_position(str(normalized_data["ru_position"]))
            elif normalized_data.get("en_position"):
                normalized_data["en_position"], _ = self._normalize_positions_en(str(normalized_data["en_position"]))

            if normalized_data.get("middle_name_ru") and not normalized_data.get("middle_name_en"):
                normalized_data["middle_name_en"] = self._translit(str(normalized_data["middle_name_ru"]))
            if normalized_data.get("gender") and not normalized_data.get("appeal"):
                normalized_data["appeal"] = "Г-н" if normalized_data["gender"] == "М" else "Г-жа"

            if not normalized_data.get("surname_ru") and hit.get("name"):
                full_name = self._normalize_spaces(str(hit.get("name", "")))
                parts = [part for part in full_name.split() if part]
                if len(parts) >= 1:
                    normalized_data["surname_ru"] = parts[0]
                if len(parts) >= 2:
                    normalized_data["name_ru"] = parts[1]
                if len(parts) >= 3:
                    normalized_data["middle_name_ru"] = parts[2]

            fio_ru = " ".join(x for x in [normalized_data.get("surname_ru", ""), normalized_data.get("name_ru", ""), normalized_data.get("middle_name_ru", "")] if x).strip()
            if query_words and fio_ru:
                fio_lower = self._normalize_spaces(fio_ru.lower())
                surname = self._normalize_spaces(str(normalized_data.get("surname_ru", "")).lower())
                if not (any(word in surname for word in query_words) or all(word in fio_lower for word in query_words)):
                    logger.debug("Кандидат %d: частичное совпадение ФИО", idx)

            score = self._score_hit({"source": hit.get("source", ""), "data": normalized_data}, query)
            candidates.append({
                "data": normalized_data,
                "source": str(hit.get("source", "")),
                "type": hit_type,
                "url": str(hit.get("url", "")),
                "score": score,
                "fio_ru": fio_ru,
                "org_ru": self._clean_ru_org_name(str(normalized_data.get("ru_org", ""))),
                "position_ru": self._normalize_spaces(str(normalized_data.get("ru_position", ""))),
                "leader_ru": fio_ru,
                "inn": self._normalize_spaces(str(normalized_data.get("inn", ""))),
                "query_for_autofill": self._normalize_spaces(str(normalized_data.get("inn", "")))
                or (fio_ru if hit_type == "person" else self._clean_ru_org_name(str(normalized_data.get("ru_org", ""))))
                or fio_ru,
                "revenue": str(int(normalized_data.get("revenue", 0) or 0)),
            })

        dedup: dict[tuple[str, str], dict[str, Any]] = {}
        for candidate in candidates:
            key = (candidate.get("fio_ru", "").lower(), candidate.get("org_ru", "").lower())
            if key not in dedup or float(candidate.get("score", 0)) > float(dedup[key].get("score", 0)):
                dedup[key] = candidate

        ranked = sorted(dedup.values(), key=lambda x: float(x.get("score", 0)), reverse=True)[:20]
        for item in ranked:
            item["score"] = f"{float(item['score']):.2f}"
        if ranked:
            logger.info("Top candidate: %s", ranked[0].get("fio_ru", ""))
        logger.info("Кандидаты после фильтрации: %d из %d (режим: %s)", len(ranked), len(hits), search_type or "auto")
        return ranked

    def _normalize_position_ru(self, position: str) -> str:
        position = re.sub(r"(Факторы риска|Дисквалификация|Нахождение под)", "", position)
        position = self._normalize_spaces(position)
        position = re.sub(r"[,;:]+$", "", position)
        parts = [part.strip() for part in position.split(",") if part.strip()]
        normalized_parts: list[str] = []
        stop_words = {"и", "на", "в", "по", "за", "с", "под", "над"}
        for part in parts:
            words = part.split()
            normalized_words: list[str] = []
            for idx, word in enumerate(words):
                if idx == 0 or word.lower() not in stop_words:
                    normalized_words.append(word.capitalize())
                else:
                    normalized_words.append(word.lower())
            normalized_parts.append(" ".join(normalized_words))
        return ", ".join(normalized_parts)

    def _revenue_billions(self, revenue_mln: int | str | None) -> str:
        revenue = int(revenue_mln or 0)
        if revenue <= 0:
            return "—"
        return f"{revenue / 1000:.2f}"

    def _render_search_results(
        self,
        q: str,
        normalized: str,
        candidates: list[dict[str, str]],
        similar: list[sqlite3.Row] | None = None,
        form_values: dict[str, str] | None = None,
    ) -> str:
        similar = similar or []
        form_values = form_values or {}
        surname = form_values.get("surname", "")
        name = form_values.get("name", "")
        middle_name = form_values.get("middle_name", "")
        inn = form_values.get("inn", "")
        company = form_values.get("company", "")
        search_type = form_values.get("search_type", "")
        auto_checked = "checked" if not search_type else ""
        company_checked = "checked" if search_type == "company" else ""
        person_checked = "checked" if search_type == "person" else ""

        logger.info("Рендер поиска: кандидатов=%d, similar=%d", len(candidates), len(similar))
        if candidates:
            blocks = "".join(
                (
                    "<form method='post' action='/autofill/review' style='margin: 10px 0;'>"
                    f"<input type='hidden' name='company_name' value='{escape(c['query_for_autofill'])}' />"
                    f"<input type='hidden' name='hit_type' value='{escape(c.get('type', ''))}' />"
                    f"<input type='hidden' name='search_type' value='{escape(search_type)}' />"
                    "<button type='submit' style='width: 100%; text-align: left; border: 1px solid #ddd; padding: 15px; border-radius: 8px; cursor: pointer; background: white;'>"
                    + f"<h4 style='margin: 0 0 8px;'>{escape(c['fio_ru'] or c['org_ru'] or 'Вариант')}</h4>"
                    + f"<p style='margin: 4px 0;'><b>Организация:</b> {escape(c['org_ru'] or '—')}</p>"
                    + (f"<p style='margin: 4px 0;'><b>Руководитель:</b> {escape(c.get('leader_ru', ''))}</p>" if c.get('leader_ru') else "")
                    + f"<p style='margin: 4px 0;'><b>Должность:</b> {escape(c['position_ru'] or '—')}</p>"
                    + f"<p style='margin: 4px 0;'><b>Выручка:</b> {escape(self._revenue_billions(c.get('revenue')))} млрд руб</p>"
                    + f"<p style='margin: 4px 0;'><b>ИНН:</b> {escape(c.get('inn', '') or '—')}</p>"
                    + f"<p style='margin: 4px 0;'><small><span style='background: {'#e3f2fd' if c.get('type') == 'company' else '#fce4ec'}; padding: 2px 8px; border-radius: 4px; font-size: 11px;'>{'🏢 Юр. лицо' if c.get('type') == 'company' else '👤 Физ. лицо'}</span> | Источник: {escape(c['source'])}</small></p>"
                    "<span style='display: inline-block; margin-top: 10px;'>Автозаполнить</span>"
                    "</button></form>"
                )
                for c in candidates
            )
            not_found = f"<h3>Варианты по '{escape(q)}':</h3>{blocks}"
        elif q:
            not_found = (
                f"<p>Нет данных. Создать вручную?</p>"
                f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(q)}' /><input type='hidden' name='search_type' value='{escape(search_type)}' /><button>Автозаполнить из открытых источников</button></form>"
                f"<a href='/create/manual?q={escape(q)}'>Создать вручную</a>"
            )
        else:
            not_found = ""

        items = "".join(f"<li><a href='/card/{r['id']}'>{escape(r['ru_org'])}</a></li>" for r in similar)
        return (
            "<h1>Карточки компаний/участников</h1>"
            "<form method='get' action='/' style='margin-bottom: 16px;'>"
            "<div style='display: flex; flex-direction: column; gap: 12px; margin-bottom: 12px;'>"
            "<div><label style='display:block; margin-bottom:4px;'>Фамилия</label><input name='surname' value='{surname}' style='width:100%;'/></div>"
            "<div><label style='display:block; margin-bottom:4px;'>Имя</label><input name='name' value='{name}' style='width:100%;'/></div>"
            "<div><label style='display:block; margin-bottom:4px;'>Отчество</label><input name='middle_name' value='{middle_name}' style='width:100%;'/></div>"
            "<div><label style='display:block; margin-bottom:4px;'>ИНН</label><input name='inn' value='{inn}' style='width:100%;'/></div>"
            "<div><label style='display:block; margin-bottom:4px;'>Название компании</label><input name='company' value='{company}' style='width:100%;'/></div>"
            "</div>"
            "<div style='margin: 12px 0;'>"
            "<label style='cursor: pointer; margin-right: 16px;'><input type='radio' name='search_type' value='' {auto_checked}/><span style='margin-left: 4px;'>🔄 Авто</span></label>"
            "<label style='cursor: pointer; margin-right: 16px;'><input type='radio' name='search_type' value='company' {company_checked}/><span style='margin-left: 4px;'>🏢 Только организации</span></label>"
            "<label style='cursor: pointer;'><input type='radio' name='search_type' value='person' {person_checked}/><span style='margin-left: 4px;'>👤 Только физ. лица</span></label>"
            "</div>"
            "<details style='margin-bottom: 10px;'><summary>Общий запрос (обратная совместимость)</summary>"
            "<input name='q' value='{q}' style='margin-top: 8px; width: 100%;'/>"
            "</details>"
            "<button>Найти</button></form>"
            "{norm}"
            "{not_found}"
            "{similar}"
        ).format(
            q=escape(q),
            surname=escape(surname),
            name=escape(name),
            middle_name=escape(middle_name),
            inn=escape(inn),
            company=escape(company),
            auto_checked=auto_checked,
            company_checked=company_checked,
            person_checked=person_checked,
            norm=f"<p><b>Нормализовано:</b> {escape(normalized)}</p>" if normalized else "",
            not_found=not_found,
            similar=f"<h3>Похожие варианты</h3><ul>{items}</ul>" if similar and not candidates else "",
        )

    def _search_by_inn(self, inn: str, search_type: str = "") -> tuple[list[dict[str, Any]], list[str]]:
        trace = [f"Поиск по ИНН: {inn}"]
        hits, source_trace = self._search_external_sources(inn, no_cache=False, search_type=search_type)
        for hit in hits:
            data = hit.get("data", {})
            logger.debug(
                "Хит от %s: data keys=%s, ru_org=%s",
                hit.get("source", "unknown"),
                list(data.keys()) if isinstance(data, dict) else "N/A",
                data.get("ru_org") if isinstance(data, dict) else "N/A",
            )
        trace.extend(source_trace)
        return hits, trace

    def _search_by_person(self, full_name: str, search_type: str = "") -> tuple[list[dict[str, Any]], list[str]]:
        trace = [f"Поиск по персоне: {full_name}"]
        hits, source_trace = self._search_external_sources(full_name, no_cache=False, search_type=search_type)
        trace.extend(source_trace)
        return hits, trace

    def _search_by_company(self, company_name: str, search_type: str = "") -> tuple[list[dict[str, Any]], list[str]]:
        trace = [f"Поиск по компании: {company_name}"]
        hits: list[dict[str, Any]] = []
        input_type = INPUT_TYPE_ORG_TEXT
        normalized_name, _ = self.normalize_ru_org(company_name)
        trace.append(f"Нормализовано: {normalized_name}")
        providers = self._provider_chain(input_type, normalized_name)

        for provider in providers:
            if not self._should_call_provider(provider, input_type):
                continue
            trace.append(f"Запрос к источнику: {provider['name']}")
            data = self._call_provider_with_retry(provider, normalized_name, input_type, max_retries=0, search_type=search_type)
            if data:
                for item in data:
                    hit = {
                        "source": provider["name"],
                        "url": item.get("url", ""),
                        "data": item,
                        "type": item.get("type", "unknown"),
                    }
                    logger.debug(
                        "Хит от %s: data keys=%s, ru_org=%s",
                        provider["name"],
                        list(item.keys()) if isinstance(item, dict) else "N/A",
                        item.get("ru_org") if isinstance(item, dict) else "N/A",
                    )
                    hits.append(hit)
                trace.append(f"Найдено {len(data)} записей в {provider['name']}")
                break

        if not hits:
            trace.append("Ничего не найдено, пробуем альтернативные написания")
            for alt_name in self._generate_company_name_variants(company_name):
                if alt_name == company_name:
                    continue
                trace.append(f"Попытка: {alt_name}")
                for provider in providers:
                    if not self._should_call_provider(provider, input_type):
                        continue
                    data = self._call_provider_with_retry(provider, alt_name, input_type, max_retries=0, search_type=search_type)
                    if data:
                        for item in data:
                            hit = {
                                "source": provider["name"],
                                "url": item.get("url", ""),
                                "data": item,
                                "type": item.get("type", "unknown"),
                            }
                            logger.debug(
                                "Хит от %s: data keys=%s, ru_org=%s",
                                provider["name"],
                                list(item.keys()) if isinstance(item, dict) else "N/A",
                                item.get("ru_org") if isinstance(item, dict) else "N/A",
                            )
                            hits.append(hit)
                        trace.append(f"Найдено {len(data)} записей по '{alt_name}'")
                        break
                if hits:
                    break
        return hits, trace

    def _search_by_criteria(self, params: dict[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
        trace: list[str] = []
        source_hits: list[dict[str, Any]] = []
        candidates: list[dict[str, str]] = []
        search_type = params.get("search_type", "")
        queried_providers: set[str] = set()

        if params.get("inn"):
            trace.append("Обнаружен ИНН в запросе")
            source_hits, source_trace = self._search_by_inn(params["inn"], search_type=search_type)
            trace.extend(source_trace)
            queried_providers.update(hit.get("source", "") for hit in source_hits)
        elif search_type == "company":
            trace.append("Режим: ТОЛЬКО ОРГАНИЗАЦИИ (юр. лица)")
            if params.get("company"):
                source_hits, source_trace = self._search_by_company(params["company"], search_type=search_type)
                trace.extend(source_trace)
                queried_providers.update(hit.get("source", "") for hit in source_hits)
            elif params.get("surname") or params.get("name"):
                full_name = " ".join(filter(None, [params.get("surname", ""), params.get("name", "")]))
                trace.append(f"ФИО интерпретировано как название компании: {full_name}")
                source_hits, source_trace = self._search_by_company(full_name, search_type=search_type)
                trace.extend(source_trace)
                queried_providers.update(hit.get("source", "") for hit in source_hits)
        elif search_type == "person":
            trace.append("Режим: ТОЛЬКО ФИЗ. ЛИЦА")
            if params.get("surname") or params.get("name") or params.get("middle_name"):
                full_name = " ".join(filter(None, [params.get("surname", ""), params.get("name", ""), params.get("middle_name", "")]))
                trace.append(f"Поиск по персоне: {full_name}")
                source_hits, source_trace = self._search_by_person(full_name, search_type=search_type)
                trace.extend(source_trace)
                queried_providers.update(hit.get("source", "") for hit in source_hits)
                candidates = self._build_person_candidates(source_hits, full_name, search_type=search_type)
        else:
            trace.append("Режим: АВТО (физ. + юр. лица)")
            if params.get("surname") or params.get("name") or params.get("middle_name"):
                full_name = " ".join(filter(None, [params.get("surname", ""), params.get("name", ""), params.get("middle_name", "")]))
                trace.append(f"Обнаружено ФИО в запросе: {full_name}")
                source_hits, source_trace = self._search_by_person(full_name, search_type=search_type)
                trace.extend(source_trace)
                queried_providers.update(hit.get("source", "") for hit in source_hits)
                if params.get("company"):
                    source_hits = [
                        hit
                        for hit in source_hits
                        if self._company_name_matches(str(hit.get("data", {}).get("ru_org", "")), params["company"])
                    ]
                    trace.append(f"Фильтрация по компании: {params['company']}")
                candidates = self._build_person_candidates(source_hits, full_name, search_type=search_type)
            elif params.get("company"):
                trace.append(f"Поиск по названию компании: {params['company']}")
                source_hits, source_trace = self._search_by_company(params["company"], search_type=search_type)
                trace.extend(source_trace)
                queried_providers.update(hit.get("source", "") for hit in source_hits)

        dedup_hits: list[dict[str, Any]] = []
        seen_hits: set[tuple[str, str]] = set()
        for hit in source_hits:
            source_name = str(hit.get("source", ""))
            hit_key = (source_name, str(hit.get("url", "")) or json.dumps(hit.get("data", {}), ensure_ascii=False, sort_keys=True))
            if hit_key in seen_hits:
                logger.debug("Провайдер %s уже запрошен, пропускаем дубликат", source_name)
                continue
            seen_hits.add(hit_key)
            dedup_hits.append(hit)
        source_hits = dedup_hits
        if queried_providers:
            trace.append("Провайдеры в поиске: " + ", ".join(sorted(p for p in queried_providers if p)))

        if not candidates and source_hits and search_type != "person":
            primary_query = params.get("inn") or params.get("company") or " ".join(
                filter(None, [params.get("surname", ""), params.get("name", ""), params.get("middle_name", "")])
            )
            best_hit = max(source_hits, key=lambda h: self._score_hit(h, primary_query))
            profile = dict(best_hit.get("data", {}))
            candidates = [{
                "fio_ru": " ".join(x for x in [profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")] if x).strip(),
                "org_ru": profile.get("ru_org", ""),
                "position_ru": profile.get("ru_position", ""),
                "inn": profile.get("inn", ""),
                "revenue": str(profile.get("revenue", 0) or 0),
                "source": best_hit.get("source", ""),
                "type": best_hit.get("type", profile.get("type", "unknown")),
                "query_for_autofill": profile.get("inn", "") or primary_query,
            }]

        return source_hits, candidates, trace

    def _dedup_source_hits(self, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        result: list[dict[str, Any]] = []
        for hit in hits:
            data = hit.get("data", {}) if isinstance(hit.get("data"), dict) else {}
            source_key = self._normalize_spaces(str(hit.get("source", ""))).lower()
            inn_key = self._normalize_spaces(str(data.get("inn", ""))).lower()
            org_key = self._normalize_spaces(str(data.get("ru_org", ""))).lower()
            key = (source_key, inn_key, "" if inn_key else org_key)
            if key in seen:
                continue
            seen.add(key)
            result.append(hit)
        return result

    def search_page(self, query: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        q = (query.get("q") or [""])[0].strip()
        surname = (query.get("surname") or [""])[0].strip()
        name = (query.get("name") or [""])[0].strip()
        middle_name = (query.get("middle_name") or [""])[0].strip()
        inn = (query.get("inn") or [""])[0].strip()
        company = (query.get("company") or [""])[0].strip()
        search_type = (query.get("search_type") or [""])[0].strip()

        if not search_type:
            if surname or name or middle_name:
                search_type = "person"
            elif company:
                search_type = "company"
            elif q:
                input_type = self.detect_input_type(q)
                if input_type == INPUT_TYPE_PERSON_TEXT:
                    search_type = "person"
                elif input_type == INPUT_TYPE_ORG_TEXT:
                    search_type = "company"
                else:
                    search_type = ""
            else:
                search_type = ""

        logger.info(
            "Поиск: surname=%s, name=%s, company=%s, search_type=%s",
            surname,
            name,
            company,
            search_type or "auto",
        )

        if q and not any([surname, name, middle_name, inn, company]):
            input_type = self.detect_input_type(q)
            if input_type == INPUT_TYPE_INN:
                inn = q
            elif input_type == INPUT_TYPE_PERSON_TEXT:
                surname, name, middle_name = self._split_fio_ru(q)
            else:
                company = q

        if not any([q, surname, name, middle_name, inn, company]):
            content = self._render_search_results("", "", [], [], form_values={"search_type": search_type})
            body = self._page("Карточки компаний/участников", content)
            return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

        person_query = " ".join(filter(None, [surname, name, middle_name]))
        db_query = inn or company or person_query or q
        normalized = ""
        if company:
            normalized, _ = self.normalize_ru_org(company)
        elif db_query:
            normalized, _ = self.normalize_ru_org(db_query)

        with self._connect() as db:
            if inn:
                exact = db.execute("SELECT * FROM cards WHERE json_extract(data_json, '$.profile.inn')=? ORDER BY id DESC", (inn,)).fetchall()
            else:
                exact = db.execute("SELECT * FROM cards WHERE ru_org=? OR json_extract(data_json, '$.profile.source_id')=? ORDER BY id DESC", (normalized, db_query)).fetchall()
            token = (normalized or db_query).split()[0] if (normalized or db_query) else ""
            similar = db.execute("SELECT * FROM cards WHERE ru_org LIKE ? ORDER BY id DESC LIMIT 10", (f"%{token}%",)).fetchall() if token else []

        if exact:
            return "", "302 Found", [("Location", f"/card/{exact[0]['id']}")]

        _, candidates, _ = self._search_by_criteria({
            "surname": surname,
            "name": name,
            "middle_name": middle_name,
            "inn": inn,
            "company": company,
            "search_type": search_type,
        })

        content = self._render_search_results(
            q,
            normalized,
            candidates,
            similar,
            form_values={
                "surname": surname,
                "name": name,
                "middle_name": middle_name,
                "inn": inn,
                "company": company,
                "search_type": search_type,
            },
        )
        body = self._page("Карточки компаний/участников", content)
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def autofill_review(self, form: dict[str, list[str]], *, wants_json: bool = False) -> tuple[str, str, list[tuple[str, str]]]:
        raw = self._get_one(form, "company_name")
        hit_type = self._normalize_spaces(self._get_one(form, "hit_type")).lower()
        forced_search_type = self._normalize_spaces(self._get_one(form, "search_type")).lower()
        no_cache = self._get_one(form, "no_cache") == "1"
        input_type = self.detect_input_type(raw)
        if hit_type in {"company", "person"}:
            input_type = INPUT_TYPE_ORG_TEXT if hit_type == "company" else INPUT_TYPE_PERSON_TEXT
        elif forced_search_type in {"company", "person"}:
            input_type = INPUT_TYPE_ORG_TEXT if forced_search_type == "company" else INPUT_TYPE_PERSON_TEXT
        normalized_raw = self._normalize_spaces(raw).lower()
        cache_key = f"search:{input_type}:{hit_type}:{forced_search_type}:{normalized_raw}"

        person_mode = forced_search_type == "person" or hit_type == "person" or input_type == INPUT_TYPE_PERSON_TEXT
        raw_tokens = [tok for tok in self._normalize_spaces(raw).split() if tok]
        if person_mode and len(raw_tokens) < 2:
            logger.info("Person-mode autocreate skipped before build: insufficient name tokens")
            manual_payload = {
                "q": raw,
                "error": "Ничего не найдено: укажите минимум имя и фамилию",
            }
            response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
            with self._active_searches_lock:
                    self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
            return response

        with self._active_searches_lock:
            cached_result = self._autofill_result_cache.get(cache_key)
        if cached_result and time.time() < float(cached_result.get("expires_at", 0)):
            logger.info("Используем кэш результата autofill для %s", raw)
            return cached_result["response"]

        with self._active_searches_lock:
            if cache_key in self._active_searches:
                logger.info("Поиск уже выполняется для %s, используем последний кэш", raw)
                if cached_result:
                    return cached_result["response"]

            self._active_searches[cache_key] = time.time()
        try:
            if self._get_one(form, "reset_cache") == "1" and input_type == INPUT_TYPE_INN:
                dropped = self._clear_cache_for_inn(self._extract_inn(raw))
                reset_note = [f"Кэш по ИНН очищен: {dropped}"]
            elif self._get_one(form, "reset_cache") == "1" and input_type == INPUT_TYPE_PERSON_TEXT:
                dropped = self._clear_cache_for_person(raw)
                reset_note = [f"Кэш по персоне очищен: {dropped}"]
            else:
                reset_note = []

            effective_hit_type = hit_type if hit_type in {"company", "person"} else ""
            if not effective_hit_type and forced_search_type in {"company", "person"}:
                effective_hit_type = forced_search_type

            fast_mode_used = input_type == INPUT_TYPE_INN
            if fast_mode_used:
                search_trace = ["⚡ FAST INN MODE: only ФНС ЕГРЮЛ"]
                source_hits, fast_trace = self._search_external_sources(
                    raw,
                    no_cache=no_cache,
                    search_type=forced_search_type,
                    provider_names=["ФНС ЕГРЮЛ"],
                )
                search_trace.extend(fast_trace)
            else:
                source_hits, search_trace = self._search_external_sources(raw, no_cache=no_cache, search_type=forced_search_type)

            source_hits = self._dedup_source_hits(source_hits)

            if not effective_hit_type and source_hits:
                first_hit_type = self._normalize_spaces(str(source_hits[0].get("type", ""))).lower()
                if first_hit_type in {"company", "person"}:
                    effective_hit_type = first_hit_type
            search_trace = reset_note + search_trace

            def extract_data(hits: list[dict[str, Any]]) -> dict[str, str]:
                extracted: dict[str, str] = {}
                for hit in hits:
                    data = hit.get("data", {})
                    if not isinstance(data, dict):
                        continue
                    for field in [
                        "surname_ru",
                        "name_ru",
                        "middle_name_ru",
                        "ru_org",
                        "en_org",
                        "ru_position",
                        "en_position",
                        "gender",
                        "inn",
                    ]:
                        value = self._normalize_spaces(str(data.get(field, "")))
                        if field in {"ru_org", "en_org", "ru_position"} and self.is_block_page_value(value):
                            value = ""
                        if value and not extracted.get(field):
                            extracted[field] = value
                return extracted

            extracted_data = extract_data(source_hits)
            profile, field_sources = self._build_profile_from_sources(source_hits, raw, input_type, forced_type=effective_hit_type)

            if fast_mode_used and not self._is_profile_complete(profile):
                search_trace.append("🔍 AUTOCOMPLETE MODE: FAST profile incomplete, enabling extended providers")
                extended_hits, extended_trace = self._search_external_sources(raw, no_cache=no_cache, search_type=forced_search_type)
                search_trace.extend(extended_trace)
                source_hits = self._dedup_source_hits(source_hits + extended_hits)
                extracted_data = extract_data(source_hits)
                profile, field_sources = self._build_profile_from_sources(source_hits, raw, input_type, forced_type=effective_hit_type)
            if effective_hit_type in {"company", "person"}:
                profile["type"] = effective_hit_type
            if forced_search_type in {"company", "person"}:
                profile["search_type"] = forced_search_type
            for key, value in extracted_data.items():
                if not value or profile.get(key):
                    continue
                if key in {"surname_ru", "name_ru", "middle_name_ru"}:
                    surname = self._normalize_spaces(str(extracted_data.get("surname_ru", "")))
                    name = self._normalize_spaces(str(extracted_data.get("name_ru", "")))
                    middle = self._normalize_spaces(str(extracted_data.get("middle_name_ru", "")))
                    accepted, _ = self._validate_leader_fio_candidate(surname, name, middle)
                    if not accepted:
                        continue
                profile[key] = value
                field_sources[key] = "Источник данных"

            candidate_sur, candidate_nam, candidate_pat, candidate_source = self._pick_best_leader_fio(source_hits)
            self._sanitize_profile_fio(profile)
            current_sur = self._normalize_spaces(profile.get("surname_ru", ""))
            current_nam = self._normalize_spaces(profile.get("name_ru", ""))
            current_pat = self._normalize_spaces(profile.get("middle_name_ru", ""))
            current_source = field_sources.get("surname_ru", "")
            has_valid_current = self._is_valid_leader_fio(current_sur, current_nam, current_pat)
            if candidate_sur and candidate_nam:
                if (
                    not has_valid_current
                    or self._leader_source_rank(candidate_source) < self._leader_source_rank(current_source)
                ):
                    profile["surname_ru"] = candidate_sur
                    profile["name_ru"] = candidate_nam
                    profile["middle_name_ru"] = candidate_pat
                    field_sources["surname_ru"] = candidate_source
                    field_sources["name_ru"] = candidate_source
                    if candidate_pat:
                        field_sources["middle_name_ru"] = candidate_source
            final_source = field_sources.get("surname_ru", field_sources.get("name_ru", ""))
            final_fio = " ".join(part for part in (profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")) if part)
            accepted, reason = self._validate_leader_fio_candidate(profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", ""))
            logger.info("leader_fio_candidate source=%s fio='%s' accepted=%s reason=%s", final_source, final_fio, accepted, reason)
            if final_fio and not accepted:
                for field in ("surname_ru", "name_ru", "middle_name_ru", "surname", "name", "family_name", "first_name"):
                    profile[field] = ""
                    field_sources.pop(field, None)

            logger.info("=== AUTOFILL REVIEW ===")
            logger.info("source_hits count: %d", len(source_hits))
            for i, hit in enumerate(source_hits):
                logger.info("Hit %d: source=%s, type=%s", i, hit.get("source"), hit.get("type"))
                logger.info("  data keys: %s", list(hit.get("data", {}).keys()) if isinstance(hit.get("data"), dict) else "N/A")
                logger.info("  ru_org: %s", hit.get("data", {}).get("ru_org") if isinstance(hit.get("data"), dict) else "N/A")
                logger.info("  surname_ru: %s", hit.get("data", {}).get("surname_ru") if isinstance(hit.get("data"), dict) else "N/A")

            logger.info("Profile after build:")
            for field in self._required_fields_for_profile(profile):
                logger.info("  required.%s: '%s' (from %s)", field, profile.get(field), field_sources.get(field))

            if not profile.get("ru_org") and not profile.get("surname_ru") and not profile.get("name_ru"):
                logger.error("Профиль пустой после построения из %d хитов!", len(source_hits))
            filled_fields = [k for k, v in profile.items() if v and k not in {"title", "appeal"}]
            logger.info("Заполненные поля профиля: %s", filled_fields)

            profile, notes = self.apply_card_rules(profile)
            if source_hits:
                notes.append(f"Источники: найдено {len(source_hits)}")
            else:
                notes.append("Источники: не получено (в источниках нет данных по запросу)")

            _ = self._field_statuses(profile, notes)
            manual_payload = {
                "q": profile.get("ru_org", "") or raw,
                "en_org": profile.get("en_org", ""),
                "person_ru": "  ".join(
                    x for x in [profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")] if x
                ).strip(),
                "person_en": "  ".join(
                    x for x in [profile.get("family_name", ""), profile.get("first_name", ""), profile.get("middle_name_en", "")] if x
                ).strip(),
                "gender": profile.get("gender", ""),
                "ru_position": profile.get("ru_position", ""),
                "en_position": profile.get("en_position", profile.get("position", "")),
            }
            for key, value in profile.items():
                manual_payload[f"profile_{key}"] = value

            if forced_search_type == "person" and not (self._normalize_spaces(profile.get("surname_ru", "")) and self._normalize_spaces(profile.get("name_ru", ""))):
                logger.info("autocreate_skipped: reason=invalid_fields details=person_without_name")
                manual_payload["error"] = "Ничего не найдено: укажите минимум имя и фамилию"
                response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
                with self._active_searches_lock:
                    self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
                return response

            if self._is_garbage_org_title(profile.get("ru_org", ""), raw):
                logger.info("autocreate_skipped: reason=garbage_title")
                manual_payload["error"] = "Нужно уточнить: добавьте ИНН или корректное название организации"
                response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
                with self._active_searches_lock:
                    self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
                return response

            if self._detect_profile_type(profile) == "company" and not self._normalize_spaces(profile.get("inn", "")):
                logger.info("autocreate_skipped: reason=low_confidence details=company_without_inn")
                manual_payload["error"] = "Нужно уточнить: добавьте ИНН или ОГРН"
                response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
                with self._active_searches_lock:
                    self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
                return response
            if forced_search_type == "company":
                surname_ru = self._normalize_spaces(profile.get("surname_ru", ""))
                name_ru = self._normalize_spaces(profile.get("name_ru", ""))
                middle_name_ru = self._normalize_spaces(profile.get("middle_name_ru", ""))
                family_name = self._normalize_spaces(profile.get("family_name", ""))
                first_name = self._normalize_spaces(profile.get("first_name", ""))
                ru_position = self._normalize_spaces(profile.get("ru_position", ""))
                has_fio_ru = bool(surname_ru and name_ru)
                has_fio_en = bool(family_name and first_name)
                has_fio = has_fio_ru or has_fio_en
                has_position = bool(self._normalize_spaces(profile.get("ru_position", "")))
                logger.info(
                    "autocreate_company_leader_check surname_ru='%s' name_ru='%s' middle_name_ru='%s' family_name='%s' first_name='%s' ru_position='%s' sources={surname_ru:%s,name_ru:%s,middle_name_ru:%s,family_name:%s,first_name:%s,ru_position:%s}",
                    surname_ru,
                    name_ru,
                    middle_name_ru,
                    family_name,
                    first_name,
                    ru_position,
                    field_sources.get("surname_ru", ""),
                    field_sources.get("name_ru", ""),
                    field_sources.get("middle_name_ru", ""),
                    field_sources.get("family_name", ""),
                    field_sources.get("first_name", ""),
                    field_sources.get("ru_position", ""),
                )
                if has_position and not has_fio:
                    logger.info("autocreate_skipped: reason=invalid_fields details=company_without_leader")
                    manual_payload["error"] = "Не найден руководитель компании, уточните/выберите из списка"
                    response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
                    with self._active_searches_lock:
                        self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
                    return response

            missing_fields = self._missing_required_fields(profile)
            logger.info("autofill.card_type=%s", self._detect_profile_type(profile))
            logger.info("autofill.required_fields=%s", ", ".join(self._required_fields_for_profile(profile)))
            logger.info("autofill.missing_fields=%s", ", ".join(missing_fields) if missing_fields else "none")
            if missing_fields:
                logger.info("autocreate_skipped: reason=invalid_fields missing=%s", ",".join(missing_fields))
                manual_payload["error"] = f"Заполните обязательные поля: {', '.join(missing_fields)}"
                response = self._autofill_redirect_response(f"/create/manual?{urlencode(manual_payload)}", wants_json=wants_json)
            else:
                logger.info("Все обязательные поля заполнены! Создаем карточку автоматически.")
                card_type = self._detect_profile_type(profile)
                before_ru_org = profile.get("ru_org", "")
                before_en_org = profile.get("en_org", "")
                profile, final_notes = self.apply_card_rules(profile, card_type=card_type)
                notes.extend(final_notes)
                logger.info(
                    "apply_card_rules.before_insert card_type=%s ru_org: '%s' -> '%s'; en_org: '%s' -> '%s'",
                    card_type,
                    before_ru_org,
                    profile.get("ru_org", ""),
                    before_en_org,
                    profile.get("en_org", ""),
                )
                card_obj = Card.from_profile(profile)
                profile["family_name"] = card_obj.family_name or profile.get("family_name", "")
                profile["first_name"] = card_obj.first_name or profile.get("first_name", "")
                profile["middle_name_en"] = (card_obj.middle_name_en or profile.get("middle_name_en", "")).strip()
                card_id = self._create_autofill_card(profile, notes, source_hits, search_trace, field_sources)
                logger.info("Карточка #%d создана автоматически", card_id)
                response = self._autofill_redirect_response(f"/card/{card_id}", wants_json=wants_json)
            with self._active_searches_lock:
                self._autofill_result_cache[cache_key] = {"response": response, "expires_at": time.time() + 20}
            return response
        finally:
            with self._active_searches_lock:
                self._active_searches.pop(cache_key, None)

    def autofill_confirm(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        action = self._get_one(form, "action") or "edit"
        if action == "cancel":
            return "", "302 Found", [("Location", "/")]
        notes = form.get("notes", [])
        source_names = form.get("source_name", [])
        search_trace = form.get("search_trace", [])
        field_provenance = {
            key.removeprefix("field_source_"): self._get_one(form, key)
            for key in form
            if key.startswith("field_source_") and self._get_one(form, key)
        }
        profile_data = {
            key.removeprefix("profile_"): self._get_one(form, key)
            for key in form
            if key.startswith("profile_")
        }
        ru_org = profile_data.get("ru_org", self._get_one(form, "ru_org"))
        en_org = profile_data.get("en_org", self._get_one(form, "en_org"))
        profile_data["ru_org"] = ru_org
        profile_data["en_org"] = en_org
        if profile_data.get("position") and not profile_data.get("en_position"):
            profile_data["en_position"] = profile_data.get("position", "")
        if profile_data.get("appeal") and not profile_data.get("salutation"):
            profile_data["salutation"] = profile_data.get("appeal", "")
        input_value = self._get_one(form, "input_value")
        if self.detect_input_type(input_value) == INPUT_TYPE_INN:
            profile_data["inn"] = input_value
        elif self.detect_input_type(input_value) == INPUT_TYPE_URL:
            profile_data["source_id"] = input_value

        if action == "edit":
            q = ru_org or input_value
            manual_payload = {
                "q": q,
                "en_org": en_org,
                "person_ru": " ".join(x for x in [profile_data.get("surname_ru", ""), profile_data.get("name_ru", ""), profile_data.get("middle_name_ru", "")] if x).strip(),
                "person_en": " ".join(x for x in [profile_data.get("family_name", ""), profile_data.get("first_name", ""), profile_data.get("middle_name_en", profile_data.get("middle_name", ""))] if x).strip(),
                "gender": profile_data.get("gender", ""),
                "ru_position": profile_data.get("ru_position", ""),
                "en_position": profile_data.get("en_position", ""),
            }
            for key, value in profile_data.items():
                manual_payload[f"profile_{key}"] = value
            return "", "302 Found", [("Location", f"/create/manual?{urlencode(manual_payload)}")]

        profile_data, rule_notes = self.apply_card_rules(profile_data)
        notes = notes + rule_notes

        missing_fields = self._missing_required_fields(profile_data)
        if missing_fields:
            params = {f"profile_{key}": value for key, value in profile_data.items()}
            params["q"] = ru_org or input_value
            params["en_org"] = en_org
            params["error"] = f"Заполните обязательные поля: {', '.join(missing_fields)}"
            return "", "302 Found", [("Location", f"/create/manual?{urlencode(params)}")]

        card_obj = Card.from_profile(profile_data)
        profile_data["family_name"] = card_obj.family_name or profile_data.get("family_name", "")
        profile_data["first_name"] = card_obj.first_name or profile_data.get("first_name", "")
        profile_data["middle_name_en"] = card_obj.middle_name_en or profile_data.get("middle_name_en", profile_data.get("middle_name", ""))
        card_id = self._create_autofill_card(profile_data, notes, [{"source": s} for s in source_names], search_trace, field_provenance)
        return "", "302 Found", [("Location", f"/card/{card_id}")]

    def manual_get(self, query: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        q = (query.get("q") or [""])[0]
        en_org_param = (query.get("en_org") or [""])[0]
        person_ru = (query.get("person_ru") or [""])[0]
        person_en = (query.get("person_en") or [""])[0]
        gender = (query.get("gender") or [""])[0]
        ru_position = (query.get("ru_position") or [""])[0]
        en_position = (query.get("en_position") or [""])[0]
        error = (query.get("error") or [""])[0]
        profile_prefill = {key.removeprefix("profile_"): (values[0] if values else "") for key, values in query.items() if key.startswith("profile_")}
        q_digits = re.sub(r"\D", "", q)
        q_looks_like_registry_id = bool(re.fullmatch(r"\d{10}|\d{12}|\d{13}|\d{15}", q_digits))
        ru_org, _ = self.normalize_ru_org(q) if q and not q_looks_like_registry_id else ("", [])

        ru_org = profile_prefill.get("ru_org", ru_org)
        en_org = profile_prefill.get("en_org", en_org_param)

        surname_ru = profile_prefill.get("surname_ru", "")
        name_ru = profile_prefill.get("name_ru", "")
        middle_name_ru = profile_prefill.get("middle_name_ru", "")
        if not surname_ru and person_ru:
            person_ru_parts = person_ru.split()
            surname_ru = person_ru_parts[0] if len(person_ru_parts) > 0 else ""
            name_ru = person_ru_parts[1] if len(person_ru_parts) > 1 else ""
            middle_name_ru = person_ru_parts[2] if len(person_ru_parts) > 2 else ""

        family_name = profile_prefill.get("family_name", "")
        first_name = profile_prefill.get("first_name", "")
        middle_name_en = profile_prefill.get("middle_name_en", "")
        if not family_name and person_en:
            person_en_parts = person_en.split()
            family_name = person_en_parts[0] if len(person_en_parts) > 0 else ""
            first_name = person_en_parts[1] if len(person_en_parts) > 1 else ""
            middle_name_en = person_en_parts[2] if len(person_en_parts) > 2 else ""
        appeal = profile_prefill.get("appeal", self._derive_salutation(gender))
        inn = profile_prefill.get("inn", (query.get("inn") or [""])[0])
        if not profile_prefill and q_looks_like_registry_id and not inn:
            inn = q_digits
        if profile_prefill:
            ru_position = profile_prefill.get("ru_position", ru_position)
            en_position = profile_prefill.get("en_position", profile_prefill.get("position", en_position))
            gender = profile_prefill.get("gender", gender)
            appeal = profile_prefill.get("appeal", appeal)
            inn = profile_prefill.get("inn", inn)
        male_selected = " selected" if gender == "М" else ""
        female_selected = " selected" if gender == "Ж" else ""
        error_html = f"<p style='color:#b22'>{escape(error)}</p>" if error else ""
        content = (
            "<h2>Ручное создание</h2>"
            f"{error_html}"
            "<form method='post' action='/create/manual'>"
            f"<p>Титул <input name='title' value='{escape(profile_prefill.get('title', ''))}'></p>"
            f"<p>Обращение <input name='appeal' value='{escape(appeal)}'></p>"
            f"<p>Family name <input name='family_name' value='{escape(family_name)}'></p>"
            f"<p>First name <input name='first_name' value='{escape(first_name)}'></p>"
            f"<p>Middle name (EN) <input name='middle_name_en' value='{escape(middle_name_en)}'></p>"
            f"<p>Фамилия <input name='surname_ru' value='{escape(surname_ru)}'></p>"
            f"<p>Имя <input name='name_ru' value='{escape(name_ru)}'></p>"
            f"<p>Middle name. рус <input name='middle_name_ru' value='{escape(middle_name_ru)}'></p>"
            f"<p>ИНН организации <input name='inn' value='{escape(inn)}' pattern='[0-9]{10,12}' title='10-12 цифр'></p>"
            f"<p>Организация RU (название) * <input name='ru_org' required value='{escape(ru_org)}'></p>"
            f"<p>Organization EN * <input name='en_org' required value='{escape(en_org)}'></p>"
            f"<p>Пол <select name='gender'><option value=''>--</option><option{male_selected}>М</option><option{female_selected}>Ж</option></select></p>"
            f"<p>Должность RU * <input name='ru_position' required value='{escape(ru_position)}'></p>"
            f"<p>Position EN * <input name='en_position' required value='{escape(en_position)}'></p>"
            "<button>Сохранить</button></form>"
            "<script>document.querySelector(\"form[action='/create/manual']\")?.addEventListener('submit', function (e) {"
            "  const required = this.querySelectorAll('[required]');"
            "  let invalid = false;"
            "  required.forEach(function (el) {"
            "    if (!el.value.trim()) { el.style.borderColor = '#b22'; invalid = true; } else { el.style.borderColor = ''; }"
            "  });"
            "  if (invalid) { e.preventDefault(); alert('Заполните обязательные поля, выделенные *'); }"
            "});</script>"
        )
        body = self._page("Ручное создание", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def manual_post(self, form: dict[str, list[str]], *, wants_json: bool = False) -> tuple[str, str, list[tuple[str, str]]]:
        ru_org_raw = self._normalize_spaces(self._get_one(form, "ru_org"))
        inn_input = self._extract_inn(self._get_one(form, "inn") or ru_org_raw)
        ru_org_input = ru_org_raw
        autofill_notes: list[str] = []
        auto_profile: dict[str, str] = {}

        if re.fullmatch(r"\d{10,12}", ru_org_raw):
            inn_input = ru_org_raw
            egrul = self._parse_egrul(ru_org_raw)
            if egrul:
                ru_org_input = str(egrul.get("ru_org", ""))
                auto_profile = {
                    "surname_ru": str(egrul.get("surname_ru", "")),
                    "name_ru": str(egrul.get("name_ru", "")),
                    "middle_name_ru": str(egrul.get("middle_name_ru", "")),
                    "ru_position": str(egrul.get("ru_position", "")),
                }
                autofill_notes.append("Организация RU определена по ИНН из ФНС ЕГРЮЛ")

        ru_org, ru_notes = self.normalize_ru_org(ru_org_input)
        en_org, en_notes = self.normalize_en_org(self._get_one(form, "en_org"), ru_org)
        gender = self._get_one(form, "gender")
        errors: list[str] = []
        surname_ru = self._get_one(form, "surname_ru") or auto_profile.get("surname_ru", "")
        name_ru = self._get_one(form, "name_ru") or auto_profile.get("name_ru", "")
        middle_name_ru = self._get_one(form, "middle_name_ru") or auto_profile.get("middle_name_ru", "")
        ru_position = self._get_one(form, "ru_position") or auto_profile.get("ru_position", "")
        en_position = self._get_one(form, "en_position")
        if not en_position and ru_position:
            en_position = self._generate_en_position(ru_position)
        if not gender and (middle_name_ru or ru_position):
            gender = self._infer_gender(middle_name_ru, ru_position)
        if (surname_ru or name_ru) and gender not in {"М", "Ж"}:
            errors.append("Пол обязателен: М/Ж")
        profile = {
            "title": self._get_one(form, "title"),
            "appeal": self._get_one(form, "appeal") or self._derive_salutation(gender),
            "family_name": self._get_one(form, "family_name"),
            "first_name": self._get_one(form, "first_name"),
            "middle_name_en": self._get_one(form, "middle_name_en"),
            "surname_ru": surname_ru,
            "name_ru": name_ru,
            "middle_name_ru": middle_name_ru,
            "gender": gender,
            "inn": inn_input,
            "ru_org": ru_org,
            "en_org": en_org,
            "ru_position": ru_position,
            "en_position": en_position,
        }
        missing_required = self._missing_required_fields(profile)
        if missing_required:
            errors.append(f"Заполните обязательные поля: {', '.join(missing_required)}")

        search_type = self._normalize_spaces(self._get_one(form, "search_type")).lower()
        en_org_raw = self._normalize_spaces(self._get_one(form, "en_org"))
        required_checks = [
            ("ru_org", ru_org_raw, "Заполните обязательное поле: ru_org"),
            ("en_org", en_org_raw, "Заполните обязательное поле: en_org"),
        ]
        if search_type == "company":
            required_checks.append(("inn", self._normalize_spaces(self._get_one(form, "inn")), "Заполните обязательное поле: inn"))
        for field, value, message in required_checks:
            if self._normalize_spaces(str(value)):
                continue
            if wants_json:
                body = json.dumps({"error": message, "field": field}, ensure_ascii=False)
                return body, "400 Bad Request", [("Content-Type", "application/json; charset=utf-8")]
            params = {"error": message}
            for key, value_raw in profile.items():
                params[f"profile_{key}"] = value_raw
            return "", "302 Found", [("Location", f"/create/manual?{urlencode(params)}")]

        notes = ru_notes + en_notes + autofill_notes + errors
        status = self._status(notes, bool(ru_org and en_org))
        if errors:
            params = {
                "q": ru_org,
                "en_org": en_org,
                "inn": profile["inn"],
                "gender": gender,
                "ru_position": self._get_one(form, "ru_position"),
                "en_position": self._get_one(form, "en_position"),
                "error": "; ".join(errors),
            }
            for key, value in profile.items():
                params[f"profile_{key}"] = value
            return "", "302 Found", [("Location", f"/create/manual?{urlencode(params)}")]

        card_obj = Card.from_profile(profile)
        profile["family_name"] = card_obj.family_name or profile["family_name"]
        profile["first_name"] = card_obj.first_name or profile["first_name"]
        profile["middle_name_en"] = card_obj.middle_name_en or profile["middle_name_en"]

        data = {
            "notes": notes,
            "profile": profile,
            "gender": gender,
            "ru_position": self._get_one(form, "ru_position"),
            "en_position": self._get_one(form, "en_position"),
        }
        with self._connect() as db:
            cur = db.execute(
                "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
                (ru_org, en_org, status, "manual", self._now(), self._now(), json.dumps(data, ensure_ascii=False)),
            )
            card_id = cur.lastrowid
            db.commit()
        self._write_audit("create_manual", card_id, {"ru_org": ru_org, "status": status})
        return "", "302 Found", [("Location", f"/card/{card_id}")]

    def card_view(self, card_id: int) -> tuple[str, str, list[tuple[str, str]]]:
        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
            audits = db.execute("SELECT * FROM audits WHERE card_id=? ORDER BY id DESC", (card_id,)).fetchall()
        if not card:
            return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]
        entries = "".join(
            f"<li>{escape(a['created_at'])} — {escape(a['action'])} ({escape(a['actor'])})</li>" for a in audits
        )
        payload = json.loads(card["data_json"] or "{}")
        trace = payload.get("search_trace", [])
        trace_html = "<h3>Как происходил поиск</h3><ol>" + "".join(f"<li>{escape(step)}</li>" for step in trace) + "</ol>" if trace else ""
        profile = payload.get("profile", {})
        if not profile:
            profile = {field: "" for field, _ in CARD_FIELDS}
            profile["ru_org"] = card["ru_org"]
            profile["en_org"] = card["en_org"]
        lines = "".join(f"<tr><td>{escape(label)}</td><td>{escape(self._profile_value(profile, field))}</td></tr>" for field, label in CARD_FIELDS)
        content = (
            f"<h2>Карточка #{card['id']}</h2>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            f"{lines}</table>"
            f"<p>Статус: {escape(card['status'])}</p>"
            f"<p>Источник: {escape(card['source'])}</p>"
            f"<p><a href='/card/{card['id']}/edit'>Редактировать карточку</a></p>"
            f"<a href='/card/{card['id']}/export'>Показать данные карточки на сайте</a>"
            f"<br><a href='/card/{card['id']}/export/xlsx'>Скачать Excel</a>"
            f"{trace_html}"
            "<h3>Audit log</h3><ul>" + entries + "</ul>"
        )
        body = self._page(f"Карточка #{card['id']}", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def card_edit_get(self, card_id: int) -> tuple[str, str, list[tuple[str, str]]]:
        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not card:
            return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

        payload = json.loads(card["data_json"] or "{}")
        profile = payload.get("profile", {})
        if not profile:
            profile = {field: "" for field, _ in CARD_FIELDS}
            profile["ru_org"] = card["ru_org"]
            profile["en_org"] = card["en_org"]
        inputs = "".join(
            "<tr>"
            f"<td>{escape(label)}</td>"
            f"<td><input name='{escape(field)}' value='{escape(self._profile_value(profile, field))}'></td>"
            "</tr>"
            for field, label in CARD_FIELDS
        )
        content = (
            f"<h2>Редактирование карточки #{card['id']}</h2>"
            f"<form method='post' action='/card/{card['id']}/edit'>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            f"{inputs}</table>"
            "<p><button>Сохранить изменения</button></p>"
            "</form>"
        )
        body = self._page(f"Редактирование карточки #{card['id']}", content, back_href=f"/card/{card_id}")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def card_edit_post(self, card_id: int, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        profile = {field: self._get_one(form, field) for field, _ in CARD_FIELDS}
        ru_org, ru_notes = self.normalize_ru_org(profile.get("ru_org", ""))
        en_org, en_notes = self.normalize_en_org(profile.get("en_org", ""), ru_org)
        profile["ru_org"] = ru_org
        profile["en_org"] = en_org
        profile["position"] = profile.get("en_position", "")
        notes = ru_notes + en_notes
        status = self._status(notes, bool(ru_org and en_org))

        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
            if not card:
                return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

            payload = json.loads(card["data_json"] or "{}")
            payload["notes"] = notes
            payload["profile"] = profile
            db.execute(
                "UPDATE cards SET ru_org=?, en_org=?, status=?, updated_at=?, data_json=? WHERE id=?",
                (ru_org, en_org, status, self._now(), json.dumps(payload, ensure_ascii=False), card_id),
            )
            db.commit()

        self._write_audit("edit", card_id, {"ru_org": ru_org, "en_org": en_org, "status": status})
        return "", "302 Found", [("Location", f"/card/{card_id}")]

    def export_preview(self, card_id: int) -> tuple[str, str, list[tuple[str, str]]]:
        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not card:
            return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

        payload = json.loads(card["data_json"] or "{}")
        profile = payload.get("profile", {})
        field_sources = payload.get("field_provenance", payload.get("field_sources", {}))
        if not profile:
            profile = {field: "" for field, _ in CARD_FIELDS}
            profile["ru_org"] = card["ru_org"]
            profile["en_org"] = card["en_org"]
        lines = "\n".join(f"{label}: {self._profile_value(profile, field)}" for field, label in CARD_FIELDS)
        source_rows = "".join(
            f"<tr><td>{escape(label)}</td><td>{escape(field_sources.get(field, '—'))}</td></tr>"
            for field, label in CARD_FIELDS
        )
        source_names = payload.get("source_hits", [])
        sources_list = "<ul>" + "".join(f"<li>{escape(source)}</li>" for source in source_names) + "</ul>" if source_names else "<p>Источники не зафиксированы.</p>"
        content = (
            f"<h2>Данные карточки #{card['id']}</h2>"
            f"<pre>{escape(lines)}</pre>"
            "<h3>Откуда взята информация (по полям)</h3>"
            "<table border='1' cellpadding='6' cellspacing='0'><tr><th>Поле</th><th>Источник</th></tr>"
            f"{source_rows}</table>"
            "<h3>Список использованных источников</h3>"
            f"{sources_list}"
        )
        body = self._page(f"Данные карточки #{card['id']}", content, back_href=f"/card/{card_id}")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def export_csv(self, card_id: int) -> tuple[str, str, list[tuple[str, str]]]:
        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not card:
            return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

        payload = json.loads(card["data_json"] or "{}")
        profile = payload.get("profile", {}) if isinstance(payload, dict) else {}
        csv_fields = [
            "id",
            "surname_ru",
            "name_ru",
            "middle_name_ru",
            "family_name",
            "first_name",
            "middle_name_en",
            "gender",
            "inn",
            "ru_position",
            "en_position",
            "ru_org",
            "en_org",
            "status",
            "source",
            "created_at",
        ]
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=csv_fields)
        writer.writeheader()
        writer.writerow(
            {
                "id": card["id"],
                "surname_ru": profile.get("surname_ru", ""),
                "name_ru": profile.get("name_ru", ""),
                "middle_name_ru": profile.get("middle_name_ru", ""),
                "family_name": profile.get("family_name", ""),
                "first_name": profile.get("first_name", ""),
                "middle_name_en": profile.get("middle_name_en", ""),
                "gender": profile.get("gender", ""),
                "inn": profile.get("inn", ""),
                "ru_position": profile.get("ru_position", ""),
                "en_position": profile.get("en_position", profile.get("position", "")),
                "ru_org": profile.get("ru_org", card["ru_org"]),
                "en_org": profile.get("en_org", card["en_org"]),
                "status": card["status"],
                "source": card["source"],
                "created_at": card["created_at"],
            }
        )
        headers = [
            ("Content-Type", "text/csv; charset=utf-8"),
            ("Content-Disposition", f'attachment; filename="card_{card_id}.csv"'),
        ]
        return buffer.getvalue(), "200 OK", headers

    def export_xlsx(self, card_id: int) -> tuple[str | bytes, str, list[tuple[str, str]]]:
        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not card:
            return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

        payload = json.loads(card["data_json"] or "{}")
        profile = payload.get("profile", {})
        try:
            import openpyxl

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Карточка"
            for field, label in CARD_FIELDS:
                ws.append([label, profile.get(field, "")])
            ws.append(["Статус", card["status"]])
            ws.append(["Источник", card["source"]])
            ws.append(["Дата создания", card["created_at"]])
            buf = io.BytesIO()
            wb.save(buf)
            return (
                buf.getvalue(),
                "200 OK",
                [
                    ("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                    ("Content-Disposition", f'attachment; filename="card_{card_id}.xlsx"'),
                ],
            )
        except ImportError:
            return self.export_csv(card_id)


def _startup_diagnostics(app: CompanyWebApp) -> None:
    playwright_cli = shutil.which("playwright")
    browsers_info = "playwright CLI not found"
    if playwright_cli:
        try:
            proc = subprocess.run([playwright_cli, "install", "--list"], capture_output=True, text=True, timeout=10, check=False)
            details = (proc.stdout or proc.stderr or "").strip().splitlines()
            browsers_info = details[0] if details else f"playwright CLI found (exit={proc.returncode})"

            list_output = f"{proc.stdout or ''}\n{proc.stderr or ''}".lower()
            has_chromium = "chromium" in list_output and "not installed" not in list_output
            if not has_chromium:
                def _install_chromium() -> None:
                    logger.info("Playwright: auto-install chromium started")
                    try:
                        install_proc = subprocess.run([playwright_cli, "install", "chromium"], capture_output=True, text=True, timeout=300, check=False)
                        if install_proc.returncode == 0:
                            logger.info("Playwright: auto-install done (exit=0)")
                        else:
                            logger.warning("Playwright: auto-install failed (exit=%s)", install_proc.returncode)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Playwright: auto-install failed: %s", exc)

                threading.Thread(target=_install_chromium, daemon=True).start()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Playwright browsers check failed: %s", exc)
            browsers_info = f"playwright CLI found, browser list failed: {exc}"

    logger.info("=== Startup diagnostics ===")
    logger.info("Python: %s", sys.version.split()[0])
    logger.info("Interpreter: %s", sys.executable)
    logger.info("Playwright browsers: %s", browsers_info)
    logger.info("ScrapeClient initialized: %s", bool(getattr(app, "scrape_client", None)))
    logger.info("Resource check dlya_anala.xlsx exists: %s", Path(resource_path("dlya_anala.xlsx")).exists())


def _pick_free_port(host: str, start: int = 8000, end: int = 8050) -> int:
    for p in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            if s.connect_ex((host, p)) != 0:
                return p
    raise RuntimeError("No free port available")


def _open_browser(url: str) -> None:
    try:
        webbrowser.open(url, new=1)
    except Exception:
        pass


def run_server(db_path: str = "cards.db", host: str = "127.0.0.1", port: int = 8000) -> None:
    resolved_db_path = os.getenv("NADIN_DB_PATH", db_path)

    host = os.getenv("NADIN_HOST", host)
    port = int(os.getenv("NADIN_PORT", str(port)))

    # если порт занят — берём свободный (иначе “exe запустился, но сайта нет”)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((host, port)) == 0:
                port = _pick_free_port(host, 8000, 8050)
    except Exception:
        pass

    app = CompanyWebApp(db_path=resolved_db_path)
    _startup_diagnostics(app)

    url = f"http://{host}:{port}/"

    with make_server(host, port, app, server_class=ThreadingWSGIServer) as httpd:
        # открываем браузер один раз после старта
        threading.Thread(target=_open_browser, args=(url,), daemon=True).start()
        httpd.serve_forever()


if __name__ == "__main__":
    run_server()
