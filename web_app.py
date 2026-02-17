from __future__ import annotations

import csv
import logging
import io
import json
import re
import sqlite3
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
from difflib import SequenceMatcher
from urllib.request import Request
from urllib.request import urlopen
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Callable
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from urllib.parse import parse_qs
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlparse
from wsgiref.simple_server import make_server

from card_bot import Card

logger = logging.getLogger(__name__)

RU_TO_EN_OPF = {
    "ООО": "LLC",
    "АО": "JSC",
    "ФГБУ": "FSBI",
    "ПАО": "PJSC",
    "ОАО": "OJSC",
    "АНО": "ANO",
    "ИП": "IE",
    "МУП": "MUE",
    "МАУ": "MAI",
    "ЧУ": "PI",
}
EN_TO_RU_OPF = {v: k for k, v in RU_TO_EN_OPF.items()}
FULL_RU_OPF = {
    "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ": "ООО",
    "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ПАО",
    "АКЦИОНЕРНОЕ ОБЩЕСТВО": "АО",
    "ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ОАО",
}
PASSPORT_MAP = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "YO", "Ж": "ZH",
    "З": "Z", "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M", "Н": "N", "О": "O",
    "П": "P", "Р": "R", "С": "S", "Т": "T", "У": "U", "Ф": "F", "Х": "KH", "Ц": "TS",
    "Ч": "CH", "Ш": "SH", "Щ": "SHCH", "Ъ": "", "Ы": "Y", "Ь": "", "Э": "E", "Ю": "YU", "Я": "YA",
}

SOURCE_DOMAINS = {
    "egrul.nalog.ru": "ЕГРЮЛ",
    "www.rusprofile.ru": "rusprofile.ru",
    "rusprofile.ru": "rusprofile.ru",
    "www.list-org.com": "list-org.com",
    "list-org.com": "list-org.com",
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
    {"name": "ФНС ЕГРЮЛ", "kind": "egrul", "supports_inn": True, "supports_name": False, "supports_url": False, "is_person_source": True},
    {"name": "rusprofile.ru", "kind": "rusprofile", "supports_inn": True, "supports_name": True, "supports_url": True, "is_person_source": True},
    {"name": "list-org.com", "kind": "list_org", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": True},
    {"name": "focus.kontur.ru", "kind": "kontur", "supports_inn": True, "supports_name": True, "supports_url": False, "is_person_source": False},
]

RUSPROFILE_NOISE_RE = re.compile(
    r"(Факторы риска|Дисквалификация|Нахождение под|Общие сведения|Связи|Регион регистрации|Показать)",
    flags=re.IGNORECASE,
)


def is_person_query(raw: str) -> bool:
    """Определяет, является ли запрос по человеку (ФИО, ИНН 12 цифр, URL /person/)."""
    value = raw.strip().lower()
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
    ("position", "Position"),
    ("revenue_mln", "Выручка (млн руб)"),
    ("is_media", "СМИ"),
    ("is_ru_registered", "Зарегистрировано в РФ"),
]

FIELD_PRIORITIES: dict[str, list[str]] = {
    "surname_ru": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "focus.kontur.ru"],
    "name_ru": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "focus.kontur.ru"],
    "middle_name_ru": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "focus.kontur.ru"],
    "gender": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com"],
    "ru_position": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "focus.kontur.ru"],
    "position": ["ФНС ЕГРЮЛ"],
    "ru_org": ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "focus.kontur.ru"],
    "en_org": ["ФНС ЕГРЮЛ"],
}


class CompanyWebApp:
    def __init__(self, db_path: str = "cards.db") -> None:
        self.db_path = Path(db_path)
        self._source_cache: dict[str, dict[str, Any]] = {}
        self._positive_cache_ttl = 30 * 24 * 60 * 60
        self._negative_cache_ttl = 4 * 60 * 60
        self._domain_last_call: dict[str, float] = {}
        self._domain_throttle_seconds = 3
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

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
                """
            )
            db.commit()

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        query = parse_qs(environ.get("QUERY_STRING", ""))
        form = self._parse_form(environ) if method == "POST" else {}

        try:
            if path == "/" and method == "GET":
                body, status, headers = self.search_page(query)
            elif path == "/autofill/review" and method == "POST":
                body, status, headers = self.autofill_review(form)
            elif path == "/autofill/confirm" and method == "POST":
                body, status, headers = self.autofill_confirm(form)
            elif path == "/create/manual" and method == "GET":
                body, status, headers = self.manual_get(query)
            elif path == "/create/manual" and method == "POST":
                body, status, headers = self.manual_post(form)
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
            elif re.fullmatch(r"/card/\d+/export", path) and method == "GET":
                card_id = int(path.split("/")[-2])
                body, status, headers = self.export_preview(card_id)
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

    def _get_one(self, data: dict[str, list[str]], key: str) -> str:
        return (data.get(key) or [""])[0]

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _page(self, title: str, content: str, back_href: str = "/") -> str:
        nav = (
            "<nav style='margin-bottom: 16px'>"
            "<a href='/' style='margin-right: 12px'>🏠 Главная</a>"
            f"<a href='{escape(back_href)}'>← Назад</a>"
            "</nav>"
        )
        return f"<html><head><meta charset='utf-8'><title>{escape(title)}</title></head><body>{nav}{content}</body></html>"

    def _normalize_spaces(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _strip_noise(self, text: str) -> str:
        return re.sub(r"[\"'“”«»()\[\]{}.,;:!?]", " ", text)

    def _strip_punct(self, text: str, russian: bool = False) -> str:
        allowed = "A-Za-z0-9А-Яа-яЁё -" if russian else "A-Za-z0-9 -"
        return re.sub(rf"[^{allowed}]", "", text)

    def detect_input_type(self, raw: str) -> str:
        value = self._normalize_spaces(raw)
        if re.fullmatch(r"\d{10,12}", value):
            return INPUT_TYPE_INN
        if re.match(r"https?://", value, flags=re.IGNORECASE):
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
        return all(re.fullmatch(r"[А-Яа-яЁё]+", part) for part in parts)

    def _clean_ru_org_name(self, value: str) -> str:
        return re.sub(r"^Организация\s+", "", self._normalize_spaces(value), flags=re.IGNORECASE).strip()

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
        parts = self._normalize_spaces(fio).split()
        if len(parts) >= 3:
            return parts[0].capitalize(), parts[1].capitalize(), parts[2].capitalize()
        if len(parts) == 2:
            return parts[0].capitalize(), parts[1].capitalize(), ""
        if len(parts) == 1:
            return parts[0].capitalize(), "", ""
        return "", "", ""

    def _normalize_positions_ru(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        items = [self._normalize_spaces(x) for x in raw.split(",") if self._normalize_spaces(x)]
        normalized: list[str] = []
        for item in items:
            cleaned = item.replace("ИО", "Исполняющий обязанности")
            if cleaned != item:
                notes.append("Должность RU: сокращения раскрыты")
            normalized.append(cleaned[:1].upper() + cleaned[1:] if cleaned else "")
        return ", ".join(normalized), notes

    def _normalize_positions_en(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        raw = raw.replace(" and ", ", ").replace(" & ", ", ")
        items = [self._normalize_spaces(x) for x in raw.split(",") if self._normalize_spaces(x)]
        normalized = [" ".join(w.capitalize() for w in item.split()) for item in items]
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
            source = field_sources.get(field, "—") if value else "Нужно заполнить"
            status = "Заполнено" if value else "Нужно заполнить"
            if field == "revenue_mln":
                source = field_sources.get("revenue_mln", field_sources.get("revenue", "Источник"))
                status = "Справочно"
            if field == "gender" and value:
                source = field_sources.get(field, "Автоопределение")
            if field == "en_org" and value:
                source = field_sources.get(field, "Транслитерация из RU")
            rows += f"<tr><td>{label}</td><td>{escape(value)}</td><td>{source}</td><td>{status}</td></tr>"
        return f"<table border='1' cellpadding='6' cellspacing='0'><tr><th>Поле</th><th>Значение</th><th>Источник</th><th>Статус</th></tr>{rows}</table>"

    def normalize_ru_org(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._strip_punct(raw, russian=True)
        cleaned = re.sub(r"\b(НЕ|НЕТ)\s+(ООО|АО|ПАО|ИП|ЗАО|ОАО)\b", r"\2", cleaned, flags=re.IGNORECASE)
        cleaned = self._normalize_spaces(self._strip_noise(cleaned.upper()))
        for full, short in FULL_RU_OPF.items():
            if full in cleaned:
                cleaned = cleaned.replace(full, short)
                notes.append("RU организация: полная ОПФ сокращена")
        tokens = cleaned.split()
        opf = ""
        if tokens and tokens[0] in RU_TO_EN_OPF:
            opf, tokens = tokens[0], tokens[1:]
        elif tokens and tokens[-1] in RU_TO_EN_OPF:
            opf, tokens = tokens[-1], tokens[:-1]
        else:
            notes.append("RU организация: ОПФ должна быть в конце")

        def _normalize_token(tok: str) -> str:
            if tok.isupper() and (len(tok) <= 3 or not re.search(r"[АЕЁИОУЫЭЮЯ]", tok)):
                return tok
            return tok.capitalize()

        name = " ".join(_normalize_token(tok) for tok in tokens)
        if name and not re.search(r"[А-ЯЁ]", name[0]):
            name = name.capitalize()
        return self._normalize_spaces(f"{name} {opf}" if opf else name), notes

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
        if not cleaned and re.search(r"[A-Za-zА-Яа-яЁё]", fallback_ru):
            ru_parts = fallback_ru.split()
            opf_ru = ru_parts[-1] if ru_parts and ru_parts[-1] in RU_TO_EN_OPF else ""
            name_tokens = ru_parts[:-1] if opf_ru else ru_parts
            name = " ".join(self._translit(tok) for tok in name_tokens)
            cleaned = self._normalize_spaces(f"{name} {RU_TO_EN_OPF.get(opf_ru, '')}")
            if cleaned:
                notes.append("Organization EN: автотранслит — требует перевода или подтверждения" if not is_ru_registered else "Транслит допустим (зарегистрировано в РФ)")

        if not cleaned:
            return "", notes

        cleaned = unicodedata.normalize("NFKD", cleaned)
        cleaned = "".join(ch for ch in cleaned if ord(ch) < 128)
        parts = cleaned.split()
        opf = ""
        if parts and parts[0].upper() in EN_TO_RU_OPF:
            opf, parts = parts[0].upper(), parts[1:]
        elif parts and parts[-1].upper() in EN_TO_RU_OPF:
            opf, parts = parts[-1].upper(), parts[:-1]
        else:
            notes.append("Organization EN: OPF should be at the end")

        name = " ".join(p.capitalize() for p in parts)
        if name.startswith("The ") and not is_media:
            notes.append("Organization EN: The в начале запрещен")
        return self._normalize_spaces(f"{name} {opf}" if opf else name), notes

    def _status(self, notes: list[str], required_ok: bool) -> str:
        if not required_ok:
            return "Черновик / Нужно дополнить"
        if any("должна" in n.lower() or "forbidden" in n.lower() for n in notes):
            return "Ошибка формата"
        if any("автотранслит" in n.lower() for n in notes):
            return "Нужно проверить"
        if notes:
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
            names = ["rusprofile.ru", "ФНС ЕГРЮЛ", "list-org.com"]
            provider_map = {provider["name"]: provider for provider in SOURCE_PROVIDERS}
            return [provider_map[name] for name in names if name in provider_map]
        if self._is_foreign_query(raw):
            names = [
                "OpenCorporates",
                "OffshoreLeaks",
                "Corporation Wiki",
                "Global Brownbook",
                "Companies & Orgs Search Engine",
                "Wikidata",
            ]
        elif input_type == INPUT_TYPE_URL:
            names = [
                "ФНС ЕГРЮЛ",
                "list-org.com",
                "OpenCorporates",
                "OffshoreLeaks",
                "rusprofile.ru",
                "zachestnyibiznes.ru",
                "focus.kontur.ru",
                "checko.ru",
            ]
        else:
            names = [
                "ФНС ЕГРЮЛ",
                "list-org.com",
                "OpenCorporates",
                "OffshoreLeaks",
                "rusprofile.ru",
                "Corporation Wiki",
                "Global Brownbook",
                "Companies & Orgs Search Engine",
                "FAROS OSINT",
                "OCCRP Aleph",
                "zachestnyibiznes.ru",
                "focus.kontur.ru",
                "checko.ru",
                "ФНС Интеграция ЕГРЮЛ/ЕГРИП",
                "Федресурс",
                "КАД Арбитр",
                "ЕИС Закупки",
                "Банк России",
            ]
        provider_map = {provider["name"]: provider for provider in SOURCE_PROVIDERS}
        return [provider_map[name] for name in names if name in provider_map]

    def _get_cache(self, cache_key: str) -> list[dict[str, Any]] | None:
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

        self._source_cache[cache_key] = {"hits": hits, "expires_at": float(row["expires_at"])}
        return hits

    def _set_cache(self, cache_key: str, hits: list[dict[str, Any]], ttl: int = 3600) -> None:
        expires_at = time.time() + ttl
        payload = json.dumps(hits, ensure_ascii=False)
        self._source_cache[cache_key] = {"hits": hits, "expires_at": expires_at}
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO source_cache(cache_key, payload_json, expires_at) VALUES (?, ?, ?)",
                (cache_key, payload, expires_at),
            )
            db.commit()

    def _clear_cache_for_inn(self, inn: str) -> int:
        key_fragment = f":{inn}"
        dropped = [k for k in self._source_cache if key_fragment in k]
        for key in dropped:
            self._source_cache.pop(key, None)
        with self._connect() as db:
            cur = db.execute("DELETE FROM source_cache WHERE cache_key LIKE ?", (f"%:{inn}%",))
            db.commit()
            return len(dropped) + int(cur.rowcount)

    def _clear_cache_for_person(self, query: str) -> int:
        normalized = re.sub(r"\s+", "", self._normalize_spaces(query).lower())
        if not normalized:
            return 0
        key_fragment = f"person:{normalized}"
        dropped = [k for k in self._source_cache if key_fragment in k.lower()]
        for key in dropped:
            self._source_cache.pop(key, None)
        with self._connect() as db:
            cur = db.execute("DELETE FROM source_cache WHERE lower(cache_key) LIKE ?", (f"%{key_fragment}%",))
            db.commit()
            return len(dropped) + int(cur.rowcount)

    def _should_call_provider(self, provider: dict[str, Any], input_type: str) -> bool:
        if input_type == INPUT_TYPE_INN:
            return bool(provider.get("supports_inn"))
        if input_type == INPUT_TYPE_URL:
            return bool(provider.get("supports_url"))
        return bool(provider.get("supports_name"))

    def _call_provider(self, provider: dict[str, Any], raw: str, input_type: str) -> list[dict[str, Any]] | dict[str, Any] | None:
        kind = provider.get("kind")
        person_query = input_type == INPUT_TYPE_PERSON_TEXT or is_person_query(raw)
        if kind == "egrul":
            inn = raw if input_type == INPUT_TYPE_INN else self._extract_inn(raw)
            parsed = self._parse_egrul(inn)
            return parsed
        if kind == "list_org":
            return self._parse_list_org(raw)
        if kind == "rusprofile":
            return self._collect_rusprofile_profiles(raw, input_type, is_person=person_query)
        if kind == "kontur":
            return self._parse_kontur(raw)
        return None

    def _extract_revenue(self, text: str) -> int:
        digits = re.sub(r"[^\d]", "", text or "")
        return int(digits) if digits else 0

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

    def _search_external_sources(self, raw: str, no_cache: bool = False) -> tuple[list[dict[str, Any]], list[str]]:
        input_type = self.detect_input_type(raw)
        hits: list[dict[str, Any]] = []
        trace: list[str] = [f"1. Тип ввода: {input_type}", f"2. Ключ поиска: {raw}"]
        hits_by_provider: dict[str, int] = {}
        providers = self._provider_chain(input_type, raw)

        def load_provider(provider: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str]:
            try:
                data = self._call_provider(provider, raw, input_type)
            except (requests.Timeout, TimeoutError) as exc:
                logger.warning("Provider %s timeout for %s: %s", provider.get("name"), raw, exc)
                return provider["name"], [], "provider_timeout_skipped"
            provider_hits: list[dict[str, Any]] = []
            if isinstance(data, list):
                provider_hits = [
                    {"source": provider["name"], "url": item.get("url", ""), "data": item}
                    for item in data
                    if item
                ]
            elif data:
                provider_hits = [{"source": provider["name"], "url": data.get("url", ""), "data": data}]

            if provider_hits:
                return provider["name"], provider_hits, "provider_called_ok"
            return provider["name"], [], "provider_called_empty"

        active_providers = [provider for provider in providers if self._should_call_provider(provider, input_type)]
        if active_providers:
            with ThreadPoolExecutor(max_workers=min(5, len(active_providers))) as executor:
                futures = {executor.submit(load_provider, provider): provider for provider in active_providers}
                try:
                    for future in as_completed(futures, timeout=15):
                        provider = futures[future]
                        try:
                            provider_name, provider_hits, state = future.result()
                            if provider_hits:
                                hits.extend(provider_hits)
                            hits_by_provider[provider_name] = len(provider_hits)
                            trace.append(f"Источник: {provider_name} — {state}")
                        except Exception as exc:  # noqa: BLE001
                            logger.error("Provider %s failed for %s: %s", provider.get("name"), raw, exc)
                            trace.append(f"Источник: {provider['name']} — provider_error ({exc})")
                            hits_by_provider[provider["name"]] = 0
                except FuturesTimeoutError:
                    trace.append("Источники: global_timeout (15s)")
                    for future, provider in futures.items():
                        if not future.done():
                            trace.append(f"Источник: {provider['name']} — global_timeout")
                            hits_by_provider[provider["name"]] = 0
        for provider in providers:
            if not self._should_call_provider(provider, input_type):
                continue
            hits_by_provider.setdefault(provider["name"], 0)

        trace.append("hits_by_provider: " + ", ".join(f"{k}={v}" for k, v in hits_by_provider.items()))
        hits.sort(key=lambda item: self._score_hit(item, raw), reverse=True)
        if not hits:
            trace.append("Источники: не получено")
        return hits, trace

    def _domain_throttle(self, url: str) -> None:
        host = urlparse(url).netloc.lower()
        if not host:
            return
        last_call = self._domain_last_call.get(host, 0)
        wait_for = self._domain_throttle_seconds - (time.time() - last_call)
        if wait_for > 0:
            time.sleep(wait_for)
        self._domain_last_call[host] = time.time()

    def _fetch_page(self, url: str, timeout: int = 30) -> str:
        try:
            self._domain_throttle(url)
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Fetch failed for {url}: {exc}")
            return ""

    def _request(self, url: str, timeout: int = 10) -> requests.Response:
        self._domain_throttle(url)
        return requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})

    def _parse_egrul(self, query: str) -> dict[str, Any] | None:
        if not re.fullmatch(r"\d{10,12}", query):
            return None
        url = f"https://egrul.itsoft.ru/{query}.json"
        try:
            resp = self._request(url, timeout=10)
            if not resp.ok:
                return None
            content_type = resp.headers.get("content-type", "")
            if "json" not in content_type and "javascript" not in content_type:
                return None
            data = resp.json()
            sv_yul = data.get("СвЮЛ") or {}
            company = data.get("company") or {}
            ru_org_raw = (
                data.get("НаимСокр")
                or data.get("name")
                or (sv_yul.get("НаимСокр") if isinstance(sv_yul, dict) else "")
                or (company.get("short_name") if isinstance(company, dict) else "")
                or (company.get("name") if isinstance(company, dict) else "")
                or data.get("ru_org")
                or ""
            )
            ru_org = self._clean_ru_org_name(str(ru_org_raw).replace("ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО", "ПАО"))

            director = data.get("director") or {}
            surname_ru = str(director.get("surname") or director.get("surname_ru") or "")
            name_ru = str(director.get("name") or director.get("name_ru") or "")
            middle_name_ru = str(director.get("patronymic") or director.get("middle_name_ru") or "")
            position = str(director.get("position") or data.get("ru_position") or "")

            if not surname_ru:
                dol_list = data.get("СведДолжнФЛ") or (sv_yul.get("СведДолжнФЛ") if isinstance(sv_yul, dict) else []) or []
                if isinstance(dol_list, list) and dol_list:
                    head = dol_list[0] or {}
                    fio_str = str(head.get("ФИО") or head.get("ФИОПолн") or "")
                    if fio_str:
                        surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_str)
                    else:
                        sv_fl = head.get("СвФЛ") or {}
                        if isinstance(sv_fl, dict):
                            fio_full = str(sv_fl.get("ФИОПолн") or "")
                            if fio_full:
                                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_full)
                            else:
                                surname_ru = str(sv_fl.get("Фамилия") or "").capitalize()
                                name_ru = str(sv_fl.get("Имя") or "").capitalize()
                                middle_name_ru = str(sv_fl.get("Отчество") or "").capitalize()
                    if not position:
                        dolzhn = head.get("СвДолжн") or {}
                        position = str((dolzhn.get("НаимДолжн") if isinstance(dolzhn, dict) else "") or head.get("Должность") or "")

            inn = str(
                (company.get("inn") if isinstance(company, dict) else "")
                or data.get("inn")
                or data.get("ИННЮЛ")
                or data.get("ИНН")
                or query
            )
            rev_raw = (
                data.get("revenue")
                or (data.get("ФинПоказ") or {}).get("Выручка")
                or ((sv_yul.get("ФинПоказ") if isinstance(sv_yul, dict) else {}) or {}).get("Выручка")
                or 0
            )
            gender_raw = str(data.get("gender") or director.get("gender") or "").strip().lower()
            if gender_raw in {"1", "м", "m", "male", "мужской"} or "муж" in gender_raw:
                gender = "М"
            elif gender_raw in {"2", "ж", "f", "female", "женский"} or "жен" in gender_raw:
                gender = "Ж"
            else:
                gender = self._infer_gender(middle_name_ru, position)

            return {
                "url": url,
                "inn": inn,
                "ogrn": str(data.get("ogrn") or data.get("ОГРН") or ""),
                "ru_org": ru_org,
                "en_org": str(data.get("en_org") or ""),
                "surname_ru": surname_ru,
                "name_ru": name_ru,
                "middle_name_ru": middle_name_ru,
                "gender": gender,
                "ru_position": position or "Генеральный директор",
                "en_position": str(data.get("en_position") or ""),
                "revenue": self._extract_revenue(str(rev_raw)),
            }
        except Exception as exc:  # noqa: BLE001
            logger.error("EGRUL request failed for %s: %s", query, exc)
            return None

    def _search_list_org(self, query: str, is_person: bool = False) -> list[dict[str, str]]:
        type_param = "fio" if is_person else "all"
        url = f"https://www.list-org.com/search?type={type_param}&name={quote(query)}"
        logger.info("list-org search: %s", url)
        html = self._fetch_page(url, timeout=30)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        hits: list[dict[str, str]] = []
        for p_tag in soup.select("p"):
            a_tag = p_tag.select_one("a")
            if isinstance(a_tag, Tag) and "boss" in str(a_tag.get("href", "")):
                name = self._normalize_spaces(a_tag.get_text(" ", strip=True))
                p_text = self._normalize_spaces(p_tag.get_text(" ", strip=True))
                org = p_text.split(" - ", 1)[1] if " - " in p_text else ""
                href = str(a_tag.get("href", ""))
                hits.append({
                    "source": "list-org.com",
                    "name": name,
                    "org": org,
                    "url": "https://www.list-org.com" + href if href.startswith("/") else href,
                })
        return hits[:10]

    def _parse_list_org(self, query: str) -> list[dict[str, Any]] | dict[str, Any] | None:
        input_type = self.detect_input_type(query)
        if input_type == INPUT_TYPE_PERSON_TEXT:
            hits: list[dict[str, Any]] = []
            for item in self._search_list_org(query, is_person=True):
                person_url = item.get("url", "")
                if not person_url:
                    continue
                detail_html = self._fetch_page(person_url, timeout=30)
                if not detail_html:
                    continue
                detail_soup = BeautifulSoup(detail_html, "lxml")
                text = detail_soup.get_text(" ", strip=True)
                fio = item.get("name", "")
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio)
                org_match = re.search(r'(ПАО|АО|ООО|ИП|КХ|СПК|ЗАО)\s*[«"]?[^,.]+', text)
                position_match = re.search(r"(Президент|Генеральный директор|Директор|Председатель)[^,.]*", text, flags=re.IGNORECASE)
                hits.append({
                    "url": person_url,
                    "ru_org": self._clean_ru_org_name(item.get("org") or (org_match.group(0) if org_match else "")),
                    "surname_ru": surname_ru,
                    "name_ru": name_ru,
                    "middle_name_ru": middle_name_ru,
                    "gender": normalize_gender(middle_name_ru),
                    "ru_position": position_match.group(0).strip() if position_match else "",
                    "inn": self._extract_inn(text),
                    "revenue": self._extract_revenue_from_soup(detail_soup),
                })
                if len(hits) >= 5:
                    break
            return hits

        search_url = f"https://www.list-org.com/search?val={quote(query)}"
        search_html = self._fetch_page(search_url, timeout=30)
        if not search_html:
            return None
        soup = BeautifulSoup(search_html, "lxml")
        company_link = soup.find("a", href=re.compile(r"/company/\d+"))
        if not isinstance(company_link, Tag):
            return None
        company_url = "https://www.list-org.com" + str(company_link.get("href", ""))
        detail_html = self._fetch_page(company_url, timeout=30)
        if not detail_html:
            return None
        detail_soup = BeautifulSoup(detail_html, "lxml")
        h1 = detail_soup.find("h1")
        ru_org = self._clean_ru_org_name(h1.get_text(strip=True) if isinstance(h1, Tag) else "")
        text = detail_soup.get_text(" ", strip=True)
        fio_match = re.search(r"Руководитель[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", text)
        position_match = re.search(r"Руководитель[^А-ЯЁ]{0,40}[А-ЯЁа-яё\s]+\(([^)]+)\)", text)
        surname_ru = name_ru = middle_name_ru = ""
        if fio_match:
            surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_match.group(1))
        return {
            "url": company_url,
            "ru_org": ru_org,
            "surname_ru": surname_ru,
            "name_ru": name_ru,
            "middle_name_ru": middle_name_ru,
            "ru_position": position_match.group(1).strip() if position_match else "",
            "inn": self._extract_inn(text),
            "revenue": self._extract_revenue_from_soup(detail_soup),
        }

    def _search_rusprofile(self, query: str, is_person: bool = False) -> list[dict[str, str]]:
        search_url = f"https://www.rusprofile.ru/search?query={quote(query)}"
        logger.info("rusprofile search: %s", search_url)
        html = self._fetch_page(search_url, timeout=30)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        hits: list[dict[str, str]] = []
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
            if len(hits) >= 10:
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
                if len(hits) >= 10:
                    break

        if is_person:
            hits = [h for h in hits if h.get("type") == "person"]
        logger.info("rusprofile hits: %s", hits)
        return hits[:10]

    def _parse_rusprofile(self, url: str) -> dict[str, Any]:
        html = self._fetch_page(url, timeout=15)
        if not html:
            return {}
        soup = BeautifulSoup(html, "lxml")
        profile: dict[str, Any] = {"url": url, "source": "rusprofile.ru"}
        page_text = soup.get_text(" ", strip=True)
        is_person = "/person/" in url or "/ip/" in url

        if is_person:
            h1 = soup.find("h1")
            full_name = self._normalize_spaces(h1.get_text(" ", strip=True)) if isinstance(h1, Tag) else ""
            parts = full_name.split()
            profile["surname_ru"] = parts[0] if parts else ""
            profile["name_ru"] = parts[1] if len(parts) > 1 else ""
            profile["middle_name_ru"] = parts[2] if len(parts) > 2 else ""
            patronymic = profile["middle_name_ru"]
            profile["gender"] = "М" if patronymic.lower().endswith(("вич", "ич")) else "Ж" if patronymic.lower().endswith("вна") else ""
            inn_match = re.search(r"ИНН[:\s]*(\d{10,12})", page_text)
            profile["inn"] = inn_match.group(1) if inn_match else ""

            org_match = re.search(r"\b(ПАО|АО|ООО|ОАО|ЗАО|ФГУП|ФГБУ|АНО|МУП|НКО|ИП)\s+[«\"]?([А-ЯЁа-яёA-Za-z0-9\-]+(?:\s+[А-ЯЁа-яёA-Za-z0-9\-]+)*)[»\"]?", page_text)
            if org_match:
                raw_org = f"{org_match.group(1)} {org_match.group(2).strip()}"
                if not re.search(r"^(НЕ|НЕТ|ЛИКВИДИРОВАНО)", raw_org, flags=re.IGNORECASE) and not RUSPROFILE_NOISE_RE.search(raw_org):
                    profile["ru_org"] = self._clean_ru_org_name(raw_org)

            pos_match = re.search(
                r"(Генеральный директор|Президент|Председатель правления|Председатель|Директор|Руководитель|Заместитель)\s*([А-ЯЁа-яё\s,]{0,40}?)",
                page_text,
                flags=re.IGNORECASE,
            )
            if pos_match:
                pos_text = pos_match.group(0).strip()
                if not RUSPROFILE_NOISE_RE.search(pos_text):
                    profile["ru_position"] = pos_text.split(".")[0].split(",")[0]

            rev_match = re.search(r"([\d\s]+(?:[.,]\d+)?)\s*млн\s*руб", page_text)
            if rev_match:
                try:
                    profile["revenue"] = int(float(rev_match.group(1).replace(" ", "").replace(",", ".")) * 1_000_000)
                except ValueError:
                    profile["revenue"] = 0
            else:
                profile["revenue"] = self._extract_revenue_from_soup(soup)
        else:
            title = soup.find("h1")
            profile["ru_org"] = self._clean_ru_org_name(title.get_text(strip=True) if isinstance(title, Tag) else "")
            inn_match = re.search(r"ИНН[:\s]*(\d{10})", page_text)
            profile["inn"] = inn_match.group(1) if inn_match else ""
            profile["revenue"] = self._extract_revenue_from_soup(soup)
            fio_match = re.search(r"Руководитель[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)", page_text)
            if fio_match:
                surname_ru, name_ru, middle_name_ru = self._split_fio_ru(fio_match.group(1))
                profile.update({"surname_ru": surname_ru, "name_ru": name_ru, "middle_name_ru": middle_name_ru})
        return profile

    def _collect_rusprofile_profiles(self, query: str, input_type: str, is_person: bool = False) -> list[dict[str, Any]] | dict[str, Any] | None:
        person_mode = is_person or input_type == INPUT_TYPE_PERSON_TEXT or is_person_query(query)
        if input_type == INPUT_TYPE_URL and "rusprofile.ru" in query and ("/person/" in query or "/ip/" in query):
            person_mode = True

        if input_type == INPUT_TYPE_URL and "rusprofile.ru" in query:
            profile = self._parse_rusprofile(query)
            return profile or None

        hits = self._search_rusprofile(query, is_person=person_mode)
        profiles: list[dict[str, Any]] = []
        for hit in hits:
            if hit.get("url"):
                profile = self._parse_rusprofile(hit["url"])
                if profile:
                    if hit.get("org") and not profile.get("ru_org"):
                        profile["ru_org"] = hit.get("org", "")
                    if hit.get("position") and not profile.get("ru_position"):
                        profile["ru_position"] = hit.get("position", "")
                    profiles.append(profile)

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
        now = time.time()
        last = self._domain_last_call.get(domain, 0)
        if now - last < self._domain_throttle_seconds:
            time.sleep(self._domain_throttle_seconds)
        self._domain_last_call[domain] = now
        return True

    def _save_rate_limited(self, provider_name: str, key: str, retry_seconds: int = 300) -> None:
        cache_key = f"{provider_name}:{key}"
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
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        patterns = [
            r"Генеральн(?:ый|ого) директор[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
            r"Руководитель[^А-ЯЁ]{0,40}([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            tokens = match.group(1).split()
            if len(tokens) >= 3:
                return tokens[0], tokens[1], " ".join(tokens[2:])
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

    def _fetch_from_list_org(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if not inn:
            return self._provider_fallback_from_catalog("list-org.com", normalized, inn)
        url = f"https://www.list-org.com/search?type=inn&val={inn}"
        if not self._throttle_acquire("www.list-org.com"):
            return None, "rate_limited", "throttle"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as response:
                html = response.read().decode("utf-8", errors="ignore")
            surname_ru, name_ru, middle_name_ru = self._extract_director_from_html(html)
            org_name = self._extract_org_from_html(html)
            if not org_name and not surname_ru:
                fallback, state, reason = self._provider_fallback_from_catalog("list-org.com", normalized, inn)
                if fallback:
                    return fallback, state, reason
                return None, "empty", "not found"
            return {
                "source": "list-org.com",
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
                self._save_rate_limited("list-org.com", f"list:{inn}", 180)
                return None, "rate_limited", "429"
            fallback, state, reason = self._provider_fallback_from_catalog("list-org.com", normalized, inn)
            if fallback:
                return fallback, state, reason
            return None, "error", str(exc)

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
                fallback, state, reason = self._provider_fallback_from_catalog("list-org.com", normalized, inn)
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
                fallback, state, reason = self._provider_fallback_from_catalog("list-org.com", normalized, inn)
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
        providers = {item["name"]: item for item in SOURCE_PROVIDERS}
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
            if not value:
                continue
            if skip_person_noise and not self._source_is_person_eligible(source_name, data):
                continue
            return value, source_name
        return "", ""

    def _merge_person_hits(self, hits: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
        merged: dict[str, str] = {}
        merged_sources: dict[str, str] = {}
        fields = ["surname_ru", "name_ru", "middle_name_ru", "gender", "ru_position", "ru_org", "inn"]
        for field in fields:
            best_value = ""
            best_source = ""
            for hit in hits:
                source = str(hit.get("source", ""))
                raw_value = str(hit.get("data", {}).get(field, ""))
                value = self._normalize_spaces(raw_value)
                if field == "ru_org":
                    value = self._clean_ru_org_name(value)
                if not value:
                    continue
                if len(value) > len(best_value):
                    best_value = value
                    best_source = source
            if best_value:
                merged[field] = best_value
                merged_sources[field] = best_source
        return merged, merged_sources

    def _merge_profiles(self, hits: list[dict[str, Any]], query: str) -> dict[str, Any]:
        """Универсальная логика объединения данных из нескольких источников."""
        if not hits:
            return {}
        ranked_hits = sorted(hits, key=lambda item: self._score_hit(item, query), reverse=True)
        best_data = dict(ranked_hits[0].get("data", ranked_hits[0]))
        for hit in ranked_hits[1:]:
            data = hit.get("data", hit)
            for field in ["ru_org", "en_org", "ru_position", "en_position", "middle_name_ru", "middle_name_en"]:
                if not best_data.get(field) and data.get(field):
                    best_data[field] = data[field]
        return best_data

    def _normalize_card_data(self, profile: dict[str, str], field_sources: dict[str, str]) -> dict[str, str]:
        """Универсальная нормализация данных карточки."""
        if profile.get("ru_position") and not profile.get("en_position"):
            profile["en_position"], _ = self._normalize_positions_en(profile["ru_position"])
            if profile.get("en_position"):
                field_sources.setdefault("en_position", "Автогенерация из RU")
        if profile.get("middle_name_ru") and not profile.get("middle_name_en"):
            profile["middle_name_en"] = self._translit(profile["middle_name_ru"])
            if profile.get("middle_name_en"):
                field_sources.setdefault("middle_name_en", "Транслитерация из RU")
        if not profile.get("appeal") and profile.get("gender"):
            profile["appeal"] = "Г-н" if profile["gender"] == "М" else "Г-жа"
            field_sources.setdefault("appeal", "Автоопределение")
        return profile

    def _build_profile_from_sources(
        self,
        source_hits: list[dict[str, Any]],
        raw_name: str,
        input_type: str,
    ) -> tuple[dict[str, str], dict[str, str]]:
        profile = {field: "" for field, _ in CARD_FIELDS}
        field_sources: dict[str, str] = {}

        for field, _ in CARD_FIELDS:
            skip_person_noise = field in {"surname_ru", "name_ru", "middle_name_ru", "gender", "ru_position", "position"}
            value, source_name = self._pick_field_by_priority(field, source_hits, skip_person_noise=skip_person_noise)
            if value:
                profile[field] = value
                field_sources[field] = source_name

        if input_type == INPUT_TYPE_PERSON_TEXT:
            merged_person, merged_sources = self._merge_person_hits(source_hits)
            profile.update(merged_person)
            field_sources.update({k: v for k, v in merged_sources.items() if v})
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
                if candidate:
                    profile["ru_org"] = candidate
                    field_sources["ru_org"] = item.get("source", "unknown")
                    break

        if not profile["ru_org"] and input_type != INPUT_TYPE_INN:
            profile["ru_org"] = raw_name
            field_sources["ru_org"] = "Нормализация запроса"

        profile["ru_org"], _ = self.normalize_ru_org(profile["ru_org"])
        if profile["ru_org"]:
            field_sources["ru_org"] = field_sources.get("ru_org", "Нормализация/источник")

        if not profile["en_org"] and input_type != INPUT_TYPE_INN:
            profile["en_org"], _ = self.normalize_en_org("", profile["ru_org"])
            if profile["en_org"]:
                field_sources["en_org"] = "Транслитерация из RU"
        else:
            profile["en_org"], _ = self.normalize_en_org(profile["en_org"], profile["ru_org"])
            if profile["en_org"] and not field_sources.get("en_org"):
                field_sources["en_org"] = "Нормализация/источник"

        if profile.get("surname_ru") or profile.get("name_ru"):
            profile["family_name"] = profile.get("family_name") or self._translit(profile.get("surname_ru", ""))
            profile["first_name"] = profile.get("first_name") or self._translit(profile.get("name_ru", ""))
            profile["middle_name"] = profile.get("middle_name") or self._translit(profile.get("middle_name_ru", ""))

        if input_type == INPUT_TYPE_PERSON_TEXT and not profile.get("surname_ru") and not profile.get("name_ru"):
            sur, nam, patr = self._split_fio_ru(raw_name)
            profile["surname_ru"] = sur
            profile["name_ru"] = nam
            profile["middle_name_ru"] = patr
            if sur:
                profile["family_name"] = self._translit(sur)
            if nam:
                profile["first_name"] = self._translit(nam)

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

        profile["appeal"] = self._derive_salutation(profile.get("gender", ""))
        profile["ru_position"], _ = self._normalize_positions_ru(profile.get("ru_position", ""))
        profile["position"], _ = self._normalize_positions_en(profile.get("position", profile.get("en_position", "")))
        profile["salutation"] = profile.get("appeal", "")
        profile["en_position"] = profile.get("position", "")
        profile = self._normalize_card_data(profile, field_sources)

        return profile, field_sources

    def _write_audit(self, action: str, card_id: int | None, details: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO audits(card_id, action, actor, created_at, details) VALUES(?,?,?,?,?)",
                (card_id, action, "web-user", self._now(), json.dumps(details, ensure_ascii=False)),
            )
            db.commit()

    def _build_person_candidates(self, hits: list[dict[str, Any]], query: str = "") -> list[dict[str, str]]:
        query_words = [w for w in self._normalize_spaces(query.lower()).split() if w]
        filtered_hits = hits
        if query_words:
            strict = []
            for hit in hits:
                data = hit.get("data", {})
                surname = self._normalize_spaces(str(data.get("surname_ru", "")).lower())
                fio = self._normalize_spaces(
                    " ".join(x for x in [data.get("surname_ru", ""), data.get("name_ru", ""), data.get("middle_name_ru", "")] if x).lower()
                )
                if any(word in surname for word in query_words) or all(word in fio for word in query_words):
                    strict.append(hit)
            if strict:
                filtered_hits = strict

        seen: dict[tuple[str, str], dict[str, str]] = {}
        for hit in filtered_hits:
            data = hit.get("data", {})
            fio_ru = " ".join(x for x in [data.get("surname_ru", ""), data.get("name_ru", ""), data.get("middle_name_ru", "")] if x).strip()
            if not fio_ru:
                continue
            org_ru = self._clean_ru_org_name(str(data.get("ru_org", "")))
            key = (fio_ru.lower(), org_ru.lower())
            if key in seen:
                existing_score = float(seen[key].get("score", 0))
                candidate_score = self._score_hit(hit, query)
                if existing_score >= candidate_score:
                    continue
            score = self._score_hit(hit, query)
            revenue = int(data.get("revenue", 0) or 0)
            seen[key] = {
                "fio_ru": fio_ru,
                "org_ru": org_ru,
                "position_ru": self._normalize_spaces(str(data.get("ru_position", ""))),
                "inn": self._normalize_spaces(str(data.get("inn", ""))),
                "source": str(hit.get("source", "")),
                "query_for_autofill": self._normalize_spaces(str(data.get("inn", ""))) or fio_ru,
                "score": f"{score:.2f}",
                "revenue": str(revenue),
            }
        ranked = sorted(seen.values(), key=lambda c: float(c.get("score", 0)), reverse=True)[:6]
        if ranked:
            logger.info("Top candidate: %s", ranked[0].get("fio_ru", ""))
        return ranked

    def _revenue_billions(self, revenue_mln: int | str | None) -> str:
        revenue = int(revenue_mln or 0)
        if revenue <= 0:
            return "—"
        return f"{revenue / 1000:.2f}"

    def _render_search_results(self, q: str, normalized: str, candidates: list[dict[str, str]], similar: list[sqlite3.Row] | None = None) -> str:
        similar = similar or []
        if candidates:
            blocks = "".join(
                (
                    "<form method='post' action='/autofill/review' style='margin: 10px 0;'>"
                    f"<input type='hidden' name='company_name' value='{escape(c['query_for_autofill'])}' />"
                    "<button type='submit' style='width: 100%; text-align: left; border: 1px solid #ddd; padding: 15px; border-radius: 8px; cursor: pointer; background: white;'>"
                    f"<h4 style='margin: 0 0 8px;'>{escape(c['fio_ru'] or c['org_ru'] or 'Вариант')}</h4>"
                    f"<p style='margin: 4px 0;'><b>Организация:</b> {escape(c['org_ru'] or '—')}</p>"
                    f"<p style='margin: 4px 0;'><b>Должность:</b> {escape(c['position_ru'] or '—')}</p>"
                    f"<p style='margin: 4px 0;'><b>Выручка:</b> {escape(self._revenue_billions(c.get('revenue')))} млрд руб</p>"
                    f"<p style='margin: 4px 0;'><b>ИНН:</b> {escape(c.get('inn', '') or '—')}</p>"
                    f"<p style='margin: 4px 0;'><small>Источник: {escape(c['source'])}</small></p>"
                    "<span style='display: inline-block; margin-top: 10px;'>Автозаполнить</span>"
                    "</button></form>"
                )
                for c in candidates
            )
            not_found = f"<h3>Варианты по '{escape(q)}':</h3>{blocks}"
        else:
            not_found = (
                f"<p>Нет данных. Создать вручную?</p>"
                f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(q)}' /><button>Автозаполнить из открытых источников</button></form>"
                f"<a href='/create/manual?q={escape(q)}'>Создать вручную</a>"
            ) if q else ""

        items = "".join(f"<li><a href='/card/{r['id']}'>{escape(r['ru_org'])}</a></li>" for r in similar)
        return (
            "<h1>Карточки компаний/участников</h1>"
            "<form method='get' action='/'><input name='q' value='{q}' /><button>Найти</button></form>"
            "{norm}"
            "{not_found}"
            "{similar}"
        ).format(
            q=escape(q),
            norm=f"<p><b>Нормализовано:</b> {escape(normalized)}</p>" if normalized else "",
            not_found=not_found,
            similar=f"<h3>Похожие варианты</h3><ul>{items}</ul>" if similar and not candidates else "",
        )

    def search_page(self, query: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        q = (query.get("q") or [""])[0].strip()
        if not q:
            content = self._render_search_results("", "", [], [])
            body = self._page("Карточки компаний/участников", content)
            return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

        normalized, _ = self.normalize_ru_org(q)
        input_type = self.detect_input_type(q)

        with self._connect() as db:
            if input_type == INPUT_TYPE_INN:
                exact = db.execute("SELECT * FROM cards WHERE json_extract(data_json, '$.profile.inn')=? ORDER BY id DESC", (q,)).fetchall()
            else:
                exact = db.execute("SELECT * FROM cards WHERE ru_org=? OR json_extract(data_json, '$.profile.source_id')=? ORDER BY id DESC", (normalized, q)).fetchall()
            similar = db.execute("SELECT * FROM cards WHERE ru_org LIKE ? ORDER BY id DESC LIMIT 10", (f"%{normalized.split()[0]}%",)).fetchall()

        if exact:
            return "", "302 Found", [("Location", f"/card/{exact[0]['id']}")]

        candidates: list[dict[str, str]] = []
        source_hits: list[dict[str, Any]] = []
        source_hits, _ = self._search_external_sources(q, no_cache=False)
        if input_type == INPUT_TYPE_URL and not source_hits:
            url_hit = self._parse_url_detail(q)
            if url_hit:
                source_hits = [{"source": "URL detail", "url": q, "data": url_hit}]
        if input_type == INPUT_TYPE_PERSON_TEXT:
            candidates = self._build_person_candidates(source_hits, q)
        elif source_hits:
            best_hit = max(source_hits, key=lambda h: self._score_hit(h, q))
            profile = dict(best_hit.get("data", {}))
            candidates = [{
                "fio_ru": " ".join(x for x in [profile.get("surname_ru", ""), profile.get("name_ru", ""), profile.get("middle_name_ru", "")] if x).strip(),
                "org_ru": profile.get("ru_org", ""),
                "position_ru": profile.get("ru_position", ""),
                "inn": profile.get("inn", ""),
                "revenue": str(profile.get("revenue", 0) or 0),
                "source": best_hit.get("source", ""),
                "query_for_autofill": profile.get("inn", "") or q,
            }]

        content = self._render_search_results(q, normalized, candidates, similar)
        body = self._page("Карточки компаний/участников", content)
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def autofill_review(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        raw = self._get_one(form, "company_name")
        no_cache = self._get_one(form, "no_cache") == "1"
        input_type = self.detect_input_type(raw)
        if self._get_one(form, "reset_inn_cache") == "1" and input_type == INPUT_TYPE_INN:
            dropped = self._clear_cache_for_inn(self._extract_inn(raw))
            reset_note = [f"Кэш по ИНН очищен: {dropped}"]
        elif self._get_one(form, "reset_inn_cache") == "1" and input_type == INPUT_TYPE_PERSON_TEXT:
            dropped = self._clear_cache_for_person(raw)
            reset_note = [f"Кэш по персоне очищен: {dropped}"]
        else:
            reset_note = []
        source_hits, search_trace = self._search_external_sources(raw, no_cache=no_cache)
        search_trace = reset_note + search_trace
        profile, field_sources = self._build_profile_from_sources(source_hits, raw, input_type)

        ru_org, ru_notes = self.normalize_ru_org(profile["ru_org"])
        en_org, en_notes = self.normalize_en_org(profile["en_org"], ru_org, is_media=profile.get("is_media") in {True, "1", "true"}, is_ru_registered=profile.get("is_ru_registered") in {True, "1", "true"})
        ru_pos, ru_pos_notes = self._normalize_positions_ru(profile.get("ru_position", ""))
        en_pos, en_pos_notes = self._normalize_positions_en(profile.get("position", profile.get("en_position", "")))
        profile["ru_org"] = ru_org
        profile["en_org"] = en_org
        profile["ru_position"] = ru_pos
        profile["position"] = en_pos
        profile["appeal"] = self._derive_salutation(profile.get("gender", ""))
        notes = ru_notes + en_notes + ru_pos_notes + en_pos_notes
        if source_hits:
            notes.append(f"Источники: найдено {len(source_hits)}")
        else:
            notes.append("Источники: не получено (в источниках нет данных по запросу)")

        field_statuses = self._field_statuses(profile, notes)

        source_hidden = "".join(f"<input type='hidden' name='source_name' value='{escape(item['source'])}'/>" for item in source_hits)
        trace_hidden = "".join(f"<input type='hidden' name='search_trace' value='{escape(step)}'/>" for step in search_trace)
        field_source_hidden = "".join(
            f"<input type='hidden' name='field_source_{escape(field)}' value='{escape(source)}'/>"
            for field, source in field_sources.items()
        )
        profile_hidden = "".join(
            f"<input type='hidden' name='profile_{escape(field)}' value='{escape(value)}'/>"
            for field, value in profile.items()
        )
        hidden = "".join(f"<input type='hidden' name='notes' value='{escape(n)}'/>" for n in notes)
        source_list = (
            "<h3>Найдено в доступных источниках</h3><ul>"
            + "".join(
                f"<li>{escape(item['source'])}: {escape(item['data'].get('ru_org', '') or '/')} / </li>" for item in source_hits
            )
            + "</ul>"
        ) if source_hits else "<p>В доступных источниках совпадений не найдено.</p>"
        source_table_rows = self._render_profile(profile, field_sources, notes)
        if any("автотранслит" in n.lower() for n in notes):
            source_table_rows = "<div style='background:#fff7cc;padding:8px;border:1px solid #e0c95b'>⚠ Требуется ручная проверка перевода организации EN (автотранслит)</div>" + source_table_rows
        search_trace_list = "<h3>Как происходил поиск</h3><ol>" + "".join(f"<li>{escape(step)}</li>" for step in search_trace) + "</ol>"
        content = (
            "<h2>Автосбор: черновик</h2>"
            f"{source_list}"
            f"{search_trace_list}"
            "<h3>Карточка и источники по полям</h3>"
            f"{source_table_rows}"
            "<form method='post' action='/autofill/confirm'>"
            f"<p>RU: <input name='ru_org' value='{escape(ru_org)}'/></p>"
            f"<p>EN: <input name='en_org' value='{escape(en_org)}'/></p>"
            f"<input type='hidden' name='input_value' value='{escape(raw)}'/>"
            f"{hidden}{source_hidden}{trace_hidden}{field_source_hidden}{profile_hidden}"
            "<button name='action' value='create'>✅ Создать карту</button>"
            "<button name='action' value='edit'>✏️ Отредактировать</button>"
            "<button name='action' value='cancel'>❌ Отмена</button></form>"
            f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(raw)}'/><input type='hidden' name='no_cache' value='1'/><button>Повторить без кэша</button></form>"
            + (
                f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(raw)}'/><input type='hidden' name='reset_inn_cache' value='1'/><button>Сбросить кэш по ИНН</button></form>"
                if input_type == INPUT_TYPE_INN
                else ""
            )
        )
        body = self._page("Автосбор: черновик", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def autofill_confirm(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        action = self._get_one(form, "action") or "create"
        if action == "cancel":
            return "", "302 Found", [("Location", "/")]
        ru_org = self._get_one(form, "ru_org")
        en_org = self._get_one(form, "en_org")
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

        card_obj = Card.from_profile(profile_data)
        profile_data["family_name"] = card_obj.family_name or profile_data.get("family_name", "")
        profile_data["first_name"] = card_obj.first_name or profile_data.get("first_name", "")
        profile_data["middle_name_en"] = card_obj.middle_name_en or profile_data.get("middle_name_en", profile_data.get("middle_name", ""))
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
                            "source_hits": source_names,
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
        return "", "302 Found", [("Location", f"/card/{card_id}")]

    def manual_get(self, query: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        q = (query.get("q") or [""])[0]
        en_org = (query.get("en_org") or [""])[0]
        person_ru = (query.get("person_ru") or [""])[0]
        person_en = (query.get("person_en") or [""])[0]
        gender = (query.get("gender") or [""])[0]
        ru_position = (query.get("ru_position") or [""])[0]
        en_position = (query.get("en_position") or [""])[0]
        ru_org, _ = self.normalize_ru_org(q) if q else ("", [])
        male_selected = " selected" if gender == "М" else ""
        female_selected = " selected" if gender == "Ж" else ""
        content = (
            "<h2>Ручное создание</h2>"
            "<form method='post' action='/create/manual'>"
            f"<p>Организация RU <input name='ru_org' value='{escape(ru_org)}'></p>"
            f"<p>Organization EN <input name='en_org' value='{escape(en_org)}'></p>"
            f"<p>ФИО RU <input name='person_ru' value='{escape(person_ru)}'></p>"
            f"<p>FIO EN <input name='person_en' value='{escape(person_en)}'></p>"
            f"<p>Пол <select name='gender'><option value=''>--</option><option{male_selected}>М</option><option{female_selected}>Ж</option></select></p>"
            f"<p>Должность RU <input name='ru_position' value='{escape(ru_position)}'></p>"
            f"<p>Position EN <input name='en_position' value='{escape(en_position)}'></p>"
            "<p><label><input type='checkbox' name='is_media' value='1'> СМИ (разрешить The)</label></p>"
            "<p><label><input type='checkbox' name='is_ru_registered' value='1'> Зарегистрировано в РФ</label></p>"
            "<button>Сохранить</button></form>"
        )
        body = self._page("Ручное создание", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def manual_post(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        ru_org, ru_notes = self.normalize_ru_org(self._get_one(form, "ru_org"))
        en_org, en_notes = self.normalize_en_org(self._get_one(form, "en_org"), ru_org, is_media=self._get_one(form, "is_media") == "1", is_ru_registered=self._get_one(form, "is_ru_registered") == "1")
        person_ru = self._get_one(form, "person_ru")
        gender = self._get_one(form, "gender")
        errors: list[str] = []
        if person_ru and gender not in {"М", "Ж"}:
            errors.append("Пол обязателен: М/Ж")
        notes = ru_notes + en_notes + errors
        status = self._status(notes, bool(ru_org and en_org))
        if errors:
            return "<p>Пол обязателен: М/Ж</p>", "400 Bad Request", [("Content-Type", "text/html; charset=utf-8")]

        data = {
            "notes": notes,
            "person_ru": person_ru,
            "person_en": self._get_one(form, "person_en"),
            "gender": gender,
            "ru_position": self._get_one(form, "ru_position"),
            "en_position": self._get_one(form, "en_position"),
            "is_media": self._get_one(form, "is_media") == "1",
            "is_ru_registered": self._get_one(form, "is_ru_registered") == "1",
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
        lines = "".join(f"<tr><td>{escape(label)}</td><td>{escape(profile.get(field, ''))}</td></tr>" for field, label in CARD_FIELDS)
        content = (
            f"<h2>Карточка #{card['id']}</h2>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            f"{lines}</table>"
            f"<p>Статус: {escape(card['status'])}</p>"
            f"<p>Источник: {escape(card['source'])}</p>"
            f"<p><a href='/card/{card['id']}/edit'>Редактировать карточку</a></p>"
            f"<a href='/card/{card['id']}/export'>Показать данные карточки на сайте</a>"
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

        content = (
            f"<h2>Редактирование карточки #{card['id']}</h2>"
            f"<form method='post' action='/card/{card['id']}/edit'>"
            f"<p>Организация RU <input name='ru_org' value='{escape(card['ru_org'])}'></p>"
            f"<p>Organization EN <input name='en_org' value='{escape(card['en_org'])}'></p>"
            "<button>Сохранить изменения</button>"
            "</form>"
        )
        body = self._page(f"Редактирование карточки #{card['id']}", content, back_href=f"/card/{card_id}")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def card_edit_post(self, card_id: int, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        ru_org, ru_notes = self.normalize_ru_org(self._get_one(form, "ru_org"))
        en_org, en_notes = self.normalize_en_org(self._get_one(form, "en_org"), ru_org, is_media=self._get_one(form, "is_media") == "1", is_ru_registered=self._get_one(form, "is_ru_registered") == "1")
        notes = ru_notes + en_notes
        status = self._status(notes, bool(ru_org and en_org))

        with self._connect() as db:
            card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
            if not card:
                return "Not found", "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]

            payload = json.loads(card["data_json"] or "{}")
            payload["notes"] = notes
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
        lines = "\n".join(f"{label}: {profile.get(field, '')}" for field, label in CARD_FIELDS)
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

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=["id", "ru_org", "en_org", "status", "source", "created_at"])
        writer.writeheader()
        writer.writerow(
            {
                "id": card["id"],
                "ru_org": card["ru_org"],
                "en_org": card["en_org"],
                "status": card["status"],
                "source": card["source"],
                "created_at": card["created_at"],
            }
        )
        return buffer.getvalue(), "200 OK", [("Content-Type", "text/csv; charset=utf-8")]


def run_server(db_path: str = "cards.db", host: str = "0.0.0.0", port: int = 8000) -> None:
    app = CompanyWebApp(db_path=db_path)
    with make_server(host, port, app) as httpd:
        browser_host = "localhost" if host == "0.0.0.0" else host
        print(f"Running on http://{browser_host}:{port} (bound to {host}:{port})")
        httpd.serve_forever()


if __name__ == "__main__":
    run_server()
