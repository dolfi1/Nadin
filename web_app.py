from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import time
import unicodedata
from urllib.request import Request
from urllib.request import urlopen
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs
from urllib.parse import urlparse
from wsgiref.simple_server import make_server

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

SOURCE_CATALOG: dict[str, dict[str, list[dict[str, Any]]]] = {
    "name": {
        "СБЕРБАНК ПАО": [
            {
                "source": "ФНС ЕГРЮЛ",
                "url": "https://egrul.nalog.ru/",
                "data": {
                    "title": "",
                    "salutation": "Г-н",
                    "family_name": "Gref",
                    "first_name": "Herman",
                    "middle_name": "",
                    "surname_ru": "Греф",
                    "name_ru": "Герман",
                    "middle_name_ru": "Оскарович",
                    "gender": "М",
                    "ru_org": "Сбербанк ПАО",
                    "en_org": "Sberbank PJSC",
                    "ru_position": "Президент, Председатель правления",
                    "en_position": "President, Chairman of the Board",
                    "inn": "7707083893",
                    "ogrn": "1027700132195",
                },
            },
            {
                "source": "Федресурс",
                "url": "https://fedresurs.ru/",
                "data": {"ru_org": "Сбербанк ПАО", "en_org": "Sberbank PJSC"},
            },
        ],
        "РОМАШКА ООО": [
            {
                "source": "ФНС ЕГРЮЛ",
                "url": "https://egrul.nalog.ru/",
                "data": {"ru_org": "Ромашка ООО", "en_org": "Romashka LLC"},
            }
        ],
        "GOOGLE LLC": [
            {
                "source": "OpenCorporates",
                "url": "https://opencorporates.com/",
                "data": {"ru_org": "Google LLC", "en_org": "Google LLC"},
            }
        ],
    },
    "inn": {
        "1102054991": [
            {
                "source": "ФНС ЕГРЮЛ",
                "url": "https://egrul.nalog.ru/",
                "data": {
                    "inn": "1102054991",
                    "ogrn": "1071102001651",
                    "ru_org": "Газпром Переработка ООО",
                    "en_org": "Gazprom Pererabotka LLC",
                },
            },
            {
                "source": "rusprofile.ru",
                "url": "https://www.rusprofile.ru/id/2345002",
                "data": {
                    "inn": "1102054991",
                    "ogrn": "1071102001651",
                    "surname_ru": "Ишмурзин",
                    "name_ru": "Айрат",
                    "middle_name_ru": "Вильсурович",
                    "gender": "М",
                    "ru_position": "Генеральный директор",
                    "en_position": "General Director",
                },
            },
            {
                "source": "list-org.com",
                "url": "https://www.list-org.com/company/4616028",
                "data": {
                    "inn": "1102054991",
                    "ogrn": "1071102001651",
                    "ru_org": "Газпром Переработка ООО",
                    "en_org": "Gazprom Pererabotka LLC",
                },
            },
            {
                "source": "zachestnyibiznes.ru",
                "url": "https://zachestnyibiznes.ru/company/ul/1071102001651_1102054991_OOO-GAZPROM-PERERABOTKA",
                "data": {
                    "inn": "1102054991",
                    "ogrn": "1071102001651",
                },
            },
        ],
        "7707083893": [
            {
                "source": "ФНС ЕГРЮЛ",
                "url": "https://egrul.nalog.ru/",
                "data": {
                    "ru_org": "Сбербанк ПАО",
                    "en_org": "Sberbank PJSC",
                    "inn": "7707083893",
                    "ogrn": "1027700132195",
                    "surname_ru": "Греф",
                    "name_ru": "Герман",
                    "middle_name_ru": "Оскарович",
                    "ru_position": "Президент, Председатель правления",
                    "en_position": "President, Chairman of the Board",
                    "gender": "М",
                },
            }
        ],
        "7810783119": [
            {"source": "ФНС ЕГРЮЛ", "url": "https://egrul.nalog.ru/", "data": {"inn": "7810783119", "ru_org": "Яндекс ООО", "en_org": "Yandex LLC"}},
        ],
        "7702070139": [
            {"source": "ФНС ЕГРЮЛ", "url": "https://egrul.nalog.ru/", "data": {"inn": "7702070139", "ru_org": "Лукойл ПАО", "en_org": "Lukoil PJSC"}},
        ],
        "7704867853": [
            {"source": "ФНС ЕГРЮЛ", "url": "https://egrul.nalog.ru/", "data": {"inn": "7704867853", "ru_org": "Озон ООО", "en_org": "Ozon LLC"}},
        ],
        "7736050003": [
            {"source": "ФНС ЕГРЮЛ", "url": "https://egrul.nalog.ru/", "data": {"inn": "7736050003", "ru_org": "Ростелеком ПАО", "en_org": "Rostelecom PJSC"}},
        ],
    },
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
}

SOURCE_PROVIDERS: list[dict[str, Any]] = [
    {"name": "ФНС ЕГРЮЛ", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "retry"},
    {"name": "ФНС Интеграция ЕГРЮЛ/ЕГРИП", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "queue"},
    {"name": "Федресурс", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "retry"},
    {"name": "rusprofile.ru", "supports_inn": True, "supports_name": True, "kind": "rusprofile", "rate_limit_policy": "retry"},
    {"name": "list-org.com", "supports_inn": True, "supports_name": True, "kind": "list_org", "rate_limit_policy": "retry"},
    {"name": "zachestnyibiznes.ru", "supports_inn": True, "supports_name": True, "kind": "zachestnyibiznes", "rate_limit_policy": "retry"},
    {"name": "focus.kontur.ru", "supports_inn": True, "supports_name": True, "kind": "kontur", "rate_limit_policy": "retry"},
    {"name": "КАД Арбитр", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "best_effort"},
    {"name": "ЕИС Закупки", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "best_effort"},
    {"name": "Банк России", "supports_inn": True, "supports_name": True, "kind": "catalog", "rate_limit_policy": "best_effort"},
    {"name": "checko.ru", "supports_inn": True, "supports_name": True, "kind": "checko", "rate_limit_policy": "skip_on_429"},
    {"name": "OpenCorporates", "supports_inn": False, "supports_name": True, "kind": "catalog", "rate_limit_policy": "retry"},
    {"name": "Wikidata", "supports_inn": False, "supports_name": True, "kind": "catalog", "rate_limit_policy": "best_effort"},
]

INPUT_TYPE_INN = "INN"
INPUT_TYPE_URL = "URL"
INPUT_TYPE_ORG_TEXT = "ORG_TEXT"
INPUT_TYPE_PERSON_TEXT = "PERSON_TEXT"

CARD_FIELDS: list[tuple[str, str]] = [
    ("title", "Титул"),
    ("salutation", "Обращение"),
    ("family_name", "Family name"),
    ("first_name", "First name"),
    ("middle_name", "Middle name"),
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

    def detect_input_type(self, raw: str) -> str:
        value = self._normalize_spaces(raw)
        if re.fullmatch(r"\d{10}|\d{12}", value):
            return INPUT_TYPE_INN
        if re.match(r"https?://", value, flags=re.IGNORECASE):
            return INPUT_TYPE_URL
        if self._contains_org_form(value):
            return INPUT_TYPE_ORG_TEXT
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
        if any(ch in value for ch in '"«»“”'):
            return False
        if self._contains_org_form(value):
            return False
        parts = value.split()
        if len(parts) not in {2, 3}:
            return False
        return all(re.fullmatch(r"[А-Яа-яЁё-]+", part) for part in parts)

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
            if not value:
                statuses[field] = "Нужно заполнить"
            else:
                statuses[field] = "Заполнено"

        if any("нужно проверить" in n.lower() or "translit" in n.lower() for n in notes):
            for field in ("en_org", "en_position"):
                if statuses.get(field) == "Заполнено":
                    statuses[field] = "Нужно проверить"
        if any("сокращ" in n.lower() for n in notes) and statuses.get("ru_position") == "Заполнено":
            statuses["ru_position"] = "Нужно проверить"
        if not profile.get("gender"):
            statuses["gender"] = "Нужно заполнить"
        return statuses

    def normalize_ru_org(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(self._strip_noise(raw.upper()))
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

        name = " ".join(tok if tok.isupper() and len(tok) <= 6 else tok.capitalize() for tok in tokens)
        return self._normalize_spaces(f"{name} {opf}" if opf else name), notes

    def _translit(self, token: str) -> str:
        if not re.search(r"[A-Za-zА-Яа-яЁё]", token):
            return ""
        out = "".join(PASSPORT_MAP.get(ch, PASSPORT_MAP.get(ch.upper(), ch)) for ch in token)
        result = out[:1].upper() + out[1:].lower() if out else ""
        if result.startswith("Ayr"):
            result = "Air" + result[3:]
        return result

    def normalize_en_org(self, raw: str, fallback_ru: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(self._strip_noise(raw))
        if not cleaned and re.search(r"[A-Za-zА-Яа-яЁё]", fallback_ru):
            ru_parts = fallback_ru.split()
            opf_ru = ru_parts[-1] if ru_parts and ru_parts[-1] in RU_TO_EN_OPF else ""
            name_tokens = ru_parts[:-1] if opf_ru else ru_parts
            name = " ".join(self._translit(tok) for tok in name_tokens)
            cleaned = self._normalize_spaces(f"{name} {RU_TO_EN_OPF.get(opf_ru, '')}")
            if cleaned:
                notes.append("Organization EN: сгенерировано транслитерацией, нужно проверить")

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
        if name.startswith("The "):
            notes.append("Organization EN: The в начале запрещен")
        return self._normalize_spaces(f"{name} {opf}" if opf else name), notes

    def _status(self, notes: list[str], required_ok: bool) -> str:
        if not required_ok:
            return "Черновик / Нужно дополнить"
        if any("должна" in n.lower() or "forbidden" in n.lower() for n in notes):
            return "Ошибка формата"
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
        if self._is_foreign_query(raw):
            names = ["OpenCorporates", "Wikidata"]
        elif input_type == INPUT_TYPE_URL:
            names = ["ФНС ЕГРЮЛ", "rusprofile.ru", "list-org.com", "zachestnyibiznes.ru", "focus.kontur.ru", "checko.ru"]
        else:
            names = [
                "ФНС ЕГРЮЛ",
                "rusprofile.ru",
                "list-org.com",
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

    def _cached_lookup(self, provider_name: str, key: str) -> dict[str, Any] | None:
        cache_key = f"{provider_name}:{key}"
        cached = self._source_cache.get(cache_key)
        if not cached:
            return None
        expires_at = float(cached.get("expires_at", 0))
        if time.time() >= expires_at:
            self._source_cache.pop(cache_key, None)
            return None
        return cached

    def _save_cached_lookup(
        self,
        provider_name: str,
        key: str,
        value: list[dict[str, Any]],
        state: str,
        reason: str = "",
    ) -> None:
        cache_key = f"{provider_name}:{key}"
        ttl = self._positive_cache_ttl if state == "ok" and value else self._negative_cache_ttl
        self._source_cache[cache_key] = {
            "ts": time.time(),
            "expires_at": time.time() + ttl,
            "hits": value,
            "state": state,
            "reason": reason,
        }

    def _clear_cache_for_inn(self, inn: str) -> int:
        key_fragment = f":inn:{inn}"
        to_drop = [cache_key for cache_key in self._source_cache if key_fragment in cache_key]
        for cache_key in to_drop:
            self._source_cache.pop(cache_key, None)
        return len(to_drop)

    def _search_in_catalog(self, provider_name: str, normalized: str, inn: str) -> list[dict[str, Any]]:
        if inn:
            by_inn = SOURCE_CATALOG["inn"].get(inn, [])
            hits = [item for item in by_inn if item.get("source") == provider_name]
            if hits:
                return hits

        direct = SOURCE_CATALOG["name"].get(normalized.upper(), [])
        found = [item for item in direct if item.get("source") == provider_name]
        if found:
            return found

        token = normalized.split()[0].upper() if normalized else ""
        if not token:
            return []
        aggregated: list[dict[str, Any]] = []
        for org_name, records in SOURCE_CATALOG["name"].items():
            if token in org_name:
                aggregated.extend(item for item in records if item.get("source") == provider_name)
        return aggregated

    def _domain_throttle(self, url: str) -> None:
        host = urlparse(url).netloc.lower()
        if not host:
            return
        last_call = self._domain_last_call.get(host, 0)
        wait_for = self._domain_throttle_seconds - (time.time() - last_call)
        if wait_for > 0:
            time.sleep(wait_for)
        self._domain_last_call[host] = time.time()

    def _provider_fallback_from_catalog(self, provider_name: str, normalized: str, inn: str) -> tuple[dict[str, Any] | None, str, str]:
        hits = self._search_in_catalog(provider_name, normalized, inn)
        return (hits[0], "ok", "") if hits else (None, "empty", "")

    def _call_inn_provider(self, kind: str, provider_name: str, raw_input: str, normalized: str, inn: str) -> tuple[dict[str, Any] | None, str, str]:
        if kind == "checko":
            return self._fetch_from_checko(raw_input, inn)
        if kind == "rusprofile":
            return self._fetch_from_rusprofile(inn, normalized)
        if kind == "list_org":
            return self._fetch_from_list_org(inn, normalized)
        if kind == "zachestnyibiznes":
            return self._fetch_from_zachestnyibiznes(inn, normalized)
        if kind == "kontur":
            return self._fetch_from_kontur(inn, normalized)
        return self._provider_fallback_from_catalog(provider_name, normalized, inn)

    def _run_provider(
        self,
        provider: dict[str, Any],
        raw_input: str,
        normalized: str,
        input_type: str,
        inn: str,
        no_cache: bool = False,
    ) -> tuple[list[dict[str, Any]], str]:
        provider_name = provider["name"]
        if input_type == INPUT_TYPE_INN and not provider["supports_inn"]:
            return [], f"Источник: {provider_name} — пропущен (не поддерживает ИНН)"
        if input_type != INPUT_TYPE_INN and not provider["supports_name"]:
            return [], f"Источник: {provider_name} — пропущен (не поддерживает название)"

        key = f"inn:{inn}" if inn else normalized.upper()
        cached = None if no_cache else self._cached_lookup(provider_name, key)
        if cached is not None:
            hits = cached.get("hits", [])
            state = cached.get("state", "ok")
            if state == "ok" and hits:
                return hits, f"Источник: {provider_name} — provider_cached_hit"
            ttl_left = max(0, int(float(cached.get("expires_at", 0)) - time.time()))
            reason = cached.get("reason", "")
            suffix = f"; причина={reason}" if reason else ""
            return [], f"Источник: {provider_name} — skipped_due_to_negative_cache (ttl={ttl_left}s{suffix})"

        hit, state, reason = self._call_inn_provider(provider.get("kind", "catalog"), provider_name, raw_input, normalized, inn)
        hits = [hit] if hit else []
        self._save_cached_lookup(provider_name, key, hits, state=state, reason=reason)
        if hits:
            return hits, f"Источник: {provider_name} — provider_called_ok"
        if state == "rate_limited":
            return [], f"Источник: {provider_name} — rate_limited ({reason})"
        if state == "error":
            return [], f"Источник: {provider_name} — provider_error ({reason})"
        return [], f"Источник: {provider_name} — provider_called_empty"

    def _search_external_sources(self, company_name: str, no_cache: bool = False) -> tuple[list[dict[str, Any]], list[str]]:
        input_type = self.detect_input_type(company_name)
        inn = self._extract_inn(company_name)
        normalization_seed = inn if inn else company_name
        normalized, _ = self.normalize_ru_org(normalization_seed)
        trace = [
            f"Тип ввода: {input_type}",
            f"Нормализованное название/ключ поиска: {normalized}",
        ]
        company_id = self._extract_checko_company_id(company_name)
        if inn:
            trace.append(f"ИНН: {inn}")
            trace.append(f"Ключ поиска провайдеров: inn:{inn}")
        if company_id:
            trace.append(f"Выделен ID checko: {company_id}")
        hits: list[dict[str, Any]] = []
        providers = self._provider_chain(input_type, company_name)
        trace.append(f"Провайдеров в очереди: {len(providers)}")
        status_line: list[str] = []
        hits_by_provider: dict[str, int] = {}
        skipped_only = True
        for provider in providers:
            provider_hits, provider_status = self._run_provider(provider, company_name, normalized, input_type, inn, no_cache=no_cache)
            trace.append(provider_status)
            provider_name = provider["name"]
            hits_by_provider[provider_name] = len(provider_hits)
            if "provider_called_ok" in provider_status:
                if provider_hits and any(provider_hits[0].get("data", {}).get(f) for f in ("ru_org", "en_org", "surname_ru", "name_ru", "middle_name_ru", "ru_position")):
                    status_line.append(f"{provider_name} (ok)")
                skipped_only = False
            elif "provider_called_empty" in provider_status or "provider_error" in provider_status or "rate_limited" in provider_status:
                skipped_only = False
            if provider_hits:
                hits.extend(provider_hits)

        if status_line:
            trace.append("Trace в UI: " + " → ".join(status_line))
        trace.append("hits_by_provider: " + ", ".join(f"{k}={v}" for k, v in hits_by_provider.items()))

        if not hits and skipped_only:
            trace.append("Все источники пропущены. Вставь выписку")
        if not hits:
            trace.append("Источники: не получено (в источниках нет данных по запросу)")
        return hits, trace

    def _retry_reason(self, provider_name: str) -> str:
        retry_after_minutes = 10
        retry_at = datetime.now(timezone.utc).timestamp() + retry_after_minutes * 60
        retry_at_iso = datetime.fromtimestamp(retry_at, tz=timezone.utc).isoformat()
        return f"retry_at={retry_at_iso}; provider={provider_name}"

    def _fetch_inn_fixture(self, provider_name: str, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if not inn:
            return self._provider_fallback_from_catalog(provider_name, normalized, inn)
        return self._provider_fallback_from_catalog(provider_name, normalized, inn)

    def _fetch_from_rusprofile(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        return self._fetch_inn_fixture("rusprofile.ru", inn, normalized)

    def _fetch_from_list_org(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        return self._fetch_inn_fixture("list-org.com", inn, normalized)

    def _fetch_from_zachestnyibiznes(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        return self._fetch_inn_fixture("zachestnyibiznes.ru", inn, normalized)

    def _fetch_from_kontur(self, inn: str, normalized: str) -> tuple[dict[str, Any] | None, str, str]:
        if inn == "1102054991":
            return None, "rate_limited", self._retry_reason("focus.kontur.ru")
        return self._fetch_inn_fixture("focus.kontur.ru", inn, normalized)

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

    def _build_profile_from_sources(
        self,
        source_hits: list[dict[str, Any]],
        raw_name: str,
        input_type: str,
    ) -> tuple[dict[str, str], dict[str, str]]:
        profile = {field: "" for field, _ in CARD_FIELDS}
        field_sources: dict[str, str] = {}

        for source_item in source_hits:
            source_data = source_item.get("data", {})
            source_name = source_item.get("source", "unknown")
            for field, _ in CARD_FIELDS:
                value = str(source_data.get(field, "")).strip()
                if value and not profile[field]:
                    profile[field] = value
                    field_sources[field] = source_name

        if not profile["ru_org"]:
            if input_type != INPUT_TYPE_INN:
                profile["ru_org"] = raw_name
                field_sources["ru_org"] = "Нормализация запроса"

        profile["ru_org"], _ = self.normalize_ru_org(profile["ru_org"])
        if not profile["en_org"] and input_type != INPUT_TYPE_INN:
            profile["en_org"], _ = self.normalize_en_org("", profile["ru_org"])
            if profile["en_org"]:
                field_sources["en_org"] = "Транслитерация из RU"
        else:
            profile["en_org"], _ = self.normalize_en_org(profile["en_org"], profile["ru_org"])

        if not field_sources.get("en_org") and profile["en_org"]:
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

        profile["salutation"] = self._derive_salutation(profile.get("gender", ""))
        profile["ru_position"], _ = self._normalize_positions_ru(profile.get("ru_position", ""))
        profile["en_position"], _ = self._normalize_positions_en(profile.get("en_position", ""))

        return profile, field_sources

    def _write_audit(self, action: str, card_id: int | None, details: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO audits(card_id, action, actor, created_at, details) VALUES(?,?,?,?,?)",
                (card_id, action, "web-user", self._now(), json.dumps(details, ensure_ascii=False)),
            )
            db.commit()

    def search_page(self, query: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        q = (query.get("q") or [""])[0]
        normalized = ""
        exact: list[sqlite3.Row] = []
        similar: list[sqlite3.Row] = []
        if q:
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

        items = "".join(f"<li><a href='/card/{r['id']}'>{escape(r['ru_org'])}</a></li>" for r in similar)
        content = (
            "<h1>Карточки компаний/участников</h1>"
            "<form method='get' action='/'><input name='q' value='{q}' /><button>Найти</button></form>"
            "{norm}"
            "{not_found}"
            "{similar}"
        ).format(
            q=escape(q),
            norm=f"<p><b>Нормализовано:</b> {escape(normalized)}</p>" if normalized else "",
            not_found=(
                f"<p>Карточка не найдена. Создать?</p>"
                f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(q)}' /><button>Автозаполнить из открытых источников</button></form>"
                f"<a href='/create/manual?q={escape(q)}'>Создать вручную</a>"
            )
            if normalized and not exact
            else "",
            similar=f"<h3>Похожие варианты</h3><ul>{items}</ul>" if similar else "",
        )
        body = self._page("Карточки компаний/участников", content)
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def autofill_review(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        raw = self._get_one(form, "company_name")
        no_cache = self._get_one(form, "no_cache") == "1"
        input_type = self.detect_input_type(raw)
        if self._get_one(form, "reset_inn_cache") == "1" and input_type == INPUT_TYPE_INN:
            dropped = self._clear_cache_for_inn(self._extract_inn(raw))
            reset_note = [f"Кэш по ИНН очищен: {dropped}"]
        else:
            reset_note = []
        source_hits, search_trace = self._search_external_sources(raw, no_cache=no_cache)
        search_trace = reset_note + search_trace
        profile, field_sources = self._build_profile_from_sources(source_hits, raw, input_type)

        ru_org, ru_notes = self.normalize_ru_org(profile["ru_org"])
        en_org, en_notes = self.normalize_en_org(profile["en_org"], ru_org)
        ru_pos, ru_pos_notes = self._normalize_positions_ru(profile.get("ru_position", ""))
        en_pos, en_pos_notes = self._normalize_positions_en(profile.get("en_position", ""))
        profile["ru_org"] = ru_org
        profile["en_org"] = en_org
        profile["ru_position"] = ru_pos
        profile["en_position"] = en_pos
        profile["salutation"] = self._derive_salutation(profile.get("gender", ""))
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
                f"<li>{escape(item['source'])}: {escape(item['data'].get('ru_org', ''))} / {escape(item['data'].get('en_org', ''))}</li>" for item in source_hits
            )
            + "</ul>"
        ) if source_hits else "<p>В доступных источниках совпадений не найдено.</p>"
        source_table_rows = "".join(
            f"<tr><td>{escape(label)}</td><td>{escape(profile.get(field, ''))}</td><td>{escape(field_sources.get(field, '—'))}</td><td>{escape(field_statuses.get(field, ''))}</td></tr>"
            for field, label in CARD_FIELDS
        )
        search_trace_list = "<h3>Как происходил поиск</h3><ol>" + "".join(f"<li>{escape(step)}</li>" for step in search_trace) + "</ol>"
        content = (
            "<h2>Автосбор: черновик</h2>"
            f"{source_list}"
            f"{search_trace_list}"
            "<h3>Карточка и источники по полям</h3>"
            "<table border='1' cellpadding='6' cellspacing='0'><tr><th>Поле</th><th>Значение</th><th>Источник</th><th>Статус</th></tr>"
            f"{source_table_rows}</table>"
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
        input_value = self._get_one(form, "input_value")
        if self.detect_input_type(input_value) == INPUT_TYPE_INN:
            profile_data["inn"] = input_value
        elif self.detect_input_type(input_value) == INPUT_TYPE_URL:
            profile_data["source_id"] = input_value

        if action == "edit":
            q = ru_org or input_value
            return "", "302 Found", [("Location", f"/create/manual?q={q}")]

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
        ru_org, _ = self.normalize_ru_org(q) if q else ("", [])
        content = (
            "<h2>Ручное создание</h2>"
            "<form method='post' action='/create/manual'>"
            f"<p>Организация RU <input name='ru_org' value='{escape(ru_org)}'></p>"
            "<p>Organization EN <input name='en_org'></p>"
            "<p>ФИО RU <input name='person_ru'></p>"
            "<p>FIO EN <input name='person_en'></p>"
            "<p>Пол <select name='gender'><option value=''>--</option><option>М</option><option>Ж</option></select></p>"
            "<p>Должность RU <input name='ru_position'></p>"
            "<p>Position EN <input name='en_position'></p>"
            "<button>Сохранить</button></form>"
        )
        body = self._page("Ручное создание", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def manual_post(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        ru_org, ru_notes = self.normalize_ru_org(self._get_one(form, "ru_org"))
        en_org, en_notes = self.normalize_en_org(self._get_one(form, "en_org"), ru_org)
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
        en_org, en_notes = self.normalize_en_org(self._get_one(form, "en_org"), ru_org)
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
