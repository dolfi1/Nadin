from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
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

SOURCE_CATALOG: dict[str, list[dict[str, Any]]] = {
    "СБЕРБАНК ПАО": [
        {
            "source": "ЕГРЮЛ",
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
            },
        },
        {
            "source": "СПАРК",
            "url": "https://spark-interfax.ru/",
            "data": {
                "ru_org": "ПАО Сбербанк",
                "en_org": "Sberbank PJSC",
                "ru_position": "Президент, Председатель Правления",
                "en_position": "President, Chairman of the Board",
            },
        },
    ],
    "РОМАШКА ООО": [
        {
            "source": "Контур.Фокус",
            "url": "https://focus.kontur.ru/",
            "data": {"ru_org": "Ромашка ООО", "en_org": "Romashka LLC"},
        },
        {
            "source": "Rusprofile",
            "url": "https://www.rusprofile.ru/",
            "data": {"ru_org": "ООО Ромашка", "en_org": "Romashka LLC"},
        },
    ],
}

SOURCE_DOMAINS = {
    "egrul.nalog.ru": "ЕГРЮЛ",
    "spark-interfax.ru": "СПАРК",
    "focus.kontur.ru": "Контур.Фокус",
    "rusprofile.ru": "Rusprofile",
    "www.rusprofile.ru": "Rusprofile",
    "checko.ru": "checko.ru",
    "www.checko.ru": "checko.ru",
}

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
    ("ru_org", "Организация"),
    ("en_org", "Organization"),
    ("ru_position", "Должность"),
    ("en_position", "Position"),
]


class CompanyWebApp:
    def __init__(self, db_path: str = "cards.db") -> None:
        self.db_path = Path(db_path)
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
        out = "".join(PASSPORT_MAP.get(ch, PASSPORT_MAP.get(ch.upper(), ch)) for ch in token)
        return out[:1].upper() + out[1:].lower() if out else ""

    def normalize_en_org(self, raw: str, fallback_ru: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(self._strip_noise(raw))
        if not cleaned:
            ru_parts = fallback_ru.split()
            opf_ru = ru_parts[-1] if ru_parts and ru_parts[-1] in RU_TO_EN_OPF else ""
            name_tokens = ru_parts[:-1] if opf_ru else ru_parts
            name = " ".join(self._translit(tok) for tok in name_tokens)
            cleaned = self._normalize_spaces(f"{name} {RU_TO_EN_OPF.get(opf_ru, '')}")
            notes.append("Organization EN: сгенерировано транслитерацией, нужно проверить")

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

    def _search_external_sources(self, company_name: str) -> tuple[list[dict[str, Any]], list[str]]:
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
        if company_id:
            trace.append(f"Выделен ID checko: {company_id}")

        checko_hit, checko_status = self._fetch_from_checko(company_name, inn)
        trace.append(checko_status)
        if checko_hit:
            return [checko_hit], trace

        if input_type == INPUT_TYPE_URL:
            netloc = urlparse(company_name).netloc.lower()
            source_name = SOURCE_DOMAINS.get(netloc)
            if source_name:
                trace.append(f"{source_name}: OK (определён по URL)")
                for records in SOURCE_CATALOG.values():
                    matches = [record for record in records if record.get("source") == source_name]
                    if matches:
                        return matches, trace
                trace.append(f"{source_name}: не получено (в источниках нет данных по запросу)")
            else:
                trace.append("URL-источник: не получено (домен не поддерживается)")

        candidates = SOURCE_CATALOG.get(normalized.upper(), [])
        if candidates:
            trace.append("Каталог источников: OK")
            return candidates, trace

        token = normalized.split()[0].upper() if normalized else ""
        if not token:
            trace.append("Каталог источников: не получено (пустой ключ поиска)")
            return [], trace

        aggregated: list[dict[str, str]] = []
        for org_name, records in SOURCE_CATALOG.items():
            if token in org_name:
                aggregated.extend(records)
        if aggregated:
            trace.append(f"Каталог источников: OK (найдено {len(aggregated)})")
        else:
            trace.append("Каталог источников: не получено (в источниках нет данных по запросу)")
        return aggregated, trace

    def _fetch_from_checko(self, raw_input: str, inn: str = "") -> tuple[dict[str, Any] | None, str]:
        parsed = urlparse(raw_input) if self.detect_input_type(raw_input) == INPUT_TYPE_URL else None
        host = (parsed.netloc.lower() if parsed else "")

        if host and host not in {"checko.ru", "www.checko.ru"}:
            return None, "checko.ru: не получено (домен не checko.ru)"
        if not inn and host:
            path_match = re.search(r"-(\d{10}|\d{12})/?$", parsed.path)
            if path_match:
                inn = path_match.group(1)
        if not inn:
            return None, "checko.ru: не получено (ИНН не найден во входе)"

        url = raw_input if host in {"checko.ru", "www.checko.ru"} else f"https://checko.ru/company/by-inn/{inn}"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=8) as response:
                html = response.read().decode("utf-8", errors="ignore")
        except TimeoutError:
            return None, "checko.ru: не получено (timeout)"
        except Exception as exc:  # noqa: BLE001
            reason = str(exc).strip() or exc.__class__.__name__
            return None, f"checko.ru: не получено ({reason})"

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
            return None, "checko.ru: не получено (parse error)"

        ru_org, _ = self.normalize_ru_org(org_name)
        en_org, _ = self.normalize_en_org("", ru_org)
        return {
            "source": "checko.ru",
            "url": url,
            "data": {
                "ru_org": ru_org,
                "en_org": en_org,
            },
        }, "checko.ru: OK"

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
            profile["ru_org"] = raw_name
            field_sources["ru_org"] = "Нормализация запроса"

        profile["ru_org"], _ = self.normalize_ru_org(profile["ru_org"])
        if not profile["en_org"]:
            profile["en_org"], _ = self.normalize_en_org("", profile["ru_org"])
            field_sources["en_org"] = "Транслитерация из RU"
        else:
            profile["en_org"], _ = self.normalize_en_org(profile["en_org"], profile["ru_org"])

        if not field_sources.get("en_org") and profile["en_org"]:
            field_sources["en_org"] = "Нормализация/источник"

        if profile.get("surname_ru") or profile.get("name_ru"):
            profile["family_name"] = profile.get("family_name") or self._translit(profile.get("surname_ru", ""))
            profile["first_name"] = profile.get("first_name") or self._translit(profile.get("name_ru", ""))
            profile["middle_name"] = profile.get("middle_name") or ""

        if input_type == INPUT_TYPE_PERSON_TEXT and not profile.get("surname_ru") and not profile.get("name_ru"):
            sur, nam, patr = self._split_fio_ru(raw_name)
            profile["surname_ru"] = sur
            profile["name_ru"] = nam
            profile["middle_name_ru"] = patr
            if sur:
                profile["family_name"] = self._translit(sur)
            if nam:
                profile["first_name"] = self._translit(nam)

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
        input_type = self.detect_input_type(raw)
        source_hits, search_trace = self._search_external_sources(raw)
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
        field_sources = {
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
                            "field_sources": field_sources,
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
        field_sources = payload.get("field_sources", {})
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
