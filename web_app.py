from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs
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

SOURCE_CATALOG: dict[str, list[dict[str, str]]] = {
    "СБЕРБАНК ПАО": [
        {"source": "ЕГРЮЛ", "ru_org": "Сбербанк ПАО", "en_org": "Sberbank PJSC"},
        {"source": "СПАРК", "ru_org": "ПАО Сбербанк", "en_org": "Sberbank PJSC"},
    ],
    "РОМАШКА ООО": [
        {"source": "Контур.Фокус", "ru_org": "Ромашка ООО", "en_org": "Romashka LLC"},
        {"source": "Rusprofile", "ru_org": "ООО Ромашка", "en_org": "Romashka LLC"},
    ],
}


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

    def normalize_ru_org(self, raw: str) -> tuple[str, list[str]]:
        notes: list[str] = []
        cleaned = self._normalize_spaces(self._strip_noise(raw.upper()))
        for full, short in FULL_RU_OPF.items():
            if cleaned.startswith(full + " "):
                cleaned = cleaned.replace(full, short, 1)
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
            name = " ".join(self._translit(tok) for tok in ru_parts[:-1])
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

    def _search_external_sources(self, company_name: str) -> list[dict[str, str]]:
        normalized, _ = self.normalize_ru_org(company_name)
        candidates = SOURCE_CATALOG.get(normalized.upper(), [])
        if candidates:
            return candidates

        token = normalized.split()[0].upper() if normalized else ""
        if not token:
            return []

        aggregated: list[dict[str, str]] = []
        for org_name, records in SOURCE_CATALOG.items():
            if token in org_name:
                aggregated.extend(records)
        return aggregated

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
            with self._connect() as db:
                exact = db.execute("SELECT * FROM cards WHERE ru_org=? ORDER BY id DESC", (normalized,)).fetchall()
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
                f"<form method='post' action='/autofill/review'><input type='hidden' name='company_name' value='{escape(q)}' /><button>Автосбор из открытых источников</button></form>"
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
        source_hits = self._search_external_sources(raw)
        best = source_hits[0] if source_hits else None
        ru_seed = best["ru_org"] if best else raw
        en_seed = best["en_org"] if best else ""

        ru_org, ru_notes = self.normalize_ru_org(ru_seed)
        en_org, en_notes = self.normalize_en_org(en_seed, ru_org)
        notes = ru_notes + en_notes
        if source_hits:
            notes.append(f"Источники: найдено {len(source_hits)}")
        else:
            notes.append("Источники: совпадения не найдены, использована нормализация")

        source_hidden = "".join(
            f"<input type='hidden' name='source_name' value='{escape(item['source'])}'/>" for item in source_hits
        )
        hidden = "".join(f"<input type='hidden' name='notes' value='{escape(n)}'/>" for n in notes)
        source_list = (
            "<h3>Найдено в доступных источниках</h3><ul>"
            + "".join(
                f"<li>{escape(item['source'])}: {escape(item['ru_org'])} / {escape(item['en_org'])}</li>" for item in source_hits
            )
            + "</ul>"
        ) if source_hits else "<p>В доступных источниках совпадений не найдено.</p>"
        content = (
            "<h2>Автосбор: черновик</h2>"
            f"{source_list}"
            "<form method='post' action='/autofill/confirm'>"
            f"<p>RU: <input name='ru_org' value='{escape(ru_org)}'/></p>"
            f"<p>EN: <input name='en_org' value='{escape(en_org)}'/></p>"
            f"{hidden}{source_hidden}"
            "<button>Подтвердить и сохранить</button></form>"
        )
        body = self._page("Автосбор: черновик", content, back_href="/")
        return body, "200 OK", [("Content-Type", "text/html; charset=utf-8")]

    def autofill_confirm(self, form: dict[str, list[str]]) -> tuple[str, str, list[tuple[str, str]]]:
        ru_org = self._get_one(form, "ru_org")
        en_org = self._get_one(form, "en_org")
        notes = form.get("notes", [])
        source_names = form.get("source_name", [])
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
                    json.dumps({"notes": notes, "source_hits": source_names}, ensure_ascii=False),
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
        content = (
            f"<h2>Карточка #{card['id']}</h2>"
            f"<p>Организация RU: {escape(card['ru_org'])}</p>"
            f"<p>Organization EN: {escape(card['en_org'])}</p>"
            f"<p>Статус: {escape(card['status'])}</p>"
            f"<p>Источник: {escape(card['source'])}</p>"
            f"<p><a href='/card/{card['id']}/edit'>Редактировать карточку</a></p>"
            f"<a href='/card/{card['id']}/export'>Показать данные карточки на сайте</a>"
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

        content = (
            f"<h2>Данные карточки #{card['id']}</h2>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            f"<tr><td>id</td><td>{card['id']}</td></tr>"
            f"<tr><td>ru_org</td><td>{escape(card['ru_org'])}</td></tr>"
            f"<tr><td>en_org</td><td>{escape(card['en_org'])}</td></tr>"
            f"<tr><td>status</td><td>{escape(card['status'])}</td></tr>"
            f"<tr><td>source</td><td>{escape(card['source'])}</td></tr>"
            f"<tr><td>created_at</td><td>{escape(card['created_at'])}</td></tr>"
            "</table>"
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
        print(f"Running on http://{host}:{port}")
        httpd.serve_forever()


if __name__ == "__main__":
    run_server()
