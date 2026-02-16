from io import BytesIO
from urllib.parse import urlencode

import web_app
from web_app import CompanyWebApp


def call_app(app, method, path, query="", form=None):
    body = urlencode(form or {}).encode("utf-8")
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": BytesIO(body),
    }
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    result = b"".join(app(environ, start_response)).decode("utf-8")
    return captured["status"], captured["headers"], result


def test_search_normalization_and_manual_create(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, body = call_app(app, "GET", "/", query="q=ОБЩЕСТВО+С+ОГРАНИЧЕННОЙ+ОТВЕТСТВЕННОСТЬЮ+«РОМАШКА»")
    assert status.startswith("200")
    assert "Нормализовано:" in body
    assert "Ромашка ООО" in body

    status, headers, _ = call_app(
        app,
        "POST",
        "/create/manual",
        form={"ru_org": "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ «РОМАШКА»", "en_org": "Romashka LLC"},
    )
    assert status.startswith("302")
    assert headers["Location"].startswith("/card/")


def test_autofill_requires_review_before_save(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "ПАО Сбербанк"})
    assert status.startswith("200")
    assert "Автосбор: черновик" in review
    assert "Найдено в доступных источниках" in review
    assert "Sberbank PJSC" in review
    assert "Как происходил поиск" in review
    assert "Карточка и источники по полям" in review
    assert "Источник" in review

    status, headers, _ = call_app(
        app,
        "POST",
        "/autofill/confirm",
        form={
            "ru_org": "Сбербанк ПАО",
            "en_org": "Sberbank PJSC",
            "profile_family_name": "Gref",
            "profile_first_name": "Herman",
            "profile_name_ru": "Герман",
            "profile_surname_ru": "Греф",
            "profile_middle_name_ru": "Оскарович",
            "profile_ru_org": "Сбербанк ПАО",
            "profile_en_org": "Sberbank PJSC",
            "profile_ru_position": "Президент, Председатель правления",
            "profile_en_position": "President, Chairman of the Board",
            "field_source_family_name": "ЕГРЮЛ",
            "field_source_first_name": "ЕГРЮЛ",
            "field_source_ru_org": "ЕГРЮЛ",
            "field_source_en_org": "СПАРК",
            "source_name": "ЕГРЮЛ",
            "search_trace": "Нормализованный запрос: Сбербанк ПАО",
        },
    )
    assert status.startswith("302")

    card_status, _, page = call_app(app, "GET", headers["Location"])
    assert card_status.startswith("200")
    assert "PJSC" in page


def test_card_edit_and_save(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, headers, _ = call_app(
        app,
        "POST",
        "/create/manual",
        form={"ru_org": "Ромашка ООО", "en_org": "Romashka LLC"},
    )
    assert status.startswith("302")

    edit_status, _, edit_page = call_app(app, "GET", f"{headers['Location']}/edit")
    assert edit_status.startswith("200")
    assert "Сохранить изменения" in edit_page

    save_status, _, _ = call_app(
        app,
        "POST",
        f"{headers['Location']}/edit",
        form={"ru_org": "Сбербанк ПАО", "en_org": "Sberbank PJSC"},
    )
    assert save_status.startswith("302")

    _, _, card_page = call_app(app, "GET", headers["Location"])
    assert "Сбербанк ПАО" in card_page
    assert "Sberbank PJSC" in card_page
    assert "Редактировать карточку" in card_page


def test_csv_export_and_audit_log(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, headers, _ = call_app(
        app,
        "POST",
        "/create/manual",
        form={"ru_org": "Ромашка ООО", "en_org": "Romashka LLC"},
    )
    assert status.startswith("302")

    _, _, card_page = call_app(app, "GET", headers["Location"])
    assert "Audit log" in card_page
    assert "🏠 Главная" in card_page
    assert "← Назад" in card_page
    assert "Показать данные карточки на сайте" in card_page

    _, _, preview_page = call_app(app, "GET", f"{headers['Location']}/export")
    assert "Данные карточки" in preview_page
    assert "Romashka LLC" in preview_page
    assert "Откуда взята информация" in preview_page

    _, _, csv_body = call_app(app, "GET", f"{headers['Location']}/export.csv")
    assert "ru_org" in csv_body
    assert "Romashka LLC" in csv_body


def test_search_by_inn_finds_existing_card(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, headers, _ = call_app(
        app,
        "POST",
        "/autofill/confirm",
        form={
            "action": "create",
            "input_value": "7707083893",
            "ru_org": "Сбербанк ПАО",
            "en_org": "Sberbank PJSC",
        },
    )
    assert status.startswith("302")

    search_status, search_headers, _ = call_app(app, "GET", "/", query="q=7707083893")
    assert search_status.startswith("302")
    assert search_headers["Location"] == headers["Location"]


def test_autofill_review_has_required_actions_and_statuses(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "https://spark-interfax.ru/company/sber"})
    assert status.startswith("200")
    assert "✅ Создать карту" in review
    assert "✏️ Отредактировать" in review
    assert "❌ Отмена" in review
    assert "Нужно заполнить" in review


def test_autofill_review_fills_en_org_from_single_token(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "Газпром"})
    assert status.startswith("200")
    assert "<td>Организация</td><td>Газпром</td>" in review
    assert "<td>Organization</td><td>Gazprom</td>" in review


def test_checko_inn_lookup_uses_external_source(monkeypatch, tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return "<html><head><title>Газпром переработка ООО — checko</title></head></html>".encode("utf-8")

    monkeypatch.setattr(web_app, "urlopen", lambda *_args, **_kwargs: FakeResponse())

    status, _, review = call_app(
        app,
        "POST",
        "/autofill/review",
        form={"company_name": "https://checko.ru/company/gazprom-pererabotka-1071102001651"},
    )
    assert status.startswith("200")
    assert "Выделен ID checko: 1071102001651" in review
    assert "checko.ru: найдена организация Газпром Переработка ООО" in review
    assert "<td>Источник</td><td>checko.ru</td>" not in review
