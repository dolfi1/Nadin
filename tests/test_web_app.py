from io import BytesIO
from urllib.parse import urlencode

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
    assert "Sberbank PJSC" in review

    status, headers, _ = call_app(
        app,
        "POST",
        "/autofill/confirm",
        form={"ru_org": "Сбербанк ПАО", "en_org": "Sberbank PJSC"},
    )
    assert status.startswith("302")

    card_status, _, page = call_app(app, "GET", headers["Location"])
    assert card_status.startswith("200")
    assert "PJSC" in page


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

    _, _, csv_body = call_app(app, "GET", f"{headers['Location']}/export.csv")
    assert "ru_org" in csv_body
    assert "Romashka LLC" in csv_body
