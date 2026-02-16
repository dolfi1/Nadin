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
    assert "Источник: checko.ru — provider_called_ok" in review
    assert "<td>Источник</td><td>checko.ru</td>" not in review


def test_input_routing_does_not_fill_fio_for_org_or_inn(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _, _, org_review = call_app(app, "POST", "/autofill/review", form={"company_name": 'ООО "ГАЗПРОМ ПЕРЕРАБОТКА"'})
    assert "Тип ввода: ORG_TEXT" in org_review
    assert "<td>Фамилия</td><td></td>" in org_review
    assert "<td>Имя</td><td></td>" in org_review
    assert "<td>Отчество</td><td></td>" in org_review

    _, _, inn_review = call_app(app, "POST", "/autofill/review", form={"company_name": "1234567890"})
    assert "Тип ввода: INN" in inn_review
    assert "<td>Фамилия</td><td></td>" in inn_review
    assert "<td>Имя</td><td></td>" in inn_review


def test_full_opf_normalization_moves_short_form_to_end(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    normalized, notes = app.normalize_ru_org("ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ «РОМАШКА»")
    assert normalized == "Ромашка ООО"
    assert any("полная ОПФ сокращена" in note for note in notes)


def test_empty_title_status_is_not_filled(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "ПАО Сбербанк"})
    assert "<td>Титул</td><td></td><td>—</td><td>Нужно заполнить</td>" in review


def test_search_trace_has_no_internal_db_iterations(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "Газпром"})
    assert "Проверка источников по записи" not in review
    assert "Fallback: Вставь выписку (regex-парсинг)" in review
    assert "Источники: не получено (в источниках нет данных по запросу)" in review

def test_multisource_fallback_skips_checko_429_and_keeps_fns_data(monkeypatch, tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_urlopen(*_args, **_kwargs):
        raise RuntimeError("HTTP Error 429: Too Many Requests")

    monkeypatch.setattr(web_app, "urlopen", fake_urlopen)

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "7707083893"})
    assert status.startswith("200")
    assert "Источник: ФНС ЕГРЮЛ — provider_called_ok" in review
    assert "Источник: checko.ru — rate_limited" in review
    assert "<td>Организация</td><td>Сбербанк ПАО</td>" in review

def test_inn_input_keeps_name_and_org_empty_without_source_data(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "1234567890"})
    assert "Тип ввода: INN" in review
    assert "Ключ поиска провайдеров: inn:1234567890" in review
    assert "<td>ИНН</td><td>1234567890</td><td>Ввод пользователя/ФНС</td><td>Заполнено</td>" in review
    assert "<td>Организация</td><td></td><td>—</td><td>Нужно заполнить</td>" in review
    assert "<td>Organization</td><td></td><td>—</td><td>Нужно заполнить</td>" in review
    assert "<td>Family name</td><td></td>" in review
    assert "<td>First name</td><td></td>" in review


def test_negative_cache_reporting_and_retry_controls(monkeypatch, tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_urlopen(*_args, **_kwargs):
        raise RuntimeError("HTTP Error 429: Too Many Requests")

    monkeypatch.setattr(web_app, "urlopen", fake_urlopen)

    _, _, first_review = call_app(app, "POST", "/autofill/review", form={"company_name": "1102054991"})
    assert "rate_limited" in first_review
    assert "Повторить без кэша" in first_review
    assert "Сбросить кэш по ИНН" in first_review

    _, _, second_review = call_app(app, "POST", "/autofill/review", form={"company_name": "1102054991"})
    assert "skipped_due_to_negative_cache" in second_review

    _, _, no_cache_review = call_app(
        app,
        "POST",
        "/autofill/review",
        form={"company_name": "1102054991", "no_cache": "1"},
    )
    assert "rate_limited" in no_cache_review

    _, _, reset_review = call_app(
        app,
        "POST",
        "/autofill/review",
        form={"company_name": "1102054991", "reset_inn_cache": "1"},
    )
    assert "Кэш по ИНН очищен:" in reset_review


def test_inn_1102054991_builds_expected_profile_and_trace(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "1102054991"})
    assert status.startswith("200")
    assert "<td>Обращение</td><td>Г-н</td>" in review
    assert "<td>Family name</td><td>Ishmurzin</td>" in review
    assert "<td>First name</td><td>Airat</td>" in review
    assert "<td>Middle name</td><td>Vilsurovich</td>" in review
    assert "<td>Фамилия</td><td>Ишмурзин</td>" in review
    assert "<td>Имя</td><td>Айрат</td>" in review
    assert "<td>Отчество</td><td>Вильсурович</td>" in review
    assert "<td>Пол</td><td>М</td>" in review
    assert "<td>Организация</td><td>Газпром Переработка ООО</td>" in review
    assert "<td>Organization</td><td>Gazprom Pererabotka LLC</td>" in review
    assert "<td>Должность</td><td>Генеральный директор</td>" in review
    assert "<td>Position</td><td>General Director</td>" in review
    assert "Trace в UI: ФНС ЕГРЮЛ (ok) → list-org.com (ok) → OpenCorporates (ok) → OffshoreLeaks (ok)" in review


def test_positive_cache_hit_is_used_for_second_inn_request(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _, _, first_review = call_app(app, "POST", "/autofill/review", form={"company_name": "1102054991"})
    assert "Источник: ФНС ЕГРЮЛ — provider_called_ok" in first_review

    _, _, second_review = call_app(app, "POST", "/autofill/review", form={"company_name": "1102054991"})
    assert "Источник: ФНС ЕГРЮЛ — provider_cached_hit" in second_review


def test_five_inn_inputs_return_review_without_errors(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    inns = ["1102054991", "7707083893", "7810783119", "7702070139", "7736050003"]

    for inn in inns:
        status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": inn})
        assert status.startswith("200")
        assert "Internal error" not in review
        assert f"Ключ поиска провайдеров: inn:{inn}" in review


def test_inn_5003021311_returns_multisource_hits_and_trace(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": "5003021311"})
    assert status.startswith("200")
    assert "Источник: list-org.com — provider_called_ok" in review
    assert "Источник: OpenCorporates — provider_called_ok" in review
    assert "Источник: OffshoreLeaks — provider_called_ok" in review
    assert "Trace в UI: ФНС ЕГРЮЛ (ok) → list-org.com (ok) → OpenCorporates (ok) → OffshoreLeaks (ok)" in review
    assert "<td>Организация</td><td>Газпром Межрегионгаз ООО</td>" in review


def test_ten_inn_inputs_have_95_percent_or_better_source_hits(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    inns = [
        "5003021311",
        "1102054991",
        "7707083893",
        "7810783119",
        "7702070139",
        "7704867853",
        "7736050003",
        "7708503727",
        "7728715184",
        "7706092528",
    ]

    successful = 0
    for inn in inns:
        status, _, review = call_app(app, "POST", "/autofill/review", form={"company_name": inn})
        assert status.startswith("200")
        if "provider_called_ok" in review:
            successful += 1

    assert successful / len(inns) >= 0.95
