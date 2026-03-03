import csv
import io
import json
from urllib.parse import parse_qs, urlparse


def test_sanitize_ru_position_noise(app):
    assert app.sanitize_ru_position("Юридического лица история греф герман оскарович проверить") is None


def test_export_csv_contains_all_profile_fields(app):
    profile = {
        "surname_ru": "Иванов",
        "name_ru": "Иван",
        "middle_name_ru": "Иванович",
        "family_name": "Ivanov",
        "first_name": "Ivan",
        "middle_name_en": "Ivanovich",
        "gender": "М",
        "inn": "7707083893",
        "ru_position": "Президент",
        "en_position": "President",
        "ru_org": "ПАО СБЕРБАНК",
        "en_org": "SBERBANK PJSC",
    }
    with app._connect() as db:
        cur = db.execute(
            "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
            (
                "ПАО СБЕРБАНК",
                "SBERBANK PJSC",
                "completed",
                "manual",
                app._now(),
                app._now(),
                json.dumps({"profile": profile}, ensure_ascii=False),
            ),
        )
        card_id = cur.lastrowid
        db.commit()

    body, status, headers = app.export_csv(card_id)
    assert status == "200 OK"
    header_names = next(csv.reader(io.StringIO(body)))
    expected = [
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
    assert header_names == expected
    assert dict(headers)["Content-Disposition"] == f'attachment; filename="card_{card_id}.csv"'


def test_autofill_review_dedup_by_source_and_inn(app, monkeypatch):
    calls = []

    def fake_build(source_hits, *_args, **_kwargs):
        calls.append(source_hits)
        return {"ru_org": "ПАО СБЕРБАНК", "en_org": "SBERBANK PJSC", "inn": "7707083893"}, {}

    def fake_search(*_args, provider_names=None, **_kwargs):
        if provider_names == ["ФНС ЕГРЮЛ"]:
            return ([{"source": "zachestnyibiznes.ru", "url": "u1", "data": {"inn": "7707083893", "ru_org": "ПАО СБЕРБАНК"}}], ["fast"])
        return (
            [
                {"source": "zachestnyibiznes.ru", "url": "u2", "data": {"inn": "7707083893", "ru_org": "ПАО СБЕРБАНК"}},
                {"source": "zachestnyibiznes.ru", "url": "u3", "data": {"inn": "7707083893", "ru_org": "ПАО СБЕРБАНК"}},
            ],
            ["extended"],
        )

    monkeypatch.setattr(app, "_build_profile_from_sources", fake_build)
    monkeypatch.setattr(app, "_is_profile_complete", lambda *_a, **_k: False)
    monkeypatch.setattr(app, "_search_external_sources", fake_search)
    monkeypatch.setattr(app, "apply_card_rules", lambda profile, card_type="": (profile, []))
    monkeypatch.setattr(app, "_missing_required_fields", lambda *_a, **_k: ["ru_org"])

    app.autofill_review({"company_name": ["7707083893"]}, wants_json=False)

    assert len(calls) >= 2
    deduped_hits = calls[-1]
    assert len(deduped_hits) == 1


def test_manual_post_requires_ru_org_json(app):
    body, status, headers = app.manual_post({"en_org": ["Org Ltd"]}, wants_json=True)

    assert status == "400 Bad Request"
    assert dict(headers)["Content-Type"].startswith("application/json")
    payload = json.loads(body)
    assert payload["field"] == "ru_org"


def test_manual_post_requires_en_org_json(app):
    body, status, _ = app.manual_post({"ru_org": ["ООО Ромашка"]}, wants_json=True)

    assert status == "400 Bad Request"
    payload = json.loads(body)
    assert payload["field"] == "en_org"


def test_manual_post_company_requires_inn_json(app):
    body, status, _ = app.manual_post(
        {
            "ru_org": ["ООО Ромашка"],
            "en_org": ["Romashka LLC"],
            "search_type": ["company"],
        },
        wants_json=True,
    )

    assert status == "400 Bad Request"
    payload = json.loads(body)
    assert payload["field"] == "inn"


def test_manual_post_redirect_on_validation_error_html(app):
    body, status, headers = app.manual_post({"ru_org": ["ООО Ромашка"]}, wants_json=False)

    assert body == ""
    assert status == "302 Found"
    location = dict(headers)["Location"]
    query = parse_qs(urlparse(location).query)
    assert query.get("error", [""])[0].startswith("Заполните обязательное поле")
