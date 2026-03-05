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
        return {"en_org": "SBERBANK PJSC", "inn": "7707083893"}, {}

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
import re


def test_family_name_first_name_in_card(app, monkeypatch):
    hits = [
        {
            "source": "ФНС ЕГРЮЛ",
            "type": "company",
            "data": {
                "inn": "7707083893",
                "ru_org": "Сбербанк ПАО",
                "ru_position": "Председатель правления",
                "gender": "М",
                "surname_ru": "",
                "name_ru": "",
            },
        },
        {
            "source": "zachestnyibiznes.ru",
            "type": "company",
            "data": {
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
            },
        },
    ]

    monkeypatch.setattr(app, "_search_external_sources", lambda *_a, **_k: (hits, []))

    body, status, headers = app.autofill_review(
        {"company_name": ["7707083893"], "search_type": ["company"], "hit_type": ["company"]},
        wants_json=False,
    )

    assert body == ""
    assert status == "302 Found"
    card_id = int(dict(headers)["Location"].rsplit("/", 1)[-1])

    with app._connect() as db:
        card = db.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
    profile = json.loads(card["data_json"])["profile"]
    assert profile["surname_ru"] == "Греф"
    assert profile["family_name"] != ""
    assert profile["first_name"] != ""
    assert not re.search(r"[А-Яа-яЁё]", profile.get("middle_name_en", ""))


def test_export_xlsx_has_expected_headers(app):
    profile = {"ru_org": "ПАО СБЕРБАНК", "en_org": "SBERBANK PJSC"}
    with app._connect() as db:
        cur = db.execute(
            "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
            ("ПАО СБЕРБАНК", "SBERBANK PJSC", "Найдено", "manual", app._now(), app._now(), json.dumps({"profile": profile}, ensure_ascii=False)),
        )
        card_id = cur.lastrowid
        db.commit()

    body, status, headers = app.export_xlsx(card_id)
    assert status == "200 OK"
    disposition = dict(headers)["Content-Disposition"]
    if disposition.endswith('.xlsx"'):
        assert disposition == f'attachment; filename="card_{card_id}.xlsx"'
        assert isinstance(body, (bytes, bytearray))
    else:
        assert disposition == f'attachment; filename="card_{card_id}.csv"'
        assert isinstance(body, str)


def test_sanitize_ru_position_rejects_azerbaijani_suffixes(app):
    assert app.sanitize_ru_position("кызы") is None
    assert app.sanitize_ru_position("оглы") is None
    assert app.sanitize_ru_position("Директор") == "Директор"


def test_infer_gender_handles_suffixes_and_first_name(app):
    assert app._infer_gender("Эльман кызы") == "Ж"
    assert app._infer_gender("Сергеевна") == "Ж"
    assert app._infer_gender("Александрович") == "М"
    assert app._infer_gender("Эльман", first_name_ru="Зульфия") == "Ж"
    assert app._infer_gender("") == ""


def test_normalize_en_org_reads_opf_from_prefix(app):
    en, _ = app.normalize_en_org("", "ООО ПЯТЕРОЧКА")
    assert en == "Pyaterochka LLC"



def test_score_org_relevance_prefers_public_company_over_small_llc(app):
    query = "ВТБ"
    llc_profile = {"ru_org": "ООО ВТБ", "inn": "9715498800", "revenue": 0}
    pjsc_profile = {"ru_org": "ПАО ВТБ", "inn": "7702070139", "revenue": 0}
    assert app._score_org_relevance(pjsc_profile, query) > app._score_org_relevance(llc_profile, query)



def test_apply_card_rules_normalizes_ru_org_opf_position(app):
    profile = {"ru_org": "ВТБ ООО", "inn": "7702070139", "en_org": "", "ru_position": ""}
    normalized, _ = app.apply_card_rules(profile, "company")
    assert normalized["ru_org"] == "ООО ВТБ"



def test_build_osint_profile_extracts_revenue_from_text(app):
    profile = app._build_osint_profile(
        url="https://example.com",
        source="zachestnyibiznes.ru",
        org_name="ПАО ВТБ",
        director="",
        position="",
        page_text="Выручка 1 234 567 890 руб.",
    )
    assert profile["revenue"] == 1234567890


def test_score_org_relevance_prefers_bank_over_subsidiary_for_short_brand(app):
    query = "ВТБ"
    subsidiary_profile = {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000}
    bank_profile = {"ru_org": "ПАО БАНК ВТБ", "inn": "7702070139", "revenue": 0}
    assert app._score_org_relevance(bank_profile, query) > app._score_org_relevance(subsidiary_profile, query)


def test_search_by_criteria_company_prefers_bank_candidate(app, monkeypatch):
    hits = [
        {
            "source": "zachestnyibiznes.ru",
            "type": "company",
            "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
        },
        {
            "source": "ФНС ЕГРЮЛ",
            "type": "company",
            "data": {"ru_org": "ПАО БАНК ВТБ", "inn": "7702070139", "revenue": 0},
        },
    ]

    monkeypatch.setattr(app, "_search_by_company", lambda *_a, **_k: (hits, ["company-search"]))

    _source_hits, candidates, _trace = app._search_by_criteria({"company": "ВТБ", "search_type": "company"})

    assert candidates
    assert candidates[0]["org_ru"] == "ПАО БАНК ВТБ"


def test_validate_leader_fio_candidate_rejects_noise_stems(app):
    accepted, reason = app._validate_leader_fio_candidate("Сведениям", "Ограничен", "Учредители")
    assert not accepted
    assert reason.startswith("token_matches_noise_stem")


def test_provider_cache_key_uses_version_prefix(app, monkeypatch):
    captured = {}

    def fake_get_cache(cache_key):
        captured["key"] = cache_key
        return []

    monkeypatch.setattr(app, "_get_cache", fake_get_cache)

    provider = {
        "name": "checko.ru",
        "kind": "checko",
        "supports_inn": True,
        "supports_name": True,
        "supports_url": True,
    }

    app._call_provider(provider, "ВТБ", "ORG_TEXT", no_cache=False, search_type="company", allow_fallback=False)

    assert captured["key"].startswith("provider:v2:")


def test_search_by_company_short_brand_expands_queries_and_prefers_bank(app, monkeypatch):
    calls = []

    leasing_hit = {
        "source": "zachestnyibiznes.ru",
        "type": "company",
        "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
    }
    bank_hit = {
        "source": "ФНС ЕГРЮЛ",
        "type": "company",
        "data": {"ru_org": "ПАО БАНК ВТБ", "inn": "7702070139", "revenue": 0},
    }

    def fake_search_external_sources(raw, no_cache=False, search_type="", provider_names=None):
        calls.append(raw)
        if raw == "ВТБ":
            return [leasing_hit], ["base"]
        if "Банк ВТБ" in raw:
            return [bank_hit], ["bank_variant"]
        return [], ["empty"]

    monkeypatch.setattr(app, "_search_external_sources", fake_search_external_sources)

    hits, _trace = app._search_by_company("ВТБ", search_type="company")

    assert hits
    assert hits[0]["data"]["ru_org"] == "ПАО БАНК ВТБ"
    assert any("Банк ВТБ" in query for query in calls)


def test_detect_input_type_bank_brand_is_org_text(app):
    assert app.detect_input_type("Банк ВТБ") == "ORG_TEXT"


def test_score_org_relevance_prefers_core_bank_over_union_structure(app):
    query = "ВТБ"
    union_profile = {"ru_org": "ППО БАНКА ВТБ (ПАО) МГО ПРГУ РФ", "inn": "7704259073", "revenue": 0}
    bank_profile = {"ru_org": "БАНК ВТБ (ПАО)", "inn": "7702070139", "revenue": 0}
    assert app._score_org_relevance(bank_profile, query) > app._score_org_relevance(union_profile, query)


def test_normalize_ru_org_bank_places_opf_at_end(app):
    normalized, _ = app.normalize_ru_org("БАНК ВТБ (ПАО)")
    assert normalized == "Банк ВТБ ПАО"


def test_autofill_review_fast_inn_company_skips_extended_search(app, monkeypatch):
    calls = []

    def fake_search_external_sources(raw, no_cache=False, search_type="", provider_names=None):
        calls.append(tuple(provider_names) if provider_names is not None else None)
        return (
            [
                {
                    "source": "ФНС ЕГРЮЛ",
                    "type": "company",
                    "data": {
                        "inn": "7702070139",
                        "ru_org": "БАНК ВТБ (ПАО)",
                        "en_org": "VTB Bank PJSC",
                        "revenue": 1_000_000,
                    },
                }
            ],
            ["fast"],
        )

    monkeypatch.setattr(app, "_search_external_sources", fake_search_external_sources)
    monkeypatch.setattr(app, "_create_autofill_card", lambda *_a, **_k: 77)

    body, status, _headers = app.autofill_review(
        {"company_name": ["7702070139"], "search_type": ["company"]},
        wants_json=True,
    )

    payload = json.loads(body)
    assert status == "200 OK"
    assert payload["ok"] is True
    assert payload["card_id"] == 77
    assert calls == [("ФНС ЕГРЮЛ",)]


def test_card_view_financial_lines_format(app):
    profile = {
        "ru_org": "Банк ВТБ ПАО",
        "en_org": "VTB Bank PJSC",
        "inn": "7702070139",
        "revenue": "1000000",
        "financial_year": "2024",
    }

    with app._connect() as db:
        cur = db.execute(
            "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
            (
                profile["ru_org"],
                profile["en_org"],
                "Найдено",
                "autofill",
                app._now(),
                app._now(),
                json.dumps({"profile": profile}, ensure_ascii=False),
            ),
        )
        card_id = cur.lastrowid
        db.commit()

    body, status, _headers = app.card_view(card_id)

    assert status == "200 OK"
    assert "Выручка:</b> 1 млн руб. (2024)" in body
    assert "Прибыль:</b> Данных нет (2024)" in body


def test_search_by_company_short_brand_stops_after_confident_bank_hit(app, monkeypatch):
    calls = []

    leasing_hit = {
        "source": "zachestnyibiznes.ru",
        "type": "company",
        "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
    }
    bank_hit = {
        "source": "ФНС ЕГРЮЛ",
        "type": "company",
        "data": {"ru_org": "БАНК ВТБ (ПАО)", "inn": "7702070139", "revenue": 1_000_000},
    }

    monkeypatch.setattr(
        app,
        "_generate_company_name_variants",
        lambda _name: ["ВТБ", "Банк ВТБ", "ПАО ВТБ", "АО ВТБ ЛИЗИНГ"],
    )

    def fake_search_external_sources(raw, no_cache=False, search_type="", provider_names=None):
        calls.append(raw)
        if raw == "ВТБ":
            return [leasing_hit], ["base"]
        if raw == "Банк ВТБ":
            return [bank_hit], ["bank"]
        return [], ["empty"]

    monkeypatch.setattr(app, "_search_external_sources", fake_search_external_sources)

    hits, _trace = app._search_by_company("ВТБ", search_type="company")

    assert hits
    assert hits[0]["data"]["ru_org"] == "БАНК ВТБ (ПАО)"
    assert "АО ВТБ ЛИЗИНГ" not in calls

def test_search_by_company_filters_person_hits_for_company_mode(app, monkeypatch):
    mixed_hits = [
        {
            "source": "zachestnyibiznes.ru",
            "type": "person",
            "data": {
                "surname_ru": "Иванов",
                "name_ru": "Иван",
                "ru_org": "БАНК ВТБ (ПАО)",
                "inn": "3662140164",
            },
        },
        {
            "source": "ФНС ЕГРЮЛ",
            "type": "company",
            "data": {
                "ru_org": "БАНК ВТБ (ПАО)",
                "inn": "7702070139",
                "en_org": "VTB Bank PJSC",
            },
        },
    ]

    monkeypatch.setattr(app, "_search_external_sources", lambda *_a, **_k: (mixed_hits, ["mixed"]))
    hits, _trace = app._search_by_company("ВТБ", search_type="company")

    assert hits
    assert all(hit.get("type") != "person" for hit in hits)
    assert hits[0]["data"]["inn"] == "7702070139"


def test_autofill_review_company_clears_mismatched_leader_by_inn(app, monkeypatch):
    captured = {}

    hits = [
        {
            "source": "ФНС ЕГРЮЛ",
            "type": "company",
            "data": {
                "ru_org": "БАНК ВТБ (ПАО)",
                "en_org": "VTB Bank PJSC",
                "inn": "7702070139",
            },
        },
        {
            "source": "zachestnyibiznes.ru",
            "type": "person",
            "data": {
                "surname_ru": "Нечаев",
                "name_ru": "Сергей",
                "middle_name_ru": "Юрьевич",
                "ru_position": "Председатель",
                "ru_org": "БАНК ВТБ (ПАО)",
                "inn": "3662140164",
            },
        },
    ]

    monkeypatch.setattr(app, "_search_by_company", lambda *_a, **_k: (hits, ["company-search"]))

    def fake_create(profile_data, notes, source_hits, search_trace, field_provenance):
        captured["profile"] = profile_data
        return 123

    monkeypatch.setattr(app, "_create_autofill_card", fake_create)

    body, status, _headers = app.autofill_review(
        {"company_name": ["ВТБ"], "search_type": ["company"], "hit_type": ["company"]},
        wants_json=True,
    )

    payload = json.loads(body)
    assert status == "200 OK"
    assert payload["ok"] is True
    assert payload["card_id"] == 123
    assert captured["profile"]["inn"] == "7702070139"
    assert captured["profile"].get("surname_ru", "") == ""
    assert captured["profile"].get("name_ru", "") == ""
