import csv
import io
import json
import sys
import types
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from native_app import NativeNadinApp, Image
from main import INTERNAL_MERGED_SOURCE


def test_sanitize_ru_position_noise(app):
    assert app.sanitize_ru_position("Юридического лица история греф герман оскарович проверить") is None


def test_export_csv_contains_all_profile_fields(app):
    profile = {
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
        "en_org": "SBERBANK PJSC",
    }
    with app._connect() as db:
        cur = db.execute(
            "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
            (
                "SBERBANK PJSC",
                "completed",
                "manual",
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
                {"source": "zachestnyibiznes.ru", "url": "u3", "data": {"inn": "7707083893", "ru_org": "ПАО СБЕРБАНК"}},
                {"source": "zachestnyibiznes.ru", "url": "u3", "data": {"inn": "7707083893", "ru_org": "ПАО СБЕРБАНК"}},
            ],
            ["extended"],
        )

    monkeypatch.setattr(app, "_build_profile_from_sources", fake_build)
    monkeypatch.setattr(app, "_search_external_sources", fake_search)
    monkeypatch.setattr(app, "apply_card_rules", lambda profile, card_type="": (profile, []))
    monkeypatch.setattr(app, "_missing_required_fields", lambda *_a, **_k: ["ru_org"])

    app.autofill_review({"company_name": ["7707083893"]}, wants_json=False)

    assert len(calls) == 1
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
            "en_org": ["Romashka LLC"],
            "search_type": ["company"],
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
            "type": "company",
            "type": "company",
            "data": {
                "ru_org": "Сбербанк ПАО",
                "ru_position": "Председатель правления",
                "gender": "М",
                "surname_ru": "",
                "name_ru": "",
                "name_ru": "",
        },
        },
        {
            "type": "company",
            "type": "company",
            "data": {
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
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
    assert profile["first_name"] != ""
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
    assert app.sanitize_ru_position("оглы") is None
    assert app.sanitize_ru_position("оглы") is None
    assert app.sanitize_ru_position("Директор") == "Директор"


def test_infer_gender_handles_suffixes_and_first_name(app):
    assert app._infer_gender("Сергеевна") == "Ж"
    assert app._infer_gender("Александрович") == "М"
    assert app._infer_gender("Александрович") == "М"
    assert app._infer_gender("Эльман", first_name_ru="Зульфия") == "Ж"
    assert app._infer_gender("") == ""


def test_normalize_en_org_reads_opf_from_prefix(app):
    en, _ = app.normalize_en_org("", "ООО ПЯТЕРОЧКА")
    assert en == "Pyaterochka LLC"




def test_normalize_en_org_known_mapping_keeps_legal_form(app):
    en, _ = app.normalize_en_org("", "ПАО СБЕРБАНК")
    assert en == "Sberbank PJSC"


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
            "type": "company",
            "type": "company",
            "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
        },
        {
            "type": "company",
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
        "kind": "checko",
        "kind": "checko",
        "supports_name": True,
        "supports_url": True,
        "supports_url": True,
    }

    app._call_provider(provider, "ВТБ", "ORG_TEXT", no_cache=False, search_type="company", allow_fallback=False)

    assert captured["key"].startswith("provider:v2:")


def test_search_by_company_short_brand_expands_queries_and_prefers_bank(app, monkeypatch):
    calls = []

    leasing_hit = {
        "type": "company",
        "type": "company",
        "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
    }
    bank_hit = {
        "type": "company",
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
                    "type": "company",
                    "type": "company",
                    "data": {
                        "ru_org": "БАНК ВТБ (ПАО)",
                        "en_org": "VTB Bank PJSC",
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
    assert calls == [None]


def test_card_view_financial_lines_format(app):
    profile = {
        "en_org": "VTB Bank PJSC",
        "inn": "7702070139",
        "revenue": "1000000",
        "financial_year": "2024",
        "financial_year": "2024",
    }

    with app._connect() as db:
        cur = db.execute(
            "INSERT INTO cards(ru_org,en_org,status,source,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?)",
            (
                profile["en_org"],
                profile["en_org"],
                "autofill",
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
    assert "Прибыль:</b> Данных нет (2024)" in body
    assert "Прибыль:</b> Данных нет (2024)" in body


def test_search_by_company_short_brand_stops_after_confident_bank_hit(app, monkeypatch):
    calls = []

    leasing_hit = {
        "type": "company",
        "type": "company",
        "data": {"ru_org": "АО ВТБ ЛИЗИНГ", "inn": "7709378229", "revenue": 9_000_000_000_000},
    }
    bank_hit = {
        "type": "company",
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
            "type": "person",
            "type": "person",
            "data": {
                "name_ru": "Иван",
                "ru_org": "БАНК ВТБ (ПАО)",
                "inn": "3662140164",
                "inn": "3662140164",
        },
        },
        {
            "type": "company",
            "type": "company",
            "data": {
                "inn": "7702070139",
                "en_org": "VTB Bank PJSC",
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
            "type": "company",
            "type": "company",
            "data": {
                "en_org": "VTB Bank PJSC",
                "inn": "7702070139",
                "inn": "7702070139",
        },
        },
        {
            "type": "person",
            "type": "person",
            "data": {
                "name_ru": "Сергей",
                "middle_name_ru": "Юрьевич",
                "ru_position": "Председатель",
                "ru_org": "БАНК ВТБ (ПАО)",
                "inn": "3662140164",
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
    assert captured["profile"].get("name_ru", "") == "Андрей"
    assert captured["profile"].get("name_ru", "") == "Андрей"

def test_scrapy_pipeline_merge_available_and_extracts_leader(app):
    hits = [
        {
            "type": "company",
            "type": "company",
            "data": {
                "inn": "7702070139",
                "ru_position": "Президент-председатель правления",
                "ru_position": "Президент-председатель правления",
        },
        },
        {
            "type": "company",
            "type": "company",
            "data": {
                "inn": "7702070139",
                "surname_ru": "Костин",
                "name_ru": "Андрей",
                "middle_name_ru": "Леонидович",
                "middle_name_ru": "Леонидович",
        },
        },
    ]

    merged = app._merge_hits_with_scrapy_pipeline(hits)

    assert merged
    assert merged["leader_surname_ru"] == "Костин"
    assert merged["leader_name_ru"] == "Андрей"
    assert merged["leader_name_ru"] == "Андрей"


def test_search_external_sources_adds_scrapy_merged_hit(app, monkeypatch):
    provider = {
        "kind": "dummy",
        "kind": "dummy",
        "supports_name": True,
        "supports_url": True,
        "is_person_source": True,
        "is_person_source": True,
    }

    monkeypatch.setattr(app, "_provider_chain", lambda *_a, **_k: [provider])
    monkeypatch.setattr(app, "_should_call_provider", lambda *_a, **_k: True)
    monkeypatch.setattr(app, "_can_stop_provider_search", lambda *_a, **_k: False)
    monkeypatch.setattr(
        app,
        "_call_provider",
        lambda *_a, **_k: {
            "inn": "7702070139",
            "type": "company",
            "type": "company",
        },
    )
    monkeypatch.setattr(
        app,
        "_merge_hits_with_scrapy_pipeline",
        lambda _hits: {
            "en_org": "VTB Bank PJSC",
            "company_inn": "7702070139",
            "leader_surname_ru": "Костин",
            "leader_name_ru": "Андрей",
            "leader_middle_ru": "Леонидович",
            "leader_position_ru": "Президент",
            "leader_position_ru": "Президент",
        },
    )

    hits, trace = app._search_external_sources("ВТБ", no_cache=True, search_type="company", provider_names=["dummy-provider"])

    merged_hits = [hit for hit in hits if hit.get("source") == INTERNAL_MERGED_SOURCE]
    assert merged_hits
    assert merged_hits[0]["data"]["surname_ru"] == "Костин"
    assert not any("Scrapy pipeline" in line for line in trace)

def test_pick_best_leader_fio_prefers_target_inn(app):
    hits = [
        {
            "source": "companies.rbc.ru",
            "data": {
                "surname_ru": "Муха",
                "name_ru": "Антон",
                "middle_name_ru": "Юрьевич",
                "middle_name_ru": "Юрьевич",
        },
        },
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "surname_ru": "Костин",
                "name_ru": "Андрей",
                "middle_name_ru": "Леонидович",
                "middle_name_ru": "Леонидович",
        },
        },
    ]

    surname, name, middle, source = app._pick_best_leader_fio(hits, target_inn="7702070139")

    assert surname == "Костин"
    assert name == "Андрей"
    assert middle == "Леонидович"
    assert source == "zachestnyibiznes.ru"


def test_has_valid_leader_for_inn_checks_inn_match(app):
    hits = [
        {
            "source": "companies.rbc.ru",
            "data": {
                "surname_ru": "Муха",
                "name_ru": "Антон",
                "name_ru": "Антон",
        },
        },
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "surname_ru": "Костин",
                "name_ru": "Андрей",
                "name_ru": "Андрей",
        },
        },
    ]

    assert app._has_valid_leader_for_inn(hits, "3662140164") is True
    assert app._has_valid_leader_for_inn(hits, "3662140164") is True
    assert app._has_valid_leader_for_inn(hits, "7728168971") is False


def test_has_confident_short_brand_bank_hit_rejects_non_bank_entity(app):
    hits = [
        {
            "type": "company",
            "type": "company",
            "data": {
                "inn": "3662140164",
                "inn": "3662140164",
            },
        }
    ]

    assert app._has_confident_short_brand_bank_hit(hits, "ВТБ") is False


def test_has_confident_short_brand_bank_hit_accepts_bank_title_without_inn(app):
    hits = [
        {
            "type": "company",
            "type": "company",
            "data": {
                "inn": "",
                "inn": "",
            },
        }
    ]

    assert app._has_confident_short_brand_bank_hit(hits, "ВТБ") is True


def test_extract_fio_from_leader_obj_reads_svfl_attributes(app):
    leader_obj = {
            "@attributes": {
            "@attributes": {
                "Имя": "АНДРЕЙ",
                "Отчество": "ЛЕОНИДОВИЧ",
                "Отчество": "ЛЕОНИДОВИЧ",
        }
    }
    }

    surname, name, middle = app._extract_fio_from_leader_obj(leader_obj)
    assert surname == "КОСТИН"
    assert name == "АНДРЕЙ"
    assert middle == "ЛЕОНИДОВИЧ"


def test_search_by_company_short_brand_keeps_searching_after_non_bank_base_hit(app, monkeypatch):
    calls = []

    base_non_bank_hit = {
        "type": "company",
        "type": "company",
        "data": {"ru_org": "ООО ВТБ", "inn": "3662140164", "revenue": 0},
    }
    bank_hit = {
        "type": "company",
        "type": "company",
        "data": {"ru_org": "БАНК ВТБ (ПАО)", "inn": "7702070139", "revenue": 1_000_000},
    }

    monkeypatch.setattr(
        app,
        "_generate_company_name_variants",
        lambda _name: ["ВТБ", "Банк ВТБ", "ПАО ВТБ"],
    )

    def fake_search_external_sources(raw, no_cache=False, search_type="", provider_names=None):
        calls.append(raw)
        if raw == "ВТБ":
            return [base_non_bank_hit], ["base"]
        if raw == "Банк ВТБ":
            return [bank_hit], ["bank"]
        return [], ["empty"]

    monkeypatch.setattr(app, "_search_external_sources", fake_search_external_sources)

    hits, _trace = app._search_by_company("ВТБ", search_type="company")

    assert hits
    assert hits[0]["data"]["inn"] == "7702070139"
    assert "Банк ВТБ" in calls


def test_detect_input_type_single_token_company_query(app):
    query = "Пятерочка"
    assert app.detect_input_type(query) == "ORG_TEXT"


def test_detect_input_type_three_part_name_query(app):
    query = "Греф Герман Оскарович"
    assert app.detect_input_type(query) == "PERSON_TEXT"


def test_fetch_page_rusprofile_uses_cloudscraper_fallback(app, monkeypatch):
    calls = []

    def fake_basic(url, timeout=15, max_retries=5, block_host_on_block=True):
        calls.append((url, block_host_on_block))
        return None

    monkeypatch.setattr(app, '_fetch_page_basic', fake_basic)
    monkeypatch.setattr(app, '_fetch_rusprofile_page_with_cloudscraper', lambda url, timeout=20: '<html>cloud</html>')
    monkeypatch.setattr(app, '_fetch_page_with_headless_browser', lambda *_a, **_k: (_ for _ in ()).throw(AssertionError('browser fallback should not run')))

    html = app._fetch_page('https://www.rusprofile.ru/id/362378')

    assert html == '<html>cloud</html>'
    assert calls == [('https://www.rusprofile.ru/id/362378', False)]


def test_fetch_page_rusprofile_skips_browser_without_flag(app, monkeypatch):
    monkeypatch.setattr(app, '_fetch_rusprofile_page_with_cloudscraper', lambda *_a, **_k: None)
    monkeypatch.setattr(app, '_fetch_rusprofile_page_with_cloudscraper', lambda *_a, **_k: None)
    monkeypatch.setattr(app, '_fetch_page_with_headless_browser', lambda *_a, **_k: (_ for _ in ()).throw(AssertionError('browser fallback should not run')))

    html = app._fetch_page('https://www.rusprofile.ru/search?query=vtb')

    assert html is None


def test_extract_rusprofile_search_hits_with_selector_company(app):
    html = '<html><body><div class="result"><a href="/id/362378">Company Example</a><span>INN 7704217370</span></div></body></html>'

    hits = app._extract_rusprofile_search_hits_with_selector(html, search_type='company')

    assert hits
    assert hits[0]['inn'] == '7704217370'
    assert hits[0]['org'] == 'Company Example'
    assert hits[0]['org'] == 'Company Example'


class _NativeEngineStub:
    @staticmethod
    def _normalize_spaces(value: str) -> str:
        return " ".join(str(value or "").split())


def _make_native_app() -> NativeNadinApp:
    app = NativeNadinApp.__new__(NativeNadinApp)
    app.engine = _NativeEngineStub()
    app._last_profile_inn = ""
    app._last_profile_ogrn = ""
    app._last_profile_org = ""
    app._manual_proxy = ""
    app._auto_proxy = ""
    app._rusprofile_url_cache = {}
    return app


def test_native_app_extract_source_url_prefers_rusprofile_detail():
    app = _make_native_app()
    payload = {
        "source_hits": [
            {"url": "https://egrul.nalog.ru/index.html?query=7702070139", "source": "\u0424\u041d\u0421 \u0415\u0413\u0420\u042e\u041b"},
            {"url": "https://www.rusprofile.ru/id/362378", "source": "rusprofile.ru"},
        ]
    }

    best = app._extract_source_url(payload)

    assert best == "https://www.rusprofile.ru/id/362378"


def test_native_app_resolve_rusprofile_source_url_by_inn_lookup(monkeypatch):
    app = _make_native_app()
    app._last_profile_inn = "7702070139"
    monkeypatch.setattr(app, "_lookup_rusprofile_url", lambda query: "https://www.rusprofile.ru/id/362378")

    resolved = app._resolve_rusprofile_source_url("https://egrul.nalog.ru/index.html?query=7702070139")

    assert resolved == "https://www.rusprofile.ru/id/362378"


def test_native_app_normalize_source_url_for_screenshot_rejects_local_pdf_path():
    app = _make_native_app()

    normalized = app._normalize_source_url_for_screenshot(r"C:\Users\Admin\Downloads\sample.pdf")

    assert normalized == ""


def test_native_app_resolve_rusprofile_source_url_with_empty_source(monkeypatch):
    app = _make_native_app()
    app._last_profile_inn = "7702070139"
    monkeypatch.setattr(app, "_lookup_rusprofile_url", lambda query: "https://www.rusprofile.ru/id/362378")

    resolved = app._resolve_rusprofile_source_url("")

    assert resolved == "https://www.rusprofile.ru/id/362378"


def test_native_app_humanize_trace_line_blocked():
    app = _make_native_app()

    line = app._humanize_trace_line("trace: checko.ru - provider_blocked_403")

    assert line == "• Источник: checko.ru — временно недоступен"


def test_native_app_humanize_trace_line_empty():
    app = _make_native_app()

    line = app._humanize_trace_line("trace: focus.kontur.ru - provider_called_empty")

    assert line == "• Источник: focus.kontur.ru — данных не найдено"


def test_native_app_compose_card_rows_exists_and_formats_sources():
    app = _make_native_app()

    rows = app._compose_card_rows(
        {"ru_org": "\u0411\u0430\u043d\u043a \u0412\u0422\u0411 \u041f\u0410\u041e", "inn": "7702070139"},
        status="\u041d\u0430\u0439\u0434\u0435\u043d\u043e",
        source_names=["\u0424\u041d\u0421 \u0415\u0413\u0420\u042e\u041b", "Scrapy Merge", "\u0424\u041d\u0421 \u0415\u0413\u0420\u042e\u041b"],
        revenue_line="\u0414\u0430\u043d\u043d\u044b\u0445 \u043d\u0435\u0442 (2025)",
        profit_line="\u0414\u0430\u043d\u043d\u044b\u0445 \u043d\u0435\u0442 (2025)",
    )

    assert ("\u0421\u0442\u0430\u0442\u0443\u0441", "\u041d\u0435 \u0443\u043a\u0430\u0437\u0430\u043d") in rows
    assert ("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438", "\u0424\u041d\u0421 \u0415\u0413\u0420\u042e\u041b") in rows


def test_native_app_merge_profile_with_source_hits_does_not_generate_middle_name_en():
    app = _make_native_app()
    app.engine.normalize_en_org = lambda _en, ru: ("VTB Bank", "")
    app.engine._generate_en_position = lambda value: f"EN:{value}"
    app.engine._translit = lambda value: f"TR:{value}"

    merged = app._merge_profile_with_source_hits(
        {"ru_org": "\u0411\u0430\u043d\u043a \u0412\u0422\u0411 \u041f\u0410\u041e", "surname_ru": "\u041a\u041e\u0421\u0422\u0418\u041d", "name_ru": "\u0410\u041d\u0414\u0420\u0415\u0419", "middle_name_ru": "\u041b\u0415\u041e\u041d\u0418\u0414\u041e\u0412\u0418\u0427"},
        [{"data": {"ru_position": "\u041f\u0440\u0435\u0434\u0441\u0435\u0434\u0430\u0442\u0435\u043b\u044c \u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f", "inn": "7702070139"}}],
    )

    assert merged["family_name"] == "TR:\u041a\u041e\u0421\u0422\u0418\u041d"
    assert merged["middle_name_en"] == ""
    assert merged["en_position"] == "EN:\u041f\u0440\u0435\u0434\u0441\u0435\u0434\u0430\u0442\u0435\u043b\u044c \u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f"
    assert merged["family_name"] == "TR:\u041a\u041e\u0421\u0422\u0418\u041d"
    assert merged["middle_name_en"] == ""


def test_native_app_lookup_rusprofile_url_falls_back_to_duckduckgo():
    app = _make_native_app()
    calls = []

    def fake_fetch_page(url, timeout=0, max_retries=0):
        calls.append(url)
        if "duckduckgo.com" in url:
            return '<a href="https://www.rusprofile.ru/id/362378">VTB</a>'
        return "<html>blocked</html>"

    app.engine._fetch_page = fake_fetch_page

    resolved = app._lookup_rusprofile_url("7702070139")

    assert resolved == "https://www.rusprofile.ru/id/362378"
    assert any("duckduckgo.com" in url for url in calls)


def test_native_app_sanitize_rusprofile_detail_url_strips_tracking():
    app = _make_native_app()

    resolved = app._sanitize_rusprofile_detail_url(
        "https://www.rusprofile.ru/id/76374&amp;rut=c49c7792244ac20cc16663d2c8ccf65e4faae0f2ed411438b9ca719105795553"
    )

    assert resolved == "https://www.rusprofile.ru/id/76374"


def test_native_app_sanitize_rusprofile_detail_url_does_not_rewrite_external_id_url():
    app = _make_native_app()

    resolved = app._sanitize_rusprofile_detail_url(
        "https://companies.rbc.ru/id/1027739609391-pao-bank-vtb/"
    )

    assert resolved == ""


def test_native_app_sanitize_rusprofile_detail_url_rejects_ogrn_shaped_id():
    app = _make_native_app()

    resolved = app._sanitize_rusprofile_detail_url("https://www.rusprofile.ru/id/1027739609391")

    assert resolved == ""


def test_native_app_lookup_rusprofile_url_ignores_cached_search_url():
    app = _make_native_app()
    app._rusprofile_url_cache["7702070139"] = "https://www.rusprofile.ru/search?query=7702070139"
    calls = []

    def fake_fetch_page(url, timeout=0, max_retries=0):
        calls.append(url)
        if "duckduckgo.com" in url:
            return '<a href="https://www.rusprofile.ru/id/362378&amp;rut=test">VTB</a>'
        return "<html>blocked</html>"

    app.engine._fetch_page = fake_fetch_page

    resolved = app._lookup_rusprofile_url("7702070139")

    assert resolved == "https://www.rusprofile.ru/id/362378"
    assert any("duckduckgo.com" in url for url in calls)


def test_apply_card_rules_person_in_company_normalizes_fio_and_position(app):
    profile = {
        "middle_name_ru": "ЛЕОНИДОВИЧ",
        "ru_position": "ПРЕЗИДЕНТ, ПРЕДСЕДАТЕЛЬ ПРАВЛЕНИЯ",
        "middle_name_en": "Leonidovich",
        "middle_name_ru": "ЛЕОНИДОВИЧ",
        "ru_position": "ПРЕЗИДЕНТ, ПРЕДСЕДАТЕЛЬ ПРАВЛЕНИЯ",
        "middle_name_en": "Leonidovich",
    }

    normalized, _ = app.apply_card_rules(profile, "person_in_company")

    assert normalized["middle_name_en"] == ""
    assert normalized["en_position"] == "President, Chairman of the Board"
    assert normalized["middle_name_ru"] == "Леонидович"
    assert normalized["middle_name_en"] == ""
    assert normalized["en_position"] == "President, Chairman of the Board"


def test_format_financial_line_uses_available_historical_year(app):
    assert app._format_financial_line("300 тыс. руб. (2021)", 2025) == "300 тыс. руб. (2021)"


def test_native_app_compose_card_rows_for_inactive_company_hides_optional_fields():
    app = _make_native_app()
    app.engine._is_inactive_company_status = lambda value: value == "Ликвидирована"

    rows = app._compose_card_rows(
        {
            "company_status": "Ликвидирована",
            "surname_ru": "Иванов",
            "name_ru": "Иван",
            "ru_position": "Директор",
            "surname_ru": "Иванов",
            "name_ru": "Иван",
            "ru_position": "Директор",
        },
        status="Найдено",
        source_names=["ФНС ЕГРЮЛ"],
        revenue_line="10 тыс. руб. (2023)",
        profit_line="5 тыс. руб. (2023)",
    )

    assert rows == [
        ("Источники", "ФНС ЕГРЮЛ"),
        ("Организация", "АО РУСАГРОТРАНС"),
        ("Статус", "Ликвидирована"),
        ("Источники", "ФНС ЕГРЮЛ"),
    ]


def test_native_app_parse_rusprofile_dom_html_extracts_company_summary_and_status():
    app = _make_native_app()
    app.engine._clean_ru_org_name = lambda value: " ".join(str(value).split())
    app.engine._extract_rusprofile_company_summary = lambda _soup, _page_text: (
        "ФКП «Щелковский биокомбинат» действует с 1993 года."
    )
    app.engine._select_first_text = lambda _soup, _selectors: "Организация ликвидирована"
    app.engine._normalize_company_status_label = lambda value: "Ликвидирована" if "ликвид" in str(value).lower() else ""
    app.engine._extract_revenue_from_soup = lambda _soup: 123000
    app.engine._is_inactive_company_status = lambda value: value == "Ликвидирована"
    app.engine.normalize_en_org = lambda _en, ru: ("Rusagrotrans JSC" if ru else "", "")
    app.engine._generate_en_position = lambda value: f"EN:{value}"
    app.engine._normalize_position_ru = lambda value: " ".join(str(value).split())
    app.engine._split_fio_ru = lambda value: tuple(str(value).split()[:3])
    app.engine._infer_gender = lambda *_args, **_kwargs: "М"

    html = """
    <html>
      <head><title>АО РУСАГРОТРАНС | Rusprofile</title></head>
      <body>
        <h1>АО РУСАГРОТРАНС</h1>
        <div class="liquidation">Организация ликвидирована</div>
        <div>ИНН/КПП 7701810253 771801001</div>
        <div>ОГРН 5087746484140</div>
      </body>
    </html>
    """

    profile = app._parse_rusprofile_dom_html("https://www.rusprofile.ru/id/123945", html)

    assert profile["company_status"] == "Ликвидирована"
    assert profile["inn"] == "7701810253"
    assert profile["ogrn"] == "5087746484140"
    assert profile["en_org"] == "Rusagrotrans JSC"
    assert profile["company_summary"].startswith("ФКП")
    assert profile["revenue"] == 123000
    assert profile["en_org"] == "Rusagrotrans JSC"
    assert profile.get("ru_position", "") == ""
    assert profile.get("surname_ru", "") == ""


def test_native_app_parse_rusprofile_dom_html_ignores_404_page():
    app = _make_native_app()

    html = """
    <html>
      <head><title>404 \u0421\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 | Rusprofile</title></head>
      <body>
        <div>404</div>
        <div>\u0421\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430!</div>
        <div>\u041f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u043f\u0440\u0430\u0432\u0438\u043b\u044c\u043d\u043e\u0441\u0442\u044c URL \u0438\u043b\u0438 \u043f\u0435\u0440\u0435\u0439\u0434\u0438\u0442\u0435 \u043d\u0430 \u0413\u043b\u0430\u0432\u043d\u0443\u044e \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0443.</div>
      </body>
    </html>
    """

    profile = app._parse_rusprofile_dom_html("https://www.rusprofile.ru/id/1027739609391", html)

    assert profile == {}


def test_native_app_truncate_company_summary_text_keeps_expanded_block_and_cuts_tail():
    app = _make_native_app()

    text = (
        "ПАО Сбербанк представляет собой системообразующий финансовый институт. "
        "По организации найдено более 3219058 изменений. Исторические сведения"
        "Учредители Лицензии Связи Выводы "
        "ПАО Сбербанк представляет собой системообразующий финансовый институт. "
        "По организации найдено более 3219058 изменений. Исторические сведения"
    )

    summary = app._truncate_company_summary_text(text)

    assert "Учредители" in summary
    assert "Выводы ПАО Сбербанк представляет собой системообразующий финансовый институт." in summary
    assert "По организации найдено более" not in summary



def test_native_app_parse_rusprofile_dom_html_prefers_current_org_summary():
    app = _make_native_app()
    app.engine._clean_ru_org_name = lambda value: " ".join(str(value).split())
    app.engine._normalize_company_status_label = lambda value: "Действующая" if "действ" in str(value).lower() else ""
    app.engine._is_inactive_company_status = lambda value: False
    app.engine.normalize_en_org = lambda _en, ru: ("Sberbank PJSC" if "СБЕРБАНК" in str(ru).upper() else "", "")

    html = """
    <html>
      <head><title>ПАО СБЕРБАНК | Rusprofile</title></head>
      <body>
        <h1>ПАО СБЕРБАНК</h1>
        <section>
          <div>Главное о компании за 1 минуту</div>
          <div>АО "Статус" действует с 1997 года и специализируется на ведении реестров владельцев ценных бумаг. Показать</div>
        </section>
        <section>
          <div>Главное о компании за 1 минуту</div>
          <div>
            ПАО Сбербанк, основанное в 1991 году, является крупнейшим финансовым институтом страны.
            Учредители Лицензии Связи Финансовая устойчивость
            Выводы ПАО Сбербанк представляет собой системообразующий финансовый институт.
            По организации найдено более 3219058 изменений. Исторические сведения
          </div>
        </section>
      </body>
    </html>
    """

    profile = app._parse_rusprofile_dom_html("https://www.rusprofile.ru/id/2770356", html)

    assert "Сбербанк" in profile["company_summary"]
    assert 'АО "Статус"' not in profile["company_summary"]
    assert "Выводы" in profile["company_summary"]
    assert "По организации найдено более" not in profile["company_summary"]


def test_native_app_open_screenshot_viewer_uses_label_image_path(tmp_path, monkeypatch):
    app = _make_native_app()
    image_path = tmp_path / "preview.png"
    image_path.write_bytes(b"fake")
    app._last_screenshot_path = ""
    app.screenshot_preview_label = type("LabelStub", (), {"_image_path": str(image_path)})()
    app.screenshot_meta_var = type("VarStub", (), {"get": lambda self: "meta"})()
    called = {}
    monkeypatch.setattr(app, "_open_image_viewer", lambda path, meta: called.update({"path": path, "meta": meta}))

    result = app._open_screenshot_viewer()

    assert result == "break"
    assert called == {"path": str(image_path), "meta": "meta"}

def test_native_app_detect_screenshot_page_state_flags_zoominfo_challenge():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "www.zoominfo.com",
        "Cloudflare security check. Verify you are human before continuing.",
        "https://www.zoominfo.com/p/Henry-Schuck/1260398587",
    )

    assert state["blocked"] is True
    assert "cloudflare" in state["reason"] or "security check" in state["reason"]



def test_native_app_detect_screenshot_page_state_flags_linkedin_overlay():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "Henry Schuck | LinkedIn",
        "Sign in to view full profile. Continue with Google.",
        "https://www.linkedin.com/in/hschuck",
    )

    assert state["blocked"] is False
    assert state["overlay"] is True



def test_native_app_detect_screenshot_page_state_flags_network_changed():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "This site can't be reached",
        "Connection interrupted. ERR_NETWORK_CHANGED",
        "https://www.linkedin.com/in/hschuck",
    )

    assert state["blocked"] is True
    assert "err_network_changed" in state["reason"] or "connection interrupted" in state["reason"]



def test_native_app_detect_screenshot_page_state_flags_blank_page_for_problem_hosts():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "",
        "",
        "https://www.zoominfo.com/p/Henry-Schuck/1260398587",
    )

    assert state["blocked"] is True
    assert state["reason"] == "blank_page"


def test_native_app_detect_screenshot_page_state_flags_linkedin_join_wall():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "LinkedIn",
        "Присоединяйтесь к LinkedIn, чтобы посмотреть профиль.",
        "https://www.linkedin.com/in/hschuck",
    )

    assert state["overlay"] is True


def test_native_app_detect_screenshot_page_state_flags_russian_security_check():
    app = _make_native_app()

    state = app._detect_screenshot_page_state(
        "Проверка безопасности",
        "Выполнение проверки безопасности. Подтвердите, что вы не робот.",
        "https://example.com/report",
    )

    assert state["blocked"] is True
    assert state["reason"] == "выполнение проверки безопасности"



def test_native_app_build_screenshot_cleanup_script_contains_human_russian_labels():
    app = _make_native_app()

    script = app._build_screenshot_cleanup_script("https://example.com/report")

    for needle in ["войти", "регистрация", "присоединиться", "Отклонить", "закрыть", "не сейчас"]:
        assert needle in script
    assert "?" * 4 not in script


def test_native_app_should_use_single_frame_capture_for_card_and_interactive_hosts():
    app = _make_native_app()

    assert app._should_use_single_frame_capture("https://www.rusprofile.ru/id/2835629", "card") is True
    assert app._should_use_single_frame_capture("https://www.linkedin.com/in/hschuck", "manual") is True
    assert app._should_use_single_frame_capture("https://example.com/report", "manual") is False


def test_native_app_capture_with_headless_browser_skips_cli_for_interactive_host(monkeypatch, tmp_path):
    app = _make_native_app()

    monkeypatch.setattr(app, "_build_browser_proxy_arg", lambda allow_auto_proxy=False: "")
    monkeypatch.setattr(app, "_capture_with_controlled_browser", lambda *args, **kwargs: (False, "blocked", False))

    cli_calls = {"count": 0}

    def fake_cli(*args, **kwargs):
        cli_calls["count"] += 1
        return True, "", False

    monkeypatch.setattr(app, "_capture_with_headless_browser_cli", fake_cli)

    ok, details, used_desktop = app._capture_with_headless_browser(
        Path("chrome.exe"),
        "https://www.linkedin.com/in/hschuck",
        tmp_path / "shot.png",
        single_frame=True,
    )

    assert ok is False
    assert used_desktop is False
    assert cli_calls["count"] == 0
    assert "blocked" in details




def test_native_app_capture_with_controlled_browser_initializes_runtime_values(monkeypatch, tmp_path):
    app = _make_native_app()
    browser_path = tmp_path / "chrome.exe"
    browser_path.write_text("", encoding="utf-8")
    output_path = tmp_path / "shot.png"
    captured_command = {}

    class _FakeProcess:
        pid = 12345

        def terminate(self):
            return None

        def wait(self, timeout=None):
            return None

        def kill(self):
            return None

    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace())
    monkeypatch.setattr(app, "_reserve_debug_port", lambda: 9222)
    monkeypatch.setattr(app, "_normalize_screenshot_target", lambda url: url)
    monkeypatch.setattr(app, "_get_browser_user_agent", lambda: "UnitTest-UA")
    monkeypatch.setattr(app, "_build_browser_proxy_arg", lambda allow_auto_proxy=False: "")
    monkeypatch.setattr(app, "_warmup_screenshot_session", lambda url: (url, [], "status=200"))
    monkeypatch.setattr(app, "_build_screenshot_cleanup_script", lambda _url: "true")
    monkeypatch.setattr(app, "_is_interactive_screenshot_host", lambda _url: False)
    monkeypatch.setattr(app, "_wait_for_cdp_target", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("cdp_unavailable")))

    def fake_popen(command, *args, **kwargs):
        captured_command["value"] = list(command)
        return _FakeProcess()

    monkeypatch.setattr("native_app.subprocess.Popen", fake_popen)

    ok, details, used_desktop = app._capture_with_controlled_browser(
        browser_path,
        "https://example.com/report",
        output_path,
        headless=True,
    )

    assert ok is False
    assert used_desktop is False
    assert "cdp_unavailable" in details
    assert "--user-agent=UnitTest-UA" in captured_command["value"]



def test_native_app_on_source_screenshot_done_error_still_updates_company_summary(monkeypatch):
    app = _make_native_app()
    app._busy = False
    app._current_card_id = 7
    app._last_screenshot_path = ""
    app._last_source_names = []
    app._last_rusprofile_url = ""
    app._screenshot_preview_image = None

    class _VarStub:
        def __init__(self):
            self.value = ""

        def set(self, value):
            self.value = value

    class _WidgetStub:
        def __init__(self):
            self.calls = []

        def configure(self, **kwargs):
            self.calls.append(kwargs)

    app.screenshot_meta_var = _VarStub()
    app.source_url_var = _VarStub()
    app.status_var = _VarStub()
    app.screenshot_preview_label = _WidgetStub()
    app.copy_screenshot_button = _WidgetStub()
    app.download_screenshot_button = _WidgetStub()
    app.progress = type("ProgressStub", (), {"stop": lambda self: None})()

    summary_calls = []
    enrichment_calls = []
    monkeypatch.setattr(app, "_sanitize_rusprofile_detail_url", lambda value: value if "rusprofile.ru/id/" in value else "")
    monkeypatch.setattr(app, "_update_company_summary", lambda value: summary_calls.append(value))
    monkeypatch.setattr(app, "_schedule_rusprofile_enrichment", lambda card_id, value: enrichment_calls.append((card_id, value)))

    app._on_source_screenshot_done(
        "",
        "",
        "",
        "https://www.rusprofile.ru/id/2835629",
        "chrome_failed",
        True,
    )

    assert app._last_rusprofile_url == "https://www.rusprofile.ru/id/2835629"
    assert summary_calls == ["https://www.rusprofile.ru/id/2835629"]
    assert enrichment_calls == [(7, "https://www.rusprofile.ru/id/2835629")]

def test_native_app_capture_source_screenshot_waits_for_rusprofile_before_auto_capture(monkeypatch):
    app = _make_native_app()
    app._busy = False
    app._screenshot_busy = False
    app._current_card_id = 9
    app._last_source_url = "https://companies.rbc.ru/id/1027700132195-pao-publichnoe-aktsionernoe-obschestvo-sberbank-rossii/"
    app._last_rusprofile_url = ""
    app._last_screenshot_path = ""
    app._screenshot_preview_image = object()
    app._card_enrichment_inflight = set()

    class _VarStub:
        def __init__(self):
            self.value = ""

        def set(self, value):
            self.value = value

    class _WidgetStub:
        def __init__(self):
            self.calls = []

        def configure(self, **kwargs):
            self.calls.append(kwargs)

    app.screenshot_meta_var = _VarStub()
    app.source_url_var = _VarStub()
    app.status_var = _VarStub()
    app.screenshot_preview_label = _WidgetStub()
    app.copy_screenshot_button = _WidgetStub()
    app.download_screenshot_button = _WidgetStub()
    app.progress = type(
        "ProgressStub",
        (),
        {
            "start": lambda self, *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("progress should not start")),
            "stop": lambda self: None,
        },
    )()

    enrichment_calls = []
    monkeypatch.setattr(app, "_normalize_screenshot_target", lambda value: value)
    monkeypatch.setattr(app, "_sanitize_rusprofile_detail_url", lambda value: value if "rusprofile.ru/id/" in value else "")
    monkeypatch.setattr(app, "_resolve_rusprofile_source_url", lambda _value: (_ for _ in ()).throw(AssertionError("should not resolve immediately")))
    monkeypatch.setattr(app, "_schedule_rusprofile_enrichment", lambda card_id, value: enrichment_calls.append((card_id, value)))

    app._capture_source_screenshot(auto=True)

    assert app._screenshot_busy is False
    assert app.screenshot_meta_var.value == "Скриншот: ждем RusProfile..."
    assert app.source_url_var.value == "URL источника: ищем страницу RusProfile..."
    assert app.status_var.value == "Ищем страницу RusProfile для скриншота..."
    assert app.screenshot_preview_label.calls[-1] == {"image": "", "text": "Создается превью..."}
    assert enrichment_calls == [(9, app._last_source_url)]


def test_native_app_get_proxy_settings_skips_auto_proxy_by_default(monkeypatch):
    app = _make_native_app()
    calls = {"count": 0}

    monkeypatch.setenv("NADIN_PROXY", "")
    monkeypatch.setattr(app, "_detect_windows_proxy", lambda: "")

    def fake_fetch() -> str:
        calls["count"] += 1
        return "1.2.3.4:8080"

    monkeypatch.setattr(app, "_fetch_free_proxy_from_2ip", fake_fetch)

    proxy_type, proxy_url = app._get_proxy_settings()

    assert proxy_type == "http"
    assert proxy_url == ""
    assert calls["count"] == 0



def test_native_app_get_proxy_settings_uses_auto_proxy_on_demand(monkeypatch):
    app = _make_native_app()
    calls = {"count": 0}

    monkeypatch.setenv("NADIN_PROXY", "")
    monkeypatch.setattr(app, "_detect_windows_proxy", lambda: "")

    def fake_fetch() -> str:
        calls["count"] += 1
        return "1.2.3.4:8080"

    monkeypatch.setattr(app, "_fetch_free_proxy_from_2ip", fake_fetch)

    proxy_type, proxy_url = app._get_proxy_settings(allow_auto_proxy=True)
    again_type, again_url = app._get_proxy_settings(allow_auto_proxy=True)

    assert proxy_type == "http"
    assert proxy_url == "1.2.3.4:8080"
    assert again_type == "http"
    assert again_url == "1.2.3.4:8080"
    assert calls["count"] == 1



def test_native_app_save_captured_screenshot_frames_stitches_segments(tmp_path):
    if Image is None:
        return

    app = _make_native_app()
    frames = []
    for color in ("#d64141", "#3b82f6"):
        image = Image.new("RGB", (120, 80), color)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        image.close()
        frames.append(buffer.getvalue())

    output = tmp_path / "stitched.png"
    app._save_captured_screenshot_frames(frames, output, overlap=10)

    assert output.exists()
    with Image.open(output) as stitched:
        assert stitched.size[0] == 120
        assert stitched.size[1] > 140


def test_native_app_parse_rusprofile_dom_html_ignores_placeholder_leader_text():
    app = _make_native_app()
    app.engine._clean_ru_org_name = lambda value: " ".join(str(value).split())
    app.engine._normalize_company_status_label = lambda value: "Действующая" if "действ" in str(value).lower() else ""
    app.engine._is_inactive_company_status = lambda value: False
    app.engine.normalize_en_org = lambda _en, ru: (ru, "")

    html = """
    <html>
      <head><title>JSC STATUS | Rusprofile</title></head>
      <body>
        <h1>JSC STATUS</h1>
        <div>Статус компании Действующая</div>
        <div>Руководитель: Данные По Руководителю</div>
      </body>
    </html>
    """

    profile = app._parse_rusprofile_dom_html("https://www.rusprofile.ru/id/555", html)

    assert profile.get("surname_ru", "") == ""
    assert profile.get("name_ru", "") == ""


def test_native_app_extract_rusprofile_detail_url_from_html_prefers_matching_company_context():
    app = _make_native_app()
    app._last_profile_inn = "7707083893"
    app._last_profile_org = "SBERBANK"

    html = """
    <html>
      <body>
        <a href="/id/2770356">STATUS REGISTRAR</a>
        <div>INN 7707179242</div>
        <a href="/id/2835629">SBERBANK</a>
        <div>INN 7707083893</div>
      </body>
    </html>
    """

    resolved = app._extract_rusprofile_detail_url_from_html(html, query="7707083893")

    assert resolved == "https://www.rusprofile.ru/id/2835629"


def test_native_app_merge_rusprofile_profile_replaces_less_relevant_summary():
    app = _make_native_app()
    current_profile = {
        "ru_org": "SBERBANK",
        "company_summary": "STATUS registrar has operated since 1997 and maintains securities registers.",
    }
    rusprofile_profile = {
        "ru_org": "SBERBANK",
        "company_summary": "SBERBANK is a systemically important financial institution. The public summary notes wide regional presence.",
    }

    merged = app._merge_rusprofile_profile(current_profile, rusprofile_profile)

    assert "SBERBANK" in merged["company_summary"]
    assert "STATUS registrar" not in merged["company_summary"]


def test_native_app_extract_company_summary_candidates_from_text_handles_expanded_block():
    app = _make_native_app()

    text = (
        "Главное о компании за 1 минуту "
        "ПАО Сбербанк является крупнейшим финансовым институтом. "
        "Финансовая устойчивость "
        "Юридическая активность "
        "Выводы ПАО Сбербанк остается системообразующим банком. "
        "По организации найдено более 10 изменений"
    )

    candidates = app._extract_company_summary_candidates_from_text(text)

    assert candidates
    assert "Выводы" in candidates[0]
    assert "По организации найдено более" not in candidates[0]


def test_native_app_fetch_rusprofile_full_info_prefers_browser_expanded_summary(monkeypatch):
    app = _make_native_app()
    app.engine._clean_ru_org_name = lambda value: " ".join(str(value).split())
    app.engine._normalize_company_status_label = lambda value: ""
    app.engine._is_inactive_company_status = lambda value: False
    app._last_profile_org = "ПАО СБЕРБАНК"

    collapsed_html = """
    <html>
      <head><title>ПАО СБЕРБАНК | Rusprofile</title></head>
      <body>
        <h1>ПАО СБЕРБАНК</h1>
        <div>Главное о компании за 1 минуту ПАО Сбербанк является крупнейшим банком. Показать</div>
        <div>ОГРН 1027700132195 ИНН/КПП 7707083893 773601001</div>
      </body>
    </html>
    """
    expanded_summary = (
        "ПАО Сбербанк является крупнейшим финансовым институтом. "
        "Финансовая устойчивость "
        "Юридическая активность "
        "Выводы ПАО Сбербанк остается системообразующим банком."
    )

    monkeypatch.setattr(app, "_fetch_rusprofile_html_loose", lambda url: collapsed_html)
    monkeypatch.setattr(app, "_fetch_dom_with_headless_browser", lambda url: collapsed_html)
    monkeypatch.setattr(app, "_fetch_rusprofile_expanded_summary_with_browser", lambda url: expanded_summary)

    profile = app._fetch_rusprofile_full_info("https://www.rusprofile.ru/id/2835629")

    assert "Выводы" in profile["company_summary"]
    assert "ОГРН" not in profile["company_summary"]
    assert profile["ru_org"] == "ПАО СБЕРБАНК"



def test_native_app_finalize_company_summary_text_rejects_collapsed_intro():
    app = _make_native_app()

    raw_text = (
        "Главное о компании за 1 минуту "
        "ПАО Сбербанк является крупнейшим банком. Показать"
    )

    assert app._finalize_company_summary_text(raw_text, "ПАО СБЕРБАНК") == ""



def test_native_app_finalize_company_summary_text_keeps_full_expanded_modal():
    app = _make_native_app()

    raw_text = (
        "Главное о компании за 1 минуту "
        "ПАО Сбербанк, основанное в 1991 году, является крупнейшим банком России. "
        "Учредители Лицензии Связи "
        "Финансовая устойчивость "
        "Финансовые показатели компании за последние годы в предоставленных данных отсутствуют. "
        "Юридическая активность "
        "Юридическая активность ПАО Сбербанк исключительно высока. "
        "Надежность Риски неисполнения обязательств: Незначительные Признаки однодневки: Отсутствуют Налоговые риски: Незначительные Подробнее "
        "Выводы ПАО Сбербанк представляет собой системообразующий финансовый институт. "
        "По организации найдено более 3219058 изменений. Исторические сведения"
    )

    summary = app._finalize_company_summary_text(raw_text, "ПАО СБЕРБАНК")

    assert "Финансовая устойчивость" in summary
    assert "Юридическая активность" in summary
    assert "Надежность" in summary
    assert "Выводы" in summary
    assert "По организации найдено более" not in summary

def test_native_app_parse_company_summary_sections_splits_titles_links_and_reliability_body():
    app = _make_native_app()

    summary = (
        "ПАО Сбербанк, основанное в 1991 году, является крупнейшим банком России. "
        "Учредители Лицензии Связи "
        "Финансовая устойчивость "
        "Финансовые показатели компании за последние годы отсутствуют. "
        "Юридическая активность "
        "Юридическая активность ПАО Сбербанк исключительно высока. "
        "Арбитраж Суды общей юрисдикции Исполнительные производства "
        "Надежность "
        "Риски неисполнения обязательств: Незначительные Признаки однодневки: Отсутствуют Налоговые риски: Незначительные Подробнее "
        "Выводы ПАО Сбербанк представляет собой системообразующий финансовый институт."
    )

    sections = app._parse_company_summary_sections(summary)

    assert [section["title"] for section in sections] == [
        "Главное о компании за 1 минуту",
        "Финансовая устойчивость",
        "Юридическая активность",
        "Надежность",
        "Выводы",
    ]
    assert sections[0]["links"] == ["Учредители", "Лицензии", "Связи"]
    assert sections[2]["links"] == ["Арбитраж", "Суды общей юрисдикции", "Исполнительные производства"]
    assert "\nПризнаки однодневки:" in sections[3]["body"]
    assert sections[3]["links"] == ["Подробнее"]
def test_native_app_render_company_summary_sections_skips_service_links():
    app = _make_native_app()

    class _TextStub:
        def __init__(self):
            self.parts = []

        def delete(self, *_args, **_kwargs):
            self.parts.clear()

        def insert(self, _index, text, _tags=()):
            self.parts.append(text)

        def dump(self):
            return "".join(self.parts)

    app.company_summary_text = _TextStub()
    app._configure_company_summary_text_tags = lambda: None

    app._render_company_summary_sections([
        {
            "title": "Intro",
            "body": "First paragraph.",
            "links": ["LINK_ALPHA", "LINK_BETA"],
        },
        {
            "title": "Conclusions",
            "body": "Final paragraph.",
            "links": ["LINK_GAMMA"],
        },
    ])

    rendered = app.company_summary_text.dump()

    assert "First paragraph." in rendered
    assert "Final paragraph." in rendered
    assert "LINK_ALPHA" not in rendered
    assert "LINK_BETA" not in rendered
    assert "LINK_GAMMA" not in rendered


def test_native_app_get_screenshot_viewer_geometry_prefers_large_window():
    app = _make_native_app()
    app.winfo_screenwidth = lambda: 1920
    app.winfo_screenheight = lambda: 1080

    geometry, min_width, min_height = app._get_screenshot_viewer_geometry()

    assert geometry.startswith("1804x993+")
    assert min_width >= 980
    assert min_height >= 640

def test_native_app_should_prefer_real_desktop_capture_for_card_and_interactive_hosts():
    app = _make_native_app()

    assert app._should_prefer_real_desktop_capture("https://www.rusprofile.ru/id/2835629", "card") is True
    assert app._should_prefer_real_desktop_capture("https://www.linkedin.com/in/hschuck", "manual") is True
    assert app._should_prefer_real_desktop_capture("https://example.com/page", "manual") is False


def test_native_app_capture_webpage_screenshot_skips_annotation_for_desktop_capture(tmp_path, monkeypatch):
    app = _make_native_app()
    app._screenshot_dir = tmp_path
    browser_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    calls = {"annotate": 0, "taskbar": 0}

    monkeypatch.setattr(app, "_find_headless_browser", lambda: browser_path)

    def _fake_capture(_browser, _source_url, output_path, **_kwargs):
        output_path.write_bytes(b"desktop")
        return True, "", True

    monkeypatch.setattr(app, "_capture_with_headless_browser", _fake_capture)
    monkeypatch.setattr(
        app,
        "_annotate_screenshot_metadata",
        lambda *_args, **_kwargs: calls.__setitem__("annotate", calls["annotate"] + 1),
    )
    monkeypatch.setattr(
        app,
        "_append_synthetic_windows_taskbar",
        lambda *_args, **_kwargs: calls.__setitem__("taskbar", calls["taskbar"] + 1),
    )

    saved_path, captured_at = app._capture_webpage_screenshot(
        "https://www.rusprofile.ru/id/2835629",
        capture_context="card",
    )

    assert saved_path.exists()
    assert captured_at
    assert calls["annotate"] == 0
    assert calls["taskbar"] == 0


def test_native_app_real_desktop_screenshot_prefers_window_capture_and_redacts_taskbar(tmp_path, monkeypatch):
    if Image is None:
        return

    app = _make_native_app()
    output_path = tmp_path / "desktop.png"
    calls = {"activate": []}

    monkeypatch.setattr(app, "_find_browser_window_for_pid", lambda _pid: 321)

    def _fake_prepare(_hwnd, **kwargs):
        calls["activate"].append(kwargs.get("activate"))
        return True

    monkeypatch.setattr(app, "_prepare_browser_window_for_capture", _fake_prepare)
    monkeypatch.setattr(app, "_capture_browser_window_image", lambda _hwnd: (Image.new("RGB", (1280, 720), "white"), ""))
    monkeypatch.setattr(app, "_capture_is_mostly_black", lambda _image: False)
    monkeypatch.setattr(app, "_capture_windows_taskbar_strip", lambda: (Image.new("RGB", (1280, 40), (63, 63, 70)), ""))
    monkeypatch.setattr(app, "_capture_primary_screen_image", lambda: (_ for _ in ()).throw(AssertionError("screen fallback should not run")))

    ok, details = app._capture_real_desktop_browser_screenshot(123, output_path)

    assert ok is True
    assert details == ""
    assert output_path.exists()
    assert calls["activate"] == [False]

    with Image.open(output_path).convert("RGB") as result:
        assert result.size == (1280, 760)
        redacted_pixel = result.getpixel((640, 740))
        bottom_pixel = result.getpixel((640, 759))
        assert redacted_pixel == (63, 63, 70)
        assert bottom_pixel == (63, 63, 70)


def test_native_app_real_desktop_screenshot_falls_back_to_screen_capture_on_black_window(tmp_path, monkeypatch):
    if Image is None:
        return

    app = _make_native_app()
    output_path = tmp_path / "desktop_fallback.png"
    calls = {"activate": []}

    monkeypatch.setattr(app, "_find_browser_window_for_pid", lambda _pid: 654)

    def _fake_prepare(_hwnd, **kwargs):
        calls["activate"].append(kwargs.get("activate"))
        return True

    monkeypatch.setattr(app, "_prepare_browser_window_for_capture", _fake_prepare)
    monkeypatch.setattr(app, "_capture_browser_window_image", lambda _hwnd: (Image.new("RGB", (1280, 720), "black"), ""))
    monkeypatch.setattr(app, "_capture_is_mostly_black", lambda _image: True)
    monkeypatch.setattr(app, "_capture_primary_screen_image", lambda: (Image.new("RGB", (1280, 760), "white"), ""))

    ok, details = app._capture_real_desktop_browser_screenshot(321, output_path)

    assert ok is False
    assert details == "window_capture_black"
    assert not output_path.exists()
    assert calls["activate"] == [False]


def test_native_app_real_desktop_screenshot_retries_hidden_capture_before_error(tmp_path, monkeypatch):
    if Image is None:
        return

    app = _make_native_app()
    output_path = tmp_path / "desktop_retry.png"
    calls = {"activate": [], "captures": 0}

    monkeypatch.setattr(app, "_find_browser_window_for_pid", lambda _pid: 777)

    def _fake_prepare(_hwnd, **kwargs):
        calls["activate"].append(kwargs.get("activate"))
        return True

    def _fake_capture(_hwnd):
        calls["captures"] += 1
        if calls["captures"] == 1:
            return Image.new("RGB", (1280, 720), "black"), ""
        return Image.new("RGB", (1280, 720), "white"), ""

    monkeypatch.setattr(app, "_prepare_browser_window_for_capture", _fake_prepare)
    monkeypatch.setattr(app, "_capture_browser_window_image", _fake_capture)
    monkeypatch.setattr(app, "_capture_is_mostly_black", lambda image: image.getpixel((0, 0)) == (0, 0, 0))
    monkeypatch.setattr(app, "_capture_windows_taskbar_strip", lambda: (Image.new("RGB", (1280, 40), (63, 63, 70)), ""))
    monkeypatch.setattr(app, "_capture_primary_screen_image", lambda: (_ for _ in ()).throw(AssertionError("screen fallback should not run")))

    ok, details = app._capture_real_desktop_browser_screenshot(777, output_path)

    assert ok is True
    assert details == ""
    assert output_path.exists()
    assert calls["activate"] == [False]
    assert calls["captures"] == 2


def test_native_app_real_desktop_screenshot_visible_fallback_can_be_enabled(tmp_path, monkeypatch):
    if Image is None:
        return

    app = _make_native_app()
    output_path = tmp_path / "desktop_fallback_visible.png"
    calls = {"activate": []}

    monkeypatch.setenv("NADIN_SCREENSHOT_VISIBLE_FALLBACK", "1")
    monkeypatch.setattr(app, "_find_browser_window_for_pid", lambda _pid: 654)

    def _fake_prepare(_hwnd, **kwargs):
        calls["activate"].append(kwargs.get("activate"))
        return True

    monkeypatch.setattr(app, "_prepare_browser_window_for_capture", _fake_prepare)
    monkeypatch.setattr(app, "_capture_browser_window_image", lambda _hwnd: (Image.new("RGB", (1280, 720), "black"), ""))
    monkeypatch.setattr(app, "_capture_is_mostly_black", lambda _image: True)
    monkeypatch.setattr(app, "_capture_primary_screen_image", lambda: (Image.new("RGB", (1280, 760), "white"), ""))

    ok, details = app._capture_real_desktop_browser_screenshot(321, output_path)

    assert ok is True
    assert details == ""
    assert output_path.exists()
    assert calls["activate"] == [False, True]
