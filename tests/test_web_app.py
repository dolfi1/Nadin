from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse
import web_app
from web_app import CompanyWebApp


class FakeResponse:
    def __init__(self, text: str = "", json_data=None, ok: bool = True, headers=None):
        self.text = text
        self._json_data = json_data or {}
        self.ok = ok
        self.headers = headers or {"content-type": "application/json"}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("http error")


def test_detect_input_type_routes_inn_url_person_and_org(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    assert app.detect_input_type("7702070139") == web_app.INPUT_TYPE_INN
    assert app.detect_input_type("https://www.rusprofile.ru/id/1027739609391") == web_app.INPUT_TYPE_URL
    assert app.detect_input_type("Греф") == web_app.INPUT_TYPE_PERSON_TEXT
    assert app.detect_input_type("Греф Герман Оскарович") == web_app.INPUT_TYPE_PERSON_TEXT


def test_parse_egrul_maps_real_fields(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_get(url, **_kwargs):
        assert url == "https://egrul.itsoft.ru/7702070139.json"
        return FakeResponse(
            json_data={
                "inn": "7702070139",
                "ogrn": "1027739609391",
                "name": "Организация Банк ВТБ ПАО",
                "en_org": "VTB Bank PJSC",
                "director": {
                    "surname": "Костин",
                    "name": "Андрей",
                    "patronymic": "Леонидович",
                    "gender": "мужской",
                    "position": "Президент, Председатель правления",
                },
            }
        )

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._parse_egrul("7702070139")
    assert data is not None
    assert data["ru_org"] == "Банк ВТБ ПАО"
    assert data["surname_ru"] == "Костин"
    assert data["gender"] == "М"


def test_search_external_sources_uses_cache_between_calls(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    calls = {"count": 0}

    def fake_call_provider(provider, raw, input_type, *_args, **_kwargs):
        calls["count"] += 1
        return {"url": "http://example", "ru_org": f"org-{provider['kind']}"}

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    first_hits, first_trace = app._search_external_sources("7702070139", no_cache=False)
    second_hits, second_trace = app._search_external_sources("7702070139", no_cache=False)

    assert first_hits
    assert second_hits
    expected = sum(
        1
        for provider in app._provider_chain(web_app.INPUT_TYPE_INN, "7702070139")
        if app._should_call_provider(provider, web_app.INPUT_TYPE_INN)
    )
    assert calls["count"] == expected * 2
    assert any("hits_by_provider:" in line for line in first_trace)


def test_parse_rusprofile_person_text_extracts_fio(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_get(url, **_kwargs):
        if "search?query=" in url:
            return FakeResponse(
                '<div class="search-result__item">'
                '<a class="search-result__title-link" href="/id/1027700132195">ПАО Сбербанк</a>'
                '<a href="/person/gref-go-770303580308">Греф Герман Оскарович</a>'
                '</div>'
                + "x" * 1200
            )
        return FakeResponse('<h1>Греф Герман Оскарович</h1><a href="/id/1027700132195">ПАО Сбербанк</a>' + "x" * 1200)

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._parse_rusprofile("Греф")
    assert isinstance(data, dict)
    assert data


def test_parse_rusprofile_url_input_uses_detail_page_directly(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    called = {"urls": []}

    def fake_get(url, **_kwargs):
        called["urls"].append(url)
        return FakeResponse("<h1>ПАО Сбербанк</h1>" + "x" * 1200)

    monkeypatch.setattr(web_app.requests, "get", fake_get)
    raw_url = "https://www.rusprofile.ru/id/1027700132195"

    data = app._parse_rusprofile(raw_url)

    assert data is not None
    assert data["ru_org"] == "ПАО Сбербанк"
    assert called["urls"] == [raw_url]


def test_parse_rusprofile_person_page_extracts_name_from_h1(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_get(_url, **_kwargs):
        return FakeResponse(
            "<html><body>"
            "<h1>Греф Герман Оскарович</h1>"
            "<div class='person-main-info-position'>президент, председатель правления</div>"
            "<a href='/id/1027700132195'>ПАО Сбербанк</a>"
            "ИНН: 7707083893"
            "</body></html>"
        )

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._parse_rusprofile("https://www.rusprofile.ru/person/gref-go-770303580308")

    assert data["surname_ru"] == "Греф"
    assert data["name_ru"] == "Герман"
    assert data["middle_name_ru"] == "Оскарович"
    assert data["ru_org"] == "ПАО Сбербанк"


def test_autofill_review_creates_card_when_required_fields_present(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(app, "_search_external_sources", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(
        app,
        "_build_profile_from_sources",
        lambda *_args, **_kwargs: (
            {
                "title": "",
                "appeal": "Г-н",
                "family_name": "Gref",
                "first_name": "German",
                "middle_name_en": "Oskarovich",
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "gender": "М",
                "inn": "7707083893",
                "ru_org": "ПАО Сбербанк",
                "en_org": "Sberbank PJSC",
                "ru_position": "Президент",
                "position": "President",
                "en_position": "President",
            },
            {},
        ),
    )

    _body, status, headers = app.autofill_review({"company_name": ["7707083893"]})

    assert status == "302 Found"
    location = dict(headers)["Location"]
    assert location.startswith("/card/")

    with app._connect() as db:
        saved = db.execute("SELECT * FROM cards ORDER BY id DESC LIMIT 1").fetchone()

    assert saved is not None
    payload = json.loads(saved["data_json"])
    assert payload["profile"]["ru_org"] == "Сбербанк ПАО"
    assert payload["profile"]["inn"] == "7707083893"






def test_autofill_review_uses_source_hits_when_profile_builder_misses_fields(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(
        app,
        "_search_external_sources",
        lambda *_args, **_kwargs: (
            [
                {
                    "source": "ФНС ЕГРЮЛ",
                    "type": "company",
                    "data": {
                        "surname_ru": "Иванов",
                        "name_ru": "Иван",
                        "ru_org": "ООО Ромашка",
                        "en_org": "Romashka LLC",
                    },
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        app,
        "_build_profile_from_sources",
        lambda *_args, **_kwargs: ({field: "" for field, _ in web_app.CARD_FIELDS}, {}),
    )

    _body, status, headers = app.autofill_review({"company_name": ["ООО Ромашка"]})

    assert status == "302 Found"
    location = dict(headers)["Location"]
    assert location.startswith("/create/manual")


def test_build_profile_from_sources_fills_en_org_from_regular_sources(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [
        {
            "source": "rusprofile.ru",
            "type": "company",
            "data": {
                "ru_org": "ООО Ромашка",
                "en_org": "Romashka LLC",
                "en_position": "General Director",
                "surname_ru": "Иванов",
                "name_ru": "Иван",
            },
        }
    ]

    profile, sources = app._build_profile_from_sources(hits, "ООО Ромашка", web_app.INPUT_TYPE_ORG_TEXT)

    assert profile["en_org"] == "Romashka LLC"
    assert sources["en_org"] == "rusprofile.ru"


def test_parse_rusprofile_company_page_extracts_leader_position(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_fetch_page(*_args, **_kwargs):
        return (
            "<html><body>"
            "<h1>ПАО Сбербанк</h1>"
            "ИНН 7707083893"
            "<div>Руководитель ПРЕЗИДЕНТ, ПРЕДСЕДАТЕЛЬ ПРАВЛЕНИЯ Греф Герман Оскарович</div>"
            "</body></html>"
        )

    monkeypatch.setattr(app, "_fetch_page", fake_fetch_page)

    data = app._parse_rusprofile("https://www.rusprofile.ru/id/1027700132195")

    assert data["surname_ru"] == "Греф"
    assert data["name_ru"] == "Герман"
    assert data["middle_name_ru"] == "Оскарович"
    assert data["ru_position"] == "Президент, Председатель Правления"



def test_parse_rusprofile_company_page_extracts_from_meta_keywords_and_chief_title(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_fetch_page(*_args, **_kwargs):
        return (
            "<html><head>"
            '<meta name="keywords" content="ПАО Сбербанк, ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО &quot;СБЕРБАНК РОССИИ&quot;, Греф Герман Оскарович, ИНН 7707083893">'
            "</head><body>"
            "<div class='company-info__item'><span class='chief-title'>ПРЕЗИДЕНТ, ПРЕДСЕДАТЕЛЬ ПРАВЛЕНИЯ</span></div>"
            "</body></html>"
        )

    monkeypatch.setattr(app, "_fetch_page", fake_fetch_page)

    data = app._parse_rusprofile("https://www.rusprofile.ru/id/1027700132195")

    assert data["ru_org"] == "ПАО Сбербанк"
    assert data["surname_ru"] == "Греф"
    assert data["name_ru"] == "Герман"
    assert data["middle_name_ru"] == "Оскарович"
    assert data["inn"] == "7707083893"
    assert data["ru_position"] == "Президент, Председатель Правления"

def test_build_person_candidates_groups_fio_and_org(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [
        {"source": "rusprofile.ru", "data": {"surname_ru": "Греф", "name_ru": "Герман", "middle_name_ru": "Оскарович", "ru_org": "ПАО Сбербанк", "ru_position": "Президент"}},
        {"source": "list-org.com", "data": {"surname_ru": "Греф", "name_ru": "Герман", "middle_name_ru": "Оскарович", "ru_org": "ПАО Сбербанк", "ru_position": "Президент"}},
        {"source": "list-org.com", "data": {"surname_ru": "Греф", "name_ru": "Владимир", "middle_name_ru": "Иванович", "ru_org": "КХ \"Греф\""}},
    ]

    candidates = app._build_person_candidates(hits, "Греф")

    assert len(candidates) == 2
    assert any(c["fio_ru"] == "Греф Герман Оскарович" and c["org_ru"] == "ПАО Сбербанк" for c in candidates)
    assert any(c["fio_ru"] == "Греф Владимир Иванович" for c in candidates)


def test_search_external_sources_accepts_multi_hit_provider(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_call_provider(provider, raw, input_type, *_args, **_kwargs):
        if provider["kind"] == "rusprofile":
            return [
                {"url": "http://example/1", "surname_ru": "Греф", "name_ru": "Герман"},
                {"url": "http://example/2", "surname_ru": "Греф", "name_ru": "Кристина"},
            ]
        return None

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    hits, _ = app._search_external_sources("Греф", no_cache=True)
    rus_hits = [h for h in hits if h["source"] == "rusprofile.ru"]
    assert len(rus_hits) == 2


def test_build_profile_prefers_egrul_for_inn_person_data(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [
        {"source": "rusprofile.ru", "data": {"ru_org": "АО Статус", "surname_ru": "Иванов", "name_ru": "Иван"}},
        {"source": "ФНС ЕГРЮЛ", "data": {"ru_org": "ПАО Сбербанк", "surname_ru": "Греф", "name_ru": "Герман", "middle_name_ru": "Оскарович"}},
    ]

    profile, _ = app._build_profile_from_sources(hits, "7707083893", web_app.INPUT_TYPE_INN)
    assert profile["ru_org"] == "Сбербанк ПАО"
    assert profile["surname_ru"] == "Греф"


def test_search_page_shows_external_candidate_for_inn(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(app, "_search_external_sources", lambda *_args, **_kwargs: ([
        {"source": "ФНС ЕГРЮЛ", "data": {"inn": "7707083893", "ru_org": "ПАО Сбербанк", "surname_ru": "Греф", "name_ru": "Герман"}}
    ], []))

    body, status, _ = app.search_page({"q": ["7707083893"]})
    assert status == "200 OK"
    assert "Автозаполнить" in body
    assert "Сбербанк" in body


def test_score_hit_boosts_exact_fio_brand_and_inn(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    hit = {
        "source": "ФНС ЕГРЮЛ",
        "data": {
            "surname_ru": "Греф",
            "name_ru": "Герман",
            "middle_name_ru": "Оскарович",
            "ru_org": "ПАО Сбербанк",
            "ru_position": "Президент, Председатель правления",
            "inn": "7707083893",
            "revenue": 1200000,
        },
    }
    score = app._score_hit(hit, "7707083893")
    assert score >= 180


def test_german_gref_prioritizes_sber(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    hits = [
        {
            "source": "list-org.com",
            "data": {
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "КФХ Греф",
                "ru_position": "Директор",
                "revenue": 500,
            },
        },
        {
            "source": "rusprofile.ru",
            "data": {
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "ПАО Сбербанк",
                "ru_position": "Президент, Председатель правления",
                "inn": "7707083893",
                "revenue": 2000000,
            },
        },
    ]

    candidates = app._build_person_candidates(hits, "Герман Греф")
    assert candidates[0]["fio_ru"] == "Греф Герман Оскарович"
    assert candidates[0]["org_ru"] == "ПАО Сбербанк"


def test_person_search_case_variants_always_refetch(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    calls = {"count": 0}

    def fake_call_provider(provider, raw, input_type, *_args, **_kwargs):
        calls["count"] += 1
        return {"url": "http://example", "ru_org": "org"}

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    app._search_external_sources("Греф", no_cache=False)
    app._search_external_sources("греф", no_cache=False)

    expected = sum(1 for provider in app._provider_chain(web_app.INPUT_TYPE_PERSON_TEXT, "Греф") if app._should_call_provider(provider, web_app.INPUT_TYPE_PERSON_TEXT))
    assert calls["count"] == expected * 2


def test_score_hit_reverse_exact_name_boost(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    sber_hit = {
        "source": "rusprofile.ru",
        "data": {
            "surname_ru": "Греф",
            "name_ru": "Герман",
            "middle_name_ru": "Оскарович",
            "ru_org": "ПАО Сбербанк",
            "ru_position": "Президент",
            "revenue": 5200000,
        },
    }
    noise_hit = {
        "source": "list-org.com",
        "data": {
            "surname_ru": "Юнусов",
            "name_ru": "Герман",
            "middle_name_ru": "Петрович",
            "ru_org": "ООО Гермес",
            "ru_position": "Директор",
            "revenue": 0,
        },
    }

    assert app._score_hit(sber_hit, "Герман Греф") > app._score_hit(noise_hit, "Герман Греф")


def test_call_provider_uses_fallback_on_blocking_error(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    provider = {"name": "list-org.com", "kind": "list_org", "supports_name": True, "supports_inn": True}
    fallback_provider = {"name": "focus.kontur.ru", "kind": "kontur", "supports_name": True, "supports_inn": True}

    monkeypatch.setattr(app, "_get_fallback_providers", lambda *_args, **_kwargs: [fallback_provider])

    def fake_fetch(current_provider, *_args, **_kwargs):
        if current_provider["name"] == "list-org.com":
            raise RuntimeError("timeout 10060")
        return {"url": "https://example.org", "ru_org": "ПАО Сбербанк"}

    monkeypatch.setattr(app, "_fetch_from_provider", fake_fetch)

    result = app._call_provider(provider, "7707083893", web_app.INPUT_TYPE_INN)
    assert isinstance(result, list)
    assert result[0]["ru_org"] == "ПАО Сбербанк"


def test_parse_rusprofile_fallback_structure_extracts_name(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(
        app,
        "_fetch_page",
        lambda *_args, **_kwargs: '<div class="person-name">Греф Герман Оскарович</div>',
    )

    data = app._parse_rusprofile("https://www.rusprofile.ru/person/gref-go-770303580308")
    assert data["surname_ru"] == "Греф"
    assert data["name_ru"] == "Герман"


def test_parse_egrul_supports_legacy_payload_fields(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_get(url, **_kwargs):
        assert url == "https://egrul.itsoft.ru/7707083893.json"
        return FakeResponse(
            json_data={
                "НаимСокр": "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО СБЕРБАНК",
                "ИННЮЛ": "7707083893",
                "СведДолжнФЛ": [
                    {
                        "ФИО": "Греф Герман Оскарович",
                        "Должность": "Президент",
                    }
                ],
                "ФинПоказ": {"Выручка": "5 200 000"},
            }
        )

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._parse_egrul("7707083893")
    assert data is not None
    assert data["inn"] == "7707083893"
    assert data["ru_org"] == "Сбербанк ПАО"
    assert data
    assert data["revenue"] == 5200000


def test_parse_rusprofile_person_skips_noise_position(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(
        app,
        "_fetch_page",
        lambda *_args, **_kwargs: "<h1>Греф Герман Оскарович</h1><div>Факторы риска</div><div>Дисквалификация</div>",
    )

    data = app._parse_rusprofile("https://www.rusprofile.ru/person/gref-go-770303580308")

    assert data["surname_ru"] == "Греф"
    assert data.get("ru_position", "") == ""


def test_build_profile_generates_position_and_middle_name_en(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [{
        "source": "rusprofile.ru",
        "data": {
            "surname_ru": "Греф",
            "name_ru": "Герман",
            "middle_name_ru": "Оскарович",
            "ru_position": "Президент, Председатель правления",
            "ru_org": "ПАО Сбербанк",
        },
    }]

    profile, _sources = app._build_profile_from_sources(hits, "Греф Герман", web_app.INPUT_TYPE_PERSON_TEXT)

    assert profile["position"] == "President, Chairman Of The Board"
    assert profile["middle_name_en"] == "Oskarovich"


def test_search_page_autodetects_person_mode_when_fio_present(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    captured = {}

    def fake_search(params):
        captured.update(params)
        return [], [], []

    monkeypatch.setattr(app, "_search_by_criteria", fake_search)

    _body, status, _headers = app.search_page({"surname": ["Греф"], "search_type": [""]})

    assert status == "200 OK"
    assert captured["search_type"] == "person"


def test_search_by_company_uses_normalized_name_first(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    provider = {"name": "rusprofile.ru", "kind": "rusprofile", "supports_name": True, "supports_inn": True}
    queries = []

    monkeypatch.setattr(app, "normalize_ru_org", lambda company: ("ПАО Сбербанк", ""))
    monkeypatch.setattr(app, "_provider_chain", lambda *_args, **_kwargs: [provider])
    monkeypatch.setattr(app, "_should_call_provider", lambda *_args, **_kwargs: True)

    def fake_call(_provider, query, *_args, **_kwargs):
        queries.append(query)
        return [{"ru_org": "ПАО Сбербанк", "type": "company"}]

    monkeypatch.setattr(app, "_call_provider_with_retry", fake_call)

    hits, trace = app._search_by_company("Сбербанк")

    assert hits
    assert queries[0] == "ПАО Сбербанк"
    assert any("Нормализовано: ПАО Сбербанк" in item for item in trace)


def test_build_person_candidates_keeps_partial_matches(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [{
        "source": "rusprofile.ru",
        "data": {
            "surname_ru": "Иванов",
            "name_ru": "Петр",
            "middle_name_ru": "Сергеевич",
            "ru_org": "ООО Ромашка",
        },
    }]

    candidates = app._build_person_candidates(hits, "Греф")

    assert len(candidates) == 1
    assert candidates[0]["fio_ru"] == "Иванов Петр Сергеевич"


def test_search_rusprofile_uses_extended_timeout_and_retries(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    captured = {}

    def fake_fetch(url, timeout=0, max_retries=0):
        captured["url"] = url
        captured["timeout"] = timeout
        captured["max_retries"] = max_retries
        return "<html></html>"

    monkeypatch.setattr(app, "_fetch_page", fake_fetch)

    app._search_rusprofile("Сбербанк", search_type="company")

    assert "rusprofile.ru/search" in captured["url"]
    assert captured["timeout"] == 15
    assert captured["max_retries"] == 2


def test_fetch_page_detects_captcha_by_short_content(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(app, "_domain_throttle", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(app, "_is_localhost", lambda *_args, **_kwargs: True)

    def fake_get(*_args, **_kwargs):
        return FakeResponse(text="короткий ответ", ok=True)

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._fetch_page("https://www.rusprofile.ru/search?query=test", timeout=1, max_retries=1)

    assert data is None


def test_is_person_query_recognizes_bank_as_company():
    assert web_app.is_person_query("Сбербанк") is False


def test_split_fio_handles_empty_value(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    assert app._split_fio_ru("") == ("", "", "")


def test_collect_rusprofile_profiles_falls_back_to_direct_inn_urls(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(app, "_search_rusprofile", lambda *_args, **_kwargs: [])

    def fake_parse(url):
        return {"url": url, "ru_org": "ПАО Сбербанк"} if "/id/" in url else None

    monkeypatch.setattr(app, "_parse_rusprofile", fake_parse)

    result = app._collect_rusprofile_profiles("7707083893", web_app.INPUT_TYPE_INN)

    assert result is not None
    assert result["url"].endswith("/id/7707083893")


def test_autofill_confirm_creates_company_card_without_person_fields(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    body, status, headers = app.autofill_confirm(
        {
            "action": ["create"],
            "ru_org": ["ПАО Сбербанк"],
            "en_org": ["Sberbank PJSC"],
            "input_value": ["7707083893"],
            "profile_surname_ru": [""],
            "profile_name_ru": [""],
            "profile_gender": [""],
            "profile_ru_position": [""],
            "profile_en_position": [""],
        }
    )

    assert body == ""
    assert status == "302 Found"
    location = dict(headers)["Location"]
    assert location.startswith("/card/")


def test_manual_post_validates_required_fields_and_redirects_back(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    body, status, headers = app.manual_post(
        {
            "ru_org": ["ПАО Сбербанк"],
            "en_org": ["Sberbank PJSC"],
            "inn": ["7707083893"],
            "person_ru": ["Греф Герман"],
            "gender": [""],
            "ru_position": [""],
            "en_position": [""],
        }
    )

    assert body == ""
    assert status == "302 Found"
    assert dict(headers)["Location"].startswith("/card/")


def test_provider_chain_for_inn_prioritizes_reliable_sources(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    providers = app._provider_chain(web_app.INPUT_TYPE_INN, "7707083893")
    names = [p["name"] for p in providers]
    assert names == ["ФНС ЕГРЮЛ", "zachestnyibiznes.ru", "checko.ru", "rusprofile.ru", "focus.kontur.ru", "companies.rbc.ru"]


def test_manual_get_prefers_profile_prefill_and_person_name(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    body, status, _headers = app.manual_get(
        {
            "q": ["7707083893"],
            "person_ru": ["Греф Герман Оскарович"],
            "profile_ru_org": ["ПАО Сбербанк"],
            "profile_en_org": ["Sberbank PJSC"],
        }
    )

    assert status == "200 OK"
    assert "name='ru_org' required value='ПАО Сбербанк'" in body
    assert "name='en_org' required value='Sberbank PJSC'" in body
    assert "name='surname_ru' value='Греф'" in body
    assert "name='name_ru' value='Герман'" in body


def test_build_profile_from_sources_keeps_special_case_position_and_names(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    hits = [
        {
            "source": "special_case",
            "type": "company",
            "data": {
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "ПАО Сбербанк",
                "en_org": "Sberbank PJSC",
                "ru_position": "Президент, Председатель правления",
                "en_position": "President, Chairman of the Board",
                "gender": "М",
                "appeal": "Г-н",
            },
        },
        {
            "source": "ФНС ЕГРЮЛ",
            "type": "company",
            "data": {
                "ru_position": "Генеральный директор",
                "ru_org": "Сбербанк",
            },
        },
    ]

    profile, _sources = app._build_profile_from_sources(hits, "Сбербанк", web_app.INPUT_TYPE_ORG_TEXT)

    assert profile["surname_ru"] == "Греф"
    assert profile["name_ru"] == "Герман"
    assert profile["ru_position"] == "Президент, Председатель правления"
    assert profile["en_position"] == "President, Chairman of the Board"
    assert profile["en_org"] == "Sberbank PJSC"

def test_card_view_renders_en_position_from_profile_field(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    profile = {
        "surname_ru": "Иванов",
        "name_ru": "Иван",
        "gender": "М",
        "ru_org": "ООО Ромашка",
        "en_org": "Romashka LLC",
        "ru_position": "Генеральный директор",
        "en_position": "General Director",
    }
    card_id = app._create_autofill_card(profile, [], [], [], {})

    body, status, _headers = app.card_view(card_id)

    assert status == "200 OK"
    assert "<td>Position</td><td>General Director</td>" in body


def test_card_edit_post_updates_profile_and_redirects_to_table_view(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    profile = {
        "surname_ru": "Иванов",
        "name_ru": "Иван",
        "gender": "М",
        "ru_org": "ООО Ромашка",
        "en_org": "Romashka LLC",
        "ru_position": "Генеральный директор",
        "en_position": "General Director",
        "first_name": "Ivan",
    }
    card_id = app._create_autofill_card(profile, [], [], [], {})

    _body, status, headers = app.card_edit_post(
        card_id,
        {
            "title": ["Mr"],
            "appeal": ["Г-н"],
            "family_name": ["Ivanov"],
            "first_name": ["John"],
            "middle_name_en": ["Petrovich"],
            "surname_ru": ["Иванов"],
            "name_ru": ["Иван"],
            "middle_name_ru": ["Петрович"],
            "gender": ["М"],
            "inn": ["7707083893"],
            "ru_org": ["ООО Ромашка"],
            "en_org": ["Romashka LLC"],
            "ru_position": ["Генеральный директор"],
            "en_position": ["Chief Executive Officer"],
        },
    )

    assert status == "302 Found"
    assert dict(headers)["Location"] == f"/card/{card_id}"

    body, view_status, _headers = app.card_view(card_id)
    assert view_status == "200 OK"
    assert "<td>First name</td><td>John</td>" in body
    assert "<td>Position</td><td>Chief Executive Officer</td>" in body


def test_provider_chain_includes_extended_company_sources(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    providers = app._provider_chain(web_app.INPUT_TYPE_ORG_TEXT, "Сбербанк")
    names = [item["name"] for item in providers]

    assert "zachestnyibiznes.ru" in names
    assert "checko.ru" in names
    assert "companies.rbc.ru" in names


def test_required_fields_respect_explicit_profile_type(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    company_required = app._required_fields_for_profile({"type": "company", "ru_org": "ООО Ромашка"})
    person_required = app._required_fields_for_profile({"type": "person", "surname_ru": "Иванов", "name_ru": "Иван"})

    assert company_required == web_app.COMPANY_REQUIRED_FIELDS
    assert person_required == web_app.PERSON_REQUIRED_FIELDS

def test_autofill_review_person_mode_does_not_autocreate_from_company_only(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(
        app,
        "_search_external_sources",
        lambda *_args, **_kwargs: ([{"source": "Wikipedia", "type": "company", "data": {"ru_org": "Греф"}}], []),
    )
    monkeypatch.setattr(
        app,
        "_build_profile_from_sources",
        lambda *_args, **_kwargs: ({field: "" for field, _ in web_app.CARD_FIELDS}, {}),
    )

    _body, status, headers = app.autofill_review({"company_name": ["Греф"], "search_type": ["person"]})

    assert status == "302 Found"
    location = dict(headers)["Location"]
    assert location.startswith("/create/manual?")
    parsed = urlparse(location)
    params = parse_qs(parsed.query)
    assert "Ничего не найдено" in params.get("error", [""])[0]


def test_search_external_sources_drops_invalid_provider_hits(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_call_provider(provider, *_args, **_kwargs):
        if provider["name"] == "ФНС ЕГРЮЛ":
            return {"type": "company", "ru_org": "ООО Ромашка", "inn": "7707083893"}
        if provider["name"] == "zachestnyibiznes.ru":
            return {"type": "company", "ru_org": "ÐÑÐ°Ñ"}
        if provider["name"] == "checko.ru":
            return {"type": "person", "surname_ru": "", "name_ru": ""}
        return None

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    hits, trace = app._search_external_sources("7707083893", no_cache=True, search_type="company")

    assert len(hits) == 1
    assert hits[0]["data"]["ru_org"] == "ООО Ромашка"
    assert any("hits_by_provider:" in line for line in trace)


def test_search_external_sources_stops_early_when_company_profile_is_ready(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    calls = []

    def fake_call_provider(provider, *_args, **_kwargs):
        calls.append(provider["name"])
        if provider["name"] == "ФНС ЕГРЮЛ":
            return {"type": "company", "ru_org": "ПАО Сбербанк", "inn": "7707083893"}
        return {"type": "company", "ru_org": "Шум"}

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    hits, _ = app._search_external_sources("7707083893", no_cache=True, search_type="company")

    assert hits
    assert calls == ["ФНС ЕГРЮЛ"]


def test_fetch_page_retries_on_202_and_returns_none(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    calls = {"count": 0}

    class Resp:
        ok = True
        status_code = 202
        text = ""

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        return Resp()

    monkeypatch.setattr(web_app.requests, "get", fake_get)
    monkeypatch.setattr(web_app.time, "sleep", lambda *_args, **_kwargs: None)

    html = app._fetch_page("https://duckduckgo.com/html/?q=test", timeout=15, max_retries=2)

    assert html is None
    assert calls["count"] == 2

def test_parse_generic_osint_rejects_wikipedia_search_pages(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    monkeypatch.setattr(
        app,
        "_fetch_page",
        lambda *_args, **_kwargs: "<html><title>Результаты поиска — Википедия</title><h1>Результаты поиска</h1></html>",
    )

    data = app._parse_generic_osint("https://ru.wikipedia.org/w/index.php?search=%D0%93%D1%80%D0%B5%D1%84", "Wikipedia")

    assert data == {}


def test_missing_required_fields_company_requires_inn_or_ogrn(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    missing = app._missing_required_fields({"type": "company", "ru_org": "ООО Ромашка", "en_org": "Romashka LLC"})
    assert "inn_or_ogrn" in missing

    missing_with_inn = app._missing_required_fields({"type": "company", "ru_org": "ООО Ромашка", "inn": "7707083893"})
    assert "inn_or_ogrn" not in missing_with_inn


def test_autofill_review_person_mode_single_token_goes_manual(tmp_path):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    _body, status, headers = app.autofill_review({"company_name": ["Греф"], "search_type": ["person"]})

    assert status == "302 Found"
    location = dict(headers)["Location"]
    parsed = parse_qs(urlparse(location).query)
    assert location.startswith("/create/manual?")
    assert "минимум имя и фамилию" in parsed.get("error", [""])[0]


def test_call_provider_ddg_without_hits_returns_empty_no_fallback(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    provider = {"name": "DuckDuckGo HTML", "kind": "duckduckgo_html"}

    monkeypatch.setattr(app, "_fetch_from_provider", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(app, "_try_fallback_providers", lambda *_args, **_kwargs: [{"source": "Wikipedia", "data": {"ru_org": "X"}}])

    data = app._call_provider(provider, "query", web_app.INPUT_TYPE_ORG_TEXT)

    assert data == []
