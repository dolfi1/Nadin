from __future__ import annotations

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


def test_search_external_sources_uses_cache(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    calls = {"count": 0}

    def fake_call_provider(provider, raw, input_type):
        calls["count"] += 1
        return {"url": "http://example", "ru_org": f"org-{provider['kind']}"}

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    first_hits, first_trace = app._search_external_sources("7702070139", no_cache=False)
    second_hits, second_trace = app._search_external_sources("7702070139", no_cache=False)

    assert first_hits
    assert second_hits
    assert calls["count"] == len(web_app.SOURCE_PROVIDERS)
    assert any("provider_cached_hit" in line for line in second_trace)
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
            )
        return FakeResponse('<h1>Греф Герман Оскарович</h1><a href="/id/1027700132195">ПАО Сбербанк</a>')

    monkeypatch.setattr(web_app.requests, "get", fake_get)

    data = app._parse_rusprofile("Греф")
    assert isinstance(data, dict)
    assert data


def test_parse_rusprofile_url_input_uses_detail_page_directly(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    called = {"urls": []}

    def fake_get(url, **_kwargs):
        called["urls"].append(url)
        return FakeResponse("<h1>ПАО Сбербанк</h1>")

    monkeypatch.setattr(web_app.requests, "get", fake_get)
    raw_url = "https://www.rusprofile.ru/id/1027700132195"

    data = app._parse_rusprofile(raw_url)

    assert data is not None
    assert data["ru_org"] == "ПАО Сбербанк"
    assert called["urls"] == [raw_url]


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

    def fake_call_provider(provider, raw, input_type):
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


def test_person_search_cache_key_is_lowercase(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))
    calls = {"count": 0}

    def fake_call_provider(provider, raw, input_type):
        calls["count"] += 1
        return {"url": "http://example", "ru_org": "org"}

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    app._search_external_sources("Греф", no_cache=False)
    app._search_external_sources("греф", no_cache=False)

    expected = sum(1 for provider in app._provider_chain(web_app.INPUT_TYPE_PERSON_TEXT, "Греф") if app._should_call_provider(provider, web_app.INPUT_TYPE_PERSON_TEXT))
    assert calls["count"] == expected


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
    assert data["ru_org"].startswith("ПАО")
    assert data
    assert data["revenue"] == 5200000
