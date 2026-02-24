from __future__ import annotations

import web_app
from web_app import CompanyWebApp


def test_smoke_company_queries_do_not_crash_and_return_hits(tmp_path, monkeypatch):
    app = CompanyWebApp(db_path=str(tmp_path / "cards.db"))

    def fake_call_provider(provider, raw, input_type, *_args, **_kwargs):
        return {
            "url": f"https://example.test/{provider['kind']}",
            "source": provider["name"],
            "type": "company",
            "ru_org": raw,
            "inn": "7702070139",
            "input_type": input_type,
        }

    monkeypatch.setattr(app, "_call_provider", fake_call_provider)

    for query in ("ВТБ", "Сбербанк"):
        hits, trace = app._search_external_sources(query, no_cache=True)
        assert isinstance(hits, list)
        assert hits
        assert isinstance(trace, list)
        assert all(hit.get("type") == "company" for hit in hits)
        assert app.detect_input_type(query) in {web_app.INPUT_TYPE_ORG_TEXT, web_app.INPUT_TYPE_PERSON_TEXT}
