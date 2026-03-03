from urllib.parse import parse_qs, urlparse



def test_company_leader_fio_preserved_after_apply_card_rules(app):
    """ФИО руководителя должно сохраняться при поиске по компании."""
    source_hits = [
        {
            "source": "ФНС ЕГРЮЛ",
            "data": {
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_position": "Президент",
            },
        }
    ]

    profile, _ = app._build_profile_from_sources(source_hits, "Сбербанк", "ORG_TEXT", forced_type="")
    normalized, _ = app.apply_card_rules(profile)

    assert normalized["surname_ru"] == "Греф"
    assert normalized["name_ru"] == "Герман"



def test_company_without_leader_goes_to_manual(app, monkeypatch):
    """Если ФНС не вернул ФИО — редирект на ручную форму с сообщением."""

    def fake_search(*_args, **_kwargs):
        return (
            [{"source": "ФНС ЕГРЮЛ", "type": "company", "data": {"ru_org": "ПАО СБЕРБАНК", "inn": "7707083893", "ru_position": "Президент"}}],
            [],
        )

    monkeypatch.setattr(app, "_search_external_sources", fake_search)

    body, status, headers = app.autofill_review(
        {"company_name": ["Сбербанк"], "search_type": ["company"], "hit_type": ["company"]},
        wants_json=False,
    )

    assert body == ""
    assert status == "302 Found"
    location = dict(headers)["Location"]
    query = parse_qs(urlparse(location).query)
    assert query.get("error", [""])[0].startswith("Не найден руководитель")



def test_fns_fio_wins_over_zachestnyibiznes(app):
    """Если ФНС дал валидное ФИО — zachestnyibiznes.ru его не перебивает."""
    source_hits = [
        {
            "source": "ФНС ЕГРЮЛ",
            "data": {
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_position": "Президент",
            },
        },
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
                "surname_ru": "Иванов",
                "name_ru": "Иван",
                "middle_name_ru": "Иванович",
            },
        },
    ]

    profile, field_sources = app._build_profile_from_sources(source_hits, "Сбербанк", "ORG_TEXT", forced_type="")

    assert profile["surname_ru"] == "Греф"
    assert profile["name_ru"] == "Герман"
    assert field_sources["surname_ru"] == "ФНС ЕГРЮЛ"



def test_garbage_position_not_stored(app):
    """Мусорная строка должности из zachestnyibiznes не попадает в профиль."""
    source_hits = [
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
                "ru_position": "Юридического лица история греф герман оскарович проверить",
            },
        }
    ]

    profile, _ = app._build_profile_from_sources(source_hits, "Сбербанк", "ORG_TEXT", forced_type="")

    assert profile["ru_position"] == ""
