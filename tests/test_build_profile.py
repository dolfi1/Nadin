from urllib.parse import parse_qs, urlparse


def test_company_leader_fio_preserved(app):
    """ФИО руководителя не теряется при build → apply_card_rules для company."""
    hits = [
        {"source": "ФНС ЕГРЮЛ", "data": {"inn": "7707083893", "ru_org": "Сбербанк ПАО", "ru_position": "Председатель правления", "gender": "М"}},
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "surname_ru": "Греф",
                "name_ru": "Герман",
                "middle_name_ru": "Оскарович",
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
            },
        },
    ]
    profile, _ = app._build_profile_from_sources(hits, "7707083893", "INN", forced_type="company")
    assert profile["surname_ru"] == "Греф"
    assert profile["name_ru"] == "Герман"


def test_company_leader_autocreate(app, monkeypatch):
    """Карточка создаётся автоматически при наличии ФИО руководителя."""

    def fake_search(*_args, **_kwargs):
        return (
            [
                {
                    "source": "ФНС ЕГРЮЛ",
                    "type": "company",
                    "data": {
                        "inn": "7707083893",
                        "ru_org": "Сбербанк ПАО",
                        "ru_position": "Председатель правления",
                    },
                },
                {
                    "source": "zachestnyibiznes.ru",
                    "type": "company",
                    "data": {
                        "surname_ru": "Греф",
                        "name_ru": "Герман",
                        "middle_name_ru": "Оскарович",
                        "ru_org": "Сбербанк ПАО",
                        "inn": "7707083893",
                    },
                },
            ],
            [],
        )

    monkeypatch.setattr(app, "_search_external_sources", fake_search)
    body, status, headers = app.autofill_review(
        {"company_name": ["7707083893"], "search_type": ["company"], "hit_type": ["company"]},
        wants_json=False,
    )
    assert body == ""
    assert status == "302 Found"
    response_location = dict(headers)["Location"]
    assert response_location.startswith("/card/")


def test_fns_fio_wins_over_zachestnyibiznes(app):
    """ФНС-ФИО не перебивается zachestnyibiznes."""
    hits = [
        {"source": "ФНС ЕГРЮЛ", "data": {"surname_ru": "Иванов", "name_ru": "Иван", "ru_org": "ООО Тест", "inn": "1234567890"}},
        {"source": "zachestnyibiznes.ru", "data": {"surname_ru": "Петров", "name_ru": "Пётр", "ru_org": "ООО Тест", "inn": "1234567890"}},
    ]
    profile, sources = app._build_profile_from_sources(hits, "1234567890", "INN")
    assert profile["surname_ru"] == "Иванов"
    assert sources["surname_ru"] == "ФНС ЕГРЮЛ"


def test_garbage_position_not_stored(app):
    """Мусорная должность из zachestnyibiznes не попадает в профиль."""
    hits = [{"source": "zachestnyibiznes.ru", "data": {"ru_org": "ПАО СБЕРБАНК", "ru_position": "Юридического лица история греф герман оскарович проверить", "inn": "7707083893"}}]
    profile, _ = app._build_profile_from_sources(hits, "7707083893", "INN")
    pos = (profile.get("ru_position") or "").lower()
    assert "история" not in pos
    assert "проверить" not in pos


def test_dedup_source_hits(app):
    """Два хита с одинаковым source+inn схлопываются в один."""
    hits = [
        {"source": "zachestnyibiznes.ru", "data": {"inn": "123", "ru_org": "ООО А"}},
        {"source": "zachestnyibiznes.ru", "data": {"inn": "123", "ru_org": "ООО А copy"}},
    ]
    result = app._dedup_source_hits(hits)
    assert len(result) == 1


def test_normalize_spaces(app):
    assert app._normalize_spaces("  foo   bar  ") == "foo bar"
    assert app._normalize_spaces("") == ""


def test_sanitize_ru_position_noise(app):
    """Мусорные строки должности отклоняются."""
    assert app.sanitize_ru_position("Юридического лица история греф герман оскарович проверить") is None
    assert app.sanitize_ru_position("Генеральный директор") == "Генеральный директор"


def test_company_without_leader_goes_to_manual(app, monkeypatch):
    """Если ФНС не вернул ФИО — редирект на ручную форму с сообщением."""

    def fake_search(*_args, **_kwargs):
        return ([{"source": "ФНС ЕГРЮЛ", "type": "company", "data": {"ru_org": "ПАО СБЕРБАНК", "inn": "7707083893", "ru_position": "Президент"}}], [])

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
