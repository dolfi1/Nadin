from conftest import fns_hit, zachest_hit


def test_fio_from_zachestnyibiznes(app):
    hits = [fns_hit(), zachest_hit()]
    profile, _ = app._build_profile_from_sources(hits, "7707083893", "INN", forced_type="company")
    assert profile["surname_ru"] == "Греф"
    assert profile["name_ru"] == "Герман"


def test_fns_fio_has_priority(app):
    hits = [
        {"source": "ФНС ЕГРЮЛ", "data": {"surname_ru": "Иванов", "name_ru": "Иван", "ru_org": "ООО", "inn": "123"}},
        {"source": "zachestnyibiznes.ru", "data": {"surname_ru": "Петров", "name_ru": "Пётр", "ru_org": "ООО", "inn": "123"}},
    ]
    profile, sources = app._build_profile_from_sources(hits, "123", "INN")
    assert profile["surname_ru"] == "Иванов"
    assert sources.get("surname_ru") == "ФНС ЕГРЮЛ"


def test_garbage_position_rejected(app):
    hits = [
        {
            "source": "zachestnyibiznes.ru",
            "data": {
                "ru_org": "ПАО СБЕРБАНК",
                "inn": "7707083893",
                "ru_position": "Юридического лица история греф герман оскарович проверить",
            },
        }
    ]
    profile, _ = app._build_profile_from_sources(hits, "7707083893", "INN")
    pos = (profile.get("ru_position") or "").lower()
    assert "история" not in pos and "проверить" not in pos


def test_dedup_collapses_same_source_inn(app):
    hits = [
        {"source": "zachestnyibiznes.ru", "data": {"inn": "123", "ru_org": "ООО А"}},
        {"source": "zachestnyibiznes.ru", "data": {"inn": "123", "ru_org": "ООО А v2"}},
    ]
    assert len(app._dedup_source_hits(hits)) == 1
