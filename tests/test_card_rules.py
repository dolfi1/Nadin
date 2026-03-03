import re


def test_company_with_leader_keeps_fio(app):
    result, _ = app.apply_card_rules(
        {
            "surname_ru": "Греф",
            "name_ru": "Герман",
            "middle_name_ru": "Оскарович",
            "ru_org": "Сбербанк ПАО",
            "inn": "7707083893",
            "search_type": "company",
        }
    )
    assert result["surname_ru"] == "Греф"
    assert result["name_ru"] == "Герман"


def test_company_without_leader_strips_fio(app):
    result, _ = app.apply_card_rules({"ru_org": "Сбербанк ПАО", "inn": "7707083893", "search_type": "company"})
    assert not result.get("surname_ru")
    assert not result.get("family_name")


def test_middle_name_en_no_cyrillic(app):
    result, _ = app.apply_card_rules(
        {
            "surname_ru": "Греф",
            "name_ru": "Герман",
            "middle_name_ru": "Оскарович",
            "ru_org": "Сбербанк ПАО",
            "inn": "7707083893",
            "search_type": "company",
        }
    )
    en = result.get("middle_name_en", "")
    assert not re.search(r"[А-Яа-яЁё]", en), f"Кириллица: {en!r}"
    assert en.lower() == "oskarovich"


def test_status_informational_is_found(app):
    assert app._status(["Источники: найдено 3"], required_ok=True) == "Найдено"


def test_status_autotranslit_is_review(app):
    assert app._status(["Organization EN: автотранслит"], required_ok=True) == "Нужно проверить"
