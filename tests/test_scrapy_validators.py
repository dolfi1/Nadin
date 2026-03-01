from nadin_scrapy.validators import (
    infer_appeal,
    is_valid_leader_fio,
    normalize_en_org,
    normalize_position_en,
    normalize_ru_org,
)


def test_stop_words_are_rejected_as_fio():
    assert not is_valid_leader_fio("Юридического", "Лица", "")


def test_ru_org_reorders_opf_to_tail():
    assert normalize_ru_org("ПАО СБЕРБАНК") == "Сбербанк ПАО"


def test_ru_org_extracts_compound_opf():
    assert normalize_ru_org("ФГАОУ ВО ТЮМЕНСКИЙ ГОСУДАРСТВЕННЫЙ УНИВЕРСИТЕТ") == "Тюменский Государственный Университет ФГАОУ ВО"


def test_en_org_uses_opf_mapping():
    assert normalize_en_org("ООО Ромашка").endswith("LLC")


def test_en_org_translates_charity_foundation_pattern():
    assert normalize_en_org("Благотворительный фонд Помощь АНО") == "Charitable Foundation Pomoshch ANO"


def test_position_en_normalizes_case_and_separators():
    assert normalize_position_en("генеральный директор/президент") == "Generalnyy Direktor, Prezident"


def test_appeal_from_gender():
    assert infer_appeal("Ж") == "Г-жа"
