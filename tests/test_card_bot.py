from card_bot import Card, CardBot


def test_passport_transliteration_examples(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")

    assert bot.transliterate_ru_to_en_fio("Царев") == "Tsarev"
    assert bot.transliterate_ru_to_en_fio("Щербаков") == "Shcherbakov"
    assert bot.transliterate_ru_to_en_fio("Кремлёва") == "Kremlyova"


def test_full_card_parsing_and_components(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = bot.create_card(
        "Иванов Иван Иванович; муж; Ромашка ООО; Romashka PJSC; "
        "Генеральный директор; Chief Executive Officer"
    )

    assert card.surname_ru == "Иванов"
    assert card.name_ru == "Иван"
    assert card.patronymic_ru == "Иванович"
    assert card.surname_en == "Ivanov"
    assert card.name_en == "Ivan"
    assert card.patronymic_en == "Ivanovich"
    assert card.gender == "М"
    assert card.appeal == "Г-н"
    assert card.ru_org.endswith("ООО")
    assert card.en_org.endswith("PJSC")


def test_pao_is_always_pjsc(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    ru_org, _ = bot.normalize_ru_org('ПАО Сбербанк')

    opf = ru_org.split()[-1]
    assert bot._extract_opf_any_ru(ru_org)[0] == "ПАО"
    assert bot._extract_opf_any_en("Sberbank PJSC")[0] == "PJSC"
    assert opf == "ПАО"


def test_gender_and_required_validation(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = bot.create_card("Иванов Иван; X; Ромашка ООО; Romashka LLC; Директор; Director")

    assert card.status == "Ошибка формата"
    assert any("Пол должен быть" in n for n in card.quality_notes)


def test_csv_export_contains_split_columns(tmp_path):
    log = tmp_path / "log.jsonl"
    bot = CardBot(log_path=log)
    card = bot.create_card("Иванов Иван Иванович; М; Ромашка ООО; Romashka LLC; Генеральный директор; CEO")
    bot.confirm_card(card, confirmed_by="qa")

    path = bot.export_csv(tmp_path / "cards.csv")
    content = path.read_text(encoding="utf-8")
    assert "appeal" in content
    assert "middle_name_en" in content
    assert "CEO" in content
    assert "qa" in content


def test_latin_name_diacritics_sanitized_and_noted(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = Card(en_fio="Müller José Koç")
    bot._build_fio(card)

    assert card.family_name == "Mueller"
    assert card.first_name == "Jose"
    assert card.middle_name_en == "Koc"
    assert any("диакритика удалена" in n for n in card.quality_notes)


def test_surname_particle_and_split_behaviour(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    assert bot._split_fio("de Robert Hautequere") == ("de", "Robert", "Hautequere")
    card = Card(en_fio="Weymarn von Robert")
    bot._build_fio(card)
    assert card.family_name == "Weymarn"


def test_foreign_middle_name_en_and_ru_translit(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = Card(en_fio="Smith John William")
    bot._build_fio(card)

    assert card.middle_name_en == "William"
    assert card.patronymic_ru
    assert any("транслитерирован с английского" in n for n in card.quality_notes)
    assert bot.transliterate_en_to_ru("William")


def test_media_exception_and_ru_registered_org_notes(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    name, notes = bot.normalize_en_org("The Financial Times", is_media=True)
    assert name == "The Financial Times"
    assert not any("The в начале" in n for n in notes)

    _name2, notes2 = bot.normalize_en_org("The Coca Cola Company", is_media=False)
    assert any("The в начале" in n for n in notes2)

    _name3, notes3 = bot.normalize_en_org("Acme", is_ru_registered=True)
    assert any("Транслит допустим" in n for n in notes3)


def test_en_position_abbreviation_and_ru_abbrev_detection(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    assert bot.normalize_en_position("ceo, cfo")[0] == "CEO, CFO"
    assert bot.normalize_en_position("chairman, ceo")[0] == "Chairman, CEO"

    ru_ok, ru_notes = bot.normalize_ru_position("Исполняющий обязанности")
    assert ru_ok == "Исполняющий обязанности"
    assert not ru_notes
    _, ru_notes2 = bot.normalize_ru_position("ИО директора")
    assert any("ИО" in n for n in ru_notes2)


def test_enrich_card_generates_position_middle_name_and_appeal(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = bot.create_card("Греф Герман Оскарович; М; Сбербанк ПАО; Sberbank PJSC; Президент, Председатель правления; ")

    assert card.en_position == "President, Chairman of the Board"
    assert card.middle_name_en == "Oskarovich"
    assert card.appeal == "Г-н"


def test_from_profile_maps_ru_and_en_fallbacks():
    card = Card.from_profile(
        {
            "family_name": "Gref",
            "first_name": "German",
            "middle_name_ru": "Оскарович",
            "ru_org": "ПАО Сбербанк",
            "en_org": "Sberbank PJSC",
            "ru_position": "Президент",
            "position": "President",
            "gender": "М",
        }
    )

    assert card.surname_ru == "Gref"
    assert card.name_ru == "German"
    assert card.patronymic_ru == "Оскарович"
    assert card.en_position == "President"
