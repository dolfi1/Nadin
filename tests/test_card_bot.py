from card_bot import CardBot


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
    assert "surname_ru" in content
    assert "CEO" in content
    assert "qa" in content
