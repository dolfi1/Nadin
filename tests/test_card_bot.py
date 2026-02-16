from card_bot import CardBot


def test_ru_to_en_generation(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = bot.create_card('Иванов Иван Иванович; ООО Ромашка')

    assert card.ru_fio == 'Иванов Иван Иванович'
    assert card.en_fio == 'Ivanov Ivan Ivanovich'
    assert card.en_org.startswith('LLC')
    assert card.status == 'готово'


def test_en_to_ru_marks_ambiguity(tmp_path):
    bot = CardBot(log_path=tmp_path / "log.jsonl")
    card = bot.create_card('Yegor Petrov; LLC Yellow Energy')

    assert card.ru_org.startswith('ООО')
    assert card.status == 'нужно проверить'
    assert 'неоднозначна' in ';'.join(card.quality_notes)


def test_edit_confirm_and_csv(tmp_path):
    log = tmp_path / "log.jsonl"
    bot = CardBot(log_path=log)
    card = bot.create_card('Иванов Иван Иванович; АО Ромашка')
    bot.apply_edit(card, en_org='JSC Romashka Group')
    bot.confirm_card(card)

    path = bot.export_csv(tmp_path / 'cards.csv')
    content = path.read_text(encoding='utf-8')
    assert 'JSC Romashka Group' in content
    assert 'подтверждено' in content

    logs = log.read_text(encoding='utf-8')
    assert 'create' in logs and 'edit' in logs and 'confirm' in logs
