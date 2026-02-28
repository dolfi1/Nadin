from nadin_scrapy.pipelines import CompanyProfilePipeline


def test_pipeline_drops_invalid_fio_and_flags_review():
    pipeline = CompanyProfilePipeline()
    item = {
        "source_name": "companies.rbc.ru",
        "ru_org": "ПАО СБЕРБАНК",
        "leader_surname_ru": "Юридического",
        "leader_name_ru": "Лица",
        "leader_position_ru": "генеральный директор",
    }

    processed = pipeline.process_item(item, spider=None)

    assert processed["leader_name_ru"] == ""
    assert processed["review_required"] is True
    assert processed["en_org"].endswith("PJSC")


def test_merge_prefers_prioritized_sources():
    pipeline = CompanyProfilePipeline()
    merged = pipeline.merge_items(
        [
            {
                "source_name": "zachestnyibiznes.ru",
                "ru_org": "ООО Ромашка",
                "company_inn": "123",
                "leader_surname_ru": "Иванов",
                "leader_name_ru": "Иван",
                "leader_middle_ru": "Иванович",
                "leader_position_ru": "директор",
            },
            {
                "source_name": "ФНС ЕГРЮЛ",
                "ru_org": "ООО Ромашка",
                "company_inn": "456",
                "company_ogrn": "789",
            },
        ]
    )

    assert merged["company_inn"] == "456"
    assert merged["leader_surname_ru"] == "Иванов"
    assert merged["review_required"] is False
