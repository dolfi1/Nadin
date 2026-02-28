from __future__ import annotations

try:
    import scrapy
except Exception:  # pragma: no cover
    scrapy = None


if scrapy:
    class CompanyLeaderItem(scrapy.Item):
        query_type = scrapy.Field()
        company_inn = scrapy.Field()
        company_ogrn = scrapy.Field()
        ru_org = scrapy.Field()
        en_org = scrapy.Field()
        leader_surname_ru = scrapy.Field()
        leader_name_ru = scrapy.Field()
        leader_middle_ru = scrapy.Field()
        leader_position_ru = scrapy.Field()
        leader_position_en = scrapy.Field()
        gender = scrapy.Field()
        appeal = scrapy.Field()
        source_url = scrapy.Field()
        source_name = scrapy.Field()
        confidence = scrapy.Field()
        raw_snippet = scrapy.Field()
        blocked = scrapy.Field()
        review_required = scrapy.Field()
else:
    class CompanyLeaderItem(dict):
        """Fallback item type for environments without Scrapy."""
