from __future__ import annotations

import scrapy

from nadin_scrapy.items import CompanyLeaderItem
from nadin_scrapy.validators import split_fio


class ZachestnyibiznesSpider(scrapy.Spider):
    name = "zachestnyibiznes"
    allowed_domains = ["zachestnyibiznes.ru"]

    def __init__(self, query: str, query_type: str = "ORG_QUERY", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.query_type = query_type

    def start_requests(self):
        yield scrapy.Request(f"https://zachestnyibiznes.ru/search?query={self.query}", callback=self.parse)

    def parse(self, response):
        card = CompanyLeaderItem()
        card["query_type"] = self.query_type
        card["source_name"] = "zachestnyibiznes.ru"
        card["source_url"] = response.url
        card["raw_snippet"] = response.text[:500]

        org_card = response.css(".company-card, .organization-card, .card").get()
        if org_card:
            card["company_inn"] = response.css("[data-inn]::attr(data-inn), .company-card [href*='inn']::text").get("")
            card["ru_org"] = response.css("h1::text, .company-card__title::text").get("")
            leader_value = response.xpath(
                "(//*[self::th or self::dt][contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬДИРЕКТОРПРЕЗИДЕНТ', 'руководительдиректорпрезидент'), 'руководитель')"
                " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬДИРЕКТОРПРЕЗИДЕНТ', 'руководительдиректорпрезидент'), 'директор')"
                " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬДИРЕКТОРПРЕЗИДЕНТ', 'руководительдиректорпрезидент'), 'президент')]/following-sibling::*[1]//text())[1]"
            ).get("")
            surname, name, middle = split_fio(leader_value)
            card["leader_surname_ru"] = surname
            card["leader_name_ru"] = name
            card["leader_middle_ru"] = middle
            card["leader_position_ru"] = response.xpath(
                "(//*[contains(translate(normalize-space(string(.)), 'ДОЛЖНОСТЬ', 'должность'), 'должность')]/following-sibling::*[1]//text())[1]"
            ).get("")

        card["confidence"] = 0.68 if card.get("leader_name_ru") else 0.3
        card["review_required"] = not bool(card.get("leader_name_ru"))
        yield card
