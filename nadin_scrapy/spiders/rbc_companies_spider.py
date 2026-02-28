from __future__ import annotations

import scrapy

from nadin_scrapy.items import CompanyLeaderItem
from nadin_scrapy.validators import LEADER_LABEL_RE, split_fio


class RbcCompaniesSpider(scrapy.Spider):
    name = "rbc_companies"
    allowed_domains = ["companies.rbc.ru"]

    def __init__(self, query: str, query_type: str = "ORG_QUERY", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.query_type = query_type

    def start_requests(self):
        yield scrapy.Request(f"https://companies.rbc.ru/search/?query={self.query}", callback=self.parse)

    def parse(self, response):
        card = CompanyLeaderItem()
        card["query_type"] = self.query_type
        card["source_name"] = "companies.rbc.ru"
        card["source_url"] = response.url
        card["raw_snippet"] = response.text[:500]
        card["company_inn"] = response.css("[itemprop='taxID']::text").get("")
        card["ru_org"] = response.css("h1::text").get("")

        leader_label_node = response.xpath(
            "//*[self::th or self::dt][contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'руководитель')"
            " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'генеральный директор')"
            " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'президент')]"
        ).get()

        if leader_label_node:
            leader_text = response.xpath(
                "(//*[self::th or self::dt][contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'руководитель')"
                " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'генеральный директор')"
                " or contains(translate(normalize-space(string(.)), 'РУКОВОДИТЕЛЬГЕНЕРАЛЬНЫЙДИРЕКТОРПРЕЗИДЕНТ', 'руководительгенеральныйдиректорпрезидент'), 'президент')]/following-sibling::*[1]//text())[1]"
            ).get("")
            if LEADER_LABEL_RE.search(leader_text):
                leader_text = ""
            surname, name, middle = split_fio(leader_text)
            card["leader_surname_ru"] = surname
            card["leader_name_ru"] = name
            card["leader_middle_ru"] = middle

        card["leader_position_ru"] = response.xpath(
            "(//*[contains(translate(normalize-space(string(.)), 'ДОЛЖНОСТЬ', 'должность'), 'должность')]/following-sibling::*[1]//text())[1]"
        ).get("")
        card["confidence"] = 0.7 if card.get("leader_name_ru") else 0.4
        card["review_required"] = not bool(card.get("leader_name_ru"))
        yield card
