from __future__ import annotations

import scrapy

from nadin_scrapy.items import CompanyLeaderItem


class EgrulSpider(scrapy.Spider):
    name = "egrul"
    allowed_domains = ["egrul.nalog.ru"]

    def __init__(self, query: str, query_type: str = "ORG_QUERY", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.query_type = query_type

    def start_requests(self):
        # Endpoint may change; spider intentionally keeps extraction strict and conservative.
        url = f"https://egrul.nalog.ru/index.html?query={self.query}"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        card = CompanyLeaderItem()
        card["query_type"] = self.query_type
        card["source_name"] = "ФНС ЕГРЮЛ"
        card["source_url"] = response.url
        card["raw_snippet"] = response.text[:500]
        card["company_inn"] = response.css("[data-inn]::attr(data-inn)").get("")
        card["company_ogrn"] = response.css("[data-ogrn]::attr(data-ogrn)").get("")
        card["ru_org"] = response.css(".res-text::text").get("")
        card["confidence"] = 0.85 if card.get("company_inn") else 0.35
        card["review_required"] = not bool(card.get("company_inn") and card.get("ru_org"))
        yield card
