from __future__ import annotations

from .validators import (
    infer_appeal,
    is_valid_leader_fio,
    normalize_en_org,
    normalize_position_en,
    normalize_position_ru,
    normalize_ru_org,
)

FIELD_PRIORITIES = {
    "ru_org": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru"],
    "company_inn": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru"],
    "company_ogrn": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru"],
    "leader_surname_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "ФНС ЕГРЮЛ"],
    "leader_name_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "ФНС ЕГРЮЛ"],
    "leader_middle_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "ФНС ЕГРЮЛ"],
    "leader_position_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "ФНС ЕГРЮЛ"],
}


class CompanyProfilePipeline:
    def __init__(self) -> None:
        self._buffer: dict[str, dict[str, object]] = {}

    def process_item(self, item, spider):
        source_name = str(item.get("source_name", ""))
        if item.get("leader_surname_ru") or item.get("leader_name_ru"):
            if not is_valid_leader_fio(
                str(item.get("leader_surname_ru", "")),
                str(item.get("leader_name_ru", "")),
                str(item.get("leader_middle_ru", "")),
            ):
                item["review_required"] = True
                item["confidence"] = min(float(item.get("confidence", 0.5)), 0.35)
                item["leader_surname_ru"] = ""
                item["leader_name_ru"] = ""
                item["leader_middle_ru"] = ""

        item["ru_org"] = normalize_ru_org(str(item.get("ru_org", "")))
        item["en_org"] = normalize_en_org(str(item.get("ru_org", "")))
        item["leader_position_ru"] = normalize_position_ru(str(item.get("leader_position_ru", "")))
        item["leader_position_en"] = normalize_position_en(str(item.get("leader_position_ru", "")))

        if not item.get("appeal"):
            item["appeal"] = infer_appeal(str(item.get("gender", "")))

        if source_name not in {p for priorities in FIELD_PRIORITIES.values() for p in priorities}:
            item["review_required"] = True

        return item

    def merge_items(self, items: list[dict[str, object]]) -> dict[str, object]:
        merged: dict[str, object] = {"review_required": False, "sources": {}}
        for field, priorities in FIELD_PRIORITIES.items():
            for source in priorities:
                match = next((it for it in items if str(it.get("source_name", "")) == source and it.get(field)), None)
                if match:
                    merged[field] = match[field]
                    merged["sources"][field] = source
                    break
        merged["ru_org"] = normalize_ru_org(str(merged.get("ru_org", "")))
        merged["en_org"] = normalize_en_org(str(merged.get("ru_org", "")))
        merged["leader_position_en"] = normalize_position_en(str(merged.get("leader_position_ru", "")))
        if merged.get("leader_surname_ru") and merged.get("leader_name_ru"):
            merged["review_required"] = bool(not merged.get("leader_middle_ru"))
        else:
            merged["review_required"] = True
        return merged
