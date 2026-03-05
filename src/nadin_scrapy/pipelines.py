from __future__ import annotations

from .validators import (
    infer_appeal,
    is_valid_leader_fio,
    normalize_en_org,
    normalize_position_en,
    normalize_position_ru,
    normalize_ru_org,
    normalize_spaces,
)

FIELD_PRIORITIES = {
    "ru_org": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "company_inn": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "company_ogrn": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "leader_surname_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru", "ФНС ЕГРЮЛ"],
    "leader_name_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru", "ФНС ЕГРЮЛ"],
    "leader_middle_ru": ["companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru", "ФНС ЕГРЮЛ"],
    "leader_position_ru": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
    "gender": ["ФНС ЕГРЮЛ", "companies.rbc.ru", "zachestnyibiznes.ru", "rusprofile.ru", "focus.kontur.ru"],
}


class CompanyProfilePipeline:
    def process_item(self, item: dict, spider=None) -> dict:  # noqa: ANN001
        normalized = dict(item)
        source_name = normalize_spaces(str(normalized.get("source_name", "")))

        normalized["ru_org"] = normalize_ru_org(str(normalized.get("ru_org", "")))
        normalized["en_org"] = normalize_en_org(str(normalized.get("ru_org", "")))
        normalized["leader_position_ru"] = normalize_position_ru(str(normalized.get("leader_position_ru", "")))
        normalized["leader_position_en"] = normalize_position_en(str(normalized.get("leader_position_ru", "")))
        normalized["company_inn"] = normalize_spaces(str(normalized.get("company_inn", "")))
        normalized["company_ogrn"] = normalize_spaces(str(normalized.get("company_ogrn", "")))
        normalized["gender"] = normalize_spaces(str(normalized.get("gender", "")))

        if normalized.get("leader_surname_ru") or normalized.get("leader_name_ru"):
            if not is_valid_leader_fio(
                str(normalized.get("leader_surname_ru", "")),
                str(normalized.get("leader_name_ru", "")),
                str(normalized.get("leader_middle_ru", "")),
            ):
                normalized["leader_surname_ru"] = ""
                normalized["leader_name_ru"] = ""
                normalized["leader_middle_ru"] = ""
                normalized["review_required"] = True

        if not normalized.get("appeal"):
            normalized["appeal"] = infer_appeal(str(normalized.get("gender", "")))

        if source_name:
            normalized["source_name"] = source_name
        return normalized

    def merge_items(self, items: list[dict]) -> dict:
        if not items:
            return {}

        merged: dict = {"review_required": False, "sources": {}}

        for field, priorities in FIELD_PRIORITIES.items():
            for source in priorities:
                match = next(
                    (
                        it
                        for it in items
                        if normalize_spaces(str(it.get("source_name", ""))) == source
                        and normalize_spaces(str(it.get(field, "")))
                    ),
                    None,
                )
                if match is not None:
                    merged[field] = normalize_spaces(str(match.get(field, "")))
                    merged["sources"][field] = source
                    break

        merged["ru_org"] = normalize_ru_org(str(merged.get("ru_org", "")))
        merged["en_org"] = normalize_en_org(str(merged.get("ru_org", "")))
        merged["leader_position_ru"] = normalize_position_ru(str(merged.get("leader_position_ru", "")))
        merged["leader_position_en"] = normalize_position_en(str(merged.get("leader_position_ru", "")))

        if not merged.get("appeal"):
            merged["appeal"] = infer_appeal(str(merged.get("gender", "")))

        has_leader = bool(merged.get("leader_surname_ru") and merged.get("leader_name_ru"))
        if not has_leader:
            merged["review_required"] = True
        elif not merged.get("leader_middle_ru"):
            merged["review_required"] = True

        return merged
