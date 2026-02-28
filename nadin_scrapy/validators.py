from __future__ import annotations

import re
import unicodedata

from constants import PASSPORT_MAP, RU_TO_EN_OPF

FIO_STOP_TOKENS = {
    "юридического", "лица", "инвестиции", "мероприятия", "проверка", "история", "физлиц", "организации",
}

LEADER_LABEL_RE = re.compile(
    r"(руководитель|генеральный\s+директор|директор|президент|председатель\s+правления)",
    flags=re.IGNORECASE,
)

RU_FIO_TOKEN_RE = re.compile(r"^[А-ЯЁ][а-яё\-]{1,49}$")


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def split_fio(value: str) -> tuple[str, str, str]:
    tokens = [token for token in normalize_spaces(value).split(" ") if token]
    if len(tokens) < 2:
        return "", "", ""
    surname, name = tokens[0], tokens[1]
    middle = tokens[2] if len(tokens) > 2 else ""
    return surname, name, middle


def is_valid_fio_token(token: str) -> bool:
    normalized = normalize_spaces(token)
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in FIO_STOP_TOKENS:
        return False
    return bool(RU_FIO_TOKEN_RE.fullmatch(normalized))


def is_valid_leader_fio(surname: str, name: str, middle: str = "") -> bool:
    if not is_valid_fio_token(surname) or not is_valid_fio_token(name):
        return False
    if middle and not is_valid_fio_token(middle):
        return False
    joined = f"{surname} {name} {middle}".lower()
    return not any(stop in joined for stop in FIO_STOP_TOKENS)


def normalize_ru_org(value: str) -> str:
    raw = normalize_spaces(value).strip('"«»')
    if not raw:
        return ""
    raw = re.sub(r"[()]", "", raw)
    raw = raw.replace("ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО", "ПАО")
    parts = raw.split()
    if parts and parts[0].upper() in RU_TO_EN_OPF:
        opf = parts[0].upper()
        name = " ".join(parts[1:]).title()
        return normalize_spaces(f"{name} {opf}")
    return raw


def transliterate(value: str) -> str:
    result = []
    for char in value:
        mapped = PASSPORT_MAP.get(char.upper())
        if mapped is None:
            result.append(char)
        elif char.islower():
            result.append(mapped.lower())
        else:
            result.append(mapped)
    text = "".join(result)
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_en_org(ru_org: str) -> str:
    ru_norm = normalize_ru_org(ru_org)
    if not ru_norm:
        return ""
    tokens = ru_norm.split()
    opf = tokens[-1].upper() if tokens else ""
    opf_en = RU_TO_EN_OPF.get(opf, opf)
    org_name = " ".join(tokens[:-1]) if opf_en != opf else ru_norm
    org_name_en = transliterate(org_name).title()
    if opf_en != opf:
        return f"{org_name_en} {opf_en}".strip()
    return org_name_en


def normalize_position_ru(value: str) -> str:
    tokens = [token.capitalize() for token in re.split(r"[,;/]+", value) if normalize_spaces(token)]
    return ", ".join(tokens)


def normalize_position_en(value: str) -> str:
    raw = transliterate(value)
    tokens = [normalize_spaces(token).title() for token in re.split(r"[,;/]+", raw) if normalize_spaces(token)]
    return ", ".join(tokens)


def infer_appeal(gender: str) -> str:
    if gender == "М":
        return "Г-н"
    if gender == "Ж":
        return "Г-жа"
    return ""
