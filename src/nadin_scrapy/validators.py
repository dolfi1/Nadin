from __future__ import annotations

import re
import unicodedata

from constants import PASSPORT_MAP, RU_TO_EN_OPF

_FIO_TOKEN_RE = re.compile(r"^[А-ЯЁ][а-яё\-]{1,49}$")
_QUOTES_RE = re.compile(r'^["\'«»„“”]+|["\'«»„“”]+$')
_STOP_TOKENS = {
    "юридического",
    "лица",
    "лиц",
    "организации",
    "руководитель",
    "директор",
    "банк",
    "история",
    "проверка",
    "сведения",
    "ограничен",
    "учредители",
}
_NOISE_STEMS = ("сведен", "огранич", "учредит", "истор", "проверк")
_OPF_TOKENS = ("ООО", "ПАО", "АО", "ОАО", "ЗАО", "НКО")


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def _clean_token(value: str) -> str:
    return normalize_spaces(_QUOTES_RE.sub("", value or ""))


def _is_valid_fio_token(token: str) -> bool:
    if not token:
        return False
    lowered = token.lower()
    if lowered in _STOP_TOKENS:
        return False
    if any(lowered.startswith(stem) for stem in _NOISE_STEMS):
        return False
    return bool(_FIO_TOKEN_RE.fullmatch(token))


def is_valid_leader_fio(surname: str, name: str, middle: str = "") -> bool:
    sur = _clean_token(surname)
    nam = _clean_token(name)
    mid = _clean_token(middle)
    if not (_is_valid_fio_token(sur) and _is_valid_fio_token(nam)):
        return False
    if mid and not _is_valid_fio_token(mid):
        return False
    return True


def _extract_opf(ru_org_raw: str) -> tuple[str, str]:
    text = normalize_spaces(ru_org_raw)
    if not text:
        return "", ""
    tokens = [_clean_token(tok) for tok in text.split() if _clean_token(tok)]
    if not tokens:
        return "", ""

    found_opf = ""
    remaining: list[str] = []
    for token in tokens:
        upper = token.upper()
        if upper in _OPF_TOKENS and not found_opf:
            found_opf = upper
            continue
        remaining.append(token)

    if not found_opf and tokens:
        first_upper = tokens[0].upper()
        last_upper = tokens[-1].upper()
        if first_upper in _OPF_TOKENS:
            found_opf = first_upper
            remaining = tokens[1:]
        elif last_upper in _OPF_TOKENS:
            found_opf = last_upper
            remaining = tokens[:-1]

    return normalize_spaces(" ".join(remaining)), found_opf


def normalize_ru_org(value: str) -> str:
    name, opf = _extract_opf(value)
    if opf and name:
        return f"{opf} {name}".strip()
    return name or opf


def _translit(value: str) -> str:
    result: list[str] = []
    for ch in value:
        mapped = PASSPORT_MAP.get(ch.upper())
        if mapped is None:
            result.append(ch)
        elif ch.islower():
            result.append(mapped.lower())
        else:
            result.append(mapped)
    raw = "".join(result)
    raw = unicodedata.normalize("NFKD", raw)
    return "".join(ch for ch in raw if not unicodedata.combining(ch))


def normalize_en_org(ru_org: str) -> str:
    ru_normalized = normalize_ru_org(ru_org)
    if not ru_normalized:
        return ""
    parts = ru_normalized.split()
    opf_ru = parts[0].upper() if parts and parts[0].upper() in RU_TO_EN_OPF else ""
    name_parts = parts[1:] if opf_ru else parts
    translit_name = " ".join(_translit(part).title() for part in name_parts if part)
    opf_en = RU_TO_EN_OPF.get(opf_ru, "") if opf_ru else ""
    return normalize_spaces(f"{translit_name} {opf_en}")


def normalize_position_ru(value: str) -> str:
    cleaned = normalize_spaces(value)
    if not cleaned:
        return ""
    return ", ".join(part.strip().capitalize() for part in re.split(r"[,;/]+", cleaned) if part.strip())


def normalize_position_en(value: str) -> str:
    cleaned = normalize_spaces(value)
    if not cleaned:
        return ""
    translit = _translit(cleaned)
    return ", ".join(part.strip().title() for part in re.split(r"[,;/]+", translit) if part.strip())


def infer_appeal(gender: str) -> str:
    g = normalize_spaces(gender).upper()
    if g == "М":
        return "Г-н"
    if g == "Ж":
        return "Г-жа"
    return ""
