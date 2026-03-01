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

RU_QUOTES_RE = re.compile(r'^["«»„“”\']+|["«»„“”\']+$')
RU_ABBREVIATIONS = {
    "ФГАОУ", "ФГБОУ", "ФГБУ", "ФГБУК", "ВО", "МИД", "РАН", "СПБ", "СПБГУ", "МГИМО", "МИСИС", "НИИ",
}
RU_SMALL_WORDS = {"и", "по", "в", "во", "при", "на", "им.", "имени"}
KNOWN_TOPO_REPLACEMENTS = {
    "САНКТ ПЕТЕРБУРГ": "САНКТ-ПЕТЕРБУРГ",
    "САНКТ ПЕТЕРБУРГСКИЙ": "САНКТ-ПЕТЕРБУРГСКИЙ",
}
OPF_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("ФГАОУ ВО", re.compile(r"\bФГАОУ\s+ВО\b", flags=re.IGNORECASE)),
    ("ФГБОУ ВО", re.compile(r"\bФГБОУ\s+ВО\b", flags=re.IGNORECASE)),
    ("ФГБУК", re.compile(r"\bФГБУК\b", flags=re.IGNORECASE)),
    ("ФГБУ", re.compile(r"\bФГБУ\b", flags=re.IGNORECASE)),
    ("СПБ ГБУЗ", re.compile(r"\bСПБ\s+ГБУЗ\b", flags=re.IGNORECASE)),
    ("ГБУЗ", re.compile(r"\bГБУЗ\b", flags=re.IGNORECASE)),
    ("ООО", re.compile(r"\bООО\b", flags=re.IGNORECASE)),
    ("ПАО", re.compile(r"\bПАО\b", flags=re.IGNORECASE)),
    ("АО", re.compile(r"\bАО\b", flags=re.IGNORECASE)),
    ("АНО", re.compile(r"\bАНО\b", flags=re.IGNORECASE)),
]

COMMON_RU_TO_EN = {
    "федеральное": "Federal",
    "агентство": "Agency",
    "фонд": "Foundation",
    "благотворительный": "Charitable",
    "институт": "Institute",
    "центр": "Center",
    "университет": "University",
    "поликлиника": "Polyclinic",
    "оркестр": "Orchestra",
    "заповедник": "Reserve",
    "филиал": "Branch",
    "национальный": "National",
    "исследовательский": "Research",
}

KNOWN_CITY_EN = {
    "санкт-петербургский": "Saint-Petersburg",
    "санкт-петербург": "Saint-Petersburg",
}


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
    name_ru, ru_opf = extract_opf_ru(value)
    name_ru_norm = normalize_ru_name(name_ru)
    return normalize_spaces(f"{name_ru_norm} {ru_opf}") if ru_opf else name_ru_norm


def extract_opf_ru(ru_org_raw: str) -> tuple[str, str]:
    cleaned = normalize_spaces(ru_org_raw)
    for target, replacement in KNOWN_TOPO_REPLACEMENTS.items():
        cleaned = re.sub(rf"\b{target}\b", replacement, cleaned, flags=re.IGNORECASE)
    opf_hits: list[str] = []
    for opf, pattern in OPF_PATTERNS:
        if pattern.search(cleaned):
            opf_hits.append(opf)
            cleaned = pattern.sub(" ", cleaned)
    ru_opf = opf_hits[0] if opf_hits else ""
    return normalize_spaces(cleaned), ru_opf


def normalize_ru_name(name_ru: str) -> str:
    cleaned = normalize_spaces(name_ru)
    cleaned = RU_QUOTES_RE.sub("", cleaned)
    tokens = cleaned.split()
    normalized_tokens: list[str] = []
    for idx, token in enumerate(tokens):
        token_clean = RU_QUOTES_RE.sub("", token)
        upper = token_clean.upper()
        if upper in RU_ABBREVIATIONS:
            normalized_tokens.append(upper if upper != "СПБ" else "СПб")
        elif idx > 0 and token_clean.lower() in RU_SMALL_WORDS:
            normalized_tokens.append(token_clean.lower())
        else:
            normalized_tokens.append(token_clean[:1].upper() + token_clean[1:].lower())
    return normalize_spaces(" ".join(normalized_tokens))


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
    name_ru, ru_opf = extract_opf_ru(ru_org)
    name_ru_norm = normalize_ru_name(name_ru)
    return build_en_name(name_ru_norm, ru_opf)


def build_en_name(name_ru_norm: str, ru_opf: str) -> str:
    if not name_ru_norm:
        return ""

    prepared = re.sub(r"\bимени\s+([^,]+)", lambda m: f"named after {transliterate(m.group(1))}", name_ru_norm, flags=re.IGNORECASE)
    prepared = re.sub(r"\bим\.\s*([^,]+)", lambda m: f"named after {transliterate(m.group(1))}", prepared, flags=re.IGNORECASE)
    prepared = re.sub(
        r"Российской\s+академии\s+наук",
        "of the Russian Academy of Sciences",
        prepared,
        flags=re.IGNORECASE,
    )

    branch_match = re.match(r"^(Санкт\-Петербургский)\s+филиал\s+(.+)$", prepared, flags=re.IGNORECASE)
    if branch_match:
        city_ru, tail = branch_match.groups()
        city_en = KNOWN_CITY_EN.get(city_ru.lower(), transliterate(city_ru).title())
        prepared = f"{city_en} Branch of {tail}"

    words = []
    for token in prepared.split():
        low = token.lower()
        if low in COMMON_RU_TO_EN:
            words.append(COMMON_RU_TO_EN[low])
        elif re.fullmatch(r"№\d+", token):
            words.append(token)
        elif re.search(r"[А-Яа-яЁё]", token):
            words.append(transliterate(token).title())
        else:
            words.append(token)

    opf_en = RU_TO_EN_OPF.get(ru_opf.upper(), "") if ru_opf else ""
    return normalize_spaces(f"{' '.join(words)} {opf_en}")


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
