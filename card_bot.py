from __future__ import annotations

import csv
import json
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


RU_TO_EN_OPF: Dict[str, str] = {
    "ООО": "LLC",
    "АО": "JSC",
    "ФГБУ": "FSBI",
    "ПАО": "PJSC",
    "ОАО": "OJSC",
    "АНО": "ANO",
    "ИП": "IE",
    "МУП": "MUE",
    "МАУ": "MAI",
    "ЧУ": "PI",
}
EN_TO_RU_OPF: Dict[str, str] = {v: k for k, v in RU_TO_EN_OPF.items()}

PASSPORT_MAP = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "YO", "Ж": "ZH", "З": "Z",
    "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M", "Н": "N", "О": "O", "П": "P", "Р": "R",
    "С": "S", "Т": "T", "У": "U", "Ф": "F", "Х": "KH", "Ц": "TS", "Ч": "CH", "Ш": "SH", "Щ": "SHCH",
    "Ъ": "", "Ы": "Y", "Ь": "", "Э": "E", "Ю": "YU", "Я": "YA",
}
EN_SMALL_WORDS = {"a", "an", "the", "and", "or", "of", "for", "to", "in", "on", "at", "by"}
SURNAME_PARTICLES = {"von", "van", "de", "du", "der", "den", "la", "le", "di", "da", "dos", "des"}
EN_POSITION_ABBREVIATIONS = {
    "CEO", "CFO", "CTO", "COO", "CMO", "CIO", "CHRO", "CSO", "CPO", "CCO", "CLO", "CDO", "CISO", "MD", "VP", "EVP", "SVP", "GM",
}
APPEAL_MAP = {"М": "Г-н", "Ж": "Г-жа"}
UMLAUT_MAP = {"ä": "ae", "ö": "oe", "ü": "ue", "Ä": "Ae", "Ö": "Oe", "Ü": "Ue", "ß": "ss"}
EN_TO_RU_TRANSLIT = {
    "A": "А", "B": "Б", "C": "К", "D": "Д", "E": "Е", "F": "Ф", "G": "Г", "H": "Х", "I": "И", "J": "Дж", "K": "К", "L": "Л",
    "M": "М", "N": "Н", "O": "О", "P": "П", "Q": "К", "R": "Р", "S": "С", "T": "Т", "U": "У", "V": "В", "W": "В", "X": "Кс",
    "Y": "Й", "Z": "З",
}


@dataclass
class Card:
    ru_fio: str = ""
    en_fio: str = ""
    surname_ru: str = ""
    name_ru: str = ""
    patronymic_ru: str = ""
    surname_en: str = ""
    name_en: str = ""
    appeal: str = ""
    family_name: str = ""
    first_name: str = ""
    middle_name_en: str = ""
    gender: str = ""
    inn: str = ""
    ru_org: str = ""
    en_org: str = ""
    ru_position: str = ""
    en_position: str = ""
    is_media: bool = False
    is_ru_registered: bool = False
    status: str = "OK"
    quality_notes: List[str] = field(default_factory=list)
    created_at: str = ""
    created_by: str = "system"
    confirmed_at: str = ""
    confirmed_by: str = ""

    @property
    def patronymic_en(self) -> str:
        return self.middle_name_en

    @patronymic_en.setter
    def patronymic_en(self, value: str) -> None:
        self.middle_name_en = value

    @classmethod
    def from_profile(cls, profile: dict) -> "Card":
        payload = {field: profile.get(field, "") for field in cls.__dataclass_fields__}
        payload["surname_ru"] = profile.get("surname_ru") or profile.get("family_name", "")
        payload["name_ru"] = profile.get("name_ru") or profile.get("first_name", "")
        payload["patronymic_ru"] = profile.get("patronymic_ru") or profile.get("middle_name_ru", "")
        payload["surname_en"] = profile.get("surname_en") or profile.get("family_name", "")
        payload["name_en"] = profile.get("name_en") or profile.get("first_name", "")
        payload["middle_name_en"] = profile.get("middle_name_en") or profile.get("middle_name", "")
        payload["family_name"] = profile.get("family_name") or profile.get("surname_en", "")
        payload["first_name"] = profile.get("first_name") or profile.get("name_en", "")
        payload["ru_org"] = profile.get("ru_org", "")
        payload["en_org"] = profile.get("en_org", "")
        payload["ru_position"] = profile.get("ru_position", "")
        payload["en_position"] = profile.get("en_position") or profile.get("position", "")
        return cls(**payload)


class CardBot:
    def __init__(self, log_path: str = "card_changes.log") -> None:
        self.log_path = Path(log_path)
        self.cards: List[Card] = []

    def create_card(self, text: str, created_by: str = "system") -> Card:
        data = self._parse_user_input(text)
        card = Card(created_by=created_by, created_at=self._now())
        card.ru_fio = self._normalize_spaces(data.get("ru_fio", ""))
        card.gender = self._normalize_gender(data.get("gender", ""))
        card.appeal = APPEAL_MAP.get(card.gender, "")
        card.ru_org = self._normalize_spaces(data.get("ru_org", ""))
        card.en_org = self._normalize_spaces(data.get("en_org", ""))
        card.ru_position = self._normalize_spaces(data.get("ru_position", ""))
        card.en_position = self._normalize_spaces(data.get("en_position", ""))
        card.inn = self._normalize_spaces(data.get("inn", ""))
        card.is_media = str(data.get("is_media", "")).lower() in {"1", "true", "on", "yes"}
        card.is_ru_registered = str(data.get("is_ru_registered", "")).lower() in {"1", "true", "on", "yes"}

        if not card.ru_fio and data.get("en_fio"):
            card.en_fio = self._normalize_spaces(data["en_fio"])
        self._build_fio(card)
        self._enrich_card(card)

        card.ru_org, ru_org_notes = self.normalize_ru_org(card.ru_org)
        card.en_org, en_org_notes = self.normalize_en_org(card.en_org, is_media=card.is_media, is_ru_registered=card.is_ru_registered)

        card.ru_position, ru_pos_notes = self.normalize_ru_position(card.ru_position)
        card.en_position, en_pos_notes = self.normalize_en_position(card.en_position)

        notes = []
        notes.extend(card.quality_notes)
        notes.extend(self._validate_card(card))
        notes.extend(ru_org_notes)
        notes.extend(en_org_notes)
        notes.extend(ru_pos_notes)
        notes.extend(en_pos_notes)

        if card.gender not in {"М", "Ж"}:
            notes.append("Пол должен быть М или Ж")

        card.quality_notes = self._unique(notes)
        card.status = self._calc_status(card.quality_notes)

        self.cards.append(card)
        self._log_change("create", None, card)
        return card

    def apply_edit(self, card: Card, **updates: str) -> Card:
        before = Card(**asdict(card))
        for key, value in updates.items():
            if hasattr(card, key):
                if isinstance(getattr(card, key), bool):
                    setattr(card, key, str(value).lower() in {"1", "true", "on", "yes"})
                else:
                    setattr(card, key, self._normalize_spaces(value))

        card.gender = self._normalize_gender(card.gender)
        card.appeal = APPEAL_MAP.get(card.gender, "")
        self._build_fio(card)
        self._enrich_card(card)
        card.ru_org, ru_org_notes = self.normalize_ru_org(card.ru_org)
        card.en_org, en_org_notes = self.normalize_en_org(card.en_org, is_media=card.is_media, is_ru_registered=card.is_ru_registered)
        card.ru_position, ru_pos_notes = self.normalize_ru_position(card.ru_position)
        card.en_position, en_pos_notes = self.normalize_en_position(card.en_position)
        notes = []
        notes.extend(card.quality_notes)
        notes.extend(self._validate_card(card))
        notes.extend(ru_org_notes + en_org_notes + ru_pos_notes + en_pos_notes)
        if card.gender not in {"М", "Ж"}:
            notes.append("Пол должен быть М или Ж")
        card.quality_notes = self._unique(notes)
        card.status = self._calc_status(card.quality_notes)

        self._log_change("edit", before, card)
        return card

    def confirm_card(self, card: Card, confirmed_by: str = "system") -> Card:
        before = Card(**asdict(card))
        card.confirmed_by = confirmed_by
        card.confirmed_at = self._now()
        self._log_change("confirm", before, card)
        return card

    def export_csv(self, destination: str) -> Path:
        path = Path(destination)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "surname_ru", "name_ru", "patronymic_ru", "appeal", "surname_en", "name_en", "family_name", "first_name", "middle_name_en",
                    "gender", "inn", "ru_org", "en_org", "ru_position", "en_position", "is_media", "is_ru_registered", "status", "quality_notes",
                    "created_by", "created_at", "confirmed_by", "confirmed_at",
                ],
            )
            writer.writeheader()
            for card in self.cards:
                row = asdict(card)
                row["quality_notes"] = "; ".join(card.quality_notes)
                writer.writerow({k: row.get(k, "") for k in writer.fieldnames})
        return path

    def render_card(self, card: Card) -> str:
        notes = "\n".join(f"- {x}" for x in card.quality_notes) if card.quality_notes else "- Нет"
        return (
            "[RU]\n"
            f"Фамилия: {card.surname_ru}\n"
            f"Имя: {card.name_ru}\n"
            f"Отчество: {card.patronymic_ru or '—'}\n"
            f"Пол: {card.gender}\n"
            f"Обращение: {card.appeal or '—'}\n"
            f"Организация: {card.ru_org}\n"
            f"Должность: {card.ru_position}\n\n"
            "[EN]\n"
            f"Surname: {card.family_name or card.surname_en}\n"
            f"Name: {card.first_name or card.name_en}\n"
            f"Middle name (EN): {card.middle_name_en or '—'}\n"
            f"Organization: {card.en_org}\n"
            f"Position: {card.en_position}\n\n"
            f"Статус: {card.status}\n"
            f"Нарушения:\n{notes}"
        )

    def transliterate_ru_to_en_fio(self, fio: str) -> str:
        return " ".join(self._transliterate_token(tok) for tok in fio.split())

    def normalize_ru_org(self, org: str) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_punct(org, russian=True)
        if cleaned != org:
            notes.append("RU организация: кавычки/знаки удалены")
        opf, name = self._extract_opf_any_ru(cleaned)
        name = self._normalize_spaces(name)
        if opf:
            cleaned = f"{name} {opf}".strip()
        else:
            cleaned = name
            notes.append("RU организация: ОПФ должна быть в конце")

        if re.search(r"\b[А-Я]{2,}\b", name):
            for abbr in re.findall(r"\b[А-Яа-я]{2,}\b", name):
                if abbr.lower() != abbr and abbr.upper() != abbr and len(abbr) <= 6:
                    notes.append("RU организация: аббревиатуры только CAPS")
                    break

        if name and not name[0].isupper():
            notes.append("RU организация: неверный кейс")
        if self._has_double_name(name):
            notes.append("RU организация: двойное название")
        return cleaned, self._unique(notes)

    def normalize_en_org(self, org: str, is_media: bool = False, is_ru_registered: bool = False) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_punct(org, russian=False)
        if cleaned != org:
            notes.append("EN organization: quotes/punctuation removed")

        opf, name = self._extract_opf_any_en(cleaned)
        name = self._title_case_en(name)
        if name.startswith("The ") and not is_media:
            notes.append("EN organization: The в начале запрещен")
        if self._contains_non_ascii_letters(name):
            notes.append("EN organization: диакритика/не-ASCII запрещены")
            name = self._ascii_sanitize(name)
        if opf:
            cleaned = f"{name} {opf}".strip()
        else:
            cleaned = name
            if is_ru_registered:
                notes.append("Транслит допустим (зарегистрировано в РФ)")
            else:
                notes.append("Organization EN: автотранслит — требует перевода или подтверждения")
                notes.append("EN organization: OPF should be at the end")

        return cleaned, self._unique(notes)

    def normalize_ru_position(self, text: str) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_position(text)
        if cleaned != text:
            notes.append("RU должность: запрещенные символы удалены")
        if cleaned and not re.fullmatch(r"[А-Яа-яЁё\- ,]+", cleaned):
            notes.append("RU должность: только русский язык")
        abbrev_tokens = [tok for tok in cleaned.split() if re.fullmatch(r"[А-ЯЁ]{2,5}", tok)]
        if abbrev_tokens:
            notes.append(f"RU должность: сокращения запрещены ({', '.join(abbrev_tokens)})")
        if " и " in cleaned:
            notes.append("RU должность: несколько ролей только через запятую")
        return self._sentence_case_ru_list(cleaned), self._unique(notes)

    def normalize_en_position(self, text: str) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_position(text)
        if cleaned != text:
            notes.append("EN position: forbidden punctuation removed")
        if cleaned and not re.fullmatch(r"[A-Za-z\- ,]+", cleaned):
            notes.append("EN position: only English allowed")
        if " and " in cleaned.lower() or "&" in cleaned:
            notes.append("EN position: multiple roles must be comma-separated")
        return self._title_case_en_positions(cleaned), self._unique(notes)

    def _generate_en_position(self, ru_position: str) -> str:
        position_map = {
            "Президент": "President",
            "Председатель правления": "Chairman of the Board",
            "Генеральный директор": "CEO",
            "Директор": "Director",
            "Руководитель": "Head",
            "Исполнительный директор": "Executive Director",
            "Главный исполнительный директор": "Chief Executive Officer",
            "Главный финансовый директор": "Chief Financial Officer",
        }
        positions = [p.strip() for p in self._normalize_spaces(ru_position).split(",") if p.strip()]
        en_positions: List[str] = []
        for pos in positions:
            translated = ""
            for ru_title, en_title in position_map.items():
                if ru_title.lower() in pos.lower():
                    translated = en_title
                    break
            en_positions.append(translated or self.transliterate_ru_to_en_fio(pos))
        return ", ".join(en_positions)

    def _generate_middle_name_en(self, middle_name_ru: str) -> str:
        value = self._normalize_spaces(middle_name_ru)
        if not value or value == "—":
            return ""
        return self.transliterate_ru_to_en_fio(value)

    def _enrich_card(self, card: Card) -> None:
        if card.ru_position and not card.en_position:
            card.en_position = self._generate_en_position(card.ru_position)
        if card.patronymic_ru and not card.middle_name_en:
            card.middle_name_en = self._generate_middle_name_en(card.patronymic_ru)
        if card.gender and not card.appeal:
            card.appeal = APPEAL_MAP.get(card.gender, "")

    def _build_fio(self, card: Card) -> None:
        card.quality_notes = []
        if card.ru_fio:
            ru_parts = self._split_fio(card.ru_fio)
            card.surname_ru, card.name_ru, card.patronymic_ru = ru_parts
            card.surname_en = self._sanitize_latin_name(self._transliterate_token(card.surname_ru))
            card.name_en = self._sanitize_latin_name(self._transliterate_token(card.name_ru))
            card.middle_name_en = ""
            card.family_name = self._apply_surname_particles([card.surname_en])[0] if card.surname_en else ""
            card.first_name = card.name_en
            card.en_fio = " ".join(x for x in [card.family_name, card.first_name] if x)
            card.ru_fio = " ".join(x for x in [card.surname_ru, card.name_ru, card.patronymic_ru] if x)
        elif card.en_fio:
            en_parts = self._split_fio(card.en_fio)
            card.family_name, card.first_name, card.middle_name_en = en_parts
            card.surname_en = self._sanitize_latin_name(card.family_name)
            card.name_en = self._sanitize_latin_name(card.first_name)
            sanitized_middle = self._sanitize_latin_name(card.middle_name_en)
            if sanitized_middle != card.middle_name_en or card.surname_en != card.family_name or card.name_en != card.first_name:
                card.quality_notes.append("EN имя: диакритика удалена")
            card.family_name = self._apply_surname_particles([card.surname_en])[0] if card.surname_en else ""
            card.first_name = card.name_en
            card.middle_name_en = sanitized_middle
            if card.middle_name_en and all(ord(ch) < 128 for ch in card.middle_name_en):
                card.patronymic_ru = self.transliterate_en_to_ru(card.middle_name_en)
                card.quality_notes.append("Middle name RU: транслитерирован с английского, требует проверки")
            card.en_fio = " ".join(x for x in [card.family_name, card.first_name, card.middle_name_en] if x)

    def _split_fio(self, fio: str) -> Tuple[str, str, str]:
        parts = [p for p in self._normalize_spaces(fio).split(" ") if p]
        if len(parts) < 2:
            return (parts[0] if parts else "", "", "")

        def _norm(tok: str) -> str:
            return tok.lower() if tok.lower() in SURNAME_PARTICLES else tok[:1].upper() + tok[1:].lower()

        norm = [_norm(p) for p in parts[:3]]
        if len(norm) == 2:
            return norm[0], norm[1], ""
        return norm[0], norm[1], norm[2]

    def _apply_surname_particles(self, tokens: list[str]) -> list[str]:
        out = []
        for idx, tok in enumerate(tokens):
            if idx > 0 and tok.lower() in SURNAME_PARTICLES:
                out.append(tok.lower())
            else:
                out.append(tok)
        return out

    def _sanitize_latin_name(self, token: str) -> str:
        for source, target in UMLAUT_MAP.items():
            token = token.replace(source, target)
        normalized = unicodedata.normalize("NFKD", token)
        return "".join(ch for ch in normalized if ord(ch) < 128)

    def transliterate_en_to_ru(self, token: str) -> str:
        chars: list[str] = []
        for ch in token:
            mapped = EN_TO_RU_TRANSLIT.get(ch.upper())
            if mapped:
                chars.append(mapped.lower() if ch.islower() else mapped)
            elif ch.isascii():
                chars.append(ch)
        return "".join(chars)

    def _parse_user_input(self, text: str) -> Dict[str, str]:
        text = text.strip()
        if not text:
            return {}

        labels = {
            "фио": "ru_fio", "пол": "gender", "организация": "ru_org", "organization": "en_org", "должность": "ru_position",
            "position": "en_position", "name": "en_fio", "inn": "inn", "is_media": "is_media", "is_ru_registered": "is_ru_registered",
        }
        result: Dict[str, str] = {}
        for raw_line in [ln.strip() for ln in re.split(r"[\n]+", text) if ln.strip()]:
            if ":" not in raw_line:
                continue
            key, value = [x.strip() for x in raw_line.split(":", 1)]
            norm_key = key.lower().rstrip("*")
            if norm_key in labels:
                result[labels[norm_key]] = value
        if result:
            return result

        parts = [p.strip() for p in re.split(r"[;|]+", text) if p.strip()]
        fields = ["ru_fio", "gender", "ru_org", "en_org", "ru_position", "en_position"]
        out: Dict[str, str] = {}
        for idx, part in enumerate(parts[: len(fields)]):
            out[fields[idx]] = part
        if len(parts) == 2:
            out = {"ru_fio": parts[0], "ru_org": parts[1]}
        return out

    def _is_valid_en_organization(self, value: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z0-9&.,'\- ]+", value))

    def _is_valid_ru_organization(self, value: str) -> bool:
        return bool(re.fullmatch(r"[А-Яа-яЁё0-9«»\"'().,\- ]+", value))

    def _validate_card(self, card: Card) -> List[str]:
        notes = []
        if not card.surname_ru and not card.family_name:
            notes.append("Фамилия RU обязательна")
        if not card.name_ru and not card.first_name:
            notes.append("Имя RU обязательно")
        if not card.gender or card.gender not in {"М", "Ж"}:
            notes.append("Пол должен быть М или Ж")
        if not card.ru_org:
            notes.append("Организация RU обязательна")
        if not card.en_org:
            notes.append("Organization EN обязательна")
        if not card.ru_position:
            notes.append("Должность RU обязательна")
        if not card.en_position:
            notes.append("Position EN обязательна")
        if card.middle_name_en and not card.patronymic_ru:
            card.patronymic_ru = self.transliterate_en_to_ru(card.middle_name_en)
            notes.append("Middle name RU: транслитерирован с английского, требует проверки")
        if card.en_org and not self._is_valid_en_organization(card.en_org):
            notes.append("Organization EN: нарушены правила написания")
        if card.ru_org and not self._is_valid_ru_organization(card.ru_org):
            notes.append("Организация RU: нарушены правила написания")
        return notes

    def _normalize_gender(self, value: str) -> str:
        norm = value.strip().lower()
        mapping = {
            "м": "М", "муж": "М", "мужской": "М", "m": "М", "male": "М",
            "ж": "Ж", "жен": "Ж", "женский": "Ж", "f": "Ж", "female": "Ж",
        }
        return mapping.get(norm, value.strip().upper())

    def _extract_opf_any_ru(self, text: str) -> Tuple[str, str]:
        parts = text.split()
        if not parts:
            return "", ""
        if parts[0] in RU_TO_EN_OPF:
            return parts[0], " ".join(parts[1:])
        if parts[-1] in RU_TO_EN_OPF:
            return parts[-1], " ".join(parts[:-1])
        return "", text

    def _extract_opf_any_en(self, text: str) -> Tuple[str, str]:
        parts = text.split()
        if not parts:
            return "", ""
        first = parts[0].upper()
        last = parts[-1].upper()
        if first in EN_TO_RU_OPF:
            return first, " ".join(parts[1:])
        if last in EN_TO_RU_OPF:
            return last, " ".join(parts[:-1])
        return "", text

    def _strip_punct(self, text: str, russian: bool) -> str:
        allowed = "A-Za-z0-9А-Яа-яЁё -"
        return re.sub(rf"[^{allowed}]", "", text)

    def _strip_position(self, text: str) -> str:
        return re.sub(r"[\"'();.]", "", text)

    def _transliterate_token(self, token: str) -> str:
        if not token:
            return ""
        out = "".join(PASSPORT_MAP.get(ch, PASSPORT_MAP.get(ch.upper(), ch)) for ch in token)
        if not out:
            return ""
        return out[:1].upper() + out[1:].lower()

    def _has_double_name(self, value: str) -> bool:
        compact = value.lower()
        return bool(re.search(r"\b(или|aka|\/|-)\b", compact) and " " in compact)

    def _contains_non_ascii_letters(self, text: str) -> bool:
        return any(ord(ch) > 127 and ch.isalpha() for ch in text)

    def _ascii_sanitize(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKD", text)
        return "".join(ch for ch in normalized if ord(ch) < 128)

    def _title_case_en(self, text: str) -> str:
        words = self._normalize_spaces(text).split()
        out: List[str] = []
        for i, w in enumerate(words):
            wl = w.lower()
            if i > 0 and wl in EN_SMALL_WORDS:
                out.append(wl)
            elif w.isupper() and len(w) <= 5:
                out.append(w)
            else:
                out.append(w[:1].upper() + w[1:].lower())
        return " ".join(out)

    def _title_case_en_positions(self, text: str) -> str:
        chunks = [self._normalize_spaces(c) for c in text.split(",") if self._normalize_spaces(c)]
        fixed = []
        for chunk in chunks:
            words = []
            for i, w in enumerate(chunk.split()):
                if w.upper() in EN_POSITION_ABBREVIATIONS:
                    words.append(w.upper())
                elif w.isupper() and len(w) <= 6:
                    words.append(w)
                elif i > 0 and w.lower() in EN_SMALL_WORDS:
                    words.append(w.lower())
                else:
                    words.append(w[:1].upper() + w[1:].lower())
            fixed.append(" ".join(words))
        return ", ".join(fixed)

    def _sentence_case_ru_list(self, text: str) -> str:
        chunks = [self._normalize_spaces(c) for c in text.split(",") if self._normalize_spaces(c)]
        out = []
        for c in chunks:
            out.append(c[:1].upper() + c[1:].lower() if c else c)
        return ", ".join(out)

    def _calc_status(self, notes: Iterable[str]) -> str:
        notes = list(notes)
        if any("обяз" in n.lower() or "только" in n.lower() or "должен" in n.lower() for n in notes):
            return "Ошибка формата"
        if notes:
            return "Нужно проверить"
        return "OK"

    def _unique(self, notes: Iterable[str]) -> List[str]:
        return list(dict.fromkeys(n for n in notes if n))

    def _normalize_spaces(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _log_change(self, action: str, before: Card | None, after: Card) -> None:
        payload = {
            "time": self._now(),
            "action": action,
            "before": asdict(before) if before else None,
            "after": asdict(after),
        }
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_cli() -> None:
    bot = CardBot()
    print("Введите поля через ';': ФИО; Пол; Организация RU; Organization EN; Должность RU; Position EN")
    while True:
        raw = input("> ").strip()
        if raw.lower() in {"exit", "quit", "q"}:
            break
        card = bot.create_card(raw)
        print(bot.render_card(card))


if __name__ == "__main__":
    run_cli()
