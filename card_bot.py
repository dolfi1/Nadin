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
    "А": "A",
    "Б": "B",
    "В": "V",
    "Г": "G",
    "Д": "D",
    "Е": "E",
    "Ё": "YO",
    "Ж": "ZH",
    "З": "Z",
    "И": "I",
    "Й": "Y",
    "К": "K",
    "Л": "L",
    "М": "M",
    "Н": "N",
    "О": "O",
    "П": "P",
    "Р": "R",
    "С": "S",
    "Т": "T",
    "У": "U",
    "Ф": "F",
    "Х": "KH",
    "Ц": "TS",
    "Ч": "CH",
    "Ш": "SH",
    "Щ": "SHCH",
    "Ъ": "",
    "Ы": "Y",
    "Ь": "",
    "Э": "E",
    "Ю": "YU",
    "Я": "YA",
}

EN_SMALL_WORDS = {"a", "an", "the", "and", "or", "of", "for", "to", "in", "on", "at", "by"}


@dataclass
class Card:
    ru_fio: str = ""
    en_fio: str = ""
    surname_ru: str = ""
    name_ru: str = ""
    patronymic_ru: str = ""
    surname_en: str = ""
    name_en: str = ""
    patronymic_en: str = ""
    gender: str = ""
    ru_org: str = ""
    en_org: str = ""
    ru_position: str = ""
    en_position: str = ""
    status: str = "OK"
    quality_notes: List[str] = field(default_factory=list)
    created_at: str = ""
    created_by: str = "system"
    confirmed_at: str = ""
    confirmed_by: str = ""


class CardBot:
    def __init__(self, log_path: str = "card_changes.log") -> None:
        self.log_path = Path(log_path)
        self.cards: List[Card] = []

    def create_card(self, text: str, created_by: str = "system") -> Card:
        data = self._parse_user_input(text)
        card = Card(created_by=created_by, created_at=self._now())
        card.ru_fio = self._normalize_spaces(data.get("ru_fio", ""))
        card.gender = self._normalize_gender(data.get("gender", ""))
        card.ru_org = self._normalize_spaces(data.get("ru_org", ""))
        card.en_org = self._normalize_spaces(data.get("en_org", ""))
        card.ru_position = self._normalize_spaces(data.get("ru_position", ""))
        card.en_position = self._normalize_spaces(data.get("en_position", ""))

        if not card.ru_fio and data.get("en_fio"):
            card.en_fio = self._normalize_spaces(data["en_fio"])
        self._build_fio(card)

        card.ru_org, ru_org_notes = self.normalize_ru_org(card.ru_org)
        card.en_org, en_org_notes = self.normalize_en_org(card.en_org)

        card.ru_position, ru_pos_notes = self.normalize_ru_position(card.ru_position)
        card.en_position, en_pos_notes = self.normalize_en_position(card.en_position)

        notes = []
        notes.extend(self._validate_required(card))
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
                setattr(card, key, self._normalize_spaces(value))

        self._build_fio(card)
        card.ru_org, ru_org_notes = self.normalize_ru_org(card.ru_org)
        card.en_org, en_org_notes = self.normalize_en_org(card.en_org)
        card.ru_position, ru_pos_notes = self.normalize_ru_position(card.ru_position)
        card.en_position, en_pos_notes = self.normalize_en_position(card.en_position)
        notes = []
        notes.extend(self._validate_required(card))
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
                    "surname_ru",
                    "name_ru",
                    "patronymic_ru",
                    "surname_en",
                    "name_en",
                    "patronymic_en",
                    "gender",
                    "ru_org",
                    "en_org",
                    "ru_position",
                    "en_position",
                    "status",
                    "quality_notes",
                    "created_by",
                    "created_at",
                    "confirmed_by",
                    "confirmed_at",
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
            f"Организация: {card.ru_org}\n"
            f"Должность: {card.ru_position}\n\n"
            "[EN]\n"
            f"Surname: {card.surname_en}\n"
            f"Name: {card.name_en}\n"
            f"Patronymic: {card.patronymic_en or '—'}\n"
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

    def normalize_en_org(self, org: str) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_punct(org, russian=False)
        if cleaned != org:
            notes.append("EN organization: quotes/punctuation removed")

        opf, name = self._extract_opf_any_en(cleaned)
        name = self._title_case_en(name)
        if name.startswith("The "):
            notes.append("EN organization: The в начале запрещен")
        if self._contains_non_ascii_letters(name):
            notes.append("EN organization: диакритика/не-ASCII запрещены")
            name = self._ascii_sanitize(name)
        if opf:
            cleaned = f"{name} {opf}".strip()
        else:
            cleaned = name
            notes.append("EN organization: OPF should be at the end")

        return cleaned, self._unique(notes)

    def normalize_ru_position(self, text: str) -> Tuple[str, List[str]]:
        notes: List[str] = []
        cleaned = self._strip_position(text)
        if cleaned != text:
            notes.append("RU должность: запрещенные символы удалены")
        if cleaned and not re.fullmatch(r"[А-Яа-яЁё\- ,]+", cleaned):
            notes.append("RU должность: только русский язык")
        if re.search(r"\b[А-ЯЁ]{2,}\b", cleaned):
            notes.append("RU должность: сокращения запрещены")
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

    def _build_fio(self, card: Card) -> None:
        if card.ru_fio:
            ru_parts = self._split_fio(card.ru_fio)
            card.surname_ru, card.name_ru, card.patronymic_ru = ru_parts
            card.surname_en = self._transliterate_token(card.surname_ru)
            card.name_en = self._transliterate_token(card.name_ru)
            card.patronymic_en = self._transliterate_token(card.patronymic_ru) if card.patronymic_ru else ""
            card.en_fio = " ".join(x for x in [card.surname_en, card.name_en, card.patronymic_en] if x)
            card.ru_fio = " ".join(x for x in [card.surname_ru, card.name_ru, card.patronymic_ru] if x)
        elif card.en_fio:
            en_parts = self._split_fio(card.en_fio)
            card.surname_en, card.name_en, card.patronymic_en = en_parts
            card.en_fio = " ".join(x for x in [card.surname_en, card.name_en, card.patronymic_en] if x)

    def _split_fio(self, fio: str) -> Tuple[str, str, str]:
        parts = [p for p in self._normalize_spaces(fio).split(" ") if p]
        if len(parts) < 2:
            return (parts[0] if parts else "", "", "")
        if len(parts) == 2:
            return parts[0].title(), parts[1].title(), ""
        return parts[0].title(), parts[1].title(), parts[2].title()

    def _parse_user_input(self, text: str) -> Dict[str, str]:
        text = text.strip()
        if not text:
            return {}

        # Labeled block mode
        labels = {
            "фио": "ru_fio",
            "пол": "gender",
            "организация": "ru_org",
            "organization": "en_org",
            "должность": "ru_position",
            "position": "en_position",
            "name": "en_fio",
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

        # Compact one-line mode
        parts = [p.strip() for p in re.split(r"[;|]+", text) if p.strip()]
        fields = ["ru_fio", "gender", "ru_org", "en_org", "ru_position", "en_position"]
        out: Dict[str, str] = {}
        for idx, part in enumerate(parts[: len(fields)]):
            out[fields[idx]] = part
        if len(parts) == 2:
            out = {"ru_fio": parts[0], "ru_org": parts[1]}
        return out

    def _validate_required(self, card: Card) -> List[str]:
        notes = []
        if not card.surname_ru or not card.name_ru:
            notes.append("ФИО: фамилия и имя обязательны")
        if not card.gender:
            notes.append("Пол обязателен")
        if not card.ru_org:
            notes.append("Организация RU обязательна")
        if not card.en_org:
            notes.append("Organization EN обязательна")
        if not card.ru_position:
            notes.append("Должность RU обязательна")
        if not card.en_position:
            notes.append("Position EN обязательна")
        return notes

    def _normalize_gender(self, value: str) -> str:
        norm = value.strip().lower()
        mapping = {
            "м": "М",
            "муж": "М",
            "мужской": "М",
            "m": "М",
            "male": "М",
            "ж": "Ж",
            "жен": "Ж",
            "женский": "Ж",
            "f": "Ж",
            "female": "Ж",
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
                if w.isupper() and len(w) <= 6:
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
