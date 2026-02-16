from __future__ import annotations

import csv
import json
import re
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

# "Как принято у нас" — внутренние исключения, которые имеют приоритет над общими правилами.
ORG_EXCEPTIONS_RU_TO_EN: Dict[str, str] = {
    'ПАО "Сбербанк"': 'Sberbank PJSC',
    'ГК "Росатом"': 'Rosatom State Corporation',
}
ORG_EXCEPTIONS_EN_TO_RU: Dict[str, str] = {v: k for k, v in ORG_EXCEPTIONS_RU_TO_EN.items()}

PASSPORT_MAP = {
    "А": "A",
    "Б": "B",
    "В": "V",
    "Г": "G",
    "Д": "D",
    "Е": "E",
    "Ё": "E",
    "Ж": "ZH",
    "З": "Z",
    "И": "I",
    "Й": "I",
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
    "Ъ": "IE",
    "Ы": "Y",
    "Ь": "",
    "Э": "E",
    "Ю": "IU",
    "Я": "IA",
}

EN_MULTI_TO_RU = {
    "SHCH": "Щ",
    "ZH": "Ж",
    "KH": "Х",
    "TS": "Ц",
    "CH": "Ч",
    "SH": "Ш",
    "YU": "Ю",
    "YA": "Я",
    "YO": "Ё",
    "YE": "Е",  # неоднозначно для обратной конвертации
}
EN_SINGLE_TO_RU = {
    "A": "А",
    "B": "Б",
    "C": "К",
    "D": "Д",
    "E": "Е",  # неоднозначно
    "F": "Ф",
    "G": "Г",
    "H": "Х",
    "I": "И",
    "J": "ДЖ",
    "K": "К",
    "L": "Л",
    "M": "М",
    "N": "Н",
    "O": "О",
    "P": "П",
    "Q": "К",
    "R": "Р",
    "S": "С",
    "T": "Т",
    "U": "У",
    "V": "В",
    "W": "В",
    "X": "КС",
    "Y": "Й",
    "Z": "З",
}
AMBIGUOUS_MARKERS = {"E", "YE", "YO", "Y", "'", "`"}


@dataclass
class Card:
    ru_fio: str = ""
    en_fio: str = ""
    ru_org: str = ""
    en_org: str = ""
    status: str = "готово"
    quality_notes: List[str] = field(default_factory=list)


class CardBot:
    def __init__(self, log_path: str = "card_changes.log") -> None:
        self.log_path = Path(log_path)
        self.cards: List[Card] = []

    def create_card(self, text: str) -> Card:
        fio, org = self._split_input(text)
        has_cyrillic = bool(re.search(r"[А-Яа-яЁё]", text))

        if has_cyrillic:
            card = Card(ru_fio=fio, ru_org=org)
            card.en_fio = self.transliterate_ru_to_en_fio(card.ru_fio)
            card.en_org = self.convert_org_ru_to_en(card.ru_org)
        else:
            card = Card(en_fio=fio, en_org=org)
            card.ru_fio, ambiguous = self.transliterate_en_to_ru_fio(card.en_fio)
            card.ru_org, org_ambiguous = self.convert_org_en_to_ru(card.en_org)
            if ambiguous or org_ambiguous:
                card.status = "нужно проверить"

        card = self._normalize(card)
        self.cards.append(card)
        self._log_change("create", None, card)
        return card

    def apply_edit(self, card: Card, **updates: str) -> Card:
        before = Card(**asdict(card))
        for key, value in updates.items():
            if hasattr(card, key):
                setattr(card, key, value.strip())
        self._log_change("edit", before, card)
        return card

    def confirm_card(self, card: Card) -> Card:
        before = Card(**asdict(card))
        card.status = "подтверждено"
        self._log_change("confirm", before, card)
        return card

    def export_csv(self, destination: str) -> Path:
        path = Path(destination)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ru_fio",
                    "en_fio",
                    "ru_org",
                    "en_org",
                    "status",
                    "quality_notes",
                ],
            )
            writer.writeheader()
            for card in self.cards:
                row = asdict(card)
                row["quality_notes"] = "; ".join(card.quality_notes)
                writer.writerow(row)
        return path

    def render_card(self, card: Card) -> str:
        notes = "; ".join(card.quality_notes) if card.quality_notes else "—"
        return (
            "[RU]\n"
            f"ФИО: {card.ru_fio}\n"
            f"Организация: {card.ru_org}\n\n"
            "[EN]\n"
            f"Name: {card.en_fio}\n"
            f"Organization: {card.en_org}\n\n"
            f"Статус: {card.status}\n"
            f"Качество: {notes}"
        )

    def convert_org_ru_to_en(self, org: str) -> str:
        org = self._normalize_spaces(org)
        if org in ORG_EXCEPTIONS_RU_TO_EN:
            return ORG_EXCEPTIONS_RU_TO_EN[org]

        opf, rest = self._extract_opf_ru(org)
        if opf and opf in RU_TO_EN_OPF:
            return f"{RU_TO_EN_OPF[opf]} {self._transliterate_free_text(rest)}".strip()
        return self._transliterate_free_text(org)

    def convert_org_en_to_ru(self, org: str) -> Tuple[str, bool]:
        org = self._normalize_spaces(org)
        if org in ORG_EXCEPTIONS_EN_TO_RU:
            return ORG_EXCEPTIONS_EN_TO_RU[org], False

        opf, rest = self._extract_opf_en(org)
        ambiguous = False
        ru_name = self._transliterate_en_to_ru_wordwise(rest)
        if opf and opf in EN_TO_RU_OPF:
            result = f"{EN_TO_RU_OPF[opf]} {ru_name}".strip()
        else:
            result = self._transliterate_en_to_ru_wordwise(org)

        if re.search(r"\b(E|YE|YO)\b", org.upper()) or "Y" in org.upper():
            ambiguous = True
        return result, ambiguous

    def transliterate_ru_to_en_fio(self, fio: str) -> str:
        return " ".join(self._transliterate_token(tok) for tok in fio.split())

    def transliterate_en_to_ru_fio(self, fio: str) -> Tuple[str, bool]:
        ambiguous = self._contains_ambiguity(fio)
        return self._transliterate_en_to_ru_wordwise(fio), ambiguous

    def _normalize(self, card: Card) -> Card:
        card.ru_fio = self._title_case_ru(card.ru_fio)
        card.en_fio = self._title_case_en(card.en_fio)
        card.ru_org = self._normalize_spaces(card.ru_org)
        card.en_org = self._normalize_spaces(card.en_org)

        if card.status != "нужно проверить":
            card.status = "готово"
        if card.status == "нужно проверить" and "обратная транслитерация неоднозначна" not in card.quality_notes:
            card.quality_notes.append("обратная транслитерация неоднозначна")
        return card

    def _split_input(self, text: str) -> Tuple[str, str]:
        parts = [p.strip() for p in re.split(r"[\n;|]+", text) if p.strip()]
        if len(parts) < 2:
            raise ValueError("Ожидается ввод в формате: ФИО + организация")
        return parts[0], parts[1]

    def _extract_opf_ru(self, org: str) -> Tuple[str, str]:
        m = re.match(r"^([А-ЯЁ]{2,5})\s+(.*)$", org)
        if not m:
            return "", org
        return m.group(1), m.group(2)

    def _extract_opf_en(self, org: str) -> Tuple[str, str]:
        m = re.match(r"^([A-Z]{2,5})\s+(.*)$", org)
        if not m:
            return "", org
        return m.group(1), m.group(2)

    def _transliterate_token(self, token: str) -> str:
        up = token.upper()
        out = "".join(PASSPORT_MAP.get(ch, ch) for ch in up)
        return out.capitalize()

    def _transliterate_free_text(self, text: str) -> str:
        return " ".join(self._transliterate_token(part) for part in text.split())

    def _transliterate_en_to_ru_wordwise(self, text: str) -> str:
        return " ".join(self._transliterate_en_to_ru_word(part) for part in text.split())

    def _transliterate_en_to_ru_word(self, word: str) -> str:
        source = re.sub(r"[^A-Za-z]", "", word).upper()
        i = 0
        out = []
        while i < len(source):
            matched = False
            for k, v in sorted(EN_MULTI_TO_RU.items(), key=lambda x: len(x[0]), reverse=True):
                if source.startswith(k, i):
                    out.append(v)
                    i += len(k)
                    matched = True
                    break
            if matched:
                continue
            ch = source[i]
            out.append(EN_SINGLE_TO_RU.get(ch, ch))
            i += 1
        return "".join(out).capitalize()

    def _contains_ambiguity(self, text: str) -> bool:
        upper = text.upper()
        return any(marker in upper for marker in AMBIGUOUS_MARKERS)

    def _normalize_spaces(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _title_case_ru(self, text: str) -> str:
        return " ".join(w[:1].upper() + w[1:].lower() for w in text.split())

    def _title_case_en(self, text: str) -> str:
        return " ".join(w[:1].upper() + w[1:].lower() for w in text.split())

    def _log_change(self, action: str, before: Card | None, after: Card) -> None:
        payload = {
            "time": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "before": asdict(before) if before else None,
            "after": asdict(after),
        }
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_cli() -> None:
    bot = CardBot()
    print("Введите карточку в формате: ФИО; организация")
    while True:
        raw = input("> ").strip()
        if raw.lower() in {"exit", "quit", "q"}:
            break
        card = bot.create_card(raw)
        print(bot.render_card(card))
        action = input("Подтвердить (y), редактировать (e), пропустить (Enter): ").strip().lower()
        if action == "y":
            bot.confirm_card(card)
            print("Сохранено как подтверждено.")
        elif action == "e":
            field = input("Поле для правки (ru_fio/en_fio/ru_org/en_org): ").strip()
            value = input("Новое значение: ").strip()
            bot.apply_edit(card, **{field: value})
            print(bot.render_card(card))

        export = input("Экспортировать CSV сейчас? (y/N): ").strip().lower()
        if export == "y":
            path = bot.export_csv("cards_export.csv")
            print(f"CSV сохранен: {path}")


if __name__ == "__main__":
    run_cli()
