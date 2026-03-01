from __future__ import annotations

from typing import Dict

RU_TO_EN_OPF: Dict[str, str] = {
    "ООО": "LLC",
    "АО": "JSC",
    "ПАО": "PJSC",
    "АНО": "ANO",
    "ФГБУ": "FSBI",
    "ФГБУК": "FGBIC",
    "ФГАОУ ВО": "FSAEI HE",
    "ФГБОУ ВО": "FSBEI HE",
    "СПБ ГБУЗ": "SPb SBIH",
    "ГБУЗ": "SBIH",
    "ОАО": "OJSC",
    "ИП": "IE",
    "МУП": "MUE",
    "МАУ": "MAI",
    "ЧУ": "PI",
}

PASSPORT_MAP: Dict[str, str] = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "YO", "Ж": "ZH", "З": "Z",
    "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M", "Н": "N", "О": "O", "П": "P", "Р": "R",
    "С": "S", "Т": "T", "У": "U", "Ф": "F", "Х": "KH", "Ц": "TS", "Ч": "CH", "Ш": "SH", "Щ": "SHCH",
    "Ъ": "", "Ы": "Y", "Ь": "", "Э": "E", "Ю": "YU", "Я": "YA",
}

POSITION_TRANSLATIONS: Dict[str, str] = {
    "Ректор": "Rector",
    "Президент": "President",
    "Председатель правления": "Chairman of the Board",
    "Президент, Председатель правления": "President, Chairman of the Board",
    "Генеральный директор": "CEO",
    "Директор": "Director",
    "Министр": "Minister",
    "Губернатор": "Governor",
}
