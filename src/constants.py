from __future__ import annotations

from typing import Dict

BASE_MODE_PROVIDER_KINDS = ["egrul"]
EXTENDED_MODE_PROVIDER_KINDS = [
    "egrul",
    "wikipedia_html",
    "duckduckgo_html",
    "rusprofile",
    "zachestnyibiznes_scrape",
    "checko",
    "kontur",
    "rbc_companies_scrape",
    "tbank_leadership_scrape",
]

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
    "ректор": "Rector",
    "президент": "President",
    "председатель правления": "Chairman of the Management Board",
    "президент, председатель правления": "President, Chairman of the Management Board",
    "генеральный директор": "General Director",
    "генеральный директор (гендиректор)": "General Director",
    "директор": "Director",
    "исполнительный директор": "Executive Director",
    "финансовый директор": "Chief Financial Officer",
    "коммерческий директор": "Commercial Director",
    "технический директор": "Chief Technical Officer",
    "директор по информационным технологиям": "Chief Information Officer",
    "председатель совета директоров": "Chairman of the Board of Directors",
    "первый заместитель генерального директора": "First Deputy General Director",
    # Исполняющие обязанности
    "исполняющий обязанности директора": "Acting Director",
    "исполняющий обязанности генерального директора": "Acting General Director",
    "исполняющий обязанности ректора": "Acting Rector",
    "исполняющий обязанности": "Acting",
    # Врачи и медицина
    "главный врач": "Chief Medical Officer",
    "заместитель главного врача": "Deputy Chief Medical Officer",
    # Заместители
    "заместитель генерального директора": "Deputy General Director",
    "заместитель директора": "Deputy Director",
    "заместитель председателя": "Deputy Chairman",
    "заместитель руководителя": "Deputy Head",
    "заместитель ректора": "Deputy Rector",
    # Академические
    "проректор": "Vice-Rector",
    "декан": "Dean",
    # Общие
    "председатель": "Chairman",
    "управляющий": "Managing Director",
    "вице-президент": "Vice President",
    "руководитель": "Head",
    "начальник": "Head",
    "министр": "Minister",
    "губернатор": "Governor",
}
