import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def app(tmp_path):
    with patch("main.ScrapeClient", return_value=MagicMock()):
        from main import CompanyWebApp

        return CompanyWebApp(db_path=str(tmp_path / "test.db"))


def fns_hit(inn="7707083893", surname="", name="", position="Председатель правления"):
    return {
        "source": "ФНС ЕГРЮЛ",
        "data": {
            "inn": inn,
            "ru_org": "Сбербанк ПАО",
            "en_org": "Sberbank PJSC",
            "surname_ru": surname,
            "name_ru": name,
            "middle_name_ru": "",
            "gender": "М",
            "ru_position": position,
        },
    }


def zachest_hit(surname="Греф", name="Герман", middle="Оскарович"):
    return {
        "source": "zachestnyibiznes.ru",
        "data": {
            "surname_ru": surname,
            "name_ru": name,
            "middle_name_ru": middle,
            "ru_org": "ПАО СБЕРБАНК",
            "inn": "7707083893",
        },
    }
