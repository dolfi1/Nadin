import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def app(tmp_path):
    from main import CompanyWebApp

    return CompanyWebApp(db_path=str(tmp_path / "test.db"))
