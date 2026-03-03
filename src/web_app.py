import os
import sys
import traceback
from datetime import datetime

from main import *  # noqa: F401,F403

LOG_PATH = os.path.join(
    os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__),
    "runtime.log",
)


def log(message: str) -> None:
    with open(LOG_PATH, "a", encoding="utf-8") as file:
        file.write(f"[{datetime.now()}] {message}\n")


try:
    log("APP START")
except Exception:
    pass


if __name__ == "__main__":
    try:
        run_server()
    except Exception:
        try:
            log("Unhandled exception:\n" + traceback.format_exc())
        except Exception:
            pass
        raise
