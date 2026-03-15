from pathlib import Path

p = Path("src/native_app.py")
lines = p.read_text(encoding="utf-8").splitlines()
repl = {
    1405: '                    "Учредители",',
    1406: '                    "Лицензии",',
    1407: '                    "Связи",',
    1408: '                    "Арбитраж",',
    1409: '                    "Суды общей юрисдикции",',
    1410: '                    "Исполнительные производства",',
    1411: '                    "Подробнее",',
}
for ln, val in repl.items():
    if ln - 1 >= len(lines):
        raise RuntimeError(f"line {ln} out of range")
    lines[ln - 1] = val
p.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("patched")
