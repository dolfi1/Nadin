
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import site
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse


_OPTIONAL_DEPS_DIR = Path(__file__).resolve().parents[1] / ".deps"
if _OPTIONAL_DEPS_DIR.exists():
    _optional_path = str(_OPTIONAL_DEPS_DIR)
    if _optional_path not in sys.path:
        sys.path.append(_optional_path)

try:
    _USER_SITE = site.getusersitepackages()
except Exception:  # noqa: BLE001
    _USER_SITE = ""
try:
    _USER_SITE_EXISTS = bool(_USER_SITE) and Path(_USER_SITE).exists()
except OSError:
    _USER_SITE_EXISTS = False
if _USER_SITE_EXISTS and _USER_SITE not in sys.path:
    site.addsitedir(_USER_SITE)

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:  # noqa: BLE001
    Image = None
    ImageDraw = None
    ImageFont = None

try:
    import fitz
except Exception:  # noqa: BLE001
    fitz = None


def _configure_tk_env_for_frozen() -> None:
    if not getattr(sys, "frozen", False):
        return

    roots: list[Path] = []
    meipass = getattr(sys, "_MEIPASS", "")
    if meipass:
        roots.append(Path(meipass))

    exe_dir = Path(sys.executable).resolve().parent
    roots.append(exe_dir / "_internal")
    roots.append(exe_dir)

    tcl_dirs = ("_tcl_data", "tcl8.6", "tcl8", "tcl")
    tk_dirs = ("_tk_data", "tk8.6", "tk8", "tk")

    tcl_candidate: Path | None = None
    tk_candidate: Path | None = None

    for root in roots:
        if not root.exists():
            continue

        if tcl_candidate is None:
            for folder in tcl_dirs:
                candidate = root / folder
                if (candidate / "init.tcl").exists():
                    tcl_candidate = candidate
                    break

        if tk_candidate is None:
            for folder in tk_dirs:
                candidate = root / folder
                if (candidate / "tk.tcl").exists() or (candidate / "ttk" / "ttk.tcl").exists():
                    tk_candidate = candidate
                    break

        if tcl_candidate is not None and tk_candidate is not None:
            break

    if tcl_candidate is not None:
        os.environ["TCL_LIBRARY"] = str(tcl_candidate)
    if tk_candidate is not None:
        os.environ["TK_LIBRARY"] = str(tk_candidate)


_configure_tk_env_for_frozen()

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from app_paths import configure_runtime_env
from logging_setup import setup_logging
from main import CompanyWebApp, SHORT_BRAND_BANK_HINTS, SHORT_BRAND_KNOWN_INN

logger = logging.getLogger(__name__)


class NativeNadinApp(tk.Tk):
    CARD_FIELDS: list[tuple[str, str]] = [
        ("Титул", "title"),
        ("Обращение", "appeal"),
        ("Family name", "family_name"),
        ("First name", "first_name"),
        ("Middle name (EN)", "middle_name_en"),
        ("Фамилия", "surname_ru"),
        ("Имя", "name_ru"),
        ("Middle name. рус", "middle_name_ru"),
        ("Пол", "gender"),
        ("ИНН", "inn"),
        ("Организация", "ru_org"),
        ("Organization", "en_org"),
        ("Должность", "ru_position"),
        ("Position", "en_position"),
    ]

    def __init__(self, engine: CompanyWebApp) -> None:
        super().__init__()
        self.engine = engine
        self.title("Nadin")
        self.geometry("1260x840")
        self.minsize(1040, 680)

        self.candidates: list[dict[str, Any]] = []
        self._busy = False
        self._screenshot_busy = False
        self._suppress_variant_event = False
        self._last_autofill_key = ""
        self._current_card_rows: list[tuple[str, str]] = []
        self._active_search_token = 0
        self._active_autofill_token = 0
        self._card_drag_anchor: str | None = None
        self._entry_undo_state: dict[str, tuple[str, int, int]] = {}

        self._last_source_url = ""
        self._pending_source_url = ""
        self._last_screenshot_path = ""
        self._screenshot_preview_image: tk.PhotoImage | None = None

        self._last_profile_inn = ""
        self._last_profile_ogrn = ""
        self._last_profile_org = ""
        self._last_source_names: list[str] = []
        self._rusprofile_url_cache: dict[str, str] = {}
        self._fns_pdf_target_cache: dict[str, tuple[str, str, float]] = {}

        app_data_dir = Path(os.getenv("APP_DATA_DIR", os.getcwd()))
        self._screenshot_dir = app_data_dir / "screenshots"
        self._screenshot_dir.mkdir(parents=True, exist_ok=True)

        self.surname_var = tk.StringVar()
        self.name_var = tk.StringVar()
        self.middle_var = tk.StringVar()
        self.inn_var = tk.StringVar()
        self.company_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Готово")
        self.card_title_var = tk.StringVar(value="Карточка")
        self.screenshot_meta_var = tk.StringVar(value="Скриншот источника: —")
        self.source_url_var = tk.StringVar(value="URL источника: —")

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill=tk.X)

        fields = [
            ("Фамилия", self.surname_var),
            ("Имя", self.name_var),
            ("Отчество", self.middle_var),
            ("ИНН", self.inn_var),
            ("Компания", self.company_var),
        ]
        for col, (label, var) in enumerate(fields):
            ttk.Label(top, text=label).grid(row=0, column=col, sticky="w", padx=(0, 8))
            entry = ttk.Entry(top, textvariable=var, width=22)
            entry.grid(row=1, column=col, sticky="ew", padx=(0, 8))
            self._bind_entry_shortcuts(entry)
            entry.bind("<Return>", self._on_enter_pressed)
            entry.bind("<KP_Enter>", self._on_enter_pressed)
            top.columnconfigure(col, weight=1)

        action_frame = ttk.Frame(self, padding=(10, 0, 10, 8))
        action_frame.pack(fill=tk.X)
        self.search_button = ttk.Button(action_frame, text="Найти", command=self._search)
        self.search_button.pack(side=tk.LEFT)

        self.copy_card_button = ttk.Button(action_frame, text="Копировать карточку", command=self._copy_full_card)
        self.copy_card_button.pack(side=tk.LEFT, padx=(8, 0))

        self.screenshot_button = ttk.Button(action_frame, text="Скриншот источника", command=self._capture_source_screenshot_manual)
        self.screenshot_button.pack(side=tk.LEFT, padx=(8, 0))

        self.download_screenshot_button = ttk.Button(
            action_frame,
            text="Скачать скриншот",
            command=self._download_screenshot,
            state=tk.DISABLED,
        )
        self.download_screenshot_button.pack(side=tk.LEFT, padx=(8, 0))

        self.progress = ttk.Progressbar(action_frame, mode="indeterminate", length=260)
        self.progress.pack(side=tk.LEFT, padx=(16, 0), fill=tk.X)

        split = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        split.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        card_frame = ttk.Frame(split)
        split.add(card_frame, weight=9)
        ttk.Label(card_frame, textvariable=self.card_title_var).pack(anchor="w")

        card_container = ttk.Frame(card_frame)
        card_container.pack(fill=tk.BOTH, expand=True)
        self.card_tree = ttk.Treeview(
            card_container,
            columns=("field", "value"),
            show="headings",
            selectmode="extended",
            height=18,
        )
        self.card_tree.heading("field", text="Поле")
        self.card_tree.heading("value", text="Значение")
        self.card_tree.column("field", width=200, anchor="w", stretch=False)
        self.card_tree.column("value", width=470, anchor="w", stretch=True)
        card_scroll_y = ttk.Scrollbar(card_container, orient="vertical", command=self.card_tree.yview)
        card_scroll_x = ttk.Scrollbar(card_container, orient="horizontal", command=self.card_tree.xview)
        self.card_tree.configure(yscrollcommand=card_scroll_y.set, xscrollcommand=card_scroll_x.set)
        self.card_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        card_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        card_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        card_container.bind("<Configure>", self._on_card_container_resize)
        self.card_tree.bind("<Control-c>", self._copy_selected_card_value)
        self.card_tree.bind("<Control-C>", self._copy_selected_card_value)
        self.card_tree.bind("<Control-a>", self._select_all_card_rows)
        self.card_tree.bind("<Control-A>", self._select_all_card_rows)
        self.card_tree.bind("<Double-1>", self._copy_selected_card_value)
        self.card_tree.bind("<ButtonPress-1>", self._on_card_tree_button_press, add="+")
        self.card_tree.bind("<B1-Motion>", self._on_card_tree_drag, add="+")

        variants_frame = ttk.Frame(split)
        split.add(variants_frame, weight=8)
        ttk.Label(variants_frame, text="Варианты").pack(anchor="w")

        variants_container = ttk.Frame(variants_frame)
        variants_container.pack(fill=tk.BOTH, expand=True)
        self.result_tree = ttk.Treeview(
            variants_container,
            columns=("org", "inn", "source"),
            show="headings",
            height=18,
        )
        self.result_tree.heading("org", text="Компания")
        self.result_tree.heading("inn", text="ИНН")
        self.result_tree.heading("source", text="Источник")
        self.result_tree.column("org", width=430, anchor="w")
        self.result_tree.column("inn", width=130, anchor="center", stretch=False)
        self.result_tree.column("source", width=220, anchor="w", stretch=False)
        variants_scroll_y = ttk.Scrollbar(variants_container, orient="vertical", command=self.result_tree.yview)
        variants_scroll_x = ttk.Scrollbar(variants_container, orient="horizontal", command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=variants_scroll_y.set, xscrollcommand=variants_scroll_x.set)
        self.result_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        variants_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        variants_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        variants_container.bind("<Configure>", self._on_variants_container_resize)
        self.result_tree.bind("<<TreeviewSelect>>", self._on_variant_selected)
        self.result_tree.bind("<Control-c>", self._copy_selected_variant_value)
        self.result_tree.bind("<Control-C>", self._copy_selected_variant_value)

        screenshot_frame = ttk.LabelFrame(self, text="Скриншот источника", padding=(8, 6))
        screenshot_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        self.screenshot_preview_label = ttk.Label(screenshot_frame, text="Превью отсутствует", anchor="center", width=44)
        self.screenshot_preview_label.pack(side=tk.LEFT, padx=(0, 10))

        screenshot_info = ttk.Frame(screenshot_frame)
        screenshot_info.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(screenshot_info, textvariable=self.screenshot_meta_var).pack(anchor="w")
        ttk.Label(screenshot_info, textvariable=self.source_url_var, wraplength=740).pack(anchor="w", pady=(4, 0))

        trace_frame = ttk.Frame(self)
        trace_frame.pack(fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))
        ttk.Label(trace_frame, text="Лог поиска").pack(anchor="w")
        self.trace_text = tk.Text(trace_frame, height=7, wrap=tk.WORD, state=tk.DISABLED)
        self.trace_text.pack(fill=tk.BOTH, expand=False)

        status = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor="w", padding=(8, 4))
        status.pack(fill=tk.X, side=tk.BOTTOM)

        self.bind("<Return>", self._on_enter_pressed)
        self.bind("<KP_Enter>", self._on_enter_pressed)

        self._render_card_rows([], "Карточка")

    def _bind_entry_shortcuts(self, entry: ttk.Entry) -> None:
        shortcuts = [
            ("<Control-c>", "<<Copy>>"),
            ("<Control-C>", "<<Copy>>"),
            ("<Control-Insert>", "<<Copy>>"),
            ("<Control-x>", "<<Cut>>"),
            ("<Control-X>", "<<Cut>>"),
            ("<Shift-Delete>", "<<Cut>>"),
            ("<Control-v>", "<<Paste>>"),
            ("<Control-V>", "<<Paste>>"),
            ("<Shift-Insert>", "<<Paste>>"),
            ("<Control-z>", "<<Undo>>"),
            ("<Control-Z>", "<<Undo>>"),
            ("<Control-a>", "<<SelectAll>>"),
            ("<Control-A>", "<<SelectAll>>"),
        ]
        for sequence, virtual_event in shortcuts:
            entry.bind(sequence, lambda event, ve=virtual_event: self._entry_generate_virtual(event, ve))

        # Layout-independent shortcuts by physical key (Windows keycodes).
        entry.bind("<Control-KeyPress>", self._entry_handle_ctrl_key, add="+")

    def _entry_handle_ctrl_key(self, event: tk.Event) -> str | None:
        keycode = int(getattr(event, "keycode", -1))
        char = str(getattr(event, "char", "")).lower()
        keysym = str(getattr(event, "keysym", "")).lower()

        mapping = {
            67: "<<Copy>>",      # C
            86: "<<Paste>>",     # V
            88: "<<Cut>>",       # X
            90: "<<Undo>>",      # Z
            65: "<<SelectAll>>", # A
        }
        virtual_event = mapping.get(keycode)
        if not virtual_event:
            fallback_by_key = {
                "c": "<<Copy>>",
                "v": "<<Paste>>",
                "x": "<<Cut>>",
                "z": "<<Undo>>",
                "a": "<<SelectAll>>",
            }
            virtual_event = fallback_by_key.get(keysym) or fallback_by_key.get(char)
        if not virtual_event:
            return None
        return self._entry_generate_virtual(event, virtual_event)

    def _entry_generate_virtual(self, event: tk.Event, virtual_event: str) -> str:
        widget = event.widget
        if not isinstance(widget, (tk.Entry, ttk.Entry)):
            return "break"

        if virtual_event == "<<Undo>>":
            self._entry_restore_last_state(widget)
            return "break"

        if virtual_event == "<<SelectAll>>":
            try:
                widget.selection_range(0, tk.END)
                widget.icursor(tk.END)
            except tk.TclError:
                pass
            return "break"

        if virtual_event in {"<<Paste>>", "<<Cut>>"}:
            self._entry_remember_state(widget)

        try:
            widget.event_generate(virtual_event)
        except tk.TclError:
            pass
        return "break"

    def _entry_remember_state(self, widget: tk.Entry | ttk.Entry) -> None:
        value = widget.get()
        try:
            sel_start = int(widget.index("sel.first"))
            sel_end = int(widget.index("sel.last"))
        except tk.TclError:
            cursor = int(widget.index(tk.INSERT))
            sel_start = cursor
            sel_end = cursor
        self._entry_undo_state[str(widget)] = (value, sel_start, sel_end)

    def _entry_restore_last_state(self, widget: tk.Entry | ttk.Entry) -> None:
        snapshot = self._entry_undo_state.get(str(widget))
        if snapshot is None:
            return
        value, sel_start, sel_end = snapshot

        try:
            widget.delete(0, tk.END)
            widget.insert(0, value)
            if sel_end > sel_start:
                widget.selection_range(sel_start, sel_end)
            widget.icursor(sel_end)
        except tk.TclError:
            pass

    def _on_enter_pressed(self, _event: tk.Event | None = None) -> str:
        self._search()
        return "break"

    def _on_card_tree_button_press(self, event: tk.Event) -> None:
        row_id = self.card_tree.identify_row(event.y)
        self._card_drag_anchor = row_id or None

    def _on_card_tree_drag(self, event: tk.Event) -> str:
        if not self._card_drag_anchor:
            return "break"
        row_id = self.card_tree.identify_row(event.y)
        if not row_id:
            return "break"

        children = list(self.card_tree.get_children(""))
        try:
            start = children.index(self._card_drag_anchor)
            end = children.index(row_id)
        except ValueError:
            return "break"

        lo, hi = sorted((start, end))
        self.card_tree.selection_set(children[lo : hi + 1])
        self.card_tree.focus(row_id)
        return "break"

    def _select_all_card_rows(self, _event: object | None = None) -> str:
        rows = list(self.card_tree.get_children(""))
        if rows:
            self.card_tree.selection_set(rows)
            self.card_tree.focus(rows[0])
        return "break"

    def _on_card_container_resize(self, event: tk.Event) -> None:
        total_width = max(int(event.width) - 28, 340)
        field_width = min(240, max(180, int(total_width * 0.34)))
        value_width = max(230, total_width - field_width)
        self.card_tree.column("field", width=field_width, stretch=False)
        self.card_tree.column("value", width=value_width, stretch=True)

    def _on_variants_container_resize(self, event: tk.Event) -> None:
        total_width = max(int(event.width) - 28, 430)
        inn_width = 130
        source_width = 220
        org_width = max(220, total_width - inn_width - source_width)
        self.result_tree.column("org", width=org_width, stretch=True)
        self.result_tree.column("inn", width=inn_width, stretch=False)
        self.result_tree.column("source", width=source_width, stretch=False)

    def _set_busy(self, busy: bool, status: str = "") -> None:
        self._busy = busy
        state = tk.DISABLED if busy else tk.NORMAL
        self.search_button.configure(state=state)
        self.screenshot_button.configure(state=tk.DISABLED if (busy or self._screenshot_busy) else tk.NORMAL)

        download_state = tk.NORMAL if (not busy and not self._screenshot_busy and self._last_screenshot_path) else tk.DISABLED
        self.download_screenshot_button.configure(state=download_state)

        if busy:
            self.progress.start(12)
            self.configure(cursor="watch")
        else:
            self.progress.stop()
            self.configure(cursor="")
        if status:
            self.status_var.set(status)

    def _collect_params(self) -> dict[str, str]:
        return {
            "surname": self.surname_var.get().strip(),
            "name": self.name_var.get().strip(),
            "middle_name": self.middle_var.get().strip(),
            "inn": self.inn_var.get().strip(),
            "company": self.company_var.get().strip(),
            "search_type": "",
        }

    def _search(self) -> None:
        if self._busy:
            return
        params = self._collect_params()
        if not any((params["surname"], params["name"], params["middle_name"], params["inn"], params["company"])):
            messagebox.showwarning("Nadin", "Заполните хотя бы одно поле")
            return

        self._active_search_token += 1
        search_token = self._active_search_token

        self._set_busy(True, "Поиск...")
        self._last_autofill_key = ""

        def worker() -> None:
            error = ""
            source_hits: list[dict[str, Any]] = []
            backend_candidates: list[dict[str, Any]] = []
            trace: list[str] = []
            try:
                source_hits, backend_candidates, trace = self.engine._search_by_criteria(params)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Search failed")
                error = str(exc)
            self.after(0, lambda: self._on_search_done(params, source_hits, backend_candidates, trace, error, search_token))

        threading.Thread(target=worker, daemon=True).start()

    def _brand_known_inn_for_query(self, query: str) -> str:
        token = self.engine._short_brand_token(self.engine._normalize_spaces(query))
        if not token:
            return ""
        return SHORT_BRAND_KNOWN_INN.get(token, "")

    def _candidate_looks_like_company(self, item: dict[str, Any]) -> bool:
        hit_type = self.engine._normalize_spaces(str(item.get("type", ""))).lower()
        if hit_type == "person":
            return False
        org = self.engine._normalize_spaces(str(item.get("org_ru", "")))
        return bool(org)

    def _candidate_identity(self, candidate: dict[str, Any]) -> str:
        inn = self.engine._normalize_spaces(str(candidate.get("inn", "")))
        if inn:
            return f"inn:{inn}"
        org = self.engine._normalize_spaces(str(candidate.get("org_ru", "")).lower())
        src = self.engine._normalize_spaces(str(candidate.get("source", "")).lower())
        return f"org:{org}|src:{src}"

    def _candidate_key(self, candidate: dict[str, Any], query_for_autofill: str = "") -> str:
        query = self.engine._normalize_spaces(query_for_autofill)
        return f"{self._candidate_identity(candidate)}|query:{query}"

    def _normalize_backend_candidate(self, item: dict[str, Any], query: str) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None

        org = self._clean_org_for_list(str(item.get("org_ru", "")), str(item.get("inn", "")))
        if not org:
            return None

        inn = self.engine._normalize_spaces(str(item.get("inn", "")))
        if inn and not re.fullmatch(r"\d{10}|\d{12}", inn):
            inn = ""

        source_name = self.engine._normalize_spaces(str(item.get("source", ""))) or "—"
        known_inn = self._brand_known_inn_for_query(query)
        org_lower = org.lower()

        score = 1000.0
        if known_inn and inn == known_inn:
            score += 500.0
        if known_inn and inn and inn != known_inn and "банк" not in org_lower:
            score -= 180.0

        return {
            "data": {},
            "source": source_name,
            "type": "company",
            "url": "",
            "score": score,
            "fio_ru": self.engine._normalize_spaces(str(item.get("fio_ru", ""))),
            "org_ru": org,
            "position_ru": self.engine._normalize_spaces(str(item.get("position_ru", ""))),
            "inn": inn,
            "query_for_autofill": inn or self.engine._normalize_spaces(str(item.get("query_for_autofill", ""))) or org,
            "revenue": str(item.get("revenue", "") or ""),
            "source_names": [source_name],
        }

    def _build_company_candidates_from_hits(self, source_hits: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        q_norm = self.engine._normalize_spaces(query)
        brand_token = self.engine._short_brand_token(q_norm)
        known_inn = SHORT_BRAND_KNOWN_INN.get(brand_token, "") if brand_token else ""
        bank_context = bool(brand_token and brand_token in SHORT_BRAND_BANK_HINTS)

        for hit in source_hits:
            if not isinstance(hit, dict):
                continue
            if not self.engine._hit_looks_like_company(hit):
                continue

            data = hit.get("data", {}) if isinstance(hit.get("data"), dict) else {}
            org = self._clean_org_for_list(str(data.get("ru_org", "")), str(data.get("inn", "")))
            if not org:
                continue
            if self.engine._is_garbage_org_title(org, q_norm):
                continue

            inn = self.engine._normalize_spaces(str(data.get("inn", "")))
            if inn and not re.fullmatch(r"\d{10}|\d{12}", inn):
                inn = ""

            org_lower = org.lower()
            is_bank_brand_title = bool(bank_context and brand_token and brand_token in org_lower and "банк" in org_lower)
            if known_inn and is_bank_brand_title and not inn:
                inn = known_inn

            source_name = self.engine._normalize_spaces(str(hit.get("source", ""))) or "?"
            score = float(self.engine._score_hit(hit, q_norm))

            if known_inn and inn == known_inn:
                score += 500.0
            if known_inn and inn and inn != known_inn and (bank_context or (brand_token and brand_token in org_lower)):
                score -= 220.0
            if bank_context:
                if is_bank_brand_title:
                    score += 220.0
                elif "банк" in org_lower:
                    score += 130.0
                elif brand_token and brand_token in org_lower:
                    score -= 180.0

            key = inn or f"org:{org.lower()}"
            candidate = {
                "data": data,
                "source": source_name,
                "type": "company",
                "url": str(hit.get("url", "")),
                "score": score,
                "fio_ru": " ".join(
                    x
                    for x in [
                        self.engine._normalize_spaces(str(data.get("surname_ru", ""))),
                        self.engine._normalize_spaces(str(data.get("name_ru", ""))),
                        self.engine._normalize_spaces(str(data.get("middle_name_ru", ""))),
                    ]
                    if x
                ),
                "org_ru": org,
                "position_ru": self.engine._normalize_spaces(str(data.get("ru_position", ""))),
                "inn": inn,
                "query_for_autofill": inn or org,
                "revenue": str(self.engine._parse_financial_amount(data.get("revenue", 0))),
                "source_names": [source_name],
            }

            existing = by_key.get(key)
            if existing is None:
                by_key[key] = candidate
                continue

            for src in candidate["source_names"]:
                if src and src not in existing["source_names"]:
                    existing["source_names"].append(src)

            if float(candidate["score"]) > float(existing.get("score", 0)):
                keep_sources = list(existing["source_names"])
                by_key[key] = candidate
                by_key[key]["source_names"] = keep_sources

        candidates = list(by_key.values())
        if not candidates:
            return []

        with_inn = [item for item in candidates if self.engine._normalize_spaces(str(item.get("inn", "")))]
        if with_inn:
            candidates = with_inn

        candidates.sort(key=lambda item: float(item.get("score", 0)), reverse=True)
        for item in candidates:
            source_names = [self.engine._normalize_spaces(str(x)) for x in item.get("source_names", []) if self.engine._normalize_spaces(str(x))]
            item["source"] = ", ".join(source_names) if source_names else self.engine._normalize_spaces(str(item.get("source", "")))
            item["score"] = float(item.get("score", 0))
        return candidates[:20]

    def _prepare_candidates(self, params: dict[str, str], source_hits: list[dict[str, Any]], backend_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        company_only_query = bool(
            params.get("company")
            and not params.get("inn")
            and not params.get("surname")
            and not params.get("name")
            and not params.get("middle_name")
        )

        if not company_only_query:
            return backend_candidates

        from_hits = self._build_company_candidates_from_hits(source_hits, params.get("company", ""))
        from_backend: list[dict[str, Any]] = []
        for backend_item in backend_candidates:
            normalized = self._normalize_backend_candidate(backend_item, params.get("company", ""))
            if normalized is not None:
                from_backend.append(normalized)

        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in (from_backend, from_hits):
            for item in group:
                key = self._candidate_identity(item)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)

        return merged if merged else backend_candidates

    def _on_search_done(
        self,
        params: dict[str, str],
        source_hits: list[dict[str, Any]],
        backend_candidates: list[dict[str, Any]],
        trace: list[str],
        error: str,
        search_token: int,
    ) -> None:
        if search_token != self._active_search_token:
            return

        self._set_busy(False)
        if error:
            self.status_var.set("Ошибка поиска")
            messagebox.showerror("Nadin", error)
            return

        candidates = self._prepare_candidates(params, source_hits, backend_candidates)
        self.candidates = candidates

        for iid in self.result_tree.get_children():
            self.result_tree.delete(iid)

        for idx, item in enumerate(candidates):
            org_name = self._clean_org_for_list(item.get("org_ru", ""), item.get("inn", ""))
            if not org_name:
                org_name = self.engine._normalize_spaces(str(item.get("fio_ru", "")))
            self.result_tree.insert(
                "",
                tk.END,
                iid=str(idx),
                values=(
                    org_name,
                    self.engine._normalize_spaces(str(item.get("inn", ""))),
                    self.engine._normalize_spaces(str(item.get("source", ""))) or "—",
                ),
            )

        if not candidates:
            self._render_card_rows([("Статус", "Ничего не найдено")], "Карточка")
            self.status_var.set("Найдено вариантов: 0")
        elif len(candidates) == 1:
            self._suppress_variant_event = True
            try:
                self.result_tree.selection_set("0")
                self.result_tree.focus("0")
            finally:
                self._suppress_variant_event = False
            self._autofill_candidate(candidates[0], reason="single_match")
        else:
            self._render_card_rows(
                [
                    ("Статус", "Выберите вариант справа: карточка будет сформирована автоматически"),
                    ("Вариантов", str(len(candidates))),
                ],
                "Предпросмотр карточки",
            )
            self.status_var.set(f"Найдено вариантов: {len(candidates)}. Выберите нужный вариант")

        self._write_trace(trace)

    def _selected_candidate(self) -> dict[str, Any] | None:
        selected = self.result_tree.selection()
        if not selected:
            return None
        try:
            idx = int(selected[0])
        except ValueError:
            return None
        if idx < 0 or idx >= len(self.candidates):
            return None
        return self.candidates[idx]

    def _clean_org_for_list(self, org_name: str, inn: str) -> str:
        org = self.engine._normalize_spaces(str(org_name))
        if not org:
            return ""
        inn_value = self.engine._normalize_spaces(str(inn))
        cleaned = org
        if inn_value:
            cleaned = re.sub(rf"\bИНН\s*{re.escape(inn_value)}\b", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(rf"\b{re.escape(inn_value)}\b", "", cleaned)
        cleaned = re.sub(r"\bИНН\s*\d{10,12}\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = self.engine._normalize_spaces(cleaned)
        return cleaned or org

    def _on_variant_selected(self, _event: object | None) -> None:
        if self._suppress_variant_event or self._busy:
            return
        candidate = self._selected_candidate()
        if candidate is None:
            return
        self._autofill_candidate(candidate, reason="selected_variant")

    def _profile_from_candidate(self, candidate: dict[str, Any]) -> dict[str, str]:
        profile: dict[str, str] = {key: "" for _, key in self.CARD_FIELDS}
        raw_data = candidate.get("data", {})
        candidate_data = raw_data if isinstance(raw_data, dict) else {}

        for _, key in self.CARD_FIELDS:
            profile[key] = self.engine._normalize_spaces(str(candidate_data.get(key, "")))

        profile["inn"] = profile.get("inn") or self.engine._normalize_spaces(str(candidate.get("inn", "")))
        profile["ru_org"] = profile.get("ru_org") or self._clean_org_for_list(candidate.get("org_ru", ""), profile.get("inn", ""))
        profile["ru_position"] = profile.get("ru_position") or self.engine._normalize_spaces(str(candidate.get("position_ru", "")))

        fio_ru = self.engine._normalize_spaces(str(candidate.get("fio_ru", "")))
        if fio_ru and not (profile.get("surname_ru") and profile.get("name_ru")):
            surname_ru, name_ru, middle_name_ru = self.engine._split_fio_ru(fio_ru)
            profile["surname_ru"] = profile.get("surname_ru") or surname_ru
            profile["name_ru"] = profile.get("name_ru") or name_ru
            profile["middle_name_ru"] = profile.get("middle_name_ru") or middle_name_ru

        if profile.get("ru_org") and not profile.get("en_org"):
            try:
                profile["en_org"], _ = self.engine.normalize_en_org("", profile["ru_org"])
            except Exception:  # noqa: BLE001
                profile["en_org"] = ""
        if profile.get("ru_position") and not profile.get("en_position"):
            profile["en_position"] = self.engine._generate_en_position(profile["ru_position"])

        if profile.get("surname_ru") and not profile.get("family_name"):
            profile["family_name"] = self.engine._translit(profile["surname_ru"])
        if profile.get("name_ru") and not profile.get("first_name"):
            profile["first_name"] = self.engine._translit(profile["name_ru"])
        if profile.get("middle_name_ru") and not profile.get("middle_name_en"):
            profile["middle_name_en"] = self.engine._generate_middle_name_en(profile["middle_name_ru"])

        return profile

    def _render_candidate_preview(self, candidate: dict[str, Any]) -> None:
        profile = self._profile_from_candidate(candidate)
        source = self.engine._normalize_spaces(str(candidate.get("source", "")))

        base_year = self.engine._default_financial_year()
        revenue_line = self.engine._format_financial_line(candidate.get("revenue", ""), base_year)
        profit_line = self.engine._format_financial_line("", base_year)

        rows = self._compose_card_rows(
            profile,
            status="Предпросмотр",
            source_names=[source] if source else [],
            revenue_line=revenue_line,
            profit_line=profit_line,
        )
        self._render_card_rows(rows, "Предпросмотр карточки")

    def _resolve_query_for_autofill(self, candidate: dict[str, Any]) -> str:
        candidate_inn = self.engine._normalize_spaces(str(candidate.get("inn", "")))
        if candidate_inn and re.fullmatch(r"\d{10}|\d{12}", candidate_inn):
            query_for_autofill = candidate_inn
        else:
            query_for_autofill = self.engine._normalize_spaces(str(candidate.get("query_for_autofill", "")))

        if not query_for_autofill:
            query_for_autofill = self.company_var.get().strip() or self.inn_var.get().strip()

        company_query = self.engine._normalize_spaces(self.company_var.get())
        selected_org = self.engine._normalize_spaces(str(candidate.get("org_ru", ""))).lower()
        brand_token = self.engine._short_brand_token(company_query)
        known_bank_inn = SHORT_BRAND_KNOWN_INN.get(brand_token, "") if brand_token else ""

        if (
            known_bank_inn
            and brand_token in SHORT_BRAND_BANK_HINTS
            and not self.inn_var.get().strip()
            and (not candidate_inn or candidate_inn != known_bank_inn)
            and "банк" not in selected_org
        ):
            query_for_autofill = known_bank_inn

        return self.engine._normalize_spaces(query_for_autofill)

    def _autofill_candidate(self, candidate: dict[str, Any], reason: str = "") -> None:
        if self._busy:
            return

        query_for_autofill = self._resolve_query_for_autofill(candidate)
        if not query_for_autofill:
            return

        key = self._candidate_key(candidate, query_for_autofill)
        if key and key == self._last_autofill_key and reason == "selected_variant":
            return
        self._last_autofill_key = key

        self._pending_source_url = self.engine._normalize_spaces(str(candidate.get("url", "")))

        hit_type = self.engine._normalize_spaces(str(candidate.get("type", ""))).lower()
        if hit_type not in {"company", "person"}:
            if self.engine._normalize_spaces(str(candidate.get("org_ru", ""))):
                hit_type = "company"
            elif self.engine._normalize_spaces(str(candidate.get("fio_ru", ""))):
                hit_type = "person"
            else:
                hit_type = ""

        forced_search_type = hit_type if hit_type in {"company", "person"} else ""
        form = {
            "company_name": [query_for_autofill],
            "hit_type": [hit_type],
            "search_type": [forced_search_type],
        }

        self._render_card_rows([("Статус", "Формирование карточки...")], "Карточка")

        status_suffix = " (выбран вариант)" if reason == "selected_variant" else ""
        self._set_busy(True, f"Формирование карточки{status_suffix}...")

        self._active_autofill_token += 1
        autofill_token = self._active_autofill_token

        def worker() -> None:
            error = ""
            card_id = 0
            try:
                body, status, _headers = self.engine.autofill_review(form, wants_json=True)
                if status != "200 OK":
                    error = f"HTTP статус: {status}"
                else:
                    payload = json.loads(body)
                    if payload.get("ok") and payload.get("card_id"):
                        card_id = int(payload["card_id"])
                    else:
                        redirect = self.engine._normalize_spaces(str(payload.get("redirect", "")))
                        match = re.search(r"/card/(\d+)$", redirect)
                        if match:
                            card_id = int(match.group(1))
                        else:
                            error = f"Автоформирование не завершено: {redirect or 'нужно уточнение'}"
            except Exception as exc:  # noqa: BLE001
                logger.exception("Autofill failed")
                error = str(exc)
            self.after(0, lambda: self._on_autofill_done(card_id, error, autofill_token))

        threading.Thread(target=worker, daemon=True).start()

    def _on_autofill_done(self, card_id: int, error: str, autofill_token: int) -> None:
        if autofill_token != self._active_autofill_token:
            return

        self._set_busy(False)
        if error:
            self.status_var.set("Не удалось сформировать карточку")
            self._render_card_rows([("Статус", error)], "Карточка")
            return

        self._show_card(card_id)
        self.status_var.set(f"Карточка #{card_id} сформирована")
        if self._last_source_url:
            self.after(120, lambda: self._capture_source_screenshot(auto=True))

    def _compose_card_rows(
        self,
        profile: dict[str, str],
        *,
        status: str,
        source_names: list[str],
        revenue_line: str,
        profit_line: str,
    ) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        for label, key in self.CARD_FIELDS:
            rows.append((label, self.engine._normalize_spaces(str(profile.get(key, "")))))

        rows.append(("Выручка", revenue_line))
        rows.append(("Прибыль", profit_line))
        rows.append(("Статус", self.engine._normalize_spaces(str(status))))

        cleaned_sources: list[str] = []
        seen: set[str] = set()
        for source in source_names:
            src = self.engine._normalize_spaces(str(source))
            if not src:
                continue
            key = src.lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned_sources.append(src)

        rows.append(("Источники", ", ".join(cleaned_sources) if cleaned_sources else "—"))
        return rows

    def _extract_source_names(self, payload: dict[str, object], primary_source: str = "") -> list[str]:
        raw_hits = payload.get("source_hits", []) if isinstance(payload, dict) else []
        names: list[str] = []
        seen: set[str] = set()

        if primary_source:
            src = self.engine._normalize_spaces(primary_source)
            if src:
                names.append(src)
                seen.add(src.lower())

        if not isinstance(raw_hits, list):
            return names

        for hit in raw_hits:
            if not isinstance(hit, dict):
                continue
            source_name = self.engine._normalize_spaces(str(hit.get("source", "")))
            if not source_name:
                continue
            key = source_name.lower()
            if key in seen:
                continue
            seen.add(key)
            names.append(source_name)
        return names

    def _extract_source_url(self, payload: dict[str, Any], fallback_url: str = "") -> str:
        candidates: list[tuple[str, dict[str, Any], bool]] = []
        source_hits = payload.get("source_hits", []) if isinstance(payload, dict) else []
        if isinstance(source_hits, list):
            for hit in source_hits:
                if not isinstance(hit, dict):
                    continue
                candidate_url = self.engine._normalize_spaces(str(hit.get("url", "")))
                if candidate_url:
                    candidates.append((candidate_url, hit, False))

        fallback = self.engine._normalize_spaces(fallback_url)
        if fallback:
            candidates.append((fallback, {}, True))

        scored: list[tuple[int, str]] = []
        seen_urls: set[str] = set()
        for raw_url, hit, is_fallback in candidates:
            normalized = self._normalize_source_url_for_screenshot(raw_url, hit)
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            score = self._score_source_url_for_screenshot(normalized, is_fallback=is_fallback)
            scored.append((score, normalized))

        if not scored:
            return ""

        scored.sort(key=lambda item: (item[0], len(item[1])), reverse=True)
        best_score, best_url = scored[0]
        # Do not use landing pages when no reliable result URL is available.
        if best_score < 0:
            return ""
        return best_url

    def _score_source_url_for_screenshot(self, source_url: str, *, is_fallback: bool = False) -> int:
        normalized = self.engine._normalize_spaces(source_url)
        if not normalized:
            return -1000

        if normalized.startswith("file://"):
            parsed = urlparse(normalized)
            local_path = unquote(parsed.path)
            if re.match(r"^/[a-zA-Z]:", local_path):
                local_path = local_path[1:]
            path = Path(local_path)
            if path.suffix.lower() == ".pdf":
                return 320
            return 250

        if normalized.startswith("http://") or normalized.startswith("https://"):
            parsed = urlparse(normalized)
        else:
            local_path = Path(normalized)
            try:
                if local_path.exists():
                    if local_path.suffix.lower() == ".pdf":
                        return 310
                    return 200
            except OSError:
                return -1000
            return -1000

        host = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()

        score = 0
        if is_fallback:
            score += 120

        if self._is_machine_source_url(normalized):
            score -= 400

        if self._is_search_engine_url(normalized):
            score -= 300

        if self._is_generic_landing_url(normalized):
            score -= 260

        detail_markers = (
            "/company/",
            "/id/",
            "/ul/",
            "/person/",
            "/organization/",
            "/org/",
            "/card/",
        )
        if any(marker in path for marker in detail_markers):
            score += 220

        trusted_hosts = (
            "zachestnyibiznes.ru",
            "companies.rbc.ru",
            "rusprofile.ru",
            "focus.kontur.ru",
            "checko.ru",
            "list-org.com",
        )
        if any(host.endswith(domain) for domain in trusted_hosts):
            score += 140

        if host.endswith("nalog.ru") and "query=" in query:
            score += 20

        if host == "egrul.itsoft.ru":
            score -= 500

        if host.endswith("egrul.nalog.ru") and path in {"", "/", "/index.html"}:
            score -= 240
        if host.endswith("nalog.ru") and path in {"", "/", "/index.html"} and "query=" in query:
            score -= 240

        return score

    def _is_search_engine_url(self, source_url: str) -> bool:
        parsed = urlparse(source_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        return (
            "duckduckgo.com" in host
            or "google." in host
            or "yandex." in host
            or ("bing.com" in host and "/search" in path)
        )

    def _is_generic_landing_url(self, source_url: str) -> bool:
        parsed = urlparse(source_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower().rstrip("/")

        if host.endswith("nalog.ru") and path in {"", "/index.html", "/"}:
            return True
        if host.endswith("egrul.nalog.ru") and path in {"", "/index.html", "/"}:
            return True
        return False

    def _is_machine_source_url(self, source_url: str) -> bool:
        normalized = self.engine._normalize_spaces(source_url)
        if not (normalized.startswith("http://") or normalized.startswith("https://")):
            return True

        parsed = urlparse(normalized)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()

        if host == "egrul.itsoft.ru":
            return True
        if path.endswith(".json"):
            return True
        if "/api/" in path:
            return True
        if "format=json" in query or "output=json" in query:
            return True
        return False

    def _normalize_source_url_for_screenshot(self, source_url: str, hit: dict[str, Any] | None = None) -> str:
        normalized = self.engine._normalize_spaces(source_url)
        if not normalized:
            return ""
        if normalized.startswith("file://"):
            return normalized
        if normalized.startswith("http://") or normalized.startswith("https://"):
            parsed = urlparse(normalized)
        else:
            local_path = Path(normalized)
            try:
                if local_path.exists():
                    return str(local_path.resolve())
            except OSError:
                return ""
            return ""

        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if host == "egrul.itsoft.ru" and path.endswith(".json"):
            return ""

        return normalized
    def _candidate_pdf_roots(self) -> list[Path]:
        roots: list[Path] = []
        seen: set[str] = set()
        env_candidates = [
            os.getenv("USERPROFILE", ""),
            os.getenv("HOMEDRIVE", "") + os.getenv("HOMEPATH", ""),
            os.getenv("APP_DATA_DIR", ""),
            os.getcwd(),
        ]
        for raw in env_candidates:
            path_text = self.engine._normalize_spaces(raw)
            if not path_text:
                continue
            root = Path(path_text)
            try:
                exists_root = root.exists()
            except OSError:
                continue
            if not exists_root:
                continue
            try:
                key = str(root.resolve()).lower()
            except OSError:
                key = str(root.absolute()).lower()
            if key in seen:
                continue
            seen.add(key)
            roots.append(root)
            downloads = root / "Downloads"
            try:
                downloads_exists = downloads.exists()
            except OSError:
                downloads_exists = False
            if downloads_exists:
                try:
                    d_key = str(downloads.resolve()).lower()
                except OSError:
                    d_key = str(downloads.absolute()).lower()
                if d_key not in seen:
                    seen.add(d_key)
                    roots.append(downloads)
        return roots

    def _find_latest_pdf(self, patterns: list[tuple[str, bool]]) -> str:
        best_path: Path | None = None
        best_mtime = -1.0
        now_ts = datetime.now().timestamp()
        max_generic_age_seconds = 60 * 60 * 24 * 14

        for root in self._candidate_pdf_roots():
            for pattern, is_generic in patterns:
                try:
                    iterator = root.glob(pattern)
                except Exception:  # noqa: BLE001
                    continue
                try:
                    for candidate in iterator:
                        if not candidate.is_file():
                            continue
                        if candidate.suffix.lower() != ".pdf":
                            continue
                        try:
                            mtime = candidate.stat().st_mtime
                        except OSError:
                            continue
                        if is_generic and (now_ts - mtime) > max_generic_age_seconds:
                            continue
                        if mtime > best_mtime:
                            best_mtime = mtime
                            best_path = candidate
                except OSError:
                    continue

        if best_path is None:
            return ""
        return str(best_path.resolve())

    def _find_fns_pdf_candidate(self, hit: dict[str, Any]) -> str:
        if not isinstance(hit, dict):
            return ""

        raw_url = self.engine._normalize_spaces(str(hit.get("url", "")))
        if not raw_url:
            return ""
        parsed = urlparse(raw_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if host != "egrul.itsoft.ru" or not path.endswith(".json"):
            return ""

        data = hit.get("data", {}) if isinstance(hit.get("data"), dict) else {}
        tokens: list[str] = []
        for key in ("ogrn", "inn"):
            value = self.engine._normalize_spaces(str(data.get(key, "")))
            if re.fullmatch(r"\d{10,15}", value):
                tokens.append(value)

        stem = Path(path).stem
        if re.fullmatch(r"\d{10,15}", stem):
            tokens.append(stem)

        seen_tokens: set[str] = set()
        uniq_tokens: list[str] = []
        for token in tokens:
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            uniq_tokens.append(token)

        patterns: list[tuple[str, bool]] = []
        for token in uniq_tokens:
            patterns.append((f"ul-{token}-*.pdf", False))
            patterns.append((f"*{token}*.pdf", False))
        patterns.append(("ul-*.pdf", True))

        return self._find_latest_pdf(patterns)
    def _get_rusprofile_lookup_query(self) -> str:
        for raw in (self._last_profile_inn, self._last_profile_ogrn, self._last_profile_org):
            value = self.engine._normalize_spaces(raw)
            if value:
                return value
        return ""

    def _lookup_rusprofile_url(self, query: str) -> str:
        normalized_query = self.engine._normalize_spaces(query)
        if not normalized_query:
            return ""

        cache_key = normalized_query.lower()
        cached = self._rusprofile_url_cache.get(cache_key, "")
        if cached:
            return cached

        search_url = f"https://www.rusprofile.ru/search?query={quote(normalized_query)}"
        resolved = search_url

        try:
            html = self.engine._fetch_page(search_url, timeout=18, max_retries=1)
        except Exception:  # noqa: BLE001
            html = ""

        if html:
            path_match = re.search(r"href=['\"](?P<path>/id/\d+[^'\"]*)['\"]", html, flags=re.IGNORECASE)
            if path_match:
                resolved = f"https://www.rusprofile.ru{path_match.group('path')}"
            else:
                full_match = re.search(r"https?://(?:www\.)?rusprofile\.ru/id/\d+[^\"'\s<]*", html, flags=re.IGNORECASE)
                if full_match:
                    resolved = full_match.group(0)

        self._rusprofile_url_cache[cache_key] = resolved
        return resolved

    def _resolve_rusprofile_source_url(self, source_url: str) -> str:
        normalized = self._normalize_screenshot_target(source_url)
        if self._is_pdf_target(normalized):
            return normalized

        if normalized.startswith("http://") or normalized.startswith("https://"):
            parsed = urlparse(normalized)
            host = parsed.netloc.lower()
            path = parsed.path.lower()
            if host.endswith("rusprofile.ru") and "/id/" in path:
                return normalized
            if normalized and not self._is_machine_source_url(normalized) and not self._is_generic_landing_url(normalized):
                return normalized

        lookup_query = self._get_rusprofile_lookup_query()
        if not lookup_query:
            return normalized

        resolved = self._lookup_rusprofile_url(lookup_query)
        return resolved or normalized

    def _has_fns_source_context(self, source_url: str) -> bool:
        normalized = self._normalize_screenshot_target(source_url)
        if normalized.startswith("http://") or normalized.startswith("https://"):
            parsed = urlparse(normalized)
            host = parsed.netloc.lower()
            if host.endswith("nalog.ru") or host == "egrul.itsoft.ru":
                return True

        for source_name in self._last_source_names:
            lowered = self.engine._normalize_spaces(str(source_name)).lower()
            if not lowered:
                continue
            if "фнс" in lowered or "егрюл" in lowered:
                return True
        return False

    def _iter_fns_identifier_candidates(self, source_url: str) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()

        def add(value: str) -> None:
            candidate = self.engine._normalize_spaces(str(value))
            if not re.fullmatch(r"\d{10,15}", candidate):
                return
            if candidate in seen:
                return
            seen.add(candidate)
            candidates.append(candidate)

        add(self._last_profile_inn)
        add(self._last_profile_ogrn)

        normalized = self._normalize_screenshot_target(source_url)
        if normalized:
            for token in re.findall(r"\d{10,15}", normalized):
                add(token)

        return candidates

    def _get_cached_fns_pdf_target(self, query: str) -> tuple[str, str]:
        cached = self._fns_pdf_target_cache.get(query)
        if not cached:
            return "", ""

        target_path, source_url, cached_at = cached
        ttl_seconds = 60 * 60
        if (time.time() - cached_at) > ttl_seconds:
            self._fns_pdf_target_cache.pop(query, None)
            return "", ""

        target = self._normalize_screenshot_target(target_path)
        if not target:
            self._fns_pdf_target_cache.pop(query, None)
            return "", ""

        if target.startswith("http://") or target.startswith("https://"):
            return target, source_url

        local_path = Path(target)
        if not local_path.exists():
            self._fns_pdf_target_cache.pop(query, None)
            return "", ""

        return str(local_path.resolve()), source_url

    def _fetch_fns_egrul_pdf_target(self, query: str) -> tuple[str, str]:
        normalized_query = self.engine._normalize_spaces(query)
        if not re.fullmatch(r"\d{10,15}", normalized_query):
            return "", ""

        base_url = "https://egrul.nalog.ru"
        session = getattr(self.engine, "_http_session", None)
        if session is None:
            raise RuntimeError("HTTP session is not initialized")

        try:
            user_agent = self.engine._get_random_user_agent()
            headers = self.engine._get_random_headers(user_agent)
        except Exception:  # noqa: BLE001
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
            }

        payload = {
            "query": normalized_query,
            "region": "",
            "page": "",
            "PreventChromeAutocomplete": "",
        }
        seed_resp = session.post(base_url, data=payload, timeout=30, headers=headers)
        if not seed_resp.ok:
            raise RuntimeError(f"EGRUL search init failed ({seed_resp.status_code})")

        try:
            seed_json = seed_resp.json()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"EGRUL search init returned non-JSON: {exc}") from exc

        if bool(seed_json.get("captchaRequired")):
            raise RuntimeError("EGRUL requires captcha")

        search_token = self.engine._normalize_spaces(str(seed_json.get("t", "")))
        if not search_token:
            raise RuntimeError("EGRUL search token is empty")

        rows: list[dict[str, Any]] = []
        for _ in range(6):
            result_resp = session.get(f"{base_url}/search-result/{search_token}", timeout=30, headers=headers)
            if not result_resp.ok:
                raise RuntimeError(f"EGRUL search result failed ({result_resp.status_code})")

            try:
                result_json = result_resp.json()
            except Exception:  # noqa: BLE001
                result_json = {}

            parsed_rows = result_json.get("rows", []) if isinstance(result_json, dict) else []
            rows = [row for row in parsed_rows if isinstance(row, dict)] if isinstance(parsed_rows, list) else []
            if rows:
                break
            time.sleep(0.6)

        if not rows:
            raise RuntimeError("EGRUL search returned no rows")

        profile_inn = self.engine._normalize_spaces(self._last_profile_inn)
        profile_ogrn = self.engine._normalize_spaces(self._last_profile_ogrn)
        profile_org = self.engine._normalize_spaces(self._last_profile_org).lower()

        def row_score(row: dict[str, Any]) -> int:
            score = 0
            row_inn = self.engine._normalize_spaces(str(row.get("i", "")))
            row_ogrn = self.engine._normalize_spaces(str(row.get("o", "")))
            row_org = self.engine._normalize_spaces(str(row.get("n", "") or row.get("c", ""))).lower()

            if row_inn and row_inn == normalized_query:
                score += 120
            if row_ogrn and row_ogrn == normalized_query:
                score += 120
            if profile_inn and row_inn == profile_inn:
                score += 140
            if profile_ogrn and row_ogrn == profile_ogrn:
                score += 140
            if profile_org and row_org and profile_org in row_org:
                score += 30
            return score

        best_row = max(rows, key=row_score)
        row_token = self.engine._normalize_spaces(str(best_row.get("t", ""))) or search_token

        request_resp = session.get(f"{base_url}/vyp-request/{row_token}", timeout=30, headers=headers)
        if not request_resp.ok:
            raise RuntimeError(f"EGRUL vyp-request failed ({request_resp.status_code})")

        try:
            request_json = request_resp.json()
        except Exception:  # noqa: BLE001
            request_json = {}

        if bool(request_json.get("captchaRequired")):
            raise RuntimeError("EGRUL vyp-request requires captcha")

        request_token = self.engine._normalize_spaces(str(request_json.get("t", ""))) or row_token

        ready = False
        for _ in range(35):
            status_resp = session.get(f"{base_url}/vyp-status/{request_token}", timeout=30, headers=headers)
            if status_resp.ok:
                status_value = ""
                try:
                    status_json = status_resp.json()
                except Exception:  # noqa: BLE001
                    status_json = {}
                if isinstance(status_json, dict):
                    status_value = self.engine._normalize_spaces(str(status_json.get("status", ""))).lower()
                if status_value in {"ready", "completed", "done"}:
                    ready = True
                    break
            time.sleep(0.9)

        if not ready:
            logger.info("EGRUL PDF status did not become ready quickly for query=%s; trying direct download", normalized_query)

        download_url = f"{base_url}/vyp-download/{request_token}"
        pdf_resp = session.get(download_url, timeout=45, headers=headers)
        if not pdf_resp.ok:
            raise RuntimeError(f"EGRUL PDF download failed ({pdf_resp.status_code})")

        pdf_bytes = bytes(pdf_resp.content or b"")
        content_type = self.engine._normalize_spaces(str(pdf_resp.headers.get("content-type", ""))).lower()
        if not pdf_bytes.startswith(b"%PDF") and "pdf" not in content_type:
            raise RuntimeError("EGRUL download is not a PDF")

        pdf_dir = self._screenshot_dir / "egrul_pdf"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_query = re.sub(r"[^0-9A-Za-z_-]", "_", normalized_query)
        pdf_path = pdf_dir / f"egrul_{safe_query}_{stamp}.pdf"
        pdf_path.write_bytes(pdf_bytes)

        return str(pdf_path.resolve()), download_url

    def _resolve_fns_egrul_pdf_target(self, source_url: str) -> tuple[str, str]:
        queries = self._iter_fns_identifier_candidates(source_url)
        if not queries:
            return "", ""

        should_try = self._has_fns_source_context(source_url) or bool(self._last_profile_inn)
        if not should_try:
            return "", ""

        for query in queries:
            cached_target, cached_source = self._get_cached_fns_pdf_target(query)
            if cached_target:
                return cached_target, cached_source

            try:
                target, source = self._fetch_fns_egrul_pdf_target(query)
            except Exception as exc:  # noqa: BLE001
                logger.info("EGRUL PDF resolution failed for %s: %s", query, exc)
                continue

            if target:
                self._fns_pdf_target_cache[query] = (target, source, time.time())
                return target, source

        return "", ""
    def _merge_profile_with_source_hits(self, profile: dict[str, Any], source_hits: list[dict[str, Any]]) -> dict[str, str]:
        merged = {key: self.engine._normalize_spaces(str(value)) for key, value in profile.items()}
        fill_keys = {key for _, key in self.CARD_FIELDS}
        fill_keys.update({"revenue", "profit", "financial_year", "revenue_year", "profit_year", "year", "report_year"})

        for hit in source_hits:
            if not isinstance(hit, dict):
                continue
            data = hit.get("data", {})
            if not isinstance(data, dict):
                continue
            for key in fill_keys:
                if merged.get(key):
                    continue
                value = self.engine._normalize_spaces(str(data.get(key, "")))
                if value:
                    merged[key] = value

        if merged.get("ru_org") and not merged.get("en_org"):
            try:
                merged["en_org"], _ = self.engine.normalize_en_org("", merged["ru_org"])
            except Exception:  # noqa: BLE001
                merged["en_org"] = ""
        if merged.get("ru_position") and not merged.get("en_position"):
            merged["en_position"] = self.engine._generate_en_position(merged["ru_position"])
        if merged.get("surname_ru") and not merged.get("family_name"):
            merged["family_name"] = self.engine._translit(merged["surname_ru"])
        if merged.get("name_ru") and not merged.get("first_name"):
            merged["first_name"] = self.engine._translit(merged["name_ru"])
        if merged.get("middle_name_ru") and not merged.get("middle_name_en"):
            merged["middle_name_en"] = self.engine._generate_middle_name_en(merged["middle_name_ru"])

        return merged

    def _resolve_metric_line(
        self,
        profile: dict[str, Any],
        source_hits: list[dict[str, Any]],
        metric_keys: tuple[str, ...],
        year_keys: tuple[str, ...],
    ) -> str:
        metric_candidates: list[tuple[int, int]] = []
        year_candidates: list[int] = []

        def append_value(amount_value: Any, year_value: Any) -> None:
            raw_amount = self.engine._normalize_spaces(str(amount_value))
            year_from_amount = self.engine._parse_financial_year(raw_amount)
            if year_from_amount and re.fullmatch(r"(19\d{2}|20\d{2})", raw_amount):
                year_candidates.append(year_from_amount)
                return

            amount = self.engine._parse_financial_amount(amount_value)
            year = self.engine._parse_financial_year(year_value)
            if year:
                year_candidates.append(year)
            if amount != 0:
                metric_candidates.append((year, amount))

        for key in metric_keys:
            if key in profile:
                year_value = ""
                for y_key in year_keys:
                    if self.engine._normalize_spaces(str(profile.get(y_key, ""))):
                        year_value = profile.get(y_key, "")
                        break
                append_value(profile.get(key, 0), year_value)

        for hit in source_hits:
            data = hit.get("data", {}) if isinstance(hit.get("data"), dict) else {}
            for key in metric_keys:
                if key not in data:
                    continue
                year_value = ""
                for y_key in year_keys:
                    if self.engine._normalize_spaces(str(data.get(y_key, ""))):
                        year_value = data.get(y_key, "")
                        break
                append_value(data.get(key, 0), year_value)

        fallback_year = max(year_candidates) if year_candidates else self.engine._default_financial_year()
        if not metric_candidates:
            return f"Данных нет ({fallback_year})"

        metric_candidates.sort(key=lambda item: (item[0], abs(item[1])), reverse=True)
        best_year, best_amount = metric_candidates[0]
        year = best_year if best_year > 0 else fallback_year
        return self.engine._format_financial_line(best_amount, year)

    def _show_card(self, card_id: int) -> None:
        with self.engine._connect() as db:
            row = db.execute("SELECT id, status, source, data_json FROM cards WHERE id=?", (card_id,)).fetchone()
        if row is None:
            messagebox.showerror("Nadin", f"Карточка #{card_id} не найдена")
            return

        payload = json.loads(row["data_json"] or "{}")
        profile = payload.get("profile", {}) if isinstance(payload, dict) else {}
        if not isinstance(profile, dict):
            profile = {}
        source_hits = payload.get("source_hits", []) if isinstance(payload, dict) else []
        if not isinstance(source_hits, list):
            source_hits = []

        profile = self._merge_profile_with_source_hits(profile, source_hits)

        revenue_line = self._resolve_metric_line(
            profile,
            source_hits,
            metric_keys=("revenue", "revenue_mln", "income"),
            year_keys=("revenue_year", "financial_year", "year", "report_year"),
        )
        profit_line = self._resolve_metric_line(
            profile,
            source_hits,
            metric_keys=("profit", "net_profit", "clean_profit", "profit_clean", "чистая_прибыль", "чистая_прибыль_убыток"),
            year_keys=("profit_year", "financial_year", "year", "report_year"),
        )

        primary_source = self.engine._normalize_spaces(str(row["source"] or ""))
        source_names = self._extract_source_names(payload, primary_source=primary_source)

        rows = self._compose_card_rows(
            {k: self.engine._normalize_spaces(str(v)) for k, v in profile.items()},
            status=str(row["status"] or ""),
            source_names=source_names,
            revenue_line=revenue_line,
            profit_line=profit_line,
        )
        self._render_card_rows(rows, f"Карточка #{card_id}")
        self._last_profile_inn = self.engine._normalize_spaces(str(profile.get("inn", "")))
        self._last_profile_ogrn = self.engine._normalize_spaces(str(profile.get("ogrn", "")))
        self._last_profile_org = self.engine._normalize_spaces(str(profile.get("ru_org", "")))
        self._last_source_names = list(source_names)
        self._last_source_url = self._extract_source_url(payload, fallback_url=self._pending_source_url)
        self._pending_source_url = ""
        self.source_url_var.set(f"URL источника: {self._last_source_url or '—'}")


    def _render_card_rows(self, rows: list[tuple[str, str]], title: str) -> None:
        self.card_title_var.set(title)
        self._current_card_rows = list(rows)
        for iid in self.card_tree.get_children():
            self.card_tree.delete(iid)

        if not rows:
            rows = [("Статус", "Нет данных для отображения")]
            self._current_card_rows = list(rows)

        for label, value in rows:
            display = self.engine._normalize_spaces(str(value)) if value is not None else ""
            self.card_tree.insert("", tk.END, values=(label, display or "—"))

    def _copy_selected_card_value(self, _event: object | None = None) -> str:
        selected = self.card_tree.selection()
        if not selected:
            return "break"

        lines: list[str] = []
        for iid in selected:
            values = self.card_tree.item(iid, "values")
            if not values or len(values) < 2:
                continue
            lines.append(f"{values[0]}	{values[1]}")

        if lines:
            self.clipboard_clear()
            self.clipboard_append("\n".join(lines))
            self.status_var.set("Выбранные поля карточки скопированы")
        return "break"

    def _copy_full_card(self) -> None:
        if not self._current_card_rows:
            self.status_var.set("Нет данных для копирования")
            return

        lines: list[str] = []
        for label, value in self._current_card_rows:
            display = self.engine._normalize_spaces(str(value)) if value is not None else ""
            lines.append(f"{label}\t{display or '—'}")

        self.clipboard_clear()
        self.clipboard_append("\n".join(lines))
        self.status_var.set("Карточка скопирована")

    def _copy_selected_variant_value(self, _event: object | None = None) -> str:
        selected = self.result_tree.selection()
        if not selected:
            return "break"
        values = self.result_tree.item(selected[0], "values")
        if values:
            self.clipboard_clear()
            self.clipboard_append("	".join(str(v) for v in values))
            self.status_var.set("Выбранный вариант скопирован")
        return "break"

    def _capture_source_screenshot_manual(self) -> None:
        self._capture_source_screenshot(auto=False)

    def _capture_source_screenshot(self, auto: bool) -> None:
        if self._screenshot_busy:
            return
        if self._busy and not auto:
            return

        source_url = self._normalize_screenshot_target(self._last_source_url)

        self._screenshot_busy = True
        self.screenshot_button.configure(state=tk.DISABLED)
        self.download_screenshot_button.configure(state=tk.DISABLED)
        self.status_var.set("Создание скриншота источника...")

        def worker() -> None:
            error = ""
            saved_path = ""
            captured_at = ""
            screenshot_target = source_url
            display_source_url = source_url
            try:
                fns_target, fns_source_url = self._resolve_fns_egrul_pdf_target(source_url)
                if fns_target:
                    screenshot_target = fns_target
                    display_source_url = fns_source_url or fns_target
                else:
                    resolved = self._resolve_rusprofile_source_url(source_url) or source_url
                    screenshot_target = resolved
                    display_source_url = resolved

                if not self._is_supported_screenshot_target(screenshot_target):
                    raise RuntimeError("URL источника не поддерживается")

                path, captured_at = self._capture_webpage_screenshot(
                    screenshot_target,
                    metadata_source_url=display_source_url,
                )
                saved_path = str(path)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to capture source screenshot")
                error = str(exc)
            self.after(
                0,
                lambda: self._on_source_screenshot_done(
                    saved_path,
                    captured_at,
                    display_source_url,
                    error,
                    auto,
                ),
            )

        threading.Thread(target=worker, daemon=True).start()

    def _normalize_screenshot_target(self, source_url: str) -> str:
        value = self.engine._normalize_spaces(str(source_url))
        if not value:
            return ""
        if value.startswith("http://") or value.startswith("https://") or value.startswith("file://"):
            return value
        local_path = Path(value)
        if local_path.exists():
            return str(local_path.resolve())
        return value

    def _is_supported_screenshot_target(self, source_url: str) -> bool:
        value = self._normalize_screenshot_target(source_url)
        if not value:
            return False
        if value.startswith("http://") or value.startswith("https://") or value.startswith("file://"):
            return True
        return Path(value).exists()

    def _is_pdf_target(self, source_url: str) -> bool:
        value = self._normalize_screenshot_target(source_url).lower()
        if value.endswith(".pdf"):
            return True
        parsed = urlparse(value)
        return parsed.path.lower().endswith(".pdf")

    def _source_to_local_path(self, source_url: str) -> Path:
        value = self._normalize_screenshot_target(source_url)
        if value.startswith("file://"):
            parsed = urlparse(value)
            local = unquote(parsed.path)
            if re.match(r"^/[a-zA-Z]:", local):
                local = local[1:]
            return Path(local)
        return Path(value)

    def _read_pdf_bytes(self, source_url: str) -> bytes:
        value = self._normalize_screenshot_target(source_url)
        if value.startswith("http://") or value.startswith("https://"):
            response = self.engine._request(value, timeout=30)
            if not response.ok:
                raise RuntimeError(f"PDF download failed: {response.status_code}")
            return bytes(response.content)

        local_path = self._source_to_local_path(value)
        if not local_path.exists():
            raise RuntimeError(f"PDF file not found: {local_path}")
        return local_path.read_bytes()

    def _render_pdf_snapshot(self, source_url: str, output_path: Path) -> None:
        if fitz is None or Image is None:
            raise RuntimeError("PDF snapshot requires Pillow and PyMuPDF")

        pdf_bytes = self._read_pdf_bytes(source_url)
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            if doc.page_count <= 0:
                raise RuntimeError("PDF has no pages")

            rendered_pages: list[Image.Image] = []
            target_width = 1300
            for page in doc:
                pix = page.get_pixmap(matrix=fitz.Matrix(1.35, 1.35), alpha=False)
                image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                if image.width > target_width:
                    new_height = int(image.height * target_width / image.width)
                    image = image.resize((target_width, new_height), Image.Resampling.LANCZOS)
                rendered_pages.append(image)

        margin = 24
        gap = 18
        max_height = 13000
        total_height = sum(image.height for image in rendered_pages) + gap * (len(rendered_pages) - 1)
        if total_height > max_height:
            scale = max_height / total_height
            scaled_pages: list[Image.Image] = []
            for image in rendered_pages:
                new_w = max(320, int(image.width * scale))
                new_h = max(220, int(image.height * scale))
                scaled_pages.append(image.resize((new_w, new_h), Image.Resampling.LANCZOS))
            rendered_pages = scaled_pages

        canvas_width = max(image.width for image in rendered_pages) + margin * 2
        canvas_height = sum(image.height for image in rendered_pages) + gap * (len(rendered_pages) - 1) + margin * 2
        canvas = Image.new("RGB", (canvas_width, canvas_height), "#f3f4f6")

        y = margin
        for index, image in enumerate(rendered_pages, start=1):
            x = (canvas_width - image.width) // 2
            canvas.paste(image, (x, y))
            y += image.height + gap

        canvas.save(output_path, format="PNG")

    def _wrap_overlay_line(self, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
        words = text.split()
        if not words:
            return [""]
        lines: list[str] = []
        current = words[0]
        for word in words[1:]:
            probe = f"{current} {word}"
            bbox = draw.textbbox((0, 0), probe, font=font)
            width = bbox[2] - bbox[0]
            if width <= max_width:
                current = probe
            else:
                lines.append(current)
                current = word
        lines.append(current)
        return lines

    def _annotate_screenshot_metadata(self, image_path: Path, source_url: str, captured_at: str) -> None:
        if Image is None or ImageDraw is None or ImageFont is None:
            return

        with Image.open(image_path).convert("RGB") as body:
            font = ImageFont.load_default()
            draw_probe = ImageDraw.Draw(body)

            lines = [f"URL: {source_url}", f"Date/Time: {captured_at}"]
            wrapped: list[str] = []
            max_width = max(240, body.width - 20)
            for line in lines:
                wrapped.extend(self._wrap_overlay_line(draw_probe, line, font, max_width))

            line_height = 18
            padding = 8
            header_height = padding * 2 + line_height * len(wrapped)
            result = Image.new("RGB", (body.width, body.height + header_height), "#0f172a")
            result.paste(body, (0, header_height))

            draw = ImageDraw.Draw(result)
            y = padding
            for line in wrapped:
                draw.text((10, y), line, font=font, fill="#f8fafc")
                y += line_height

            result.save(image_path, format="PNG")

    def _find_headless_browser(self) -> Path | None:
        allow_edge = os.getenv("NADIN_SCREENSHOT_ALLOW_EDGE", "0").strip().lower() in {"1", "true", "yes"}
        env_candidates = [
            os.getenv("NADIN_SCREENSHOT_BROWSER", ""),
            os.path.join(os.getenv("ProgramFiles", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("ProgramFiles(x86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("LocalAppData", ""), "Google", "Chrome", "Application", "chrome.exe"),
        ]
        if allow_edge:
            env_candidates.extend(
                [
                    os.path.join(os.getenv("ProgramFiles", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
                    os.path.join(os.getenv("ProgramFiles(x86)", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
                    os.path.join(os.getenv("LocalAppData", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
                ]
            )

        for raw_path in env_candidates:
            candidate = raw_path.strip()
            if not candidate:
                continue
            path = Path(os.path.expandvars(candidate))
            if path.exists():
                return path

        command_candidates = ["chrome", "chrome.exe", "chromium", "chromium.exe"]
        if allow_edge:
            command_candidates.extend(["msedge", "msedge.exe"])

        for cmd in command_candidates:
            resolved = shutil.which(cmd)
            if resolved:
                return Path(resolved)
        return None

    def _capture_with_headless_browser(self, browser_path: Path, source_url: str, output_path: Path) -> tuple[bool, str]:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        attempts = ["--headless=new", "--headless"]
        last_details = ""
        user_agent = self.engine._get_random_user_agent()

        for headless_flag in attempts:
            if output_path.exists():
                output_path.unlink(missing_ok=True)
            with tempfile.TemporaryDirectory(prefix="nadin_screenshot_browser_") as profile_dir:
                command = [
                    str(browser_path),
                    headless_flag,
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-breakpad",
                    "--disable-crash-reporter",
                    "--disable-blink-features=AutomationControlled",
                    "--hide-scrollbars",
                    "--window-size=1366,1700",
                    "--lang=ru-RU",
                    "--virtual-time-budget=9000",
                    f"--user-data-dir={profile_dir}",
                    f"--user-agent={user_agent}",
                    f"--screenshot={output_path}",
                    source_url,
                ]
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                    creationflags=creationflags,
                )
            if completed.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
                return True, ""
            stderr = (completed.stderr or "").strip()
            stdout = (completed.stdout or "").strip()
            last_details = stderr or stdout or f"code={completed.returncode}"

        return False, last_details or "headless_browser_failed"

    def _capture_with_splash(self, source_url: str, output_path: Path) -> tuple[bool, str]:
        splash_base = self.engine._normalize_spaces(os.getenv("NADIN_SPLASH_URL", "http://127.0.0.1:8050/render.png"))
        if not splash_base:
            return False, "splash_disabled"

        params = (
            f"url={quote(source_url, safe='')}"
            "&wait=1"
            "&images=1"
            "&render_all=1"
            "&viewport=1366x1700"
        )
        splash_url = f"{splash_base}&{params}" if "?" in splash_base else f"{splash_base}?{params}"

        try:
            response = self.engine._request(splash_url, timeout=60)
        except Exception as exc:  # noqa: BLE001
            return False, f"splash_request_failed:{exc}"

        content_type = self.engine._normalize_spaces(str(response.headers.get("content-type", ""))).lower()
        if not response.ok:
            return False, f"splash_status={response.status_code}"
        if "image" not in content_type:
            return False, f"splash_invalid_content_type={content_type or '-'}"

        body = bytes(response.content or b"")
        if not body:
            return False, "splash_empty_body"

        output_path.write_bytes(body)
        return True, ""

    def _capture_webpage_screenshot_legacy_ie(self, source_url: str, output_path: Path) -> None:
        ps_source_url = source_url.replace("'", "''")
        ps_output_path = str(output_path).replace("'", "''")

        ps_script = f"""
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$url = '{ps_source_url}'
$out = '{ps_output_path}'
$web = New-Object System.Windows.Forms.WebBrowser
$web.ScriptErrorsSuppressed = $true
$web.ScrollBarsEnabled = $false
$web.Width = 1366
$web.Height = 768
$script:done = $false
$web.add_DocumentCompleted({{ $script:done = $true }})
$web.Navigate($url)
$deadline = (Get-Date).AddSeconds(25)
while (-not $script:done) {{
    [System.Windows.Forms.Application]::DoEvents()
    Start-Sleep -Milliseconds 120
    if ((Get-Date) -gt $deadline) {{
        throw 'page_load_timeout'
    }}
}}
$bmp = New-Object System.Drawing.Bitmap($web.Width, $web.Height)
$rect = New-Object System.Drawing.Rectangle(0, 0, $web.Width, $web.Height)
$web.DrawToBitmap($bmp, $rect)
$bmp.Save($out, [System.Drawing.Imaging.ImageFormat]::Png)
$bmp.Dispose()
$web.Dispose()
"""

        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=35,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )

        if completed.returncode != 0 or not output_path.exists():
            stderr = (completed.stderr or "").strip()
            stdout = (completed.stdout or "").strip()
            details = stderr or stdout or f"code={completed.returncode}"
            raise RuntimeError(f"IE WebBrowser fallback failed: {details}")

    def _capture_webpage_screenshot(self, source_url: str, *, metadata_source_url: str = "") -> tuple[Path, str]:
        now = datetime.now()
        captured_at = now.strftime("%d.%m.%Y %H:%M:%S")

        target = self._normalize_screenshot_target(source_url)
        meta_source_url = self.engine._normalize_spaces(metadata_source_url) or target
        host = urlparse(target).netloc or Path(target).stem or "source"
        safe_host = re.sub(r"[^a-zA-Z0-9_.-]", "_", host)
        file_name = f"{safe_host}_{now.strftime('%Y%m%d_%H%M%S')}.png"
        output_path = self._screenshot_dir / file_name

        if self._is_pdf_target(target):
            self._render_pdf_snapshot(target, output_path)
            self._annotate_screenshot_metadata(output_path, meta_source_url, captured_at)
            return output_path, captured_at

        failures: list[str] = []
        browser = self._find_headless_browser()
        if browser is not None:
            ok, details = self._capture_with_headless_browser(browser, target, output_path)
            if ok:
                self._annotate_screenshot_metadata(output_path, meta_source_url, captured_at)
                return output_path, captured_at
            failures.append(f"{browser.name}: {details}")
        else:
            failures.append("headless_browser_not_found")

        if target.startswith("http://") or target.startswith("https://"):
            splash_ok, splash_details = self._capture_with_splash(target, output_path)
            if splash_ok:
                self._annotate_screenshot_metadata(output_path, meta_source_url, captured_at)
                return output_path, captured_at
            failures.append(f"splash: {splash_details}")

        try:
            self._capture_webpage_screenshot_legacy_ie(target, output_path)
            self._annotate_screenshot_metadata(output_path, meta_source_url, captured_at)
            return output_path, captured_at
        except Exception as exc:  # noqa: BLE001
            failures.append(str(exc))

        raise RuntimeError("Не удалось создать скриншот источника: " + "; ".join(failures))

    def _on_source_screenshot_done(self, saved_path: str, captured_at: str, source_url: str, error: str, auto: bool) -> None:
        self._screenshot_busy = False
        if not self._busy:
            self.screenshot_button.configure(state=tk.NORMAL)

        if error:
            if not auto:
                messagebox.showerror("Nadin", error)
            self.status_var.set("Ошибка создания скриншота")
            if not self._busy and self._last_screenshot_path:
                self.download_screenshot_button.configure(state=tk.NORMAL)
            return

        self._last_screenshot_path = saved_path
        self._update_screenshot_preview(saved_path)
        self.screenshot_meta_var.set(f"Скриншот: {captured_at}")
        self.source_url_var.set(f"URL источника: {source_url}")

        if not self._busy:
            self.download_screenshot_button.configure(state=tk.NORMAL)
        self.status_var.set(f"Скриншот сохранен: {Path(saved_path).name}")

    def _update_screenshot_preview(self, screenshot_path: str) -> None:
        image = tk.PhotoImage(file=screenshot_path)
        max_w = 320
        max_h = 180
        ratio = max(image.width() / max_w, image.height() / max_h, 1.0)
        if ratio > 1:
            factor = int(ratio)
            if factor < ratio:
                factor += 1
            image = image.subsample(factor, factor)
        self._screenshot_preview_image = image
        self.screenshot_preview_label.configure(image=image, text="")

    def _download_screenshot(self) -> None:
        if not self._last_screenshot_path or not Path(self._last_screenshot_path).exists():
            messagebox.showwarning("Nadin", "Скриншот еще не создан")
            return

        src = Path(self._last_screenshot_path)
        target = filedialog.asksaveasfilename(
            title="Сохранить скриншот",
            initialfile=src.name,
            defaultextension=".png",
            filetypes=[("PNG image", "*.png")],
        )
        if not target:
            return

        shutil.copyfile(src, target)
        self.status_var.set(f"Скриншот сохранен в: {target}")

    def _write_trace(self, trace: list[str]) -> None:
        self.trace_text.configure(state=tk.NORMAL)
        self.trace_text.delete("1.0", tk.END)
        self.trace_text.insert("1.0", "\n".join(trace))
        self.trace_text.configure(state=tk.DISABLED)

    def _on_close(self) -> None:
        self.destroy()


def main() -> None:
    paths = configure_runtime_env()
    setup_logging()
    db_path = os.getenv("NADIN_DB_PATH", str(paths["db_dir"] / "cards.db"))
    engine = CompanyWebApp(db_path=db_path)
    app = NativeNadinApp(engine)
    app.mainloop()


if __name__ == "__main__":
    main()

