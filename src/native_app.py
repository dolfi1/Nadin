
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
from html import unescape as html_unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse


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
        ("Статус компании", "company_status"),
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
        self._card_enrichment_inflight: set[int] = set()
        self._current_card_id = 0

        self._last_source_url = ""
        self._pending_source_url = ""
        self._last_screenshot_path = ""
        self._last_screenshot_preview_path = ""
        self._screenshot_preview_image: tk.PhotoImage | None = None
        self._screenshot_viewer: tk.Toplevel | None = None
        self._screenshot_viewer_label: ttk.Label | None = None
        self._screenshot_viewer_image: tk.PhotoImage | None = None
        self._vertical_split: ttk.Panedwindow | None = None
        self._horizontal_split: ttk.Panedwindow | None = None

        self._last_profile_inn = ""
        self._last_profile_ogrn = ""
        self._last_profile_org = ""
        self._last_source_names: list[str] = []
        self._rusprofile_url_cache: dict[str, str] = {}

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
        self.screenshot_meta_var = tk.StringVar(value="Скриншот: —")
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
            entry.bind("<Button-3>", self._show_entry_context_menu, add="+")
            top.columnconfigure(col, weight=1)

        action_frame = ttk.Frame(self, padding=(10, 0, 10, 8))
        action_frame.pack(fill=tk.X)
        self.search_button = ttk.Button(action_frame, text="Найти", command=self._search)
        self.search_button.pack(side=tk.LEFT)

        self.copy_card_button = ttk.Button(action_frame, text="Копировать карточку", command=self._copy_full_card)
        self.copy_card_button.pack(side=tk.LEFT, padx=(8, 0))

        self.download_screenshot_button = ttk.Button(
            action_frame,
            text="Скачать скриншот",
            command=self._download_screenshot,
            state=tk.DISABLED,
        )
        self.download_screenshot_button.pack(side=tk.LEFT, padx=(8, 0))

        self.progress = ttk.Progressbar(action_frame, mode="indeterminate", length=260)
        self.progress.pack(side=tk.LEFT, padx=(16, 0), fill=tk.X)

        self._vertical_split = ttk.Panedwindow(self, orient=tk.VERTICAL)
        self._vertical_split.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        self._bind_paned_cursor(self._vertical_split, tk.VERTICAL)

        self._horizontal_split = ttk.Panedwindow(self._vertical_split, orient=tk.HORIZONTAL)
        self._bind_paned_cursor(self._horizontal_split, tk.HORIZONTAL)

        card_frame = ttk.Frame(self._horizontal_split)
        self._horizontal_split.add(card_frame, weight=9)
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
        self.card_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        card_container.bind("<Configure>", self._on_card_container_resize)
        self.card_tree.bind("<Control-c>", self._copy_selected_card_value)
        self.card_tree.bind("<Control-C>", self._copy_selected_card_value)
        self.card_tree.bind("<Control-a>", self._select_all_card_rows)
        self.card_tree.bind("<Control-A>", self._select_all_card_rows)
        self.card_tree.bind("<Double-1>", self._copy_selected_card_value)
        self.card_tree.bind("<ButtonPress-1>", self._on_card_tree_button_press, add="+")
        self.card_tree.bind("<B1-Motion>", self._on_card_tree_drag, add="+")
        self.card_tree.bind("<Button-3>", self._show_card_tree_context_menu, add="+")

        variants_frame = ttk.Frame(self._horizontal_split)
        self._horizontal_split.add(variants_frame, weight=8)
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
        self.result_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        variants_container.bind("<Configure>", self._on_variants_container_resize)
        self.result_tree.bind("<<TreeviewSelect>>", self._on_variant_selected)
        self.result_tree.bind("<Control-c>", self._copy_selected_variant_value)
        self.result_tree.bind("<Control-C>", self._copy_selected_variant_value)
        self.result_tree.bind("<Button-3>", self._show_result_tree_context_menu, add="+")

        self._vertical_split.add(self._horizontal_split, weight=12)

        screenshot_frame = ttk.LabelFrame(self._vertical_split, text="Превью скриншота", padding=(8, 6))
        self._vertical_split.add(screenshot_frame, weight=4)
        self.screenshot_preview_label = ttk.Label(screenshot_frame, text="Превью отсутствует", anchor="center", width=44, cursor="hand2")
        self.screenshot_preview_label.pack(side=tk.LEFT, padx=(0, 10))
        self.screenshot_preview_label.bind("<Button-1>", self._open_screenshot_viewer, add="+")

        screenshot_info = ttk.Frame(screenshot_frame)
        screenshot_info.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.screenshot_meta_entry = ttk.Entry(screenshot_info, textvariable=self.screenshot_meta_var, state="readonly")
        self.screenshot_meta_entry.pack(fill=tk.X, anchor="w")
        self._bind_entry_shortcuts(self.screenshot_meta_entry)
        self.screenshot_meta_entry.bind("<Button-3>", self._show_entry_context_menu, add="+")

        self.source_url_entry = ttk.Entry(screenshot_info, textvariable=self.source_url_var, state="readonly")
        self.source_url_entry.pack(fill=tk.X, anchor="w", pady=(4, 0))
        self._bind_entry_shortcuts(self.source_url_entry)
        self.source_url_entry.bind("<Button-3>", self._show_entry_context_menu, add="+")

        trace_frame = ttk.LabelFrame(self._vertical_split, text="Лог поиска", padding=(8, 6))
        self._vertical_split.add(trace_frame, weight=3)
        self.trace_text = tk.Text(trace_frame, height=7, wrap=tk.WORD, state=tk.DISABLED)
        self.trace_text.pack(fill=tk.BOTH, expand=True)
        self.trace_text.bind("<Control-c>", self._copy_trace_text)
        self.trace_text.bind("<Control-C>", self._copy_trace_text)
        self.trace_text.bind("<Control-a>", self._select_all_trace_text)
        self.trace_text.bind("<Control-A>", self._select_all_trace_text)
        self.trace_text.bind("<Button-3>", self._show_text_context_menu, add="+")

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

    def _popup_context_menu(self, menu: tk.Menu, event: tk.Event) -> str:
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()
        return "break"

    def _entry_is_readonly(self, widget: tk.Entry | ttk.Entry) -> bool:
        try:
            state = str(widget.cget("state")).lower()
        except tk.TclError:
            return False
        return state in {"readonly", "disabled"}

    def _show_entry_context_menu(self, event: tk.Event) -> str:
        widget = event.widget
        if not isinstance(widget, (tk.Entry, ttk.Entry)):
            return "break"
        widget.focus_set()
        readonly = self._entry_is_readonly(widget)
        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="Копировать", command=lambda: widget.event_generate("<<Copy>>"))
        menu.add_command(label="Вырезать", command=lambda: widget.event_generate("<<Cut>>"), state=(tk.DISABLED if readonly else tk.NORMAL))
        menu.add_command(label="Вставить", command=lambda: widget.event_generate("<<Paste>>"), state=(tk.DISABLED if readonly else tk.NORMAL))
        menu.add_separator()
        menu.add_command(label="Выделить всё", command=lambda: widget.event_generate("<<SelectAll>>"))
        return self._popup_context_menu(menu, event)

    def _show_text_context_menu(self, event: tk.Event) -> str:
        widget = event.widget
        if not isinstance(widget, tk.Text):
            return "break"
        widget.focus_set()
        state = str(widget.cget("state")).lower()
        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="Копировать", command=lambda: self._copy_trace_text())
        menu.add_command(label="Вставить", command=lambda: widget.event_generate("<<Paste>>"), state=(tk.NORMAL if state == "normal" else tk.DISABLED))
        menu.add_separator()
        menu.add_command(label="Выделить всё", command=lambda: self._select_all_trace_text())
        return self._popup_context_menu(menu, event)

    def _show_card_tree_context_menu(self, event: tk.Event) -> str:
        row_id = self.card_tree.identify_row(event.y)
        if row_id:
            self.card_tree.selection_set(row_id)
            self.card_tree.focus(row_id)
        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="Копировать выделенное", command=self._copy_selected_card_value)
        menu.add_command(label="Копировать карточку", command=self._copy_full_card)
        menu.add_separator()
        menu.add_command(label="Выделить всё", command=self._select_all_card_rows)
        return self._popup_context_menu(menu, event)

    def _show_result_tree_context_menu(self, event: tk.Event) -> str:
        row_id = self.result_tree.identify_row(event.y)
        if row_id:
            self.result_tree.selection_set(row_id)
            self.result_tree.focus(row_id)
        menu = tk.Menu(self, tearoff=False)
        menu.add_command(label="Копировать вариант", command=self._copy_selected_variant_value)
        return self._popup_context_menu(menu, event)

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
        total_width = max(int(event.width) - 6, 340)
        field_width = min(240, max(180, int(total_width * 0.34)))
        value_width = max(230, total_width - field_width)
        self.card_tree.column("field", width=field_width, stretch=False)
        self.card_tree.column("value", width=value_width, stretch=True)

    def _on_variants_container_resize(self, event: tk.Event) -> None:
        total_width = max(int(event.width) - 6, 430)
        inn_width = 130
        source_width = 220
        org_width = max(220, total_width - inn_width - source_width)
        self.result_tree.column("org", width=org_width, stretch=True)
        self.result_tree.column("inn", width=inn_width, stretch=False)
        self.result_tree.column("source", width=source_width, stretch=False)

    def _bind_paned_cursor(self, paned: ttk.Panedwindow, orient: str) -> None:
        paned.bind("<Motion>", lambda event, p=paned, o=orient: self._update_paned_cursor(p, o, event), add="+")
        paned.bind("<Leave>", lambda _event, p=paned: p.configure(cursor=""), add="+")

    def _update_paned_cursor(self, paned: ttk.Panedwindow, orient: str, event: tk.Event) -> None:
        try:
            pane_count = len(paned.panes())
        except Exception:  # noqa: BLE001
            pane_count = 0
        if pane_count <= 1:
            paned.configure(cursor="")
            return

        coordinate = int(event.x if orient == tk.HORIZONTAL else event.y)
        tolerance = 8
        over_sash = False
        for sash_index in range(pane_count - 1):
            try:
                sash_position = int(paned.sashpos(sash_index))
            except Exception:  # noqa: BLE001
                continue
            if abs(coordinate - sash_position) <= tolerance:
                over_sash = True
                break

        paned.configure(cursor="sb_h_double_arrow" if over_sash and orient == tk.HORIZONTAL else "sb_v_double_arrow" if over_sash else "")

    def _set_busy(self, busy: bool, status: str = "") -> None:
        self._busy = busy
        state = tk.DISABLED if busy else tk.NORMAL
        self.search_button.configure(state=state)
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

            source_name = self.engine._normalize_spaces(str(hit.get("source", ""))) or "—"
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
        profile["middle_name_en"] = self.engine._normalize_spaces(str(profile.get("middle_name_en", "")))

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

    def _extract_redirect_error(self, redirect: str) -> str:
        normalized = self.engine._normalize_spaces(redirect)
        if not normalized:
            return ""
        parsed = urlparse(normalized)
        if not parsed.path.endswith('/create/manual'):
            return ""
        query = parse_qs(parsed.query)
        return self.engine._normalize_spaces(unquote(query.get('error', [''])[0]))

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
            self.status_var.set("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0443")
            self._render_card_rows([("Состояние", error)], "Карточка")
            return

        try:
            self._show_card(card_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to render autofill result for card_id=%s", card_id)
            self.status_var.set("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043e\u0431\u0440\u0430\u0437\u0438\u0442\u044c \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0443")
            self._render_card_rows([("Состояние", str(exc))], "Карточка")
            return

        self.status_var.set(f"\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 #{card_id} \u0441\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u0430")
        if self._last_source_url or self._last_profile_inn or self._last_profile_ogrn or self._last_profile_org:
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
        normalized_profile = {
            key: self.engine._normalize_spaces(str(value))
            for key, value in profile.items()
        }
        company_status = self.engine._normalize_spaces(str(normalized_profile.get("company_status", "")))
        has_company = bool(normalized_profile.get("ru_org"))
        has_leader = bool(normalized_profile.get("surname_ru") and normalized_profile.get("name_ru"))
        inactive_checker = getattr(self.engine, "_is_inactive_company_status", None)
        inactive_company = bool(inactive_checker(company_status)) if callable(inactive_checker) else False
        compact_company = has_company and not has_leader

        if inactive_company:
            field_layout: list[tuple[str, str]] = [
                ("ИНН", "inn"),
                ("Организация", "ru_org"),
                ("Organization", "en_org"),
                ("Статус", "company_status"),
            ]
        elif compact_company:
            field_layout = [
                ("ИНН", "inn"),
                ("Организация", "ru_org"),
                ("Organization", "en_org"),
                ("Статус", "company_status"),
                ("Должность", "ru_position"),
                ("Position", "en_position"),
            ]
        else:
            field_layout = list(self.CARD_FIELDS)

        rows: list[tuple[str, str]] = []
        for label, key in field_layout:
            value = self.engine._normalize_spaces(str(normalized_profile.get(key, "")))
            if key == "company_status" and has_company and not value and not inactive_company:
                value = "Не указан"
            if compact_company and not value and key in {"en_org", "company_status", "ru_position", "en_position"}:
                continue
            rows.append((label, value))

        if not inactive_company:
            rows.append(("Выручка", revenue_line))
            rows.append(("Прибыль", profit_line))

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
        merged["middle_name_en"] = self.engine._normalize_spaces(str(merged.get("middle_name_en", "")))

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
            year = self.engine._parse_financial_year(year_value) or year_from_amount
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
            return f"\u0414\u0430\u043d\u043d\u044b\u0445 \u043d\u0435\u0442 ({fallback_year})"

        metric_candidates.sort(key=lambda item: (item[0], abs(item[1])), reverse=True)
        best_year, best_amount = metric_candidates[0]
        year = best_year if best_year > 0 else fallback_year
        return self.engine._format_financial_line(best_amount, year)

    def _show_card(self, card_id: int) -> None:
        self._current_card_id = card_id
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
        normalized_profile = {k: self.engine._normalize_spaces(str(v)) for k, v in profile.items()}
        render_card_type = "person_in_company" if (
            normalized_profile.get("ru_org")
            and normalized_profile.get("surname_ru")
            and normalized_profile.get("name_ru")
        ) else ""
        try:
            normalized_profile, _ = self.engine.apply_card_rules(normalized_profile, card_type=render_card_type)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to normalize card %s before render", card_id)
        profile = normalized_profile

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
        base_source_url = self._extract_source_url(payload, fallback_url=self._pending_source_url)
        sanitized_rusprofile_url = self._sanitize_rusprofile_detail_url(base_source_url)
        if sanitized_rusprofile_url:
            base_source_url = sanitized_rusprofile_url
            self._append_source_name(source_names, 'rusprofile.ru')

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
        self._last_source_url = base_source_url
        self._pending_source_url = ""
        self.source_url_var.set(f"URL источника: {self._last_source_url or chr(8212)}")

        if self._needs_rusprofile_enrichment(card_id, profile, source_hits, source_names, self._last_source_url):
            self._schedule_rusprofile_enrichment(card_id, self._last_source_url)

    def _render_card_rows(self, rows: list[tuple[str, str]], title: str) -> None:
        self.card_title_var.set(title)
        self._current_card_rows = list(rows)
        for iid in self.card_tree.get_children():
            self.card_tree.delete(iid)

        if not rows:
            rows = [("Состояние", "Нет данных для отображения")]
            self._current_card_rows = list(rows)

        for label, value in rows:
            display = self.engine._normalize_spaces(str(value)) if value is not None else ""
            self.card_tree.insert("", tk.END, values=(label, display or "\u2014"))

    def _copy_selected_card_value(self, _event: object | None = None) -> str:
        selected = self.card_tree.selection()
        if not selected:
            return "break"

        lines: list[str] = []
        for iid in selected:
            values = self.card_tree.item(iid, "values")
            if not values or len(values) < 2:
                continue
            lines.append(f"{values[0]}\t{values[1]}")

        if lines:
            self.clipboard_clear()
            self.clipboard_append("\n".join(lines))
            self.status_var.set("\u0412\u044b\u0431\u0440\u0430\u043d\u043d\u044b\u0435 \u043f\u043e\u043b\u044f \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438 \u0441\u043a\u043e\u043f\u0438\u0440\u043e\u0432\u0430\u043d\u044b")
        return "break"

    def _copy_full_card(self) -> None:
        if not self._current_card_rows:
            self.status_var.set("\u041d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445 \u0434\u043b\u044f \u043a\u043e\u043f\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f")
            return

        lines: list[str] = []
        for label, value in self._current_card_rows:
            display = self.engine._normalize_spaces(str(value)) if value is not None else ""
            lines.append(f"{label}\t{display or chr(8212)}")

        self.clipboard_clear()
        self.clipboard_append("\n".join(lines))
        self.status_var.set("\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u0441\u043a\u043e\u043f\u0438\u0440\u043e\u0432\u0430\u043d\u0430")

    def _copy_selected_variant_value(self, _event: object | None = None) -> str:
        selected = self.result_tree.selection()
        if not selected:
            return "break"
        values = self.result_tree.item(selected[0], "values")
        if values:
            self.clipboard_clear()
            self.clipboard_append("\t".join(str(v) for v in values))
            self.status_var.set("\u0412\u044b\u0431\u0440\u0430\u043d\u043d\u044b\u0439 \u0432\u0430\u0440\u0438\u0430\u043d\u0442 \u0441\u043a\u043e\u043f\u0438\u0440\u043e\u0432\u0430\u043d")
        return "break"

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

    def _append_source_name(self, source_names: list[str], source_name: str) -> list[str]:
        normalized = self.engine._normalize_spaces(str(source_name))
        if not normalized:
            return source_names
        seen = {self.engine._normalize_spaces(str(item)).lower() for item in source_names}
        if normalized.lower() not in seen:
            source_names.append(normalized)
        return source_names

    def _has_rusprofile_source(self, source_hits: list[dict[str, Any]], source_names: list[str] | None = None) -> bool:
        names = source_names or []
        for source_name in names:
            if self.engine._normalize_spaces(str(source_name)).lower() == 'rusprofile.ru':
                return True
        for hit in source_hits:
            if not isinstance(hit, dict):
                continue
            source_name = self.engine._normalize_spaces(str(hit.get('source', ''))).lower()
            if source_name == 'rusprofile.ru':
                return True
        return False

    def _has_rusprofile_payload(self, source_hits: list[dict[str, Any]]) -> bool:
        meaningful_keys = {
            'ru_org', 'en_org', 'inn', 'ogrn', 'surname_ru', 'name_ru', 'middle_name_ru',
            'ru_position', 'en_position', 'company_status', 'revenue', 'profit',
        }
        for hit in source_hits:
            if not isinstance(hit, dict):
                continue
            if self.engine._normalize_spaces(str(hit.get('source', ''))).lower() != 'rusprofile.ru':
                continue
            data = hit.get('data', {})
            if not isinstance(data, dict):
                continue
            for key in meaningful_keys:
                if self._profile_has_value(data.get(key, '')):
                    return True
        return False

    def _profile_has_value(self, value: Any) -> bool:
        normalized = self.engine._normalize_spaces(str(value))
        return bool(normalized and normalized != chr(8212))

    def _needs_rusprofile_enrichment(self, card_id: int, profile: dict[str, Any], source_hits: list[dict[str, Any]], source_names: list[str], source_url: str) -> bool:
        if card_id in self._card_enrichment_inflight:
            return False

        has_rusprofile_source = self._has_rusprofile_source(source_hits, source_names)
        has_rusprofile_payload = self._has_rusprofile_payload(source_hits)
        has_rusprofile_url = bool(self._sanitize_rusprofile_detail_url(source_url))
        lookup_query = self._get_rusprofile_lookup_query()
        if not has_rusprofile_url and not lookup_query:
            return False
        if has_rusprofile_payload:
            return False

        important_keys = ('surname_ru', 'name_ru', 'middle_name_ru', 'ru_position', 'en_position', 'company_status')
        missing_important = any(not self._profile_has_value(profile.get(key, '')) for key in important_keys)

        revenue_value = self.engine._parse_financial_amount(profile.get('revenue', 0))
        if revenue_value == 0:
            for hit in source_hits:
                data = hit.get('data', {}) if isinstance(hit, dict) else {}
                if isinstance(data, dict) and self.engine._parse_financial_amount(data.get('revenue', 0)) != 0:
                    revenue_value = 1
                    break

        return missing_important or revenue_value == 0 or not has_rusprofile_source

    def _merge_rusprofile_profile(self, current_profile: dict[str, Any], rusprofile_profile: dict[str, Any]) -> dict[str, Any]:
        merged = dict(current_profile)
        for key, value in rusprofile_profile.items():
            if key in {'url', 'source'}:
                continue
            if isinstance(value, str):
                normalized_value = self.engine._normalize_spaces(value)
            else:
                normalized_value = value

            if key in {'revenue', 'profit'}:
                current_amount = self.engine._parse_financial_amount(merged.get(key, 0))
                new_amount = self.engine._parse_financial_amount(normalized_value)
                if current_amount == 0 and new_amount != 0:
                    merged[key] = normalized_value
                continue

            if key.endswith('_year') or key in {'financial_year', 'year', 'report_year'}:
                current_year = self.engine._parse_financial_year(merged.get(key, ''))
                new_year = self.engine._parse_financial_year(normalized_value)
                if current_year == 0 and new_year != 0:
                    merged[key] = normalized_value
                continue

            if not self._profile_has_value(merged.get(key, '')) and self._profile_has_value(normalized_value):
                merged[key] = normalized_value
        return merged

    def _upsert_rusprofile_hit(self, source_hits: list[dict[str, Any]], source_url: str, rusprofile_profile: dict[str, Any]) -> list[dict[str, Any]]:
        cleaned_hits = [hit for hit in source_hits if isinstance(hit, dict)]
        hit_type = 'person' if self.engine._normalize_spaces(str(rusprofile_profile.get('surname_ru', ''))) else 'company'
        new_hit = {
            'source': 'rusprofile.ru',
            'url': source_url,
            'type': hit_type,
            'data': dict(rusprofile_profile),
        }
        for idx, hit in enumerate(cleaned_hits):
            source_name = self.engine._normalize_spaces(str(hit.get('source', ''))).lower()
            if source_name == 'rusprofile.ru':
                cleaned_hits[idx] = new_hit
                return cleaned_hits
        cleaned_hits.append(new_hit)
        return cleaned_hits

    def _ensure_rusprofile_source_hit(self, card_id: int, source_url: str) -> bool:
        resolved_url = self._sanitize_rusprofile_detail_url(source_url)
        if not card_id or not resolved_url:
            return False

        with self.engine._connect() as db:
            row = db.execute('SELECT data_json FROM cards WHERE id=?', (card_id,)).fetchone()
            if row is None:
                return False
            payload = json.loads(row['data_json'] or '{}')
            if not isinstance(payload, dict):
                payload = {}
            source_hits = payload.get('source_hits', [])
            if not isinstance(source_hits, list):
                source_hits = []

            updated = False
            placeholder = {
                'source': 'rusprofile.ru',
                'url': resolved_url,
                'type': 'company' if self._last_profile_org else 'person',
                'data': {},
            }
            for hit in source_hits:
                if not isinstance(hit, dict):
                    continue
                if self.engine._normalize_spaces(str(hit.get('source', ''))).lower() != 'rusprofile.ru':
                    continue
                if self.engine._normalize_spaces(str(hit.get('url', ''))) != resolved_url:
                    hit['url'] = resolved_url
                    updated = True
                break
            else:
                source_hits.append(placeholder)
                updated = True

            if updated:
                payload['source_hits'] = source_hits
                db.execute('UPDATE cards SET data_json=? WHERE id=?', (json.dumps(payload, ensure_ascii=False), card_id))
            return updated

    def _schedule_rusprofile_enrichment(self, card_id: int, source_url: str) -> None:
        if card_id in self._card_enrichment_inflight:
            return

        self._card_enrichment_inflight.add(card_id)
        if self._current_card_id == card_id:
            self.status_var.set('Карточка показана. Уточняем данные из rusprofile...')

        def worker() -> None:
            error = ''
            updated = False
            resolved_url = ''
            try:
                resolved_url = self._resolve_rusprofile_source_url(source_url) or self._resolve_rusprofile_source_url('')
                resolved_url = self._sanitize_rusprofile_detail_url(resolved_url)
                if not resolved_url:
                    raise RuntimeError('rusprofile_url_not_found')

                rusprofile_profile = self.engine._parse_rusprofile(resolved_url)
                if not isinstance(rusprofile_profile, dict) or not rusprofile_profile:
                    raise RuntimeError('rusprofile_profile_empty')

                with self.engine._connect() as db:
                    row = db.execute('SELECT data_json FROM cards WHERE id=?', (card_id,)).fetchone()
                    if row is None:
                        raise RuntimeError('card_not_found')
                    payload = json.loads(row['data_json'] or '{}')
                    if not isinstance(payload, dict):
                        payload = {}
                    profile = payload.get('profile', {})
                    if not isinstance(profile, dict):
                        profile = {}
                    source_hits = payload.get('source_hits', [])
                    if not isinstance(source_hits, list):
                        source_hits = []

                    merged_profile = self._merge_rusprofile_profile(profile, rusprofile_profile)
                    merged_hits = self._upsert_rusprofile_hit(source_hits, resolved_url, rusprofile_profile)

                    if merged_profile != profile or merged_hits != source_hits:
                        payload['profile'] = merged_profile
                        payload['source_hits'] = merged_hits
                        db.execute('UPDATE cards SET data_json=? WHERE id=?', (json.dumps(payload, ensure_ascii=False), card_id))
                        updated = True
            except Exception as exc:  # noqa: BLE001
                logger.exception('RusProfile enrichment failed for card %s', card_id)
                error = str(exc)

            self.after(0, lambda: self._on_rusprofile_enrichment_done(card_id, resolved_url, updated, error))

        threading.Thread(target=worker, daemon=True).start()

    def _on_rusprofile_enrichment_done(self, card_id: int, source_url: str, updated: bool, error: str) -> None:
        self._card_enrichment_inflight.discard(card_id)
        if error:
            if self._current_card_id == card_id and not self._busy and not self._screenshot_busy:
                self.status_var.set('Карточка сформирована')
            return
        if not updated:
            if self._current_card_id == card_id and not self._busy and not self._screenshot_busy:
                self.status_var.set('Карточка сформирована')
            return
        if self._current_card_id != card_id:
            return
        self._pending_source_url = source_url
        self._show_card(card_id)
        if not self._screenshot_busy:
            self.after(120, lambda: self._capture_source_screenshot(auto=True))
        else:
            self.status_var.set('Данные rusprofile добавлены')

    def _score_source_url_for_screenshot(self, source_url: str, *, is_fallback: bool = False) -> int:
        normalized = self.engine._normalize_spaces(source_url)
        if not normalized:
            return -1000
        if not (normalized.startswith("http://") or normalized.startswith("https://")):
            return -1000

        parsed = urlparse(normalized)
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
            "rusprofile.ru",
            "zachestnyibiznes.ru",
            "companies.rbc.ru",
            "focus.kontur.ru",
            "checko.ru",
            "list-org.com",
        )
        if any(host.endswith(domain) for domain in trusted_hosts):
            score += 140

        if host.endswith("rusprofile.ru"):
            score += 220
            if "/id/" in path:
                score += 180

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
        if not (normalized.startswith("http://") or normalized.startswith("https://")):
            return ""

        sanitized_rusprofile = self._sanitize_rusprofile_detail_url(normalized)
        if sanitized_rusprofile:
            return sanitized_rusprofile

        parsed = urlparse(normalized)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if host == "egrul.itsoft.ru" and path.endswith(".json"):
            return ""

        return normalized

    def _get_rusprofile_lookup_query(self) -> str:
        for raw in (self._last_profile_inn, self._last_profile_ogrn, self._last_profile_org):
            value = self.engine._normalize_spaces(raw)
            if value:
                return value
        return ""

    def _sanitize_rusprofile_detail_url(self, value: str) -> str:
        raw_value = self.engine._normalize_spaces(str(value))
        if not raw_value:
            return ""

        variants = [raw_value]
        for _ in range(2):
            variants.append(html_unescape(variants[-1]))
            variants.append(unquote(variants[-1]))

        patterns = (
            r'https?://(?:www\.)?rusprofile\.ru(?P<path>/id/\d+)',
            r'(?P<path>/id/\d+)',
        )

        for text_variant in variants:
            for pattern in patterns:
                match = re.search(pattern, text_variant, flags=re.IGNORECASE)
                if not match:
                    continue
                path = match.groupdict().get("path", "")
                if path:
                    return f"https://www.rusprofile.ru{path}"
        return ""

    def _extract_rusprofile_detail_url_from_html(self, html: str) -> str:
        raw_html = str(html or "")
        variants = [raw_html]
        for _ in range(2):
            variants.append(html_unescape(variants[-1]))
            variants.append(unquote(variants[-1]))

        patterns = (
            r'href=[\'"](?P<path>/id/\d+[^\'"]*)[\'"]',
            r'https?://(?:www\.)?rusprofile\.ru/id/\d+[^"\'\s<]*',
        )

        for text_variant in variants:
            for pattern in patterns:
                match = re.search(pattern, text_variant, flags=re.IGNORECASE)
                if not match:
                    continue
                path = match.groupdict().get("path", "")
                candidate = f"https://www.rusprofile.ru{path}" if path else match.group(0)
                sanitized = self._sanitize_rusprofile_detail_url(candidate)
                if sanitized:
                    return sanitized
        return ""

    def _lookup_rusprofile_url(self, query: str) -> str:
        normalized_query = self.engine._normalize_spaces(query)
        if not normalized_query:
            return ""

        cache_key = normalized_query.lower()
        cached = self._sanitize_rusprofile_detail_url(self._rusprofile_url_cache.get(cache_key, ""))
        if cached:
            return cached

        search_url = f"https://www.rusprofile.ru/search?query={quote(normalized_query)}"
        resolved = ""

        try:
            html = self.engine._fetch_page(search_url, timeout=10, max_retries=1)
        except Exception:  # noqa: BLE001
            html = ""

        is_blocked_html = False
        checker = getattr(self.engine, "_is_captcha_or_block", None)
        if html and callable(checker):
            try:
                is_blocked_html = bool(checker(html, search_url))
            except Exception:  # noqa: BLE001
                is_blocked_html = False

        if html and not is_blocked_html:
            resolved_from_html = self._extract_rusprofile_detail_url_from_html(html)
            if resolved_from_html:
                resolved = resolved_from_html

        if not resolved:
            ddg_query = quote(f"site:rusprofile.ru/id {normalized_query}")
            ddg_url = f"https://duckduckgo.com/html/?q={ddg_query}"
            try:
                ddg_html = self.engine._fetch_page(ddg_url, timeout=8, max_retries=1)
            except Exception:  # noqa: BLE001
                ddg_html = ""
            resolved_from_ddg = self._extract_rusprofile_detail_url_from_html(ddg_html)
            if resolved_from_ddg:
                resolved = resolved_from_ddg

        self._rusprofile_url_cache[cache_key] = resolved or ""
        return resolved

    def _resolve_rusprofile_source_url(self, source_url: str) -> str:
        normalized = self._normalize_screenshot_target(source_url)
        if normalized.startswith("http://") or normalized.startswith("https://"):
            sanitized = self._sanitize_rusprofile_detail_url(normalized)
            if sanitized:
                return sanitized

        lookup_query = self._get_rusprofile_lookup_query()
        if lookup_query:
            resolved = self._lookup_rusprofile_url(lookup_query)
            if resolved:
                return resolved

        if normalized.startswith("http://") or normalized.startswith("https://"):
            parsed = urlparse(normalized)
            host = parsed.netloc.lower()
            if host.endswith("rusprofile.ru"):
                return ""
            if normalized and not self._is_machine_source_url(normalized) and not self._is_generic_landing_url(normalized):
                return normalized
        return normalized

    def _capture_source_screenshot(self, auto: bool) -> None:
        if self._screenshot_busy:
            return

        source_url = self._normalize_screenshot_target(self._last_source_url)
        if not source_url:
            source_url = self._resolve_rusprofile_source_url("")
        if not source_url:
            self.screenshot_meta_var.set("Скриншот: источник не найден")
            self.source_url_var.set(f"URL источника: {chr(8212)}")
            if not self._last_screenshot_path:
                self._screenshot_preview_image = None
                self.screenshot_preview_label.configure(image="", text="Превью отсутствует")
            return

        self._screenshot_busy = True
        self.download_screenshot_button.configure(state=tk.DISABLED)
        if not self._busy:
            self.progress.start(12)
        self.screenshot_meta_var.set("Скриншот: создается...")
        self.source_url_var.set(f"URL источника: {source_url}")
        self._screenshot_preview_image = None
        self.screenshot_preview_label.configure(image="", text="Создается превью...")
        self.status_var.set("Создание скриншота rusprofile...")

        def worker() -> None:
            error = ""
            saved_path = ""
            preview_path = ""
            captured_at = ""
            display_source_url = source_url
            try:
                resolved = self._resolve_rusprofile_source_url(source_url) or source_url
                display_source_url = resolved
                logger.info("RusProfile screenshot lookup: base=%s resolved=%s", source_url, resolved)

                if not self._is_supported_screenshot_target(resolved):
                    raise RuntimeError("URL источника не поддерживается")

                path, captured_at = self._capture_webpage_screenshot(
                    resolved,
                    metadata_source_url=display_source_url,
                )
                saved_path = str(path)
                preview_path = str(self._build_screenshot_preview_asset(path))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to capture source screenshot")
                error = str(exc)
            self.after(
                0,
                lambda: self._on_source_screenshot_done(
                    saved_path,
                    preview_path,
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
        return value.startswith("http://") or value.startswith("https://")

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

    def _build_screenshot_preview_asset(self, image_path: Path) -> Path:
        if Image is None:
            return image_path

        preview_path = image_path.with_name(f"{image_path.stem}_preview.png")
        with Image.open(image_path).convert("RGB") as source:
            preview = source.copy()
            resampling = getattr(Image, "Resampling", None)
            lanczos = getattr(resampling, "LANCZOS", getattr(Image, "LANCZOS", getattr(Image, "ANTIALIAS", 1)))
            preview.thumbnail((320, 180), lanczos)
            preview.save(preview_path, format="PNG")
        return preview_path

    def _load_screenshot_font(self, size: int, *, bold: bool = False) -> ImageFont.ImageFont:
        if ImageFont is None:
            raise RuntimeError("Pillow font support is unavailable")

        windows_fonts = Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts"
        candidates = [
            windows_fonts / ("segoeuib.ttf" if bold else "segoeui.ttf"),
            windows_fonts / ("arialbd.ttf" if bold else "arial.ttf"),
        ]
        for candidate in candidates:
            if candidate.exists():
                try:
                    return ImageFont.truetype(str(candidate), size=size)
                except Exception:  # noqa: BLE001
                    continue
        return ImageFont.load_default()

    def _draw_browser_frame(self, draw: ImageDraw.ImageDraw, width: int, font: ImageFont.ImageFont, source_url: str) -> None:
        title_bar_height = 32
        toolbar_height = 46
        draw.rectangle((0, 0, width, title_bar_height), fill="#1c1f24")
        draw.rectangle((0, title_bar_height, width, title_bar_height + toolbar_height), fill="#2b2f36")
        draw.line((0, title_bar_height + toolbar_height, width, title_bar_height + toolbar_height), fill="#111318", width=1)

        tab_left = 44
        tab_top = 5
        tab_width = min(220, max(180, width // 6))
        tab_bottom = title_bar_height + 2
        draw.rounded_rectangle((tab_left, tab_top, tab_left + tab_width, tab_bottom), radius=6, fill="#2b2f36", outline="#454b55", width=1)
        draw.text((tab_left + 14, tab_top + 8), "rusprofile.ru", font=font, fill="#f3f4f6")

        button_y = 0
        button_w = 46
        right = width
        buttons = [
            (right - button_w * 3, "min"),
            (right - button_w * 2, "max"),
            (right - button_w, "close"),
        ]
        for x, kind in buttons:
            fill = "#c42b1c" if kind == "close" else "#1c1f24"
            draw.rectangle((x, button_y, x + button_w, title_bar_height), fill=fill)
            cx = x + button_w // 2
            cy = title_bar_height // 2
            if kind == "min":
                draw.line((cx - 8, cy + 5, cx + 8, cy + 5), fill="#f3f4f6", width=2)
            elif kind == "max":
                draw.rectangle((cx - 7, cy - 5, cx + 7, cy + 7), outline="#f3f4f6", width=2)
            else:
                draw.line((cx - 7, cy - 6, cx + 7, cy + 6), fill="#ffffff", width=2)
                draw.line((cx - 7, cy + 6, cx + 7, cy - 6), fill="#ffffff", width=2)

        nav_y = title_bar_height + 14
        arrow_color = "#cfd6df"
        draw.line((18, nav_y + 4, 10, nav_y + 11), fill=arrow_color, width=2)
        draw.line((10, nav_y + 11, 18, nav_y + 18), fill=arrow_color, width=2)
        draw.line((32, nav_y + 4, 40, nav_y + 11), fill=arrow_color, width=2)
        draw.line((40, nav_y + 11, 32, nav_y + 18), fill=arrow_color, width=2)

        address_left = 64
        address_top = title_bar_height + 8
        address_right = width - 150
        address_bottom = address_top + 30
        draw.rounded_rectangle(
            (address_left, address_top, address_right, address_bottom),
            radius=7,
            fill="#f5f7fa",
            outline="#7d8590",
            width=1,
        )

        text_width = max(160, address_right - address_left - 18)
        url_lines = self._wrap_overlay_line(draw, source_url, font, text_width)
        address_text = url_lines[0] if url_lines else source_url
        if len(address_text) > 120:
            address_text = address_text[:117] + "..."
        draw.text((address_left + 12, address_top + 7), address_text, font=font, fill="#111827")
    def _draw_windows_taskbar(self, draw: ImageDraw.ImageDraw, width: int, top_y: int, height: int, font: ImageFont.ImageFont, captured_dt: datetime) -> None:
        draw.rectangle((0, top_y, width, top_y + height), fill="#101114")
        icon_x = 16
        icon_y = top_y + 10
        draw.rectangle((icon_x, icon_y, icon_x + 16, icon_y + 16), fill="#2a7de1")
        draw.line((icon_x + 8, icon_y, icon_x + 8, icon_y + 16), fill="#f5f7fa", width=1)
        draw.line((icon_x, icon_y + 8, icon_x + 16, icon_y + 8), fill="#f5f7fa", width=1)

        time_text = captured_dt.strftime("%H:%M")
        date_text = captured_dt.strftime("%d.%m.%Y")
        time_bbox = draw.textbbox((0, 0), time_text, font=font)
        date_bbox = draw.textbbox((0, 0), date_text, font=font)
        text_width = max(time_bbox[2] - time_bbox[0], date_bbox[2] - date_bbox[0])
        time_x = width - text_width - 18
        draw.text((time_x, top_y + 6), time_text, font=font, fill="#f8fafc")
        draw.text((time_x, top_y + 20), date_text, font=font, fill="#d5dbe3")

    def _annotate_screenshot_metadata(self, image_path: Path, source_url: str, captured_at: str) -> None:
        if Image is None or ImageDraw is None or ImageFont is None:
            return

        with Image.open(image_path).convert("RGB") as body:
            font = self._load_screenshot_font(15)
            taskbar_font = self._load_screenshot_font(13)
            captured_dt = datetime.strptime(captured_at, "%d.%m.%Y %H:%M:%S")
            top_height = 78
            bottom_height = 40
            result = Image.new("RGB", (body.width, body.height + top_height + bottom_height), "#dfe3e8")
            result.paste(body, (0, top_height))

            draw = ImageDraw.Draw(result)
            self._draw_browser_frame(draw, body.width, font, source_url)
            self._draw_windows_taskbar(draw, body.width, top_height + body.height, bottom_height, taskbar_font, captured_dt)
            result.save(image_path, format="PNG")

    def _find_headless_browser(self) -> Path | None:
        env_candidates = [
            os.getenv("NADIN_SCREENSHOT_BROWSER", ""),
            os.path.join(os.getenv("ProgramFiles", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("ProgramFiles(x86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("LocalAppData", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.getenv("ProgramFiles", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
            os.path.join(os.getenv("ProgramFiles(x86)", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
            os.path.join(os.getenv("LocalAppData", ""), "Microsoft", "Edge", "Application", "msedge.exe"),
        ]

        for raw_path in env_candidates:
            candidate = raw_path.strip()
            if not candidate:
                continue
            path = Path(os.path.expandvars(candidate))
            if path.exists():
                return path

        command_candidates = ["chrome", "chrome.exe", "chromium", "chromium.exe", "msedge", "msedge.exe"]
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
                    "--window-size=1600,900",
                    "--lang=ru-RU",
                    "--virtual-time-budget=3000",
                    f"--user-data-dir={profile_dir}",
                    f"--user-agent={user_agent}",
                    f"--screenshot={output_path}",
                    source_url,
                ]
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                    creationflags=creationflags,
                )
            if output_path.exists() and output_path.stat().st_size > 0:
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

        use_splash = os.getenv("NADIN_SCREENSHOT_USE_SPLASH", "").strip().lower() in {"1", "true", "yes"}
        if use_splash and (target.startswith("http://") or target.startswith("https://")):
            splash_ok, splash_details = self._capture_with_splash(target, output_path)
            if splash_ok:
                self._annotate_screenshot_metadata(output_path, meta_source_url, captured_at)
                return output_path, captured_at
            failures.append(f"splash: {splash_details}")

        raise RuntimeError("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0437\u0434\u0430\u0442\u044c \u0441\u043a\u0440\u0438\u043d\u0448\u043e\u0442: " + "; ".join(failures))

    def _on_source_screenshot_done(self, saved_path: str, preview_path: str, captured_at: str, source_url: str, error: str, auto: bool) -> None:
        self._screenshot_busy = False
        if not self._busy:
            self.progress.stop()
        if error:
            self.screenshot_meta_var.set("Скриншот: ошибка")
            self.source_url_var.set(f"URL источника: {source_url or chr(8212)}")
            if not self._last_screenshot_path:
                self._screenshot_preview_image = None
                self.screenshot_preview_label.configure(image="", text="Превью отсутствует")
            self.status_var.set("Скриншот источника не создан")
            if not self._busy and self._last_screenshot_path:
                self.download_screenshot_button.configure(state=tk.NORMAL)
            return

        self._last_screenshot_path = saved_path
        self._last_screenshot_preview_path = preview_path
        self._update_screenshot_preview(preview_path or saved_path)
        self.screenshot_meta_var.set(f"Скриншот: {captured_at}")
        self.source_url_var.set(f"URL источника: {source_url}")

        sanitized_rusprofile = self._sanitize_rusprofile_detail_url(source_url)
        refresh_card = False
        if self._current_card_id and sanitized_rusprofile:
            had_rusprofile_source = 'rusprofile.ru' in {item.lower() for item in self._last_source_names}
            self._append_source_name(self._last_source_names, 'rusprofile.ru')
            refresh_card = self._ensure_rusprofile_source_hit(self._current_card_id, sanitized_rusprofile) or not had_rusprofile_source

        if refresh_card and self._current_card_id:
            self._pending_source_url = sanitized_rusprofile
            self._show_card(self._current_card_id)
        elif self._current_card_id and sanitized_rusprofile:
            self._schedule_rusprofile_enrichment(self._current_card_id, sanitized_rusprofile)

        if not self._busy:
            self.download_screenshot_button.configure(state=tk.NORMAL)
        self.status_var.set(f"Скриншот сохранен: {Path(saved_path).name}")

    def _update_screenshot_preview(self, screenshot_path: str) -> None:
        if not screenshot_path:
            return
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
        self._update_screenshot_viewer_image()

    def _open_screenshot_viewer(self, _event: tk.Event | None = None) -> str:
        if not self._last_screenshot_path or not Path(self._last_screenshot_path).exists():
            return "break"

        if self._screenshot_viewer is None or not self._screenshot_viewer.winfo_exists():
            viewer = tk.Toplevel(self)
            viewer.title("Просмотр скриншота")
            viewer.geometry("1200x780")
            viewer.minsize(720, 520)
            viewer.protocol("WM_DELETE_WINDOW", self._close_screenshot_viewer)
            viewer.bind("<Escape>", lambda _evt: self._close_screenshot_viewer())

            top_bar = ttk.Frame(viewer, padding=(10, 8))
            top_bar.pack(fill=tk.X)
            ttk.Label(top_bar, textvariable=self.screenshot_meta_var).pack(side=tk.LEFT)
            ttk.Button(top_bar, text="Закрыть", command=self._close_screenshot_viewer).pack(side=tk.RIGHT)

            body = ttk.Frame(viewer, padding=(10, 0, 10, 10))
            body.pack(fill=tk.BOTH, expand=True)
            self._screenshot_viewer_label = ttk.Label(body, anchor="center")
            self._screenshot_viewer_label.pack(fill=tk.BOTH, expand=True)
            self._screenshot_viewer = viewer

        self._update_screenshot_viewer_image()
        self._screenshot_viewer.deiconify()
        self._screenshot_viewer.lift()
        self._screenshot_viewer.focus_force()
        return "break"

    def _close_screenshot_viewer(self) -> None:
        if self._screenshot_viewer is not None and self._screenshot_viewer.winfo_exists():
            self._screenshot_viewer.destroy()
        self._screenshot_viewer = None
        self._screenshot_viewer_label = None
        self._screenshot_viewer_image = None

    def _update_screenshot_viewer_image(self) -> None:
        if self._screenshot_viewer is None or not self._screenshot_viewer.winfo_exists() or self._screenshot_viewer_label is None:
            return
        if not self._last_screenshot_path or not Path(self._last_screenshot_path).exists():
            return

        image = tk.PhotoImage(file=self._last_screenshot_path)
        max_w = max(self.winfo_screenwidth() - 120, 800)
        max_h = max(self.winfo_screenheight() - 180, 520)
        ratio = max(image.width() / max_w, image.height() / max_h, 1.0)
        if ratio > 1:
            factor = int(ratio)
            if factor < ratio:
                factor += 1
            image = image.subsample(factor, factor)

        self._screenshot_viewer_image = image
        self._screenshot_viewer_label.configure(image=image, text="")

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

    def _humanize_trace_line(self, line: str) -> str:
        value = self.engine._normalize_spaces(str(line))
        if "provider_" not in value:
            return value

        separator = ""
        for candidate in ("\u2014", "-", "\u2013"):
            if candidate in value:
                separator = candidate
        if not separator:
            return value

        head, state = value.rsplit(separator, 1)
        state = state.strip()
        if not state.startswith("provider_"):
            return value

        provider_name = head
        if ":" in head:
            provider_name = head.split(":", 1)[1].strip()
        provider_name = provider_name.lstrip("\u2713\u2714\u2705\u2716\u274c\u2022 ").strip()

        labels = {
            "provider_called_ok": "\u043e\u0442\u0432\u0435\u0442 \u043f\u043e\u043b\u0443\u0447\u0435\u043d",
            "provider_called_empty": "\u0434\u0430\u043d\u043d\u044b\u0445 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e",
            "provider_blocked_403": "\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d",
            "provider_network_error": "\u0441\u0435\u0442\u0435\u0432\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430",
            "provider_rate_limited_202": "\u0437\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043b\u043e\u0436\u0435\u043d",
            "provider_temporarily_disabled": "\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d",
            "provider_timeout_skipped": "\u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d \u043f\u043e \u0442\u0430\u0439\u043c\u0430\u0443\u0442\u0443",
            "provider_unavailable": "\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d",
            "provider_error": "\u043e\u0448\u0438\u0431\u043a\u0430 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430",
        }
        label = labels.get(state, state)
        icon = "\u2713" if state == "provider_called_ok" else "\u2022"
        return f"{icon} \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {provider_name} \u2014 {label}"

    def _copy_trace_text(self, _event: object | None = None) -> str:
        try:
            if self.trace_text.tag_ranges(tk.SEL):
                content = self.trace_text.get(tk.SEL_FIRST, tk.SEL_LAST)
            else:
                content = self.trace_text.get("1.0", tk.END).rstrip()
        except tk.TclError:
            content = self.trace_text.get("1.0", tk.END).rstrip()
        if not content:
            return "break"
        self.clipboard_clear()
        self.clipboard_append(content)
        self.status_var.set("\u041b\u043e\u0433 \u043f\u043e\u0438\u0441\u043a\u0430 \u0441\u043a\u043e\u043f\u0438\u0440\u043e\u0432\u0430\u043d")
        return "break"

    def _select_all_trace_text(self, _event: object | None = None) -> str:
        self.trace_text.tag_add(tk.SEL, "1.0", tk.END)
        self.trace_text.mark_set(tk.INSERT, "1.0")
        self.trace_text.see("1.0")
        self.trace_text.focus_set()
        return "break"

    def _write_trace(self, trace: list[str]) -> None:
        display_trace = [self._humanize_trace_line(item) for item in trace]
        self.trace_text.configure(state=tk.NORMAL)
        self.trace_text.delete("1.0", tk.END)
        self.trace_text.insert("1.0", "\n".join(display_trace))
        self.trace_text.configure(state=tk.DISABLED)

    def _on_close(self) -> None:
        self._close_screenshot_viewer()
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

