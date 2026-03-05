
from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import datetime
from typing import Any
import tkinter as tk
from tkinter import messagebox, ttk

from app_paths import configure_runtime_env
from logging_setup import setup_logging
from main import CompanyWebApp

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
        self._suppress_variant_event = False
        self._last_autofill_key = ""
        self._current_card_rows: list[tuple[str, str]] = []

        self.surname_var = tk.StringVar()
        self.name_var = tk.StringVar()
        self.middle_var = tk.StringVar()
        self.inn_var = tk.StringVar()
        self.company_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Готово")
        self.card_title_var = tk.StringVar(value="Карточка")

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
            ttk.Entry(top, textvariable=var, width=22).grid(row=1, column=col, sticky="ew", padx=(0, 8))
            top.columnconfigure(col, weight=1)

        action_frame = ttk.Frame(self, padding=(10, 0, 10, 8))
        action_frame.pack(fill=tk.X)
        self.search_button = ttk.Button(action_frame, text="Найти", command=self._search)
        self.search_button.pack(side=tk.LEFT)

        self.copy_card_button = ttk.Button(action_frame, text="Скопировать карточку", command=self._copy_card_to_clipboard)
        self.copy_card_button.pack(side=tk.LEFT, padx=(8, 0))

        self.copy_field_button = ttk.Button(action_frame, text="Скопировать поле", command=self._copy_selected_card_value)
        self.copy_field_button.pack(side=tk.LEFT, padx=(8, 0))

        self.progress = ttk.Progressbar(action_frame, mode="indeterminate", length=220)
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
            selectmode="browse",
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
        self.card_tree.bind("<Double-1>", self._copy_selected_card_value)

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

        trace_frame = ttk.Frame(self)
        trace_frame.pack(fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))
        ttk.Label(trace_frame, text="Лог поиска").pack(anchor="w")
        self.trace_text = tk.Text(trace_frame, height=7, wrap=tk.WORD, state=tk.DISABLED)
        self.trace_text.pack(fill=tk.BOTH, expand=False)

        status = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor="w", padding=(8, 4))
        status.pack(fill=tk.X, side=tk.BOTTOM)

        self._render_card_rows([], "Карточка")

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
        self.copy_card_button.configure(state=state)
        self.copy_field_button.configure(state=state)
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
            self.after(0, lambda: self._on_search_done(params, source_hits, backend_candidates, trace, error))

        threading.Thread(target=worker, daemon=True).start()

    def _candidate_looks_like_company(self, item: dict[str, Any]) -> bool:
        hit_type = self.engine._normalize_spaces(str(item.get("type", ""))).lower()
        if hit_type == "person":
            return False
        org = self.engine._normalize_spaces(str(item.get("org_ru", "")))
        return bool(org)

    def _candidate_key(self, candidate: dict[str, Any]) -> str:
        inn = self.engine._normalize_spaces(str(candidate.get("inn", "")))
        org = self.engine._normalize_spaces(str(candidate.get("org_ru", ""))).lower()
        src = self.engine._normalize_spaces(str(candidate.get("source", ""))).lower()
        if inn:
            return f"inn:{inn}"
        return f"org:{org}|src:{src}"

    def _build_company_candidates_from_hits(self, source_hits: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        q_norm = self.engine._normalize_spaces(query)

        for hit in source_hits:
            if not isinstance(hit, dict):
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

            source_name = self.engine._normalize_spaces(str(hit.get("source", ""))) or "—"
            score = self.engine._score_org_relevance(data, q_norm) + self.engine._score_hit(hit, q_norm)
            key = inn or f"org:{org.lower()}"

            candidate = {
                "data": data,
                "source": source_name,
                "type": "company",
                "url": str(hit.get("url", "")),
                "score": float(score),
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
                "revenue": str(self.engine._parse_money_amount(data.get("revenue", 0))),
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
            item["score"] = f"{float(item.get('score', 0)):.2f}"
        return candidates[:20]

    def _prepare_candidates(self, params: dict[str, str], source_hits: list[dict[str, Any]], backend_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        company_only_query = bool(
            params.get("company")
            and not params.get("inn")
            and not params.get("surname")
            and not params.get("name")
            and not params.get("middle_name")
        )

        if company_only_query:
            company_candidates = self._build_company_candidates_from_hits(source_hits, params.get("company", ""))
            if company_candidates:
                return company_candidates

        return backend_candidates

    def _on_search_done(
        self,
        params: dict[str, str],
        source_hits: list[dict[str, Any]],
        backend_candidates: list[dict[str, Any]],
        trace: list[str],
        error: str,
    ) -> None:
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
            self._render_candidate_preview(candidates[0])
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
        self._render_candidate_preview(candidate)
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

    def _autofill_candidate(self, candidate: dict[str, Any], reason: str = "") -> None:
        if self._busy:
            return

        query_for_autofill = self.engine._normalize_spaces(str(candidate.get("inn", "")))
        if not query_for_autofill:
            query_for_autofill = self.engine._normalize_spaces(str(candidate.get("query_for_autofill", "")))
        if not query_for_autofill:
            query_for_autofill = self.company_var.get().strip() or self.inn_var.get().strip()

        if not query_for_autofill:
            return

        key = self._candidate_key(candidate)
        if key and key == self._last_autofill_key:
            return
        self._last_autofill_key = key

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

        status_suffix = " (выбран вариант)" if reason == "selected_variant" else ""
        self._set_busy(True, f"Формирование карточки{status_suffix}...")

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
            self.after(0, lambda: self._on_autofill_done(card_id, error))

        threading.Thread(target=worker, daemon=True).start()

    def _on_autofill_done(self, card_id: int, error: str) -> None:
        self._set_busy(False)
        if error:
            self.status_var.set("Не удалось сформировать карточку")
            self._render_card_rows([("Статус", error)], "Карточка")
            return

        self._show_card(card_id)
        self.status_var.set(f"Карточка #{card_id} сформирована")

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

            amount = self.engine._parse_money_amount(amount_value)
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

    def _copy_selected_card_value(self, _event: object | None = None) -> None:
        selected = self.card_tree.selection()
        if not selected:
            self.status_var.set("Выберите поле карточки для копирования")
            return
        values = self.card_tree.item(selected[0], "values")
        if not values or len(values) < 2:
            return
        text = f"{values[0]}: {values[1]}"
        self.clipboard_clear()
        self.clipboard_append(text)
        self.status_var.set("Поле скопировано в буфер обмена")

    def _copy_card_to_clipboard(self) -> None:
        if not self._current_card_rows:
            self.status_var.set("Карточка пуста")
            return
        text = "\n".join(
            f"{self.engine._normalize_spaces(str(label))}: {self.engine._normalize_spaces(str(value)) or '—'}"
            for label, value in self._current_card_rows
        )
        self.clipboard_clear()
        self.clipboard_append(text)
        self.status_var.set("Карточка скопирована в буфер обмена")

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
