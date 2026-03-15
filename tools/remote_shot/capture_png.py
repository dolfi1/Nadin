#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

try:
    import cloudscraper
except Exception:
    cloudscraper = None

try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None

from cleanup_rules import (
    BUTTON_TEXTS,
    REMOVE_SELECTORS,
    SECURITY_CHALLENGE_MARKERS,
    SUSPICIOUS_TEXT_MARKERS,
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
SCRIPT_REVISION = "2026-03-15-cleanup-v1"
DEBUG_REMOTE = os.getenv('NADIN_REMOTE_DEBUG', '0').strip().lower() in {'1', 'true', 'yes'}


def _debug(msg: str) -> None:
    if DEBUG_REMOTE:
        print(f'[capture_png] {msg}', file=sys.stderr, flush=True)


def _filtered_remove_selectors() -> list[str]:
    """Avoid over-broad selectors that can remove real page content."""
    blocked_parts = (
        "[class*='banner' i]",
        "[id*='banner' i]",
        "[class*='promo' i]",
        "[id*='promo' i]",
        "[class*='install-app' i]",
        "[class*='app-banner' i]",
        "[class*='app-promo' i]",
    )
    selectors: list[str] = []
    for selector in REMOVE_SELECTORS:
        s = (selector or "").strip().lower()
        if not s:
            continue
        if any(part in s for part in blocked_parts):
            continue
        selectors.append(selector)
    return selectors


def _build_cleanup_injection(base_url: str) -> str:
    selectors = json.dumps([s for s in REMOVE_SELECTORS if (s or '').strip()], ensure_ascii=False)
    button_texts = json.dumps(BUTTON_TEXTS, ensure_ascii=False)
    suspicious = json.dumps(SUSPICIOUS_TEXT_MARKERS, ensure_ascii=False)
    return rf"""
<base href=\"{base_url}\">
<style id=\"nadin-clean-style\">
html, body {{
  background: #ffffff !important;
  overflow: auto !important;
  overscroll-behavior: auto !important;
  max-width: 100% !important;
  min-height: 100% !important;
}}
body * {{ animation: none !important; transition: none !important; }}
body::before, body::after {{ display: none !important; }}
[role=\"dialog\"], [aria-modal=\"true\"], dialog,
[class*=\"modal\" i], [id*=\"modal\" i],
[class*=\"overlay\" i], [id*=\"overlay\" i],
[class*=\"backdrop\" i], [id*=\"backdrop\" i],
[class*=\"cookie\" i], [id*=\"cookie\" i],
[class*=\"consent\" i], [id*=\"consent\" i],
[class*=\"paywall\" i], [id*=\"paywall\" i],
[class*=\"upsell\" i], [id*=\"upsell\" i],
[class*=\"smartbanner\" i], [id*=\"smartbanner\" i],
[class*=\"app-promo\" i], [id*=\"app-promo\" i],
[class*=\"app-banner\" i], [id*=\"app-banner\" i] {{
  display: none !important;
  visibility: hidden !important;
  opacity: 0 !important;
  pointer-events: none !important;
}}
</style>
<script id=\"nadin-clean-js\">
(() => {{
  const REMOVE_SELECTORS = {selectors};
  const BUTTON_TEXTS = {button_texts};
  const SUSPICIOUS_TEXT_MARKERS = {suspicious};

  const norm = (v) => (v || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\\s+/g, ' ')
    .trim()
    .toLowerCase();

  const structuralTags = new Set(['html', 'head', 'body', 'main', 'article', 'section']);

  const getRoots = () => {{
    const roots = [document];
    const seen = new Set([document]);
    const queue = [document.documentElement, document.body].filter(Boolean);
    while (queue.length) {{
      const node = queue.shift();
      if (!node) continue;

      if (node.shadowRoot && !seen.has(node.shadowRoot)) {{
        seen.add(node.shadowRoot);
        roots.push(node.shadowRoot);
        queue.push(node.shadowRoot);
      }}

      const children = node.children ? Array.from(node.children) : [];
      for (const child of children) queue.push(child);

      if (node.querySelectorAll) {{
        const hosts = node.querySelectorAll('*');
        for (const host of hosts) {{
          if (host.shadowRoot && !seen.has(host.shadowRoot)) {{
            seen.add(host.shadowRoot);
            roots.push(host.shadowRoot);
            queue.push(host.shadowRoot);
          }}
        }}
      }}
    }}
    return roots;
  }};

  const safeNodes = new Set([
    document.documentElement,
    document.head,
    document.body,
    document.querySelector('main'),
    document.querySelector('#app'),
    document.querySelector('#root'),
  ].filter(Boolean));

  const hideNode = (node) => {{
    if (!node || safeNodes.has(node)) return;
    try {{
      node.style.setProperty('display', 'none', 'important');
      node.style.setProperty('visibility', 'hidden', 'important');
      node.style.setProperty('opacity', '0', 'important');
      node.style.setProperty('pointer-events', 'none', 'important');
    }} catch (_e) {{}}
  }};

  const removeNode = (node) => {{
    if (!node || safeNodes.has(node)) return;
    hideNode(node);
    try {{ node.remove(); }} catch (_e) {{}}
  }};

  const nearestBackdrop = (node) => {{
    let cur = node;
    for (let i = 0; i < 8 && cur; i += 1) {{
      try {{
        const cls = norm(cur.className || '');
        const id = norm(cur.id || '');
        if (/(modal|overlay|backdrop|popup|dialog|auth|paywall|cookie|consent|signin|signup|subscribe)/.test(cls + ' ' + id)) {{
          return cur;
        }}
      }} catch (_e) {{}}
      cur = cur.parentElement;
    }}
    return null;
  }};

  const shouldRemoveBySelector = (node, selector) => {{
    if (!node || safeNodes.has(node)) return false;

    let style;
    try {{ style = window.getComputedStyle(node); }} catch (_e) {{ return false; }}
    if (!style) return false;

    const rect = node.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return false;

    const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
    const text = norm(node.innerText || node.textContent || '');
    const clsId = norm((node.className || '') + ' ' + (node.id || ''));
    const hasMarker = text.length > 0 && SUSPICIOUS_TEXT_MARKERS.some((token) => text.includes(token));
    const classMarker = /(modal|overlay|backdrop|popup|dialog|cookie|consent|paywall|upsell|subscribe|authwall|regwall|smartbanner|intercom|app-promo|app-banner)/.test(clsId);
    const selectorHint = norm(selector || '');
    const selectorStrong = /(modal|overlay|backdrop|popup|dialog|cookie|consent|paywall|upsell|subscribe|banner|smartbanner|intercom|authwall|regwall)/.test(selectorHint);

    const fixedLike = style.position === 'fixed' || style.position === 'sticky' || style.position === 'absolute';
    const z = Number.parseInt(style.zIndex || '0', 10);

    let score = 0;
    if (selectorStrong) score += 1;
    if (classMarker) score += 1;
    if (hasMarker) score += 2;
    if (fixedLike) score += 2;
    if (z >= 10) score += 1;
    if (areaRatio >= 0.015 && areaRatio <= 0.92) score += 1;

    if (areaRatio > 0.96 && !hasMarker && !fixedLike && z < 20) return false;
    return score >= 3;
  }};

  const removeBySelectors = () => {{
    const roots = getRoots();
    for (const selector of REMOVE_SELECTORS) {{
      for (const root of roots) {{
        let nodes = [];
        try {{ nodes = Array.from(root.querySelectorAll(selector)); }} catch (_e) {{ continue; }}
        for (const node of nodes) {{
          if (!shouldRemoveBySelector(node, selector)) continue;
          removeNode(node);
          const box = nearestBackdrop(node);
          if (box) removeNode(box);
        }}
      }}
    }}
  }};

  const clickHelpfulButtons = () => {{
    const roots = getRoots();
    for (const root of roots) {{
      let nodes = [];
      try {{
        nodes = Array.from(root.querySelectorAll('button,a,[role="button"],input[type="button"],input[type="submit"]'));
      }} catch (_e) {{
        continue;
      }}

      for (const node of nodes) {{
        const text = norm(node.innerText || node.textContent || node.value || node.getAttribute('aria-label') || '');
        if (!text) continue;
        const matched = BUTTON_TEXTS.some((token) =>
          text === token ||
          text.startsWith(token + ' ') ||
          text.endsWith(' ' + token) ||
          text.includes(token)
        );
        if (!matched) continue;
        try {{ node.click(); }} catch (_e) {{}}
      }}
    }}
  }};

  const scoreContainer = (node) => {{
    if (!node || safeNodes.has(node)) return -1;

    const tag = String((node.tagName || '')).toLowerCase();
    if (structuralTags.has(tag)) return -1;

    let style;
    try {{ style = window.getComputedStyle(node); }} catch (_e) {{ return -1; }}
    if (!style) return -1;

    const rect = node.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return -1;

    const text = norm(node.innerText || node.textContent || '');
    const hasMarker = text.length > 0 && SUSPICIOUS_TEXT_MARKERS.some((token) => text.includes(token));
    const hasButton = !!node.querySelector('button,a,[role="button"],input[type="button"],input[type="submit"]');
    const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="email" i],input[autocomplete*="username" i]');

    const z = Number.parseInt(style.zIndex || '0', 10);
    const fixedLike = style.position === 'fixed' || style.position === 'sticky' || style.position === 'absolute';
    const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
    const centered = Math.abs((rect.left + rect.right) / 2 - window.innerWidth / 2) < window.innerWidth * 0.25
      && Math.abs((rect.top + rect.bottom) / 2 - window.innerHeight / 2) < window.innerHeight * 0.25;

    let score = 0;
    if (hasMarker) score += 4;
    if (hasButton) score += 2;
    if (hasCredentialFields) score += 2;
    if (fixedLike) score += 2;
    if (centered) score += 2;
    if (z >= 10) score += 1;
    if (areaRatio >= 0.03 && areaRatio <= 0.92) score += 1;

    return score;
  }};

  const removeTextAnchoredPopups = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;

      const score = scoreContainer(node);
      if (score < 5) continue;

      let candidate = node;
      let best = node;
      let bestScore = score;
      for (let i = 0; i < 7 && candidate; i += 1) {{
        candidate = candidate.parentElement;
        if (!candidate || safeNodes.has(candidate)) break;
        const parentScore = scoreContainer(candidate);
        if (parentScore > bestScore) {{
          best = candidate;
          bestScore = parentScore;
        }}
      }}

      removeNode(best);
      const backdrop = nearestBackdrop(best);
      if (backdrop) removeNode(backdrop);
    }}
  }};

  const removeFixedLayers = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;

      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;
      const pos = style.position;
      if (pos !== 'fixed' && pos !== 'sticky' && pos !== 'absolute') continue;

      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      if (areaRatio < 0.015) continue;

      const text = norm(node.innerText || node.textContent || '');
      const hasMarker = text.length > 0 && SUSPICIOUS_TEXT_MARKERS.some((token) => text.includes(token));
      const z = Number.parseInt(style.zIndex || '0', 10);
      const topish = rect.top <= window.innerHeight * 0.25;
      const bottomish = rect.bottom >= window.innerHeight * 0.75;

      if (hasMarker || z >= 20 || (areaRatio > 0.12 && (topish || bottomish))) {{
        removeNode(node);
      }}
    }}
  }};

  const removeBackdrops = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;

      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;
      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;

      const wide = rect.width >= window.innerWidth * 0.85;
      const high = rect.height >= window.innerHeight * 0.7;
      const fixedLike = style.position === 'fixed' || style.position === 'absolute';
      const translucent = (style.backgroundColor || '').includes('rgba') || Number.parseFloat(style.opacity || '1') < 0.98;
      const z = Number.parseInt(style.zIndex || '0', 10);
      if (fixedLike && wide && high && (translucent || z >= 10)) {{
        removeNode(node);
      }}
    }}
  }};

  const removeAuthAndPromoWalls = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
        const authMarkers = [
      'join linkedin',
      'agree & join',
      'already on linkedin',
      'sign in with email',
      'sign in',
      'join now',
      'join to view',
      'create account',
      'continue with google',
      'continue with email',
      'linkedin respects your privacy',
      'cookie policy',
      'accept to consent',
      'open the app',
      'better on the app',
      '\u0432\u043e\u0439\u0442\u0438',
      '\u0432\u0445\u043e\u0434',
      '\u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440\u0438\u0440\u043e\u0432\u0430\u0442\u044c\u0441\u044f',
      '\u0441\u043e\u0437\u0434\u0430\u0442\u044c \u0430\u043a\u043a\u0430\u0443\u043d\u0442',
      '\u043f\u0440\u0438\u0441\u043e\u0435\u0434\u0438\u043d\u0438\u0442\u044c\u0441\u044f',
      '\u043f\u043e\u043b\u0438\u0442\u0438\u043a\u0430 cookie',
      '\u043f\u043e\u043b\u0438\u0442\u0438\u043a\u0430 \u043a\u043e\u043d\u0444\u0438\u0434\u0435\u043d\u0446\u0438\u0430\u043b\u044c\u043d\u043e\u0441\u0442\u0438',
      '\u043f\u0440\u0438\u043d\u044f\u0442\u044c',
      '\u043e\u0442\u043a\u043b\u043e\u043d\u0438\u0442\u044c',
      '\u043e\u0442\u043a\u0440\u044b\u0442\u044c \u0432 \u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0438',
    ];

    const isAuthText = (text) => authMarkers.some((marker) => text.includes(marker));

    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;

      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;

      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;

      const text = norm(node.innerText || node.textContent || '');
      const clsId = norm((node.className || '') + ' ' + (node.id || ''));
      const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="email" i],input[autocomplete*="username" i]');
      const hasButtons = !!node.querySelector('button,a,[role="button"],input[type="button"],input[type="submit"]');
      const hasMarker = isAuthText(text) || SUSPICIOUS_TEXT_MARKERS.some((token) => text.includes(token));
      const hasAuthCtas = /(sign in|join now|join to view|agree & join|create account|register|\u0432\u043e\u0439\u0442\u0438|\u0432\u0445\u043e\u0434|\u043f\u0440\u0438\u0441\u043e\u0435\u0434\u0438\u043d|\u0437\u0430\u0440\u0435\u0433\u0438\u0441\u0442\u0440)/.test(text);
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));

      const centered =
        Math.abs((rect.left + rect.right) / 2 - window.innerWidth / 2) < window.innerWidth * 0.30 &&
        Math.abs((rect.top + rect.bottom) / 2 - window.innerHeight / 2) < window.innerHeight * 0.30;

      const topCookie = rect.top <= window.innerHeight * 0.24 && rect.height <= window.innerHeight * 0.40 && rect.width >= window.innerWidth * 0.55;
      const bottomPromo = rect.bottom >= window.innerHeight * 0.72 && rect.right >= window.innerWidth * 0.55;
      const likelyAuthClass = /(auth|sign-?in|signin|signup|join|login|register|consent|cookie|promo|banner|smartbanner|intercom|artdeco-modal)/.test(clsId);

      const cookieHeader = topCookie && /(cookie|privacy|consent|accept|reject|settings|политика)/.test(text);
      const shouldRemoveTop = topCookie && (hasMarker || likelyAuthClass || hasButtons || hasAuthCtas || cookieHeader);
      const shouldRemoveTopStatic = rect.top <= window.innerHeight * 0.34
        && rect.height <= window.innerHeight * 0.44
        && areaRatio <= 0.52
        && hasMarker
        && (hasButtons || likelyAuthClass || hasAuthCtas);
      const shouldRemoveCenter = centered && areaRatio >= 0.010 && areaRatio <= 0.85 && (hasCredentialFields || hasMarker || likelyAuthClass || hasAuthCtas);
      const shouldRemoveAuthSheet = areaRatio >= 0.010 && areaRatio <= 0.85
        && (hasCredentialFields || hasAuthCtas)
        && (hasMarker || hasAuthCtas || hasButtons || likelyAuthClass || centered || topCookie || bottomPromo);
      const shouldRemoveBottom = bottomPromo && areaRatio <= 0.40 && (hasMarker || likelyAuthClass || hasButtons || hasAuthCtas);
      const shouldRemoveCookieNotice = hasMarker
        && hasButtons
        && areaRatio <= 0.45
        && (rect.top <= window.innerHeight * 0.45 || rect.bottom >= window.innerHeight * 0.55);

      if (!(shouldRemoveTop || shouldRemoveTopStatic || shouldRemoveCenter || shouldRemoveAuthSheet || shouldRemoveBottom || shouldRemoveCookieNotice)) continue;

      let target = node;
      let bestArea = areaRatio;
      for (let i = 0; i < 6 && target.parentElement; i += 1) {{
        const parent = target.parentElement;
        if (!parent || safeNodes.has(parent)) break;
        const pr = parent.getBoundingClientRect();
        if (!pr || pr.width <= 0 || pr.height <= 0) break;
        const pArea = (pr.width * pr.height) / Math.max(1, (window.innerWidth * window.innerHeight));
        if (pArea > 0.85) break;
        if (pArea >= bestArea && pArea <= 0.80) {{
          target = parent;
          bestArea = pArea;
        }}
      }}

      removeNode(target);
      const backdrop = nearestBackdrop(target);
      if (backdrop) removeNode(backdrop);
    }}
  }};
  const removeMarkerDrivenContainers = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;

      const text = norm(node.innerText || node.textContent || '');
      if (!text) continue;
      const hasMarker = SUSPICIOUS_TEXT_MARKERS.some((token) => text.includes(token));
      if (!hasMarker) continue;

      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;

      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      if (areaRatio > 0.80) continue;

      const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="email" i],input[autocomplete*="username" i]');
      const hasButtons = !!node.querySelector('button,a,[role="button"],input[type="button"],input[type="submit"]');
      const clsId = norm((node.className || '') + ' ' + (node.id || ''));
      const likelyPopupClass = /(auth|sign-?in|signin|signup|join|login|register|consent|cookie|promo|banner|smartbanner|intercom|modal|popup|dialog|overlay|backdrop)/.test(clsId);
      const fixedLike = style.position === 'fixed' || style.position === 'sticky' || style.position === 'absolute';
      const nearEdges = rect.top <= window.innerHeight * 0.38 || rect.bottom >= window.innerHeight * 0.62;
      if (!(hasCredentialFields || likelyPopupClass || fixedLike || (hasButtons && nearEdges))) continue;

      let target = node;
      for (let i = 0; i < 5 && target.parentElement; i += 1) {{
        const parent = target.parentElement;
        if (!parent || safeNodes.has(parent)) break;
        const pr = parent.getBoundingClientRect();
        if (!pr || pr.width <= 0 || pr.height <= 0) break;
        const pArea = (pr.width * pr.height) / Math.max(1, (window.innerWidth * window.innerHeight));
        if (pArea > 0.85) break;
        if (pArea >= areaRatio) {{
          target = parent;
        }}
      }}
      removeNode(target);
      const backdrop = nearestBackdrop(target);
      if (backdrop) removeNode(backdrop);
    }}
  }};

  const clickCloseButtons = () => {{
    const roots = getRoots();
    const closeMarkers = ['close', 'dismiss', 'skip', 'not now', 'later', '\u0437\u0430\u043a\u0440\u044b\u0442\u044c', '\u043f\u0440\u043e\u043f\u0443\u0441\u0442\u0438\u0442\u044c', '\u043f\u043e\u0437\u0436\u0435'];
    for (const root of roots) {{
      let nodes = [];
      try {{
        nodes = Array.from(root.querySelectorAll('button,a,[role="button"],[aria-label],[title]'));
      }} catch (_e) {{
        continue;
      }}
      for (const node of nodes) {{
        const text = norm(node.innerText || node.textContent || node.getAttribute('aria-label') || node.getAttribute('title') || '');
        if (!text) continue;
        const matched = closeMarkers.some((token) => text === token || text.includes(token));
        if (!matched) continue;
        const host = nearestBackdrop(node) || node.parentElement || node;
        const hostText = norm((host && (host.innerText || host.textContent)) || '');
        const hostHasMarker = hostText && SUSPICIOUS_TEXT_MARKERS.some((token) => hostText.includes(token));
        if (!hostHasMarker && text.length > 40) continue;
        try {{ node.click(); }} catch (_e) {{}}
      }}
    }}
  }};

  const removeCornerPopups = () => {{
    const nodes = Array.from(document.querySelectorAll('body *'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;
      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;
      const fixedLike = style.position === 'fixed' || style.position === 'sticky' || style.position === 'absolute';
      if (!fixedLike) continue;

      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      if (areaRatio > 0.36) continue;

      const bottomRight = rect.right >= window.innerWidth * 0.55 && rect.bottom >= window.innerHeight * 0.55;
      if (!bottomRight) continue;

      const text = norm(node.innerText || node.textContent || '');
      const clsId = norm((node.className || '') + ' ' + (node.id || ''));
      const appPromo = /(open the app|better on the app|get the app|install app|download app|\u043e\u0442\u043a\u0440\u044b\u0442\u044c \u0432 \u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0438|\u0441\u043a\u0430\u0447\u0430\u0442\u044c \u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0435)/.test(text);
      const promoClass = /(promo|banner|intercom|chat|messenger|smartbanner|app-)/.test(clsId);
      if (appPromo || promoClass) {{
        removeNode(node);
      }}
    }}
  }};


  const removeAuthForms = () => {{
    const nodes = Array.from(document.querySelectorAll('form,section,div,article,aside'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;
      let style;
      try {{ style = window.getComputedStyle(node); }} catch (_e) {{ continue; }}
      if (!style) continue;

      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      if (areaRatio > 0.72) continue;

      const text = norm(node.innerText || node.textContent || '');
      const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="email" i],input[autocomplete*="username" i]');
      const hasAuthWords = /(join linkedin|join now|agree & join|sign in with email|continue with email|create account|already on linkedin|join to view|view full profile|войти|вход|зарегистр|присоедин)/.test(text);
      const likelyFormClass = /(join-form|signup|sign-?in|signin|login|register|auth|consent|cookie|promo|modal|dialog|overlay|backdrop)/.test(norm((node.className || '') + ' ' + (node.id || '')));
      if (!(hasCredentialFields && (hasAuthWords || likelyFormClass))) continue;

      removeNode(node);
      const backdrop = nearestBackdrop(node);
      if (backdrop) removeNode(backdrop);
    }}
  }};

  const removeLinkedInArtifacts = () => {{
    const host = norm(window.location && window.location.hostname ? window.location.hostname : '');
    if (!(host.includes('linkedin.com') || host.includes('lnkd.in'))) return;

    const selectors = [
      '.global-alert',
      '.global-alert-offset',
      '.global-alert-offset-top',
      '.global-alert-queue',
      '.artdeco-global-alert-container',
      '.base-sign-in-modal',
      '.base-sign-in-modal__modal',
      '.base-sign-in-modal__content',
      '.contextual-sign-in-modal',
      '.contextual-sign-in-modal__modal',
      '.contextual-sign-in-modal__overlay',
      '.guest-sign-in-modal',
      '.guest-homepage-sign-in-modal',
      '.private-personalized-sign-in-modal',
      '.join-form',
      '.join-form-container',
      '.join-now-modal',
      '.join-overlay',
      '.authwall',
      '.auth-wall',
      '.authwall-modal',
      '.authwall-wrapper',
      '.msg-overlay-list-bubble',
      '.msg-overlay-bubble-header',
      '.msg-overlay-container',
      '[class*="open-app" i]',
      '[class*="app-banner" i]',
      '[class*="app-promo" i]',
    ];
    for (const selector of selectors) {{
      let nodes = [];
      try {{ nodes = Array.from(document.querySelectorAll(selector)); }} catch (_e) {{ continue; }}
      for (const node of nodes) removeNode(node);
    }}

    const allNodes = Array.from(document.querySelectorAll('body *'));
    for (const node of allNodes) {{
      if (!node || safeNodes.has(node)) continue;
      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      const text = norm(node.innerText || node.textContent || '');
      if (!text) continue;

      const topCookie = rect.top <= window.innerHeight * 0.24
        && rect.height <= window.innerHeight * 0.36
        && areaRatio <= 0.58
        && /(linkedin respects your privacy|cookie policy|accept to consent|settings)/.test(text);
      if (topCookie) {{
        removeNode(node);
        const backdrop = nearestBackdrop(node);
        if (backdrop) removeNode(backdrop);
      }}
    }}
  }};

  const removeGenericAuthAndCookieBlocks = () => {{
    const nodes = Array.from(document.querySelectorAll('div,section,aside,form,article'));
    for (const node of nodes) {{
      if (!node || safeNodes.has(node)) continue;
      const rect = node.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      const areaRatio = (rect.width * rect.height) / Math.max(1, (window.innerWidth * window.innerHeight));
      if (areaRatio > 0.86) continue;

      const text = norm(node.innerText || node.textContent || '');
      if (!text) continue;

      const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="email" i],input[autocomplete*="username" i]');
      const authWords = /(join linkedin|join now|join to view|agree & join|already on linkedin|sign in with email|continue with google|create account|view full profile|linkedin respects your privacy|cookie policy|accept to consent|open the app|better on the app|\\u0432\\u043e\\u0439\\u0442\\u0438|\\u0432\\u0445\\u043e\\u0434|\\u0437\\u0430\\u0440\\u0435\\u0433\\u0438\\u0441\\u0442\\u0440|\\u0440\\u0435\\u0433\\u0438\\u0441\\u0442\\u0440\\u0430\\u0446|\\u043f\\u0440\\u0438\\u0441\\u043e\\u0435\\u0434\\u0438\\u043d|\\u0441\\u043e\\u0437\\u0434\\u0430\\u0442\\u044c \\u0430\\u043a\\u043a\\u0430\\u0443\\u043d\\u0442|\\u043a\\u0443\\u043a\\u0438|\\u0444\\u0430\\u0439\\u043b\\u044b cookie|\\u043e\\u0442\\u043a\\u0440\\u044b\\u0442\\u044c \\u0432 \\u043f\\u0440\\u0438\\u043b\\u043e\\u0436\\u0435\\u043d\\u0438\\u0438|\\u043b\\u0443\\u0447\\u0448\\u0435 \\u0432 \\u043f\\u0440\\u0438\\u043b\\u043e\\u0436\\u0435\\u043d\\u0438\\u0438)/.test(text);
      const nearTop = rect.top <= window.innerHeight * 0.35;
      const nearCenter =
        Math.abs((rect.left + rect.right) / 2 - window.innerWidth / 2) < window.innerWidth * 0.35 &&
        Math.abs((rect.top + rect.bottom) / 2 - window.innerHeight / 2) < window.innerHeight * 0.35;
      const nearBottomRight = rect.right >= window.innerWidth * 0.55 && rect.bottom >= window.innerHeight * 0.55;

      if (!(authWords || hasCredentialFields)) continue;
      if (!(nearTop || nearCenter || nearBottomRight)) continue;

      removeNode(node);
      const backdrop = nearestBackdrop(node);
      if (backdrop) removeNode(backdrop);
    }}
  }};

  const clearBodyLocks = () => {{
    const classes = ['noscroll', 'no-scroll', 'modal-open', 'overflow-hidden', 'dialog-open', 'popup-open', 'lock-scroll'];
    for (const token of classes) {{
      document.documentElement.classList.remove(token);
      document.body.classList.remove(token);
    }}
    document.documentElement.style.setProperty('overflow', 'auto', 'important');
    document.body.style.setProperty('overflow', 'auto', 'important');
    document.body.style.setProperty('position', 'static', 'important');
    document.body.style.setProperty('filter', 'none', 'important');
  }};

  const safeCall = (fn) => {{
    try {{ fn(); }} catch (_e) {{}}
  }};

  const run = () => {{
    safeCall(clickHelpfulButtons);
    safeCall(clickCloseButtons);
    safeCall(removeBySelectors);
    safeCall(removeAuthAndPromoWalls);
    safeCall(removeAuthForms);
    safeCall(removeMarkerDrivenContainers);
    safeCall(removeTextAnchoredPopups);
    safeCall(removeFixedLayers);
    safeCall(removeCornerPopups);
    safeCall(removeLinkedInArtifacts);
    safeCall(removeGenericAuthAndCookieBlocks);
    safeCall(removeBackdrops);
    safeCall(clearBodyLocks);
  }};

  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', run, {{ once: true }});
  }} else {{
    run();
  }}

  const observer = new MutationObserver(() => run());
  try {{ observer.observe(document.documentElement, {{ childList: true, subtree: true }}); }} catch (_e) {{}}

  setTimeout(run, 100);
  setTimeout(run, 400);
  setTimeout(run, 900);
  setTimeout(run, 1600);
  setTimeout(run, 2500);
  setTimeout(run, 3600);
  setTimeout(run, 5200);
  setTimeout(run, 7000);
  setTimeout(run, 9000);
  setTimeout(run, 11500);
  setTimeout(run, 12000);
  setTimeout(run, 16500);
  setTimeout(run, 22000);
  setTimeout(run, 28500);
  setTimeout(() => {{ try {{ observer.disconnect(); }} catch (_e) {{}} }}, 32000);
}})();
</script>
"""


def _find_browser(base_dir: Path) -> Path:
    candidates = [
        os.getenv("NADIN_REMOTE_BROWSER", "").strip(),
        str(base_dir / "browser" / "chrome-headless-shell-linux64" / "chrome-headless-shell"),
        str(base_dir / "browser" / "chrome-linux64" / "chrome"),
        str(base_dir / "chrome-headless-shell-linux64" / "chrome-headless-shell"),
        str(base_dir / "chrome-linux64" / "chrome"),
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    ]
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw)
        if path.exists():
            return path
    raise RuntimeError("remote_browser_not_found")


def _fetch_with_cloudscraper(url: str) -> str:
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    response = scraper.get(url, timeout=30, headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8"})
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def _fetch_with_urllib(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="ignore")


def _fetch_html(url: str) -> str:
    html = ""
    errors = []
    if cloudscraper is not None:
        try:
            html = _fetch_with_cloudscraper(url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"cloudscraper: {exc}")
    if not html:
        try:
            html = _fetch_with_urllib(url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"urllib: {exc}")
    if not html:
        raise RuntimeError('; '.join(errors) or 'empty response')
    return html



def _safe_button_texts() -> list[str]:
    dangerous_tokens = (
        'join',
        'sign',
        'login',
        'log in',
        'register',
        'account',
        'profile',
        'agree & join',
        'continue with',
        'create account',
        'view full profile',
        'setting',
        '\u0432\u043e\u0439\u0442\u0438',
        '\u0432\u0445\u043e\u0434',
        '\u0440\u0435\u0433\u0438\u0441\u0442\u0440',
        '\u0430\u043a\u043a\u0430\u0443\u043d\u0442',
        '\u043f\u0440\u043e\u0444\u0438\u043b',
        '\u0441\u043e\u0433\u043b\u0430\u0441',
        '\u043f\u0440\u0438\u0441\u043e\u0435\u0434\u0438\u043d',
        '\u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c \u0441',
        '\u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a',
    )
    safe = []
    for item in BUTTON_TEXTS:
        value = (item or '').strip().lower()
        if not value:
            continue
        if any(token in value for token in dangerous_tokens):
            continue
        safe.append(value)
    return sorted(set(safe))


def _capture_with_playwright(
    browser: Path,
    url: str,
    width: int,
    height: int,
    output_path: Path,
) -> None:
    if sync_playwright is None:
        raise RuntimeError('playwright_unavailable')

    payload = {
        'remove_selectors': [s for s in REMOVE_SELECTORS if (s or '').strip()],
        'button_texts': _safe_button_texts(),
        'markers': [m for m in SUSPICIOUS_TEXT_MARKERS if m],
    }

    cleanup_js = r"""
(payload) => {
  const removeSelectors = Array.isArray(payload?.remove_selectors) ? payload.remove_selectors : [];
  const buttonTexts = Array.isArray(payload?.button_texts) ? payload.button_texts : [];
  const markers = Array.isArray(payload?.markers) ? payload.markers : [];

  const norm = (v) => String(v || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

  const safeNodes = new Set([
    document.documentElement,
    document.head,
    document.body,
    document.querySelector('main'),
    document.querySelector('#app'),
    document.querySelector('#root'),
  ].filter(Boolean));

  const roots = [document];
  const seen = new Set([document]);
  for (const host of Array.from(document.querySelectorAll('*'))) {
    if (host && host.shadowRoot && !seen.has(host.shadowRoot)) {
      seen.add(host.shadowRoot);
      roots.push(host.shadowRoot);
    }
  }

  const tokenHit = (text, tokens) => {
    const value = norm(text);
    if (!value) return false;
    return tokens.some((token) => {
      const t = norm(token);
      return !!t && (
        value === t ||
        value.startsWith(t + ' ') ||
        value.endsWith(' ' + t) ||
        value.includes(t)
      );
    });
  };

  const markerHit = (text) => {
    const value = norm(text);
    if (!value) return false;
    if (tokenHit(value, markers)) return true;
    return /(join linkedin|join now|join to view|view full profile|agree ?& ?join|continue with google|sign in with email|already on linkedin|open the app|better on the app|linkedin respects your privacy|cookie policy|accept to consent|sign in|sign up|create account|register|log in|войти|вход|зарегистр|присоедин|куки|cookie|consent|privacy)/.test(value);
  };

  const removeNode = (node) => {
    if (!node || safeNodes.has(node)) return;
    try {
      node.style.setProperty('display', 'none', 'important');
      node.style.setProperty('visibility', 'hidden', 'important');
      node.style.setProperty('opacity', '0', 'important');
      node.style.setProperty('pointer-events', 'none', 'important');
    } catch (_e) {}
    try { node.remove(); } catch (_e) {}
  };

  const clickConsentButtons = () => {
    const fallbackTokens = [
      'accept', 'accept all', 'allow', 'agree', 'ok', 'okay', 'got it',
      'reject', 'reject all', 'decline', 'deny', 'only necessary',
      'close', 'dismiss', 'skip', 'not now',
      'принять', 'принять все', 'разрешить', 'согласен', 'понятно',
      'отклонить', 'не принимать', 'только необходимые', 'закрыть'
    ];
    const tokens = [...buttonTexts, ...fallbackTokens];
    const selector = 'button,a,[role="button"],input[type="button"],input[type="submit"],[aria-label],[title]';
    for (const root of roots) {
      let nodes = [];
      try { nodes = Array.from(root.querySelectorAll(selector)); } catch (_e) { continue; }
      for (const node of nodes) {
        const text = norm(
          node.innerText ||
          node.textContent ||
          node.value ||
          node.getAttribute('aria-label') ||
          node.getAttribute('title') ||
          ''
        );
        if (!text) continue;
        if (!tokenHit(text, tokens)) continue;
        try { node.click(); } catch (_e) {}
      }
    }
  };

  const shouldRemoveNode = (node) => {
    if (!node || safeNodes.has(node)) return false;

    let style;
    try { style = window.getComputedStyle(node); } catch (_e) { return false; }
    if (!style) return false;

    const rect = node.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return false;

    const area = (rect.width * rect.height) / Math.max(1, window.innerWidth * window.innerHeight);
    if (area <= 0.003) return false;

    const clsId = norm((node.className || '') + ' ' + (node.id || ''));
    const text = norm(node.innerText || node.textContent || '');
    const z = Number.parseInt(style.zIndex || '0', 10);
    const fixedLike = style.position === 'fixed' || style.position === 'sticky' || style.position === 'absolute';
    const nearTop = rect.top <= window.innerHeight * 0.28;
    const nearBottom = rect.bottom >= window.innerHeight * 0.72;
    const nearRight = rect.right >= window.innerWidth * 0.58;
    const centered =
      Math.abs((rect.left + rect.right) / 2 - window.innerWidth / 2) < window.innerWidth * 0.34 &&
      Math.abs((rect.top + rect.bottom) / 2 - window.innerHeight / 2) < window.innerHeight * 0.36;

    const hasButtons = !!node.querySelector('button,a,[role="button"],input[type="button"],input[type="submit"]');
    const hasCredentials = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i],input[autocomplete*="username" i],input[autocomplete*="email" i]');

    const classMarker = /(cookie|consent|privacy|auth|signin|sign-in|signup|join|register|login|modal|popup|overlay|backdrop|paywall|banner|smartbanner|promo|intercom|toast|notification|app-)/.test(clsId);
    const textMarker = markerHit(text);

    if (area >= 0.92 && !fixedLike && !classMarker && !textMarker) return false;

    let score = 0;
    if (classMarker) score += 2;
    if (textMarker) score += 3;
    if (hasButtons) score += 1;
    if (hasCredentials) score += 2;
    if (fixedLike) score += 1;
    if (z >= 8) score += 1;
    if (nearTop || centered || (nearBottom && nearRight)) score += 1;
    if (area >= 0.015 && area <= 0.85) score += 1;

    return score >= 4;
  };

  const removeBySelectors = () => {
    for (const selector of removeSelectors) {
      for (const root of roots) {
        let nodes = [];
        try { nodes = Array.from(root.querySelectorAll(selector)); } catch (_e) { continue; }
        for (const node of nodes) {
          if (shouldRemoveNode(node)) removeNode(node);
        }
      }
    }
  };

  const removeHeuristicBlocks = () => {
    const nodes = Array.from(document.querySelectorAll('div,section,aside,dialog,form,article,header,footer,iframe'));
    for (const node of nodes) {
      if (shouldRemoveNode(node)) removeNode(node);
    }
  };

  const clearLocks = () => {
    const classes = ['noscroll', 'no-scroll', 'modal-open', 'overflow-hidden', 'dialog-open', 'popup-open', 'lock-scroll'];
    for (const token of classes) {
      document.documentElement.classList.remove(token);
      document.body.classList.remove(token);
    }
    document.documentElement.style.setProperty('overflow', 'auto', 'important');
    document.body.style.setProperty('overflow', 'auto', 'important');
    document.body.style.setProperty('position', 'static', 'important');
    document.body.style.setProperty('filter', 'none', 'important');
  };

  const run = () => {
    clickConsentButtons();
    removeBySelectors();
    removeHeuristicBlocks();
    clearLocks();
  };

  run();
  setTimeout(run, 120);
  setTimeout(run, 450);
  setTimeout(run, 900);
  setTimeout(run, 1600);
  setTimeout(run, 2600);
  setTimeout(run, 4200);

  return true;
}
"""

    css = """
[role='dialog'], [aria-modal='true'], dialog,
[class*='modal' i], [id*='modal' i],
[class*='overlay' i], [id*='overlay' i],
[class*='backdrop' i], [id*='backdrop' i],
[class*='cookie' i], [id*='cookie' i],
[class*='consent' i], [id*='consent' i],
[class*='smartbanner' i], [id*='smartbanner' i],
[class*='app-banner' i], [class*='app-promo' i],
[class*='authwall' i], [class*='regwall' i],
[class*='join-modal' i],
[class*='intercom' i],
.base-sign-in-modal,
.contextual-sign-in-modal,
.contextual-sign-in-modal__overlay,
.private-personalized-sign-in-modal,
.msg-overlay-container,
.msg-overlay-list-bubble,
.msg-overlay-bubble-header,
.intercom-lightweight-app,
.intercom-app,
.intercom-launcher,
.intercom-container,
.intercom-frame {
  display: none !important;
  visibility: hidden !important;
  opacity: 0 !important;
  pointer-events: none !important;
}
html, body {
  overflow: auto !important;
  position: static !important;
}
"""

    with sync_playwright() as pw:
        browser_obj = pw.chromium.launch(
            executable_path=str(browser),
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-background-networking',
                '--disable-sync',
                '--disable-component-update',
                '--disable-domain-reliability',
                '--lang=ru-RU',
            ],
        )
        context = browser_obj.new_context(
            viewport={'width': width, 'height': height},
            user_agent=USER_AGENT,
            locale='ru-RU',
            timezone_id='Europe/Moscow',
            ignore_https_errors=True,
            java_script_enabled=True,
        )
        page = context.new_page()
        page.set_default_timeout(70000)

        def run_cleanup_cycle() -> None:
            try:
                page.add_style_tag(content=css)
            except Exception:
                pass
            try:
                page.evaluate(cleanup_js, payload)
            except Exception:
                pass

        page.goto(url, wait_until='domcontentloaded', timeout=70000)
        _debug(f'playwright_url_after_goto={page.url}')
        try:
            page.wait_for_load_state('networkidle', timeout=10000)
        except Exception:
            pass

        run_cleanup_cycle()
        for delay in (0.35, 0.7, 1.1, 1.8, 2.6, 3.6, 5.0, 7.0):
            time.sleep(delay)
            run_cleanup_cycle()

        try:
            page.evaluate("window.scrollTo(0, 220)")
            page.wait_for_timeout(220)
            page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass

        run_cleanup_cycle()
        page.wait_for_timeout(500)
        _debug(f'playwright_url_before_shot={page.url}')
        try:
            _debug(f'playwright_title={page.title()}')
        except Exception:
            pass
        page.screenshot(path=str(output_path), type='png')

        html = ''
        try:
            html = page.content()
        except Exception:
            html = ''

        context.close()
        browser_obj.close()

    if _looks_blank_png(output_path):
        try:
            output_path.unlink()
        except Exception:
            pass
        raise RuntimeError('blank_playwright_render')
    if html and _looks_like_security_challenge(html):
        try:
            output_path.unlink()
        except Exception:
            pass
        raise RuntimeError('security_challenge_detected')
def _dump_dom_with_browser(browser: Path, url: str, width: int, height: int, virtual_time_budget: int = 30000) -> str:
    command = [
        str(browser),
        '--headless=new',
        '--disable-gpu',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-breakpad',
        '--disable-crash-reporter',
        '--disable-blink-features=AutomationControlled',
        '--disable-background-networking',
        '--disable-sync',
        '--disable-component-update',
        '--disable-domain-reliability',
        '--disable-notifications',
        '--disable-extensions',
        '--hide-scrollbars',
        f'--window-size={width},{height}',
        '--lang=ru-RU',
        '--force-device-scale-factor=1',
        f'--virtual-time-budget={virtual_time_budget}',
        f'--user-agent={USER_AGENT}',
        '--dump-dom',
        url,
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        timeout=75,
        check=False,
        text=True,
        encoding='utf-8',
        errors='ignore',
    )
    html = completed.stdout or ''
    if completed.returncode != 0 and '<html' not in html.lower():
        stderr = (completed.stderr or '').strip()
        raise RuntimeError(stderr or html.strip() or f'browser_dom_exit_{completed.returncode}')
    if '<html' not in html.lower():
        raise RuntimeError('browser_dom_empty')
    return html


def _looks_like_security_challenge(html: str) -> bool:
    probe = (html or '').lower()
    if not probe:
        return False

    strong_markers = (
        'challenge-platform',
        'cf-chl-',
        'checking your browser',
        'just a moment',
        'security check',
        'attention required',
        'ray id',
    )
    if any(marker in probe for marker in strong_markers):
        return True

    weak_markers = (
        'cloudflare',
        'captcha',
        'please verify you are a human',
        'verify you are human',
        'are you human',
        'press and hold',
        'access denied',
        'проверка безопасности',
        'подтвердите, что вы не робот',
    )
    weak_hits = sum(1 for marker in weak_markers if marker in probe)
    if weak_hits >= 2:
        return True

    listed_hits = sum(1 for marker in SECURITY_CHALLENGE_MARKERS if marker and marker in probe)
    return listed_hits >= 3


def _strip_modal_blocks(html: str) -> str:
    # Conservative removal: drop only blocks explicitly marked by modal/cookie/auth classes/ids.
    marker = (
        r"cookie|consent|gdpr|popup|modal|overlay|backdrop|paywall|subscribe|"
        r"interstitial|dialog|captcha|challenge|onetrust|didomi|cookiebot|cmp|"
        r"artdeco-modal|smartbanner|app-promo|app-banner|intercom|auth|signin|sign-in|signup|join|register|login"
    )
    tags = ("div", "section", "aside", "dialog", "form")
    out = html
    for _ in range(4):
        replaced_total = 0
        for tag in tags:
            rx = re.compile(
                rf'<{tag}\b(?=[^>]*(?:id|class|role|aria-label|data-testid|data-test)=["\'][^"\']*(?:{marker})[^"\']*["\'])[^>]*>.*?</{tag}>',
                flags=re.IGNORECASE | re.DOTALL,
            )
            out, replaced = rx.subn('', out)
            replaced_total += replaced
        if replaced_total == 0:
            break
    return out


def _prepare_html(url: str, html: str) -> str:
    # Keep page scripts for JS-driven sites (e.g. LinkedIn); remove only restrictive headers/tags.
    html = re.sub(
        r'<meta[^>]+http-equiv=["\'](?:content-security-policy|x-frame-options)["\'][^>]*>',
        '',
        html,
        flags=re.IGNORECASE,
    )
    html = re.sub(r'<meta[^>]+name=["\']referrer["\'][^>]*>', '', html, flags=re.IGNORECASE)
    html = re.sub(r'<link[^>]+rel=["\'](?:preload|prefetch|modulepreload)["\'][^>]*>', '', html, flags=re.IGNORECASE)
    html = re.sub(r'<body([^>]*)>', r'<body\1 style="overflow:auto !important; position:static !important;">', html, count=1, flags=re.IGNORECASE)
    html = _strip_modal_blocks(html)

    injection = _build_cleanup_injection(url)
    lower = html.lower()
    if '<head' in lower:
        if '<head>' in html:
            return html.replace('<head>', '<head>' + injection, 1)
        return re.sub(r'(<head[^>]*>)', r'\1' + injection, html, count=1, flags=re.IGNORECASE)
    if '<html' in lower:
        return re.sub(r'(<html[^>]*>)', r'\1<head>' + injection + '</head>', html, count=1, flags=re.IGNORECASE)
    return '<html><head>' + injection + '</head><body>' + html + '</body></html>'


def _run_shot(command: list[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, timeout=timeout, check=False, text=True, encoding='utf-8', errors='ignore')


def _render_html_to_png(browser: Path, html: str, width: int, height: int, output_path: Path, profile_dir: Path, html_path: Path, virtual_time_budget: int = 38000) -> None:
    html_path.write_text(html, encoding='utf-8')
    command = [
        str(browser),
        '--headless=new',
        '--disable-gpu',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-breakpad',
        '--disable-crash-reporter',
        '--disable-blink-features=AutomationControlled',
        '--disable-background-networking',
        '--disable-sync',
        '--disable-component-update',
        '--disable-domain-reliability',
        '--disable-notifications',
        '--disable-extensions',
        '--hide-scrollbars',
        '--run-all-compositor-stages-before-draw',
        f'--window-size={width},{height}',
        '--lang=ru-RU',
        '--force-device-scale-factor=1',
        f'--virtual-time-budget={virtual_time_budget}',
        f'--user-data-dir={profile_dir}',
        f'--user-agent={USER_AGENT}',
        f'--screenshot={output_path}',
        html_path.as_uri(),
    ]
    completed = _run_shot(command, timeout=70)
    if completed.returncode != 0 and not output_path.exists():
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f'browser_exit_{completed.returncode}')


def _looks_blank_png(path: Path) -> bool:
    try:
        size = path.stat().st_size
    except Exception:
        return True
    # Only tiny PNGs are unambiguously blank when Pillow is unavailable.
    if size < 18000:
        return True

    if Image is None:
        return False
    try:
        with Image.open(path) as image:
            frame = image.convert('RGB')
            width, height = frame.size
            if width <= 0 or height <= 0:
                return True

            # Fast sparse sampling to avoid heavy CPU on the server.
            step_x = max(1, width // 96)
            step_y = max(1, height // 54)
            pixels = frame.load()
            sample_count = 0
            bright_count = 0
            min_luma = 255.0
            max_luma = 0.0
            for y in range(0, height, step_y):
                for x in range(0, width, step_x):
                    r, g, b = pixels[x, y]
                    luma = (float(r) + float(g) + float(b)) / 3.0
                    sample_count += 1
                    if luma >= 242.0:
                        bright_count += 1
                    if luma < min_luma:
                        min_luma = luma
                    if luma > max_luma:
                        max_luma = luma

            if sample_count <= 0:
                return True
            bright_ratio = bright_count / float(sample_count)
            dynamic_range = max_luma - min_luma
            # Mostly bright + almost no contrast -> effectively blank canvas.
            if bright_ratio >= 0.992 and dynamic_range <= 5.0:
                return True
    except Exception:
        return False
    return False

def _capture_direct_url(
    browser: Path,
    url: str,
    width: int,
    height: int,
    output_path: Path,
    profile_dir: Path,
    *,
    virtual_time_budget: int = 22000,
) -> None:
    command = [
        str(browser),
        '--headless=new',
        '--disable-gpu',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-breakpad',
        '--disable-crash-reporter',
        '--disable-blink-features=AutomationControlled',
        '--disable-background-networking',
        '--disable-sync',
        '--disable-component-update',
        '--disable-domain-reliability',
        '--disable-notifications',
        '--disable-extensions',
        '--hide-scrollbars',
        '--run-all-compositor-stages-before-draw',
        f'--window-size={width},{height}',
        '--lang=ru-RU',
        '--force-device-scale-factor=1',
        f'--virtual-time-budget={virtual_time_budget}',
        f'--user-data-dir={profile_dir}',
        f'--user-agent={USER_AGENT}',
        f'--screenshot={output_path}',
        url,
    ]
    completed = _run_shot(command, timeout=75)
    if completed.returncode != 0 and not output_path.exists():
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f'browser_direct_exit_{completed.returncode}')


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--width', type=int, default=1536)
    parser.add_argument('--height', type=int, default=960)
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    browser = _find_browser(base_dir)
    width = max(800, min(int(args.width), 2560))
    height = max(600, min(int(args.height), 2560))

    allow_dirty_fallback = os.getenv('NADIN_REMOTE_ALLOW_DIRTY_FALLBACK', '0').strip().lower() in {'1', 'true', 'yes'}

    with tempfile.TemporaryDirectory(prefix='nadin_remote_png_') as tmp_dir:
        tmp_path = Path(tmp_dir)
        output_path = tmp_path / 'shot.png'
        profile_dir = tmp_path / 'profile'
        profile_dir.mkdir(parents=True, exist_ok=True)
        html_path = tmp_path / 'snapshot.html'

        errors = []
        challenge_detected = False
        _debug(f'revision={SCRIPT_REVISION} url={args.url} width={width} height={height} allow_dirty_fallback={allow_dirty_fallback}')

        # Preferred path: live-page render with JS cleanup executed before screenshot.
        try:
            _debug('try_playwright_capture')
            _capture_with_playwright(browser, args.url, width, height, output_path)
            if output_path.exists():
                _debug(f'playwright_output_bytes={output_path.stat().st_size}')
        except Exception as exc:  # noqa: BLE001
            _debug(f'playwright_failed={exc}')
            errors.append(f'playwright: {exc}')
            if 'security_challenge_detected' in str(exc):
                challenge_detected = True

        raw_html = ''
        rendered_clean = False

        # Secondary path: fallback to cleaned HTML render if playwright path did not produce output.
        if (not output_path.exists()) or output_path.stat().st_size <= 0:
            try:
                _debug('try_fetch_html')
                raw_html = _fetch_html(args.url)
                _debug(f'fetch_html_len={len(raw_html)}')
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

            dom_html = ''
            try:
                _debug('try_dump_dom')
                dom_html = _dump_dom_with_browser(browser, args.url, width, height)
                _debug(f'dump_dom_len={len(dom_html)}')
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

            if dom_html and (not _looks_like_security_challenge(dom_html)):
                raw_html = dom_html
            elif (not raw_html) or _looks_like_security_challenge(raw_html):
                if dom_html:
                    raw_html = dom_html

            challenge_detected = challenge_detected or (_looks_like_security_challenge(raw_html) if raw_html else False)
            if raw_html:
                try:
                    cleaned_html = _prepare_html(args.url, raw_html)
                    _debug(f'try_clean_render html_len={len(cleaned_html)}')
                    _render_html_to_png(browser, cleaned_html, width, height, output_path, profile_dir, html_path)
                    rendered_clean = output_path.exists() and output_path.stat().st_size > 0
                    if output_path.exists():
                        _debug(f'clean_render_output_bytes={output_path.stat().st_size}')
                    if rendered_clean and _looks_blank_png(output_path):
                        try:
                            output_path.unlink()
                        except Exception:
                            pass
                        rendered_clean = False
                        errors.append('blank_clean_render')
                except Exception as exc:  # noqa: BLE001
                    errors.append(str(exc))

        # Optional dirty fallback (disabled by default).
        if (not output_path.exists()) or output_path.stat().st_size <= 0:
            if allow_dirty_fallback:
                try:
                    _debug('try_dirty_fallback')
                    _capture_direct_url(browser, args.url, width, height, output_path, profile_dir)
                    if output_path.exists():
                        _debug(f'dirty_output_bytes={output_path.stat().st_size}')
                except Exception as exc:  # noqa: BLE001
                    errors.append(f'dirty_fallback: {exc}')
                if output_path.exists() and _looks_blank_png(output_path):
                    try:
                        output_path.unlink()
                    except Exception:
                        pass
                    try:
                        _capture_direct_url(
                            browser,
                            args.url,
                            width,
                            height,
                            output_path,
                            profile_dir,
                            virtual_time_budget=42000,
                        )
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f'dirty_fallback_retry: {exc}')
                    if output_path.exists() and _looks_blank_png(output_path):
                        errors.append('blank_direct_render')
            else:
                if challenge_detected:
                    raise RuntimeError('security_challenge_detected')
                if raw_html and not rendered_clean:
                    raise RuntimeError('clean_render_failed')

        if (not output_path.exists()) or output_path.stat().st_size <= 0:
            _debug(f'final_no_output errors={errors}')
            if challenge_detected:
                raise RuntimeError('security_challenge_detected')
            raise RuntimeError('; '.join(errors) if errors else 'screenshot_not_created')

        _debug(f'final_output_bytes={output_path.stat().st_size}')

        sys.stdout.buffer.write(output_path.read_bytes())
        sys.stdout.flush()
        return 0


if __name__ == '__main__':
    raise SystemExit(main())


























