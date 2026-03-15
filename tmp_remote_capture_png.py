#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
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


def _build_cleanup_injection(base_url: str) -> str:
    selectors = json.dumps(REMOVE_SELECTORS, ensure_ascii=False)
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
    .replace(/\s+/g, ' ')
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
    const hasCredentialFields = !!node.querySelector('input[type="password"],input[type="email"],input[name*="password" i],input[name*="email" i]');

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

  const run = () => {{
    clickHelpfulButtons();
    removeBySelectors();
    removeTextAnchoredPopups();
    removeFixedLayers();
    removeBackdrops();
    clearBodyLocks();
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
  setTimeout(() => {{ try {{ observer.disconnect(); }} catch (_e) {{}} }}, 13000);
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
        r"artdeco-modal|smartbanner|app-promo|app-banner|intercom"
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


def _render_html_to_png(browser: Path, html: str, width: int, height: int, output_path: Path, profile_dir: Path, html_path: Path, virtual_time_budget: int = 30000) -> None:
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

    allow_dirty_fallback = os.getenv('NADIN_REMOTE_ALLOW_DIRTY_FALLBACK', '1').strip().lower() in {'1', 'true', 'yes'}

    with tempfile.TemporaryDirectory(prefix='nadin_remote_png_') as tmp_dir:
        tmp_path = Path(tmp_dir)
        output_path = tmp_path / 'shot.png'
        profile_dir = tmp_path / 'profile'
        profile_dir.mkdir(parents=True, exist_ok=True)
        html_path = tmp_path / 'snapshot.html'

        raw_html = ''
        errors = []

        try:
            raw_html = _fetch_html(args.url)
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

        # Always try a rendered DOM snapshot from a real browser session.
        # For JS-heavy sites (LinkedIn/ZoomInfo/etc.) this is far more stable
        # than raw fetched HTML for post-clean rendering.
        dom_html = ''
        try:
            dom_html = _dump_dom_with_browser(browser, args.url, width, height)
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

        if dom_html and (not _looks_like_security_challenge(dom_html)):
            raw_html = dom_html
        elif (not raw_html) or _looks_like_security_challenge(raw_html):
            if dom_html:
                raw_html = dom_html

        rendered_clean = False
        challenge_detected = _looks_like_security_challenge(raw_html) if raw_html else False
        if raw_html:
            try:
                cleaned_html = _prepare_html(args.url, raw_html)
                _render_html_to_png(browser, cleaned_html, width, height, output_path, profile_dir, html_path)
                rendered_clean = output_path.exists() and output_path.stat().st_size > 0
                if rendered_clean and _looks_blank_png(output_path):
                    try:
                        output_path.unlink()
                    except Exception:
                        pass
                    rendered_clean = False
                    errors.append('blank_clean_render')
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

        if (not output_path.exists()) or output_path.stat().st_size <= 0:
            if allow_dirty_fallback:
                _capture_direct_url(browser, args.url, width, height, output_path, profile_dir)
                if _looks_blank_png(output_path):
                    try:
                        output_path.unlink()
                    except Exception:
                        pass
                    _capture_direct_url(
                        browser,
                        args.url,
                        width,
                        height,
                        output_path,
                        profile_dir,
                        virtual_time_budget=42000,
                    )
                    if _looks_blank_png(output_path):
                        errors.append('blank_direct_render')
            else:
                if challenge_detected:
                    raise RuntimeError('security_challenge_detected')
                if raw_html and not rendered_clean:
                    raise RuntimeError('clean_render_failed')

        if (not output_path.exists()) or output_path.stat().st_size <= 0:
            raise RuntimeError('; '.join(errors) if errors else 'screenshot_not_created')

        sys.stdout.buffer.write(output_path.read_bytes())
        sys.stdout.flush()
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
















