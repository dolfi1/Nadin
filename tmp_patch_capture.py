from pathlib import Path

p = Path("tools/remote_shot/capture_png.py")
text = p.read_text(encoding="utf-8")

insert_block = '''

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
'''

anchor = "\n  const clearBodyLocks = () => {{\n"
if insert_block.strip() not in text:
    if anchor not in text:
        raise RuntimeError("clearBodyLocks anchor not found")
    text = text.replace(anchor, insert_block + anchor, 1)

old_run = '''  const run = () => {{
    safeCall(clickHelpfulButtons);
    safeCall(clickCloseButtons);
    safeCall(removeBySelectors);
    safeCall(removeAuthAndPromoWalls);
    safeCall(removeMarkerDrivenContainers);
    safeCall(removeTextAnchoredPopups);
    safeCall(removeFixedLayers);
    safeCall(removeCornerPopups);
    safeCall(removeBackdrops);
    safeCall(clearBodyLocks);
  }};'''
new_run = '''  const run = () => {{
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
    safeCall(removeBackdrops);
    safeCall(clearBodyLocks);
  }};'''
if old_run in text:
    text = text.replace(old_run, new_run, 1)

p.write_text(text, encoding="utf-8")
print("patched capture_png")
