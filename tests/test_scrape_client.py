from scrape_client import ScrapeClient


def test_fetch_marks_202_as_blocked(monkeypatch):
    client = ScrapeClient(per_domain_min_delay=0)

    def fake_perform(_url, timeout, mode):
        return 202, "", "", ""

    monkeypatch.setattr(client, "_perform_request", fake_perform)

    result = client.fetch("https://duckduckgo.com/html/?q=sber", timeout=1, max_retries=1)

    assert result.blocked is True
    assert result.status_code == 202


def test_fetch_falls_back_when_403(monkeypatch):
    client = ScrapeClient(per_domain_min_delay=0)
    calls = []

    def fake_fetch_once(url, timeout, max_retries, mode):
        calls.append(mode)
        if mode == client.mode_default:
            from scrape_client import FetchResult

            return FetchResult(url=url, status_code=403, text="blocked", ok=False, blocked=True, mode=mode)
        from scrape_client import FetchResult

        return FetchResult(url=url, status_code=200, text="ok", ok=True, blocked=False, mode=mode)

    monkeypatch.setattr(client, "_fetch_once", fake_fetch_once)

    result = client.fetch("https://example.com", timeout=1, max_retries=1)

    assert result.ok is True
    assert calls[0] == client.mode_default
    assert client.mode_fallback in calls
