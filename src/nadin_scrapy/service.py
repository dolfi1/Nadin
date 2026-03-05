from __future__ import annotations

from typing import Any

from .pipelines import CompanyProfilePipeline


def merge_provider_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    pipeline = CompanyProfilePipeline()
    normalized = [pipeline.process_item(dict(payload), spider=None) for payload in payloads if isinstance(payload, dict)]
    return pipeline.merge_items(normalized)
