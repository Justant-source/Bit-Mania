"""Fail-Loud validation framework for data sources.

4-layer validation:
1. HTTP/Network Layer — status codes, timeouts
2. Structural Layer — DOM selectors (HTML) or required keys (JSON)
3. Schema Layer — field types and counts
4. Sanity Layer — value range checks
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import structlog

from .base import SourceHealth

logger = structlog.get_logger(__name__)


class FailureDetector:
    """Shared validation framework for all data sources."""

    def __init__(self, source_name: str):
        self.source_name = source_name

    def check_http(self, status_code: int, content_length: int = 0) -> SourceHealth:
        """Layer 1: HTTP response validation."""
        if status_code >= 500:
            return SourceHealth(
                status="BROKEN",
                failure_reason=f"HTTP {status_code} server error",
                failure_stage="http",
            )
        if status_code == 403:
            return SourceHealth(
                status="BROKEN",
                failure_reason="HTTP 403 Forbidden — possible IP ban or rate limit",
                failure_stage="http",
            )
        if status_code == 404:
            return SourceHealth(
                status="BROKEN",
                failure_reason="HTTP 404 — endpoint URL changed or removed",
                failure_stage="http",
            )
        if status_code != 200:
            return SourceHealth(
                status="DEGRADED",
                failure_reason=f"HTTP {status_code} unexpected status",
                failure_stage="http",
            )
        if content_length == 0:
            return SourceHealth(
                status="BROKEN",
                failure_reason="Empty response body",
                failure_stage="http",
            )
        return SourceHealth(status="HEALTHY", failure_stage="http")

    def check_html_structure(
        self,
        soup,
        required_selectors: dict[str, str],
        min_counts: dict[str, int] | None = None,
    ) -> SourceHealth:
        """Layer 2: HTML DOM structure validation.

        Args:
            soup: BeautifulSoup object
            required_selectors: {"name": "css_selector"} — all must exist
            min_counts: {"selector_name": min_count} — optional count check
        """
        if soup is None:
            return SourceHealth(
                status="BROKEN",
                failure_reason="HTML parsing returned None",
                failure_stage="structural",
            )

        missing_selectors = []
        for name, selector in required_selectors.items():
            elements = soup.select(selector)
            if not elements:
                missing_selectors.append(name)
            elif min_counts and name in min_counts:
                if len(elements) < min_counts[name]:
                    missing_selectors.append(f"{name} (found {len(elements)}, need {min_counts[name]})")

        if missing_selectors:
            return SourceHealth(
                status="BROKEN",
                failure_reason=f"Missing HTML selectors: {', '.join(missing_selectors)}",
                failure_stage="structural",
                fields_missing=missing_selectors,
            )
        return SourceHealth(status="HEALTHY", failure_stage="structural")

    def check_json_structure(
        self,
        data: dict | list | None,
        required_keys: set[str] | None = None,
    ) -> SourceHealth:
        """Layer 2 (JSON variant): Check required top-level keys exist."""
        if data is None:
            return SourceHealth(
                status="BROKEN",
                failure_reason="JSON response is None",
                failure_stage="structural",
            )
        if isinstance(data, list):
            if len(data) == 0:
                return SourceHealth(
                    status="BROKEN",
                    failure_reason="JSON response is empty array",
                    failure_stage="structural",
                )
            return SourceHealth(status="HEALTHY", failure_stage="structural")

        if required_keys:
            missing = required_keys - set(data.keys())
            if missing:
                present = required_keys - missing
                if len(present) == 0:
                    return SourceHealth(
                        status="BROKEN",
                        failure_reason=f"All required keys missing: {missing}",
                        failure_stage="structural",
                        fields_missing=list(missing),
                    )
                return SourceHealth(
                    status="DEGRADED",
                    failure_reason=f"Partial keys missing: {missing}",
                    failure_stage="schema",
                    fields_available=list(present),
                    fields_missing=list(missing),
                )
        return SourceHealth(status="HEALTHY", failure_stage="structural")

    def check_value_sanity(
        self,
        data: dict,
        ranges: dict[str, tuple[float, float]],
    ) -> SourceHealth:
        """Layer 4: Value range validation.

        Args:
            data: parsed data dict
            ranges: {"field_name": (min_val, max_val)}
        """
        out_of_range = []
        for field_name, (lo, hi) in ranges.items():
            val = data.get(field_name)
            if val is None:
                continue
            try:
                val = float(val)
                if val < lo or val > hi:
                    out_of_range.append(f"{field_name}={val} (expected {lo}~{hi})")
            except (TypeError, ValueError):
                continue

        if out_of_range:
            return SourceHealth(
                status="DEGRADED",
                failure_reason=f"Values out of range: {'; '.join(out_of_range)}",
                failure_stage="sanity",
            )
        return SourceHealth(status="HEALTHY", failure_stage="sanity")

    def combine(self, *healths: SourceHealth) -> SourceHealth:
        """Merge multiple validation results. Converges to worst status."""
        STATUS_RANK = {"HEALTHY": 0, "DEGRADED": 1, "BROKEN": 2}
        worst = max(healths, key=lambda h: STATUS_RANK.get(h.status, 0))

        all_missing = []
        all_available = []
        reasons = []
        for h in healths:
            if h.fields_missing:
                all_missing.extend(h.fields_missing)
            if h.fields_available:
                all_available.extend(h.fields_available)
            if h.failure_reason and h.status != "HEALTHY":
                reasons.append(f"[{h.failure_stage}] {h.failure_reason}")

        return SourceHealth(
            status=worst.status,
            last_success_at=worst.last_success_at,
            failure_reason=" | ".join(reasons) if reasons else None,
            failure_stage=worst.failure_stage,
            fields_available=list(set(all_available)),
            fields_missing=list(set(all_missing)),
        )
