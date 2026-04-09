#!/usr/bin/env python3
"""Health check CLI for LLM Advisor data sources.

Usage:
    python scripts/health_check.py --all
    python scripts/health_check.py etf_flows
    python scripts/health_check.py --all --json
"""

import asyncio
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources.base import SourceHealth
from data_sources.etf_flows import ETFFlowsSource
from data_sources.onchain_metrics import OnchainMetricsSource
from data_sources.macro_indicators import MacroIndicatorsSource
from data_sources.research_reports import ResearchReportsSource
from data_sources.derivatives import DerivativesSource


SOURCE_MAP = {
    "etf_flows": ETFFlowsSource,
    "onchain": OnchainMetricsSource,
    "macro": MacroIndicatorsSource,
    "research": ResearchReportsSource,
    "derivatives": DerivativesSource,
}

STATUS_EMOJI = {"HEALTHY": "🟢", "DEGRADED": "🟡", "BROKEN": "🔴"}
WEIGHTS = {"etf_flows": 0.25, "macro": 0.20, "onchain": 0.20, "derivatives": 0.20, "research": 0.15}
STATUS_SCORES = {"HEALTHY": 1.0, "DEGRADED": 0.5, "BROKEN": 0.0}


async def check_source(name: str) -> tuple[str, SourceHealth, dict]:
    """Check a single source. Returns (name, health, summary)."""
    cls = SOURCE_MAP[name]
    source = cls(redis_client=None, http_session=None)
    try:
        summary, health = await source.get_context()
        return name, health, summary
    except Exception as e:
        health = SourceHealth(status="BROKEN", failure_reason=str(e)[:300], failure_stage="unhandled")
        return name, health, {}
    finally:
        await source.close()


async def check_all(source_names: list[str]) -> list[tuple[str, SourceHealth, dict]]:
    """Check multiple sources in parallel."""
    tasks = [check_source(name) for name in source_names]
    return await asyncio.gather(*tasks, return_exceptions=False)


def print_results(results: list[tuple[str, SourceHealth, dict]], as_json: bool = False):
    """Print health check results."""
    if as_json:
        output = {}
        for name, health, summary in results:
            output[name] = {
                "status": health.status,
                "failure_reason": health.failure_reason,
                "failure_stage": health.failure_stage,
                "fields_missing": health.fields_missing,
                "last_success_at": str(health.last_success_at) if health.last_success_at else None,
                "confidence": summary.get("confidence", 0),
            }
        freshness = compute_freshness(results)
        output["_overall"] = {"freshness": round(freshness, 2)}
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    print()
    for name, health, summary in results:
        emoji = STATUS_EMOJI.get(health.status, "❓")
        reason = ""
        if health.failure_reason:
            reason = f"  reason={health.failure_reason[:80]}"
        if health.fields_missing:
            reason += f"  missing={health.fields_missing}"
        last_ok = ""
        if health.last_success_at:
            last_ok = f"  last_success={health.last_success_at}"
        print(f"{emoji} {name:<14s} {health.status:<10s}{last_ok}{reason}")

    freshness = compute_freshness(results)
    print()
    threshold_indicator = "✅ OK" if freshness >= 0.8 else ("⚠️ WARNING" if freshness >= 0.5 else "🚨 CRITICAL")
    print(f"Overall freshness: {freshness:.2f} / 1.00  {threshold_indicator}")
    print()


def compute_freshness(results: list[tuple[str, SourceHealth, dict]]) -> float:
    total = 0.0
    for name, health, _ in results:
        w = WEIGHTS.get(name, 0.1)
        s = STATUS_SCORES.get(health.status, 0.0)
        total += w * s
    return total


def main():
    args = sys.argv[1:]
    as_json = "--json" in args
    args = [a for a in args if a != "--json"]

    if "--all" in args or not args:
        source_names = list(SOURCE_MAP.keys())
    else:
        source_names = [a for a in args if a in SOURCE_MAP]
        if not source_names:
            print(f"Unknown source(s). Available: {', '.join(SOURCE_MAP.keys())}")
            sys.exit(1)

    results = asyncio.run(check_all(source_names))
    print_results(results, as_json=as_json)

    # Exit code: 0 if all healthy, 1 if any broken, 2 if degraded only
    statuses = {h.status for _, h, _ in results}
    if "BROKEN" in statuses:
        sys.exit(1)
    if "DEGRADED" in statuses:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
