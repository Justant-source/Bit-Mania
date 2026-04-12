#!/usr/bin/env python3
"""
Data Integrity Validator
=========================
수집된 데이터의 품질을 검증합니다:
- 타임스탬프 연속성 (gap 탐지)
- 가격 이상치 (10% 이상 점프)
- 결측치 비율
저장: docs/data_quality_report.md

사용법:
    python scripts/data/validate_data_integrity.py
"""

import sys
from datetime import timezone
from pathlib import Path

import pandas as pd

DATA_ROOT = Path(__file__).parent.parent.parent / "data"
REPORT_PATH = Path(__file__).parent.parent.parent / "docs" / "data_quality_report.md"

INTERVAL_MINUTES = {
    "1m": 1,
    "1h": 60,
    "1d": 1440,
}

PRICE_JUMP_THRESHOLD = 0.10  # 10% jump is suspicious


def check_klines(symbol: str, interval: str) -> dict:
    """Check OHLCV data integrity."""
    base = DATA_ROOT / "binance_vision" / "klines" / symbol / interval
    if not base.exists():
        return {"status": "MISSING", "path": str(base)}

    parquet_files = sorted(base.glob("**/*.parquet"))
    if not parquet_files:
        return {"status": "EMPTY", "path": str(base)}

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs).sort_values("open_time").reset_index(drop=True)

    result = {
        "status": "OK",
        "rows": len(df),
        "start": str(df["open_time"].min()),
        "end": str(df["open_time"].max()),
        "gaps": [],
        "price_jumps": [],
        "null_count": int(df.isnull().sum().sum()),
    }

    # Check timestamp gaps
    expected_delta = pd.Timedelta(minutes=INTERVAL_MINUTES.get(interval, 60))
    diffs = df["open_time"].diff().dropna()
    gap_mask = diffs > expected_delta * 1.5
    gaps = df[gap_mask]["open_time"].tolist()
    result["gaps"] = [str(g) for g in gaps[:10]]  # First 10 gaps
    result["gap_count"] = len(gaps)

    # Check price jumps
    close_pct_change = df["close"].pct_change().abs()
    jump_mask = close_pct_change > PRICE_JUMP_THRESHOLD
    jumps = df[jump_mask][["open_time", "close"]].head(5).to_dict("records")
    result["price_jumps"] = [{"ts": str(r["open_time"]), "close": r["close"]} for r in jumps]
    result["jump_count"] = int(jump_mask.sum())

    if result["gap_count"] > 0 or result["null_count"] > 0:
        result["status"] = "WARN"
    if result["gap_count"] > 100:
        result["status"] = "FAIL"

    return result


def check_funding(symbol: str) -> dict:
    """Check funding rate data integrity."""
    base = DATA_ROOT / "binance_vision" / "funding" / symbol
    if not base.exists():
        return {"status": "MISSING", "path": str(base)}

    parquet_files = sorted(base.glob("*.parquet"))
    if not parquet_files:
        return {"status": "EMPTY"}

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs).sort_values("calc_time").reset_index(drop=True)

    result = {
        "status": "OK",
        "rows": len(df),
        "start": str(df["calc_time"].min()),
        "end": str(df["calc_time"].max()),
        "null_count": int(df.isnull().sum().sum()),
        "rate_min": float(df["last_funding_rate"].min()),
        "rate_max": float(df["last_funding_rate"].max()),
        "rate_mean": float(df["last_funding_rate"].mean()),
    }
    return result


def check_fear_greed() -> dict:
    """Check Fear & Greed data."""
    path = DATA_ROOT / "fear_greed" / "fear_greed_index.parquet"
    if not path.exists():
        return {"status": "MISSING"}
    df = pd.read_parquet(path)
    return {
        "status": "OK",
        "rows": len(df),
        "start": str(df["timestamp"].min()),
        "end": str(df["timestamp"].max()),
        "value_range": f"{df['value'].min()} ~ {df['value'].max()}",
    }


def check_fred() -> dict:
    """Check FRED macro data."""
    fred_dir = DATA_ROOT / "fred"
    if not fred_dir.exists():
        return {"status": "MISSING"}
    files = list(fred_dir.glob("*.parquet"))
    results = {}
    for f in files:
        df = pd.read_parquet(f)
        series_id = f.stem
        col = series_id.lower()
        if col in df.columns:
            results[series_id] = {
                "rows": len(df),
                "start": str(df["date"].min()),
                "end": str(df["date"].max()),
            }
    return {"status": "OK" if results else "MISSING", "series": results}


def generate_report(results: dict) -> str:
    """Generate markdown quality report."""
    lines = [
        "# Data Quality Report",
        "",
        f"Generated: 2026-04-11",
        "",
        "## Summary",
        "",
    ]

    all_ok = all(
        v.get("status", "?") in ("OK", "WARN")
        for v in results.values()
        if isinstance(v, dict)
    )
    lines.append(f"**Overall Status**: {'✅ PASS' if all_ok else '❌ FAIL'}")
    lines.append("")

    # OHLCV
    lines.append("## OHLCV Data (Binance Vision)")
    lines.append("")
    lines.append("| Symbol | Interval | Status | Rows | Range | Gaps | Price Jumps |")
    lines.append("|--------|----------|--------|------|-------|------|-------------|")
    for key, v in results.items():
        if key.startswith("klines_"):
            _, sym, intv = key.split("_", 2)
            status_icon = "✅" if v["status"] == "OK" else ("⚠️" if v["status"] == "WARN" else "❌")
            rows = v.get("rows", "N/A")
            rng = f"{v.get('start', '?')[:10]} ~ {v.get('end', '?')[:10]}" if "start" in v else "N/A"
            gaps = v.get("gap_count", "N/A")
            jumps = v.get("jump_count", "N/A")
            lines.append(f"| {sym} | {intv} | {status_icon} {v['status']} | {rows} | {rng} | {gaps} | {jumps} |")
    lines.append("")

    # Funding
    lines.append("## Funding Rate Data")
    lines.append("")
    for key, v in results.items():
        if key.startswith("funding_"):
            sym = key[8:]
            lines.append(f"- **{sym}**: {v.get('status', '?')} — {v.get('rows', 'N/A')} rows, {v.get('start', '?')[:10]} ~ {v.get('end', '?')[:10]}")
            if "rate_mean" in v:
                lines.append(f"  - Rate range: {v['rate_min']:.6f} ~ {v['rate_max']:.6f}, mean: {v['rate_mean']:.6f}")
    lines.append("")

    # Fear & Greed
    fg = results.get("fear_greed", {})
    lines.append(f"## Fear & Greed Index: {fg.get('status', 'MISSING')}")
    if "rows" in fg:
        lines.append(f"- {fg['rows']} days, {fg.get('start', '?')[:10]} ~ {fg.get('end', '?')[:10]}")
    lines.append("")

    # FRED
    fred = results.get("fred", {})
    lines.append(f"## FRED Macro Data: {fred.get('status', 'MISSING')}")
    for sid, sv in fred.get("series", {}).items():
        lines.append(f"- **{sid}**: {sv['rows']} obs, {sv['start'][:10]} ~ {sv['end'][:10]}")
    lines.append("")

    lines.append("## Recommendations")
    lines.append("")
    missing = [k for k, v in results.items() if isinstance(v, dict) and v.get("status") == "MISSING"]
    if missing:
        lines.append("**Missing data** — run these scripts:")
        for m in missing:
            lines.append(f"- `{m}`: Run appropriate fetch script in `scripts/data/`")
    else:
        lines.append("All data sources present. Ready for backtesting.")

    return "\n".join(lines)


def main():
    print("\n=== Data Integrity Validator ===\n")
    results = {}

    # Check OHLCV
    for sym in ["BTCUSDT"]:
        for interval in ["1m", "1h", "1d"]:
            key = f"klines_{sym}_{interval}"
            print(f"Checking {key}...")
            results[key] = check_klines(sym, interval)
            print(f"  Status: {results[key]['status']}")

    # Check funding
    for sym in ["BTCUSDT"]:
        key = f"funding_{sym}"
        print(f"Checking {key}...")
        results[key] = check_funding(sym)
        print(f"  Status: {results[key]['status']}")

    # Check Fear & Greed
    print("Checking fear_greed...")
    results["fear_greed"] = check_fear_greed()
    print(f"  Status: {results['fear_greed']['status']}")

    # Check FRED
    print("Checking fred...")
    results["fred"] = check_fred()
    print(f"  Status: {results['fred']['status']}")

    report = generate_report(results)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report)
    print(f"\n[REPORT] Saved → {REPORT_PATH}")
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
