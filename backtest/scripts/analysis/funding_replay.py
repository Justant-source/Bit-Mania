#!/usr/bin/env python3
"""Compare #7908 with vs without Bybit funding cost — 전기간 and holdout (2025-01~).

Depends on backtest/results/funding/bybit_btcusdt_funding_8h.csv (run
fetch_bybit_funding.py first). Read-only wrt the live system; single-run reproducible.

Usage: python backtest/scripts/analysis/funding_replay.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "v12"))
import replay_lib as L  # noqa: E402
import _replay_supertrend as RS  # noqa: E402

FUNDING_CSV = Path(__file__).resolve().parents[2] / "results" / "funding" / "bybit_btcusdt_funding_8h.csv"
BASELINE = dict(st_factor=2.6, st_period=9, fast_ema=7, slow_ema=29, dir_ema=240, atr_mult=3.3)
HOLDOUT_START = RS.date_ms("2025-01-01")


def load_funding(path: Path) -> dict[int, float]:
    out = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            out[int(row["timestamp_ms"])] = float(row["funding_rate"])
    return out


def summarize(label, trades, equity_4h, funding_log):
    m = RS._metrics(trades, equity_4h)
    paid = sum(c for _, c in funding_log)
    print(f"\n=== {label} ===")
    print(f"trades={len(trades)}  CAGR={m['cagr']:.2f}%  MDD={m['mdd']:.2f}%  "
          f"Sharpe={m['sharpe']:.3f}  net={m['net_pct']:.2f}%")
    if funding_log:
        print(f"funding settlements charged={len(funding_log)}  total paid=${paid:,.2f}  "
              f"final equity share={paid / m['final'] * 100:.4f}%")
    else:
        print("funding: n/a (no-funding run)")
    return m, paid


def coverage_stats(trades, funding):
    """Fraction of holding time / trade count falling before the first funding record."""
    first_cov = min(funding) if funding else None
    if first_cov is None or not trades:
        return 0.0, 0, len(trades)
    held_before = sum(max(0, min(t[2], first_cov) - t[0]) for t in trades)
    held_total = sum(t[2] - t[0] for t in trades)
    trades_before = sum(1 for t in trades if t[0] < first_cov)
    frac = (held_before / held_total * 100) if held_total else 0.0
    return frac, trades_before, len(trades)


def main():
    funding = load_funding(FUNDING_CSV)
    first_cov, last_cov = min(funding), max(funding)
    print(f"funding coverage: {len(funding)} settlements, "
          f"{RS._u(first_cov)} .. {RS._u(last_cov)}")

    results = {}
    for label, start_ms, end_ms in [
        ("전기간", None, None),
        ("홀드아웃 2025-01~", HOLDOUT_START, None),
    ]:
        trades0, eq0 = L.run(BASELINE, start_ms=start_ms, end_ms=end_ms)
        m0, _ = summarize(f"{label} — 펀딩 없음(기존)", trades0, eq0, [])

        flog = []
        trades1, eq1 = L.run(BASELINE, start_ms=start_ms, end_ms=end_ms,
                              funding=funding, funding_log=flog)
        m1, paid = summarize(f"{label} — 펀딩 반영", trades1, eq1, flog)

        frac, n_before, n_total = coverage_stats(trades1, funding)
        print(f"  커버리지 밖(2020-03-25 이전, 0 가정) 보유시간 비율: {frac:.1f}%  "
              f"거래수 {n_before}/{n_total}건이 커버리지 이전 시작")

        results[label] = dict(no_funding=m0, funding=m1, paid=paid,
                               cov_frac=frac, n_before=n_before, n_total=n_total)

    ho = results["홀드아웃 2025-01~"]
    net_delta = ho["funding"]["net_pct"] - ho["no_funding"]["net_pct"]
    print("\n=== 판정 ===")
    print(f"홀드아웃 net: 펀딩없음 {ho['no_funding']['net_pct']:.2f}% -> "
          f"펀딩반영 {ho['funding']['net_pct']:.2f}%  (delta {net_delta:+.2f}%p)")
    print("참고: taker+15bps 비관 시나리오 홀드아웃 net = -1.6% (strategy.md '최종 판정')")


if __name__ == "__main__":
    main()
