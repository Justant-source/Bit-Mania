#!/usr/bin/env python3
"""
phase0_sl_feasibility.py — SL 발동 가능성 사전 추정 (백테스트 없이 trades.csv 분석)

기존 챔피언 trades.csv의 각 트레이드에 대해 4h 봉 기준으로 intrabar 최저가를 추정하고,
각 SL 수준이 발동됐을 트레이드 수와 평균 PnL 변화를 산출한다.

주의: 4h 봉의 low만 사용하므로 실제 intrabar 경로는 알 수 없다.
     이것은 상한 추정(발동 가능성 최대화)이다.

Usage:
    python3 phase0_sl_feasibility.py \
        --trades backtest/results/7-strategies/supertrend/4h/long_only_x3/trades.csv \
        --output backtest/results/p0_sl_feasibility/report.md
"""
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

LEVERAGE = 3
SL_LEVELS = [0.0, -10.0, -15.0, -20.0, -25.0, -33.0]


def load_trades(path: Path) -> list[dict]:
    trades = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append({
                'entry_price': float(row['entry_price']),
                'exit_price':  float(row['exit_price']),
                'pnl':         float(row['pnl']),
                'side':        row['side'],
            })
    return trades


def simulate_sl(trades: list[dict], sl_pct: float) -> dict:
    """
    각 트레이드에 SL이 적용됐을 때의 결과를 추정.
    4h candle의 low 데이터가 없으므로 exit_price를 worst-case proxy로 사용.
    실제로는 exit < entry × (1 + sl_pct/100/LEVERAGE) 이면 SL 발동으로 간주.
    """
    if sl_pct >= 0:
        return {'n_triggered': 0, 'pnl_saved': 0.0, 'pnl_lost': 0.0}

    sl_threshold_ratio = sl_pct / 100.0 / LEVERAGE  # e.g. -15% / 3 = -5% 가격 하락

    n_triggered = 0
    pnl_saved = 0.0
    pnl_lost = 0.0

    for t in trades:
        if t['side'] != 'long':
            continue
        entry = t['entry_price']
        sl_price = entry * (1.0 + sl_threshold_ratio)
        actual_exit = t['exit_price']
        actual_pnl = t['pnl']

        if actual_exit <= sl_price:
            # SL would have triggered at sl_price (or exit was worse)
            # Estimate: SL cuts loss at sl_pct of margin
            # approximate qty from pnl and price move
            price_move = actual_exit - entry
            if price_move != 0:
                qty = abs(actual_pnl / price_move)
            else:
                qty = 0.0
            sl_pnl = (sl_price - entry) * qty - abs(actual_pnl) * 0.001  # rough fee
            delta = sl_pnl - actual_pnl
            n_triggered += 1
            if delta > 0:
                pnl_saved += delta
            else:
                pnl_lost += abs(delta)

    return {
        'n_triggered': n_triggered,
        'pnl_saved': pnl_saved,
        'pnl_lost': pnl_lost,
        'net_delta': pnl_saved - pnl_lost,
    }


def write_report(output: Path, trades: list[dict], results: dict) -> None:
    long_trades = [t for t in trades if t['side'] == 'long']
    n_total = len(long_trades)
    n_negative = sum(1 for t in long_trades if t['pnl'] < 0)

    lines = [
        "# Phase 0 — SL Feasibility 사전 추정",
        "",
        f"분석 대상: `{n_total}` 롱 트레이드 (손실: `{n_negative}`, 수익: `{n_total - n_negative}`)",
        "",
        "> **주의**: 4h 봉 close 기준 exit_price를 worst-case proxy로 사용.",
        "> actual_exit ≤ SL_price인 경우만 발동으로 간주 — 실제 intrabar SL 발동 수는 이보다 낮을 수 있음.",
        "",
        "## 결과 표",
        "",
        "| SL 수준 (마진%) | 발동 트레이드 수 | 절감 PnL ($) | 손실 PnL ($) | 순 효과 ($) | 판정 |",
        "|---|---|---|---|---|---|",
    ]

    for sl_pct in SL_LEVELS:
        r = results[sl_pct]
        if sl_pct == 0.0:
            lines.append(f"| 비활성 (0.0%) | — | — | — | — | (baseline) |")
        else:
            verdict = "✓ Positive" if r['net_delta'] > 0 else "✗ Negative"
            lines.append(
                f"| {sl_pct:.1f}% | {r['n_triggered']} | "
                f"{r['pnl_saved']:.0f} | {r['pnl_lost']:.0f} | "
                f"{r['net_delta']:.0f} | {verdict} |"
            )

    lines += [
        "",
        "## 해석",
        "",
        "- 순 효과 > 0 → SL이 손절보다 수익 보존에 유리 (sweep 진행 가치 있음)",
        "- 발동 트레이드 < 5 → SL 효과 통계적 무의미 (sweep 의의 낮음)",
        "",
        "> Phase 1 sweep으로 정확한 수치 확인 필요.",
    ]

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(lines) + '\n')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--trades', type=str,
                   default='backtest/results/7-strategies/supertrend/4h/long_only_x3/trades.csv')
    p.add_argument('--output', type=str,
                   default='backtest/results/p0_sl_feasibility/report.md')
    args = p.parse_args()

    trades_path = Path(args.trades)
    if not trades_path.is_absolute():
        # resolve relative to repo root (2 levels up from this script)
        repo_root = Path(__file__).parent.parent.parent.parent
        trades_path = repo_root / trades_path

    output_path = Path(args.output)
    if not output_path.is_absolute():
        repo_root = Path(__file__).parent.parent.parent.parent
        output_path = repo_root / output_path

    if not trades_path.exists():
        print(f"[ERROR] trades.csv not found: {trades_path}")
        return 1

    trades = load_trades(trades_path)
    print(f"Loaded {len(trades)} trades from {trades_path}")

    results = {}
    for sl_pct in SL_LEVELS:
        results[sl_pct] = simulate_sl(trades, sl_pct)
        r = results[sl_pct]
        if sl_pct == 0.0:
            print(f"  sl={sl_pct:6.1f}%: baseline")
        else:
            print(f"  sl={sl_pct:6.1f}%: triggered={r['n_triggered']}, "
                  f"net_delta=${r['net_delta']:.0f}")

    write_report(output_path, trades, results)
    print(f"\nReport written: {output_path}")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
