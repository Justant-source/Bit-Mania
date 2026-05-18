#!/usr/bin/env python3
"""
p3_walk_forward.py — 상위 3 candidate 5-fold rolling OOS walk-forward 검증

각 candidate의 HP로 5개 롤링 윈도우를 Docker 백테스트로 실행하고,
OOS 평균 MDD vs 전체기간 단일 MDD 비교.

롤링 윈도우 (p0~p4 — param_sweep_v3 동일):
  p0: 2020-01-01 ~ 2021-06-30  (최근 OOS)
  p1: 2021-01-01 ~ 2026-04-30  (5년)
  p2: 2022-01-01 ~ 2026-04-30  (최근 3.4년)
  p3: 2021-01-01 ~ 2025-12-31  (4.5년)
  p4: 2023-01-01 ~ 2026-04-30  (최근 3년)

Usage (inside Jesse container):
    python3 /app/scripts/optimization/p3_walk_forward.py \
        --db /result/p1_optimization/queue.sqlite3 \
        --top-n 3 \
        --output-dir /result/p3_validation
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

WINDOWS = [
    {'name': 'p0', 'start': '2020-01-01', 'end': '2021-06-30'},
    {'name': 'p1', 'start': '2021-01-01', 'end': '2026-04-30'},
    {'name': 'p2', 'start': '2022-01-01', 'end': '2026-04-30'},
    {'name': 'p3', 'start': '2021-01-01', 'end': '2025-12-31'},
    {'name': 'p4', 'start': '2023-01-01', 'end': '2026-04-30'},
]
BACKTEST_TIMEOUT = 900  # 15 min per run


def load_top_n(db_path: Path, n: int) -> list[dict]:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct, cagr, mdd, sharpe, trades, multiplier "
        "FROM jobs WHERE status='done' AND multiplier IS NOT NULL "
        "ORDER BY multiplier DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def run_backtest(combo: dict, window: dict, out_dir: Path) -> dict | None:
    hp = {k: combo[k] for k in ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
                                  'direction_ema_len', 'atr_mult', 'sl_margin_pct']}
    hp_json = json.dumps(hp)

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', window['start'],
        '--end', window['end'],
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', hp_json,
        '--output', str(out_dir),
    ]

    run_dir = Path(tempfile.mkdtemp(prefix=f"jesse_wf_{combo['combo_id']}_{window['name']}_"))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        stats_path = out_dir / 'stats.json'
        if not stats_path.exists():
            return None
        with open(stats_path) as f:
            stats = json.load(f)
        return {
            'cagr':   stats.get('cagr_pct'),
            'mdd':    stats.get('max_drawdown_pct'),
            'sharpe': stats.get('sharpe_ratio'),
            'trades': stats.get('total_trades'),
        }
    except Exception as e:
        print(f"[ERROR combo={combo['combo_id']} {window['name']}] {e}", file=sys.stderr)
        return None
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def write_report(results: list[dict], output: Path) -> None:
    lines = [
        "# Phase 3 — Walk-Forward OOS 검증",
        "",
        "각 candidate를 5-fold rolling OOS로 실행, 전체기간 단일 MDD와 비교.",
        "",
        "> **강건 기준**: OOS 평균 MDD가 전체기간 MDD 대비 ±10% 이내",
        "",
    ]

    for r in results:
        combo = r['combo']
        windows = r['windows']
        full_mdd = combo['mdd']

        oos_mdds = [w['mdd'] for w in windows if w.get('mdd') is not None]
        oos_avg_mdd = sum(oos_mdds) / len(oos_mdds) if oos_mdds else None

        if oos_avg_mdd is not None and full_mdd is not None:
            delta = oos_avg_mdd - full_mdd
            robust = abs(delta) <= abs(full_mdd) * 0.10
            verdict = "ROBUST" if robust else "OVERFIT SUSPECT"
        else:
            verdict = "INCOMPLETE"

        sl_label = f"sl={combo['sl_margin_pct']:.0f}%" if combo['sl_margin_pct'] < 0 else "sl=off"
        lines += [
            f"## combo_id={combo['combo_id']} ({sl_label})",
            "",
            f"HP: st_factor={combo['st_factor']} | st_period={combo['st_period']} | "
            f"fast_ema={combo['fast_ema_len']} | slow_ema={combo['slow_ema_len']} | "
            f"dir_ema={combo['direction_ema_len']} | atr_mult={combo['atr_mult']}",
            "",
            f"전체기간: CAGR={combo['cagr']:.1f}% | MDD={full_mdd:.1f}% | "
            f"Sharpe={combo['sharpe']:.3f} | mult={combo['multiplier']:.2f}x",
            "",
            "| 윈도우 | 기간 | CAGR% | MDD% | Sharpe | trades |",
            "|---|---|---|---|---|---|",
        ]

        for w_def, w_res in zip(WINDOWS, windows):
            if w_res is None:
                lines.append(f"| {w_def['name']} | {w_def['start']}~{w_def['end']} "
                             "| ERR | ERR | ERR | ERR |")
            else:
                lines.append(
                    f"| {w_def['name']} | {w_def['start']}~{w_def['end']} | "
                    f"{w_res.get('cagr', 'N/A'):.1f} | {w_res.get('mdd', 'N/A'):.1f} | "
                    f"{w_res.get('sharpe', 0):.3f} | {w_res.get('trades', 0)} |"
                )

        if oos_avg_mdd is not None:
            lines.append("")
            lines.append(f"OOS 평균 MDD: **{oos_avg_mdd:.1f}%** | 전체기간 MDD: {full_mdd:.1f}% "
                         f"| delta: {delta:+.1f}%")
            lines.append(f"**판정: {verdict}**")
        lines.append("")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(lines) + '\n')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/p1_optimization/queue.sqlite3')
    p.add_argument('--top-n', type=int, default=3)
    p.add_argument('--output-dir', type=str, default='/result/p3_validation')
    args = p.parse_args()

    db_path = Path(args.db)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = load_top_n(db_path, args.top_n)
    print(f"Walk-forward: {len(candidates)} candidates × {len(WINDOWS)} windows "
          f"= {len(candidates) * len(WINDOWS)} backtests")

    all_results = []
    for combo in candidates:
        print(f"\n  combo={combo['combo_id']} mult={combo['multiplier']:.2f}x "
              f"mdd={combo['mdd']:.1f}%")
        window_results = []
        for w in WINDOWS:
            out = output_dir / f"combo_{combo['combo_id']}_{w['name']}"
            t0 = time.time()
            res = run_backtest(combo, w, out)
            elapsed = time.time() - t0
            if res:
                print(f"    {w['name']}: cagr={res['cagr']:.1f}% mdd={res['mdd']:.1f}% "
                      f"({elapsed:.0f}s)")
            else:
                print(f"    {w['name']}: FAILED ({elapsed:.0f}s)")
            window_results.append(res)
        all_results.append({'combo': combo, 'windows': window_results})

    report_path = output_dir / 'walk_forward.md'
    write_report(all_results, report_path)
    print(f"\nReport: {report_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
