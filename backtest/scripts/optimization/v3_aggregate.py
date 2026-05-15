#!/usr/bin/env python3
"""
v3_aggregate.py — 1,620 combo 분석적 10구간 슬라이스 + 펀딩비 차감 + composite 점수

각 combo의 trades.csv를 10구간으로 분할하여 구간별 CAGR/MDD/trades를 계산.
펀딩비는 각 trade의 holding period 내 8h settlement 합계로 차감.
Hard gate + composite 점수 산출 → v3_all_combos.csv, v3_winners.csv, v3_summary.md.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v3_aggregate.py \
        --db /result/v3_optimization/queue.sqlite3 \
        --output-dir /result/v3_optimization
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev

# 10구간 동등 시간 분할 (2017-08-18 ~ 2026-04-30)
WINDOWS_10 = [
    ('W01', '2017-08-18', '2018-07-01'),
    ('W02', '2018-07-01', '2019-05-13'),
    ('W03', '2019-05-13', '2020-03-25'),
    ('W04', '2020-03-25', '2021-02-04'),
    ('W05', '2021-02-04', '2021-12-17'),
    ('W06', '2021-12-17', '2022-10-29'),
    ('W07', '2022-10-29', '2023-09-10'),
    ('W08', '2023-09-10', '2024-07-22'),
    ('W09', '2024-07-22', '2025-06-03'),
    ('W10', '2025-06-03', '2026-04-15'),
]

FUNDING_PATH_PRIMARY   = '/data/funding/BTCUSDT_8h.parquet'
FUNDING_PATH_SECONDARY = '/data/funding/binance_api/BTCUSDT_2019.parquet'
FUNDING_REAL_START_MS  = 1568073600000  # 2019-09-10 08:00 UTC (first nonzero rate)


def _ts_ms(date_str: str) -> int:
    return int(datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


def load_funding_data() -> dict[int, float]:
    """Load 8h funding rates into {ts_ms: rate} dict."""
    data: dict[int, float] = {}
    try:
        import polars as pl
        dfs = []
        for path in (FUNDING_PATH_PRIMARY, FUNDING_PATH_SECONDARY):
            p = Path(path)
            if not p.exists():
                continue
            df = pl.read_parquet(p)
            rename = {}
            for col in df.columns:
                low = col.lower()
                if low in ('timestamp', 'timestamp_ms', 'time', 'ts'):
                    rename[col] = 'ts'
                elif low in ('rate', 'funding_rate', 'funding', 'value'):
                    rename[col] = 'rate'
            if rename:
                df = df.rename(rename)
            if 'ts' in df.columns and 'rate' in df.columns:
                dfs.append(df.select(['ts', 'rate']))

        if dfs:
            import polars as pl
            combined = pl.concat(dfs).unique(subset=['ts']).sort('ts')
            for row in combined.iter_rows(named=True):
                ts, rate = int(row['ts']), float(row['rate'])
                if rate != 0.0:
                    data[ts] = rate
    except Exception as e:
        print(f"  [warn] funding load failed: {e}")

    print(f"  [funding] {len(data):,} nonzero settlement records loaded")
    return data


def funding_for_trade(open_ms: int, close_ms: int, notional: float,
                      funding_data: dict[int, float]) -> float:
    """Funding cost for a long trade. Long pays when rate > 0."""
    if open_ms >= close_ms or notional <= 0:
        return 0.0
    if close_ms <= FUNDING_REAL_START_MS:
        return 0.0  # pre-funding era

    total = 0.0
    EIGHT_HOURS_MS = 8 * 3600 * 1000
    SETTLEMENT_HOURS = {0, 8, 16}

    # Find first settlement >= open_ms
    open_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    day_start = open_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_ms = int(day_start.timestamp() * 1000)

    ts = day_start_ms
    while ts < close_ms:
        for h in SETTLEMENT_HOURS:
            settlement_ms = ts + h * 3600 * 1000
            if open_ms <= settlement_ms < close_ms:
                rate = funding_data.get(settlement_ms)
                if rate is None:
                    # look back up to 3 periods (24h)
                    for lag in range(1, 4):
                        rate = funding_data.get(settlement_ms - lag * EIGHT_HOURS_MS)
                        if rate is not None:
                            break
                if rate is not None:
                    total += notional * rate
        ts += 24 * 3600 * 1000

    return total


def load_trades(trades_path: Path) -> list[dict]:
    """Load trades.csv → list of dicts with numeric fields."""
    trades = []
    if not trades_path.exists():
        return trades
    try:
        with open(trades_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    opened_at = int(float(row.get('opened_at', 0)))
                    closed_at = int(float(row.get('closed_at', 0)))
                    qty = float(row.get('qty', 0) or 0)
                    entry_price = float(row.get('entry_price', 0) or 0)
                    pnl = float(row.get('pnl', 0) or 0)
                    if closed_at > opened_at and qty > 0:
                        trades.append({
                            'opened_at': opened_at,
                            'closed_at': closed_at,
                            'qty': qty,
                            'entry_price': entry_price,
                            'pnl': pnl,
                        })
                except (ValueError, KeyError):
                    continue
    except Exception:
        pass
    return sorted(trades, key=lambda t: t['closed_at'])


def compute_window_stats(
    trades: list[dict],
    funding_data: dict[int, float],
    w_start_ms: int,
    w_end_ms: int,
    w_start_equity: float,
) -> dict:
    """Compute per-window stats from trades whose closed_at ∈ [w_start_ms, w_end_ms)."""
    window_trades = [t for t in trades if w_start_ms <= t['closed_at'] < w_end_ms]

    if not window_trades:
        return {
            'cagr_raw': 0.0, 'cagr_adj': 0.0,
            'mdd': 0.0, 'trades': 0,
            'return_pct_raw': 0.0, 'return_pct_adj': 0.0,
        }

    duration_years = (w_end_ms - w_start_ms) / (1000 * 86400 * 365.25)

    # Build per-window equity curve (raw and adjusted)
    equity_raw = w_start_equity
    equity_adj = w_start_equity
    peak_raw = equity_raw
    peak_adj = equity_adj
    mdd_raw = 0.0
    mdd_adj = 0.0

    for t in window_trades:
        # Use pre-computed adjusted PnL if available (set by process_combo); else compute
        pnl_adj = t.get('pnl_adj')
        if pnl_adj is None:
            notional = t['qty'] * t['entry_price']
            fc = funding_for_trade(t['opened_at'], t['closed_at'], notional, funding_data)
            pnl_adj = t['pnl'] - fc

        equity_raw += t['pnl']
        equity_adj += pnl_adj

        peak_raw = max(peak_raw, equity_raw)
        peak_adj = max(peak_adj, equity_adj)

        dd_raw = (equity_raw - peak_raw) / peak_raw * 100 if peak_raw > 0 else 0
        dd_adj = (equity_adj - peak_adj) / peak_adj * 100 if peak_adj > 0 else 0

        if dd_raw < mdd_raw:
            mdd_raw = dd_raw
        if dd_adj < mdd_adj:
            mdd_adj = dd_adj

    ret_raw = (equity_raw - w_start_equity) / w_start_equity if w_start_equity > 0 else 0
    ret_adj = (equity_adj - w_start_equity) / w_start_equity if w_start_equity > 0 else 0

    def annualize(simple_ret: float, years: float) -> float:
        if years <= 0:
            return simple_ret * 100
        if 1 + simple_ret <= 0:
            return -100.0
        return ((1 + simple_ret) ** (1 / years) - 1) * 100

    return {
        'cagr_raw': annualize(ret_raw, duration_years),
        'cagr_adj': annualize(ret_adj, duration_years),
        'mdd': mdd_adj,
        'trades': len(window_trades),
        'return_pct_raw': ret_raw * 100,
        'return_pct_adj': ret_adj * 100,
    }


def compute_composite(window_stats: list[dict]) -> tuple[float, bool, dict]:
    """Compute composite score, hard_pass, and summary metrics."""
    cagrs = [w['cagr_adj'] for w in window_stats]
    mdds  = [w['mdd'] for w in window_stats]
    trades_per_window = [w['trades'] for w in window_stats]

    n_positive  = sum(1 for c in cagrs if c > 0)
    worst_mdd   = min(mdds)
    total_trades = sum(trades_per_window)
    min_trades  = min(trades_per_window)
    mean_cagr   = mean(cagrs)
    cagr_std    = stdev(cagrs) if len(cagrs) > 1 else 0.0
    mean_mdd    = mean(mdds)

    hard_pass = (
        n_positive >= 7 and
        worst_mdd >= -45.0 and
        total_trades >= 100 and
        min_trades >= 3
    )

    # MDD factor (§2-2)
    if worst_mdd > -35:
        mdd_factor = 1.0 + (worst_mdd + 35) / 100
    else:
        mdd_factor = 1.0 + (worst_mdd + 35) / 50

    composite = mean_cagr * mdd_factor

    summary = {
        'n_positive': n_positive,
        'mean_cagr_adj': mean_cagr,
        'cagr_std': cagr_std,
        'worst_mdd': worst_mdd,
        'mean_mdd': mean_mdd,
        'total_trades': total_trades,
        'min_trades': min_trades,
        'mdd_factor': mdd_factor,
        'composite': composite,
        'hard_pass': hard_pass,
    }
    return composite, hard_pass, summary


def process_combo(combo: dict, output_dir: Path, funding_data: dict) -> dict | None:
    """Full pipeline for one combo: load trades → 10-window slice → composite."""
    combo_id = combo['combo_id']
    trades_path = output_dir / f"combo_{combo_id}" / 'trades.csv'
    trades = load_trades(trades_path)

    if not trades:
        return None

    # Precompute funding-adjusted PnL and cumulative equity per trade
    # equity_before[i] = equity just before trade i closes
    equity = 10000.0
    trade_pnl_adj = []
    equity_before_trade = []  # equity before each trade's close
    for t in trades:
        notional = t['qty'] * t['entry_price']
        fc = funding_for_trade(t['opened_at'], t['closed_at'], notional, funding_data)
        pnl_adj = t['pnl'] - fc
        equity_before_trade.append(equity)
        equity += pnl_adj
        trade_pnl_adj.append(pnl_adj)
    # Patch trades with adjusted PnL
    for i, t in enumerate(trades):
        t['pnl_adj'] = trade_pnl_adj[i]

    def get_equity_at(ts_ms: int) -> float:
        """Equity just before the first trade closing at or after ts_ms."""
        for i, t in enumerate(trades):
            if t['closed_at'] >= ts_ms:
                return equity_before_trade[i]
        # All trades closed before ts_ms — return final equity
        return equity

    window_stats = []
    for w_name, w_start, w_end in WINDOWS_10:
        w_start_ms = _ts_ms(w_start)
        w_end_ms   = _ts_ms(w_end)
        w_start_eq = get_equity_at(w_start_ms)
        ws = compute_window_stats(trades, funding_data, w_start_ms, w_end_ms, w_start_eq)
        ws['window'] = w_name
        window_stats.append(ws)

    composite, hard_pass, summary = compute_composite(window_stats)

    return {
        'combo_id':           combo_id,
        'st_factor':          combo['st_factor'],
        'st_period':          combo['st_period'],
        'fast_ema_len':       combo['fast_ema_len'],
        'slow_ema_len':       combo['slow_ema_len'],
        'direction_ema_len':  combo['direction_ema_len'],
        'atr_mult':           combo['atr_mult'],
        'sl_margin_pct':      combo['sl_margin_pct'],
        # Full-period raw metrics (from DB)
        'cagr_raw_full':      combo.get('cagr'),
        'mdd_full':           combo.get('mdd'),
        'sharpe_full':        combo.get('sharpe'),
        'multiplier_full':    combo.get('multiplier'),
        'trades_full':        combo.get('trades'),
        # 10-window composite
        **summary,
        # Per-window JSON
        'window_stats_json': json.dumps(window_stats),
    }


def write_outputs(results: list[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # v3_all_combos.csv
    csv_path = output_dir / 'v3_all_combos.csv'
    fieldnames = [
        'combo_id', 'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
        'direction_ema_len', 'atr_mult', 'sl_margin_pct',
        'cagr_raw_full', 'mdd_full', 'sharpe_full', 'multiplier_full', 'trades_full',
        'n_positive', 'mean_cagr_adj', 'cagr_std', 'worst_mdd', 'mean_mdd',
        'total_trades', 'min_trades', 'mdd_factor', 'composite', 'hard_pass',
        'window_stats_json',
    ]
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({k: r.get(k, '') for k in fieldnames} for r in results)
    print(f"Written: {csv_path} ({len(results)} rows)")

    # v3_winners.csv (hard_pass, sorted by composite)
    winners = sorted([r for r in results if r.get('hard_pass')],
                     key=lambda r: r.get('composite', -999), reverse=True)
    winners_path = output_dir / 'v3_winners.csv'
    with open(winners_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({k: r.get(k, '') for k in fieldnames} for r in winners)
    print(f"Winners (hard_pass): {winners_path} ({len(winners)} rows)")

    # v3_summary.md
    all_sorted = sorted(results, key=lambda r: r.get('composite', -999), reverse=True)
    lines = [
        '# v3 Fine-Grid 최적화 집계 결과',
        '',
        f'총 {len(results)}개 combo 완료 | hard_pass: {len(winners)}개',
        '',
        '> MDD 기준: 3x 레버리지 적용 계좌 잔고(equity) 기준 구간별 최대낙폭',
        '> 펀딩비: 2019-09-10 이전 구간(W01-W03 일부) funding=0 처리 (과대평가 한계)',
        '',
        '## Hard-Pass 상위 20 (composite 정렬)',
        '',
        '| 순위 | id | st_f | st_p | fe | se | de | atr | comp | n_pos | mean_cagr_adj% | worst_mdd% | trades |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    pass_only = [r for r in all_sorted if r.get('hard_pass')]
    for rank, r in enumerate(pass_only[:20], 1):
        lines.append(
            f"| {rank} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
            f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
            f"{r['atr_mult']} | {r.get('composite', 0):.2f} | "
            f"{r.get('n_positive', 0)}/10 | {r.get('mean_cagr_adj', 0):.1f} | "
            f"{r.get('worst_mdd', 0):.1f} | {r.get('total_trades', 0)} |"
        )

    lines += ['', '## 전체 Top-20 (composite 정렬, hard_pass 무관)', '',
              '| 순위 | id | st_f | st_p | fe | se | de | atr | comp | n_pos | hard_pass |',
              '|---|---|---|---|---|---|---|---|---|---|---|']
    for rank, r in enumerate(all_sorted[:20], 1):
        hp = '✅' if r.get('hard_pass') else '❌'
        lines.append(
            f"| {rank} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
            f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
            f"{r['atr_mult']} | {r.get('composite', 0):.2f} | "
            f"{r.get('n_positive', 0)}/10 | {hp} |"
        )

    lines += ['', '## 통계', '']
    if results:
        composites = [r.get('composite', 0) for r in results]
        n_pos_counts = [r.get('n_positive', 0) for r in results]
        lines.append(f"- composite 최대: {max(composites):.2f} / 최소: {min(composites):.2f} / 평균: {mean(composites):.2f}")
        lines.append(f"- n_positive 7+ (hard_pass 기초): {sum(1 for n in n_pos_counts if n >= 7)}/{len(results)}")
        lines.append(f"- hard_pass 전체: {len(winners)}/{len(results)} ({len(winners)/len(results)*100:.1f}%)")

    md_path = output_dir / 'v3_summary.md'
    md_path.write_text('\n'.join(lines) + '\n')
    print(f"Summary: {md_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v3_optimization/queue.sqlite3')
    p.add_argument('--output-dir', type=str, default='/result/v3_optimization')
    args = p.parse_args()

    db_path = args.db
    output_dir = Path(args.output_dir)

    # Load combos from DB
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    combos = conn.execute(
        "SELECT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct, cagr, mdd, sharpe, trades, multiplier "
        "FROM jobs WHERE status='done' ORDER BY combo_id"
    ).fetchall()
    conn.close()
    combos = [dict(c) for c in combos]
    print(f"Processing {len(combos)} done combos...")

    funding_data = load_funding_data()

    results = []
    failed = 0
    for i, combo in enumerate(combos):
        r = process_combo(combo, output_dir, funding_data)
        if r:
            results.append(r)
        else:
            failed += 1
        if (i + 1) % 100 == 0:
            print(f"  Processed {i+1}/{len(combos)} (failed={failed})", flush=True)

    print(f"\nDone: {len(results)} processed, {failed} no-trades")

    if results:
        write_outputs(results, output_dir)

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
