#!/usr/bin/env python3
"""
Post-hoc maker fee adjustment for V4 backtest results.

Bybit USDT Perpetual:
  taker (market order) : 0.055%  ← Jesse default
  maker (limit order)  : 0.020%  ← Bybit 지정가 수수료

For each completed backtest directory:
  1. Recalculates per-trade fee and net PnL in trades.csv
  2. Rebuilds monthly_returns.csv from new per-trade PnL
  3. Updates stats.json (fee, net_profit, finishing_balance, CAGR,
     gross P&L, profit_factor, win_rate, expectancy …)
  4. Sharpe/MDD re-derived from monthly equity simulation (approximation)
     because daily equity curves are not persisted

Usage:
    python apply_maker_fee.py [--dry-run] [--dir /path/to/result/root]
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

OLD_FEE_RATE = 0.00055   # Bybit taker
NEW_FEE_RATE = 0.0002    # Bybit maker (limit order)
FEE_RATIO    = NEW_FEE_RATE / OLD_FEE_RATE   # ≈ 0.3636

DEFAULT_RESULT_DIR = Path(
    '/home/justant/Data/Bit-Mania/cryptoengine/backtest-results/data/9-strategies'
)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _monthly_sharpe_mdd(monthly: dict[str, float], starting: float) -> tuple[float, float]:
    """Compute annualised Sharpe (√12) and MDD from monthly PnL dict.
    Returns (sharpe, mdd_pct).  mdd_pct is negative (e.g. -35.2).
    """
    rets: list[float] = []
    balance = starting
    peak = starting
    mdd = 0.0

    for month in sorted(monthly):
        pnl = monthly[month]
        if balance > 0:
            rets.append(pnl / balance)
        balance += pnl
        if balance > peak:
            peak = balance
        dd = (balance - peak) / peak * 100 if peak > 0 else 0.0
        if dd < mdd:
            mdd = dd

    if len(rets) < 2:
        return 0.0, mdd

    n = len(rets)
    mean = sum(rets) / n
    var  = sum((r - mean) ** 2 for r in rets) / (n - 1)
    std  = math.sqrt(var) if var > 0 else 0.0
    sharpe = (mean / std) * math.sqrt(12) if std > 0 else 0.0
    return sharpe, mdd


def _streaks(pnl_seq: list[float]) -> tuple[int, int]:
    """Return (max_winning_streak, max_losing_streak)."""
    win_streak = los_streak = cur = 0
    prev_win = None
    for p in pnl_seq:
        is_win = p > 0
        if is_win == prev_win:
            cur += 1
        else:
            cur = 1
        if is_win:
            win_streak = max(win_streak, cur)
        else:
            los_streak = max(los_streak, cur)
        prev_win = is_win
    return win_streak, los_streak


# ─── core ────────────────────────────────────────────────────────────────────

def adjust_result_dir(path: Path, dry_run: bool = False) -> dict | None:
    trades_p  = path / 'trades.csv'
    stats_p   = path / 'stats.json'
    monthly_p = path / 'monthly_returns.csv'
    marker    = path / 'EXECUTION_SUCCESS.marker'

    if not (marker.exists() and trades_p.exists() and stats_p.exists()):
        return None

    # Load existing stats for fields we can't recompute
    stats = json.loads(stats_p.read_text())
    raw   = stats.get('raw_metrics', {})
    starting = float(raw.get('starting_balance', 10_000.0))

    # ── 1. Adjust per-trade fee / pnl ───────────────────────────────────────
    trades: list[dict] = []
    with open(trades_p, newline='') as fh:
        for row in csv.DictReader(fh):
            old_fee = float(row['fee'])
            old_pnl = float(row['pnl'])
            new_fee = old_fee * FEE_RATIO
            new_pnl = old_pnl + (old_fee - new_fee)
            trades.append({
                'opened_at':   row['opened_at'],
                'closed_at':   row['closed_at'],
                'side':        row['side'],
                'entry_price': row['entry_price'],
                'exit_price':  row['exit_price'],
                'qty':         row['qty'],
                'pnl':         new_pnl,
                'fee':         new_fee,
                '_old_pnl':    old_pnl,
                '_old_fee':    old_fee,
            })

    if not trades:
        return None

    # ── 2. Monthly returns ───────────────────────────────────────────────────
    monthly: dict[str, float] = defaultdict(float)
    for t in trades:
        ts = int(float(t['closed_at']))
        month = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m')
        monthly[month] += t['pnl']

    # ── 3. Aggregate stats ───────────────────────────────────────────────────
    total_new_fee = sum(t['fee'] for t in trades)
    total_old_fee = sum(t['_old_fee'] for t in trades)
    fee_saving    = total_old_fee - total_new_fee

    longs  = [t for t in trades if t['side'] == 'long']
    shorts = [t for t in trades if t['side'] == 'short']
    winning = [t for t in trades if t['pnl'] > 0]
    losing  = [t for t in trades if t['pnl'] <= 0]
    win_longs  = [t for t in longs  if t['pnl'] > 0]
    win_shorts = [t for t in shorts if t['pnl'] > 0]

    gross_profit = sum(t['pnl'] for t in winning)
    gross_loss   = sum(t['pnl'] for t in losing)
    net_profit   = gross_profit + gross_loss
    finishing    = starting + net_profit

    win_rate       = len(winning) / len(trades) if trades else 0.0
    win_rate_longs = len(win_longs)  / len(longs)  if longs  else 0.0
    win_rate_short = len(win_shorts) / len(shorts) if shorts else 0.0

    avg_win  = gross_profit / len(winning) if winning else 0.0
    avg_loss = gross_loss   / len(losing)  if losing  else 0.0
    ratio_avg_win_loss = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0
    profit_factor = gross_profit / abs(gross_loss) if gross_loss != 0 else float('inf')

    expectancy     = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
    expectancy_pct = expectancy / starting * 100 if starting else 0.0
    exp_per_100    = expectancy  # same unit

    net_profit_pct = net_profit / starting * 100 if starting else 0.0

    start_str = stats.get('start', '2021-04-01')
    end_str   = stats.get('end',   '2026-04-30')
    start_dt  = datetime.strptime(start_str, '%Y-%m-%d')
    end_dt    = datetime.strptime(end_str,   '%Y-%m-%d')
    years     = (end_dt - start_dt).days / 365.25
    if finishing > 0 and years > 0:
        cagr = ((finishing / starting) ** (1.0 / years) - 1.0) * 100.0
    else:
        cagr = 0.0

    largest_win  = max((t['pnl'] for t in winning), default=0.0)
    largest_loss = min((t['pnl'] for t in losing),  default=0.0)

    win_st, los_st = _streaks([t['pnl'] for t in trades])

    # Sharpe & MDD from monthly simulation (approximation)
    sharpe, mdd_pct = _monthly_sharpe_mdd(monthly, starting)
    calmar = cagr / abs(mdd_pct) if mdd_pct != 0 else 0.0

    old_finishing = float(raw.get('finishing_balance', starting))

    summary = {
        'dir':           str(path),
        'old_fee':       total_old_fee,
        'new_fee':       total_new_fee,
        'fee_saving':    fee_saving,
        'old_balance':   old_finishing,
        'new_balance':   finishing,
        'old_cagr':      stats.get('cagr_pct', 0.0),
        'new_cagr':      cagr,
    }

    if dry_run:
        return summary

    # ── 4. Write trades.csv ─────────────────────────────────────────────────
    FIELDS = ['opened_at', 'closed_at', 'side', 'entry_price',
              'exit_price', 'qty', 'pnl', 'fee']
    with open(trades_p, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for t in trades:
            w.writerow({k: t[k] for k in FIELDS})

    # ── 5. Write monthly_returns.csv ────────────────────────────────────────
    with open(monthly_p, 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['month', 'pnl_usdt'])
        for month in sorted(monthly):
            w.writerow([month, monthly[month]])

    # ── 6. Update stats.json ────────────────────────────────────────────────
    stats['annual_return_pct'] = cagr
    stats['cagr_pct']          = cagr
    stats['sharpe_ratio']      = sharpe
    stats['max_drawdown_pct']  = mdd_pct
    stats['win_rate_pct']      = win_rate * 100.0
    stats['profit_factor']     = profit_factor
    stats['gross_profit']      = gross_profit
    stats['gross_loss']        = gross_loss
    stats['net_profit_pct']    = net_profit_pct

    raw['fee']                    = total_new_fee
    raw['net_profit']             = net_profit
    raw['net_profit_percentage']  = net_profit_pct
    raw['finishing_balance']      = finishing
    raw['gross_profit']           = gross_profit
    raw['gross_loss']             = gross_loss
    raw['profit_factor']          = profit_factor
    raw['win_rate']               = win_rate
    raw['win_rate_longs']         = win_rate_longs
    raw['win_rate_shorts']        = win_rate_short
    raw['ratio_avg_win_loss']     = ratio_avg_win_loss
    raw['total_winning_trades']   = len(winning)
    raw['total_losing_trades']    = len(losing)
    raw['average_win']            = avg_win
    raw['average_loss']           = avg_loss
    raw['expectancy']             = expectancy
    raw['expectancy_percentage']  = expectancy_pct
    raw['expected_net_profit_every_100_trades'] = exp_per_100
    raw['largest_winning_trade']  = largest_win
    raw['largest_losing_trade']   = largest_loss
    raw['winning_streak']         = win_st
    raw['losing_streak']          = los_st
    raw['annual_return']          = net_profit   # absolute amount
    raw['sharpe_ratio']           = sharpe
    raw['calmar_ratio']           = calmar

    stats_p.write_text(json.dumps(stats, indent=2, ensure_ascii=False))

    return summary


# ─── main ────────────────────────────────────────────────────────────────────

def collect_result_dirs(root: Path) -> list[Path]:
    """Find all leaf result dirs (have EXECUTION_SUCCESS.marker)."""
    return sorted(p.parent for p in root.rglob('EXECUTION_SUCCESS.marker'))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Preview only, no file writes')
    ap.add_argument('--dir', type=Path, default=DEFAULT_RESULT_DIR, help='Result root dir')
    args = ap.parse_args()

    dirs = collect_result_dirs(args.dir)
    print(f'Found {len(dirs)} completed result directories')
    print(f'Fee adjustment: {OLD_FEE_RATE*100:.4f}% (taker) → {NEW_FEE_RATE*100:.4f}% (maker, Bybit 지정가)')
    if args.dry_run:
        print('DRY RUN — no files modified\n')

    total_old_fee = total_new_fee = total_saving = 0.0
    old_bals: list[float] = []
    new_bals: list[float] = []
    skipped = 0
    updated = 0

    for d in dirs:
        r = adjust_result_dir(d, dry_run=args.dry_run)
        if r is None:
            skipped += 1
            continue
        updated += 1
        total_old_fee += r['old_fee']
        total_new_fee += r['new_fee']
        total_saving  += r['fee_saving']
        old_bals.append(r['old_balance'])
        new_bals.append(r['new_balance'])
        label = str(d).replace(str(args.dir) + '/', '')
        print(f'  {"(dry)" if args.dry_run else "OK   "} {label:<55} '
              f'${r["old_balance"]:>9,.0f} → ${r["new_balance"]:>9,.0f} '
              f'(fee saved: ${r["fee_saving"]:,.0f})')

    print()
    print(f'--- Summary {"(DRY RUN)" if args.dry_run else ""} ---')
    print(f'  Updated   : {updated}')
    print(f'  Skipped   : {skipped}')
    print(f'  Total fee  (old): ${total_old_fee:,.0f}')
    print(f'  Total fee  (new): ${total_new_fee:,.0f}')
    print(f'  Total saved     : ${total_saving:,.0f}')
    if old_bals:
        avg_old = sum(old_bals) / len(old_bals)
        avg_new = sum(new_bals) / len(new_bals)
        print(f'  Avg final balance (old): ${avg_old:,.0f}')
        print(f'  Avg final balance (new): ${avg_new:,.0f}')


if __name__ == '__main__':
    main()
