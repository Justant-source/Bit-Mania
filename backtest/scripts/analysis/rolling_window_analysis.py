#!/usr/bin/env python3
"""
Rolling Window Analysis — 7-strategies 구간별 누적 수익 평가

7개 전략 × {long_only, bidirectional} × {1h, 4h, 1D} 조합을
3~63개월 rolling window (3개월 분기 시프트)로 슬라이싱하여,
어떤 구간에서도 양의 순수익을 내지 못한 전략을 식별합니다.

Usage:
    python rolling_window_analysis.py                # 기본 실행
    python rolling_window_analysis.py --skip-plots   # heatmap 생략
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore', message='Converting to PeriodArray')

# ─── Paths ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from _paths import RESULTS_ROOT

RESULT_DIR = RESULTS_ROOT / '7-strategies'
OUT_DIR    = RESULT_DIR / 'rolling_window'

# ─── Parameters ───────────────────────────────────────────────────────────────
TIMEFRAMES  = ['1h', '4h', '1D']
STRATEGIES  = [
    'stoch', 'momentum_ma', 'supertrend',
    'tradeiq_psar_ha', 'trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce',
]
VARIANTS_1X = ['bidirectional', 'long_only']
WINDOW_SIZES_DEFAULT = list(range(3, 64, 3))  # [3,6,9,...,63] — 21가지
WINDOW_STEP_DEFAULT  = 3                       # 분기 단위 시프트
BACKTEST_START = '2021-01'
BACKTEST_END   = '2026-04'
FAIL_MEANINGFUL = -100.0  # $ (10k 기준 1%)


# ─── Parameters ───────────────────────────────────────────────────────────────

STARTING_BALANCE = 10_000.0
MARGIN_CAP_RATIO = 0.95   # 거래당 사용 가능한 잔고 최대 비율 (잔고의 95%까지 margin)
LIQUIDATION_LEVEL = 0.05  # 잔고가 starting의 5% 이하로 떨어지면 청산


# ─── Data loading ─────────────────────────────────────────────────────────────

def load_trades(strategy: str, variant: str, tf: str) -> pd.DataFrame | None:
    path = RESULT_DIR / strategy / tf / variant / 'trades.csv'
    if not path.exists():
        return None
    df = pd.read_csv(path)
    df['closed_at_ms'] = df['closed_at'].astype('int64')
    df['opened_at_ms'] = df['opened_at'].astype('int64')
    df['close_month'] = pd.to_datetime(df['closed_at_ms'], unit='ms', utc=True).dt.to_period('M')
    df['open_month']  = pd.to_datetime(df['opened_at_ms'], unit='ms', utc=True).dt.to_period('M')
    return df


def load_stats(strategy: str, variant: str, tf: str) -> dict | None:
    path = RESULT_DIR / strategy / tf / variant / 'stats.json'
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def load_leverage(strategy: str, variant: str, tf: str) -> int:
    st = load_stats(strategy, variant, tf)
    if st and st.get('leverage') is not None:
        return int(st['leverage'])
    return 1


def load_stats_net_profit(strategy: str, variant: str, tf: str) -> float | None:
    st = load_stats(strategy, variant, tf)
    return st.get('raw_metrics', {}).get('net_profit') if st else None


# ─── Virtual balance simulation ───────────────────────────────────────────────

def attach_trade_metadata(trades: pd.DataFrame, leverage: int) -> pd.DataFrame:
    """
    Trade마다 raw equity at open과 raw margin을 미리 계산.

    raw_eq_open[i] = trade i의 opened_at 시점, 그 이전에 closed된 trades의
                     누적 pnl + STARTING_BALANCE (LOCF).
    raw_margin[i]  = qty * entry_price / leverage
    """
    df = trades.copy()
    df['raw_notional'] = df['qty'] * df['entry_price']
    df['raw_margin']   = df['raw_notional'] / max(leverage, 1)

    closed_sorted = df.sort_values('closed_at_ms')
    closed_ms = closed_sorted['closed_at_ms'].values
    cum_pnl   = closed_sorted['pnl'].cumsum().values

    open_ms = df['opened_at_ms'].values
    idx = np.searchsorted(closed_ms, open_ms, side='right') - 1
    eq_open = np.where(
        idx < 0,
        STARTING_BALANCE,
        STARTING_BALANCE + np.take(cum_pnl, np.clip(idx, 0, len(cum_pnl) - 1)),
    )
    df['raw_eq_open'] = eq_open
    return df


def simulate_balance_capped_pnl(
    win_trades: pd.DataFrame,
    cap: float = MARGIN_CAP_RATIO,
    starting: float = STARTING_BALANCE,
) -> tuple[float, int, bool]:
    """
    Window 내 trades를 시간순으로 시뮬레이션. virtual_equity 추적.

    각 trade:
      size_ratio = raw_margin / raw_eq_open  (raw 백테스트에서 잔고 대비 진입 비율)
      capped     = min(size_ratio, cap)      (잔고의 cap%까지만 사용)
      v_margin   = virtual_equity * capped
      v_pnl      = v_margin * (raw_pnl / raw_margin)   # raw ROI 유지
      virtual_equity += v_pnl

    반환: (net_pnl, n_used, liquidated)
    """
    sorted_trades = win_trades.sort_values('opened_at_ms')
    v_eq = float(starting)
    used = 0
    liquidated = False

    for t in sorted_trades.itertuples(index=False):
        if t.raw_eq_open <= 0 or t.raw_margin <= 0:
            continue
        size_ratio = t.raw_margin / t.raw_eq_open
        capped     = min(size_ratio, cap)
        raw_roi    = t.pnl / t.raw_margin
        v_eq      += v_eq * capped * raw_roi
        used      += 1
        if v_eq <= starting * LIQUIDATION_LEVEL:
            v_eq = 0.0
            liquidated = True
            break

    return v_eq - starting, used, liquidated


def monthly_pnl_series(trades: pd.DataFrame, months: pd.PeriodIndex) -> pd.Series:
    """closed_at 기준 월별 raw pnl 합. validate_full_period 에서만 사용."""
    return (
        trades.groupby('close_month')['pnl']
        .sum()
        .reindex(months, fill_value=0.0)
    )


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_full_period(label: str, trades: pd.DataFrame, months: pd.PeriodIndex) -> None:
    """전체기간 raw pnl 합 vs stats.json net_profit 일치 검증 (raw 데이터 무결성)."""
    strategy, tf, variant = label.split('/')
    expected = load_stats_net_profit(strategy, variant, tf)
    if expected is None:
        return
    actual = float(monthly_pnl_series(trades, months).sum())
    diff = abs(actual - float(expected))
    if diff > 2.0:
        print(
            f"  [WARN] {label}: raw net_profit mismatch "
            f"actual={actual:.2f} expected={expected:.2f} diff={diff:.2f}",
            file=sys.stderr,
        )


# ─── Core analysis ────────────────────────────────────────────────────────────

def build_windows_detail(
    variants: list[str],
    timeframes: list[str],
    window_sizes: list[int],
    start: str,
    end: str,
    cap: float = MARGIN_CAP_RATIO,
    step: int = WINDOW_STEP_DEFAULT,
) -> pd.DataFrame:
    """
    각 (strategy, variant, tf, window_size, start_month) 조합에 대해
    잔고 시뮬레이션을 실행하여 net_pnl 산출.

    시뮬레이션 가정:
      - window 시작 시점에 STARTING_BALANCE ($10,000)을 새로 투입
      - strict filter: opened_at >= window_start AND closed_at <= window_end
      - 각 trade는 현재 잔고의 min(raw_size_ratio, cap) 만큼을 margin으로 사용
      - ROI%는 raw 백테스트와 동일, 절대 PnL은 잔고 기반으로 재계산
    """
    months = pd.period_range(start=start, end=end, freq='M')

    # 윈도우 경계 ms 미리 계산
    month_start_ms = np.array([
        int(p.start_time.tz_localize('UTC').timestamp() * 1000) for p in months
    ])
    month_end_ms = np.array([
        int(p.end_time.tz_localize('UTC').timestamp() * 1000) for p in months
    ])

    all_rows: list[pd.DataFrame] = []

    for strategy in STRATEGIES:
        for variant in variants:
            if '_x2' in variant or '_x3' in variant:
                print(f"  [WARN] 레버리지 variant 제외: {variant}", file=sys.stderr)
                continue
            for tf in timeframes:
                trades = load_trades(strategy, variant, tf)
                if trades is None:
                    print(f"  skip (no trades.csv): {strategy}/{tf}/{variant}", file=sys.stderr)
                    continue

                validate_full_period(f"{strategy}/{tf}/{variant}", trades, months)

                leverage = load_leverage(strategy, variant, tf)
                trades   = attach_trade_metadata(trades, leverage)
                trades_op_ms = trades['opened_at_ms'].values
                trades_cl_ms = trades['closed_at_ms'].values

                rows: list[dict] = []
                for size in window_sizes:
                    for i in range(size - 1, len(months), step):
                        s_idx = i - size + 1
                        s_ms  = int(month_start_ms[s_idx])
                        e_ms  = int(month_end_ms[i])

                        mask = (trades_op_ms >= s_ms) & (trades_cl_ms <= e_ms)
                        win_trades = trades.loc[mask]

                        if len(win_trades) == 0:
                            net_pnl, n_used, liq = 0.0, 0, False
                        else:
                            net_pnl, n_used, liq = simulate_balance_capped_pnl(
                                win_trades, cap=cap,
                            )

                        net_pnl = round(net_pnl, 4)
                        rows.append({
                            'strategy':           strategy,
                            'variant':            variant,
                            'timeframe':          tf,
                            'window_size_months': size,
                            'start_month':        str(months[s_idx]),
                            'end_month':          str(months[i]),
                            'net_pnl':            net_pnl,
                            'final_equity':       round(STARTING_BALANCE + net_pnl, 2),
                            'n_trades':           int(len(win_trades)),
                            'n_trades_used':      n_used,
                            'liquidated':         bool(liq),
                            'fail_strict':        net_pnl <= 0,
                            'fail_meaningful':    net_pnl < FAIL_MEANINGFUL,
                        })

                all_rows.append(pd.DataFrame(rows))

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def summarize_combo(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for (strategy, variant, tf), grp in detail.groupby(
        ['strategy', 'variant', 'timeframe'], sort=False
    ):
        worst_idx = grp['net_pnl'].idxmin()
        best_idx  = grp['net_pnl'].idxmax()
        exp = load_stats_net_profit(strategy, variant, tf)
        rows.append({
            'strategy':               strategy,
            'variant':                variant,
            'timeframe':              tf,
            'total_windows':          len(grp),
            'fail_strict_count':      int(grp['fail_strict'].sum()),
            'fail_strict_ratio':      round(float(grp['fail_strict'].mean()), 4),
            'fail_meaningful_count':  int(grp['fail_meaningful'].sum()),
            'any_positive_window':    bool((grp['net_pnl'] > 0).any()),
            'worst_window_pnl':       round(float(grp.loc[worst_idx, 'net_pnl']), 2),
            'worst_window_start':     grp.loc[worst_idx, 'start_month'],
            'worst_window_size':      int(grp.loc[worst_idx, 'window_size_months']),
            'best_window_pnl':        round(float(grp.loc[best_idx, 'net_pnl']), 2),
            'best_window_start':      grp.loc[best_idx, 'start_month'],
            'best_window_size':       int(grp.loc[best_idx, 'window_size_months']),
            'full_period_pnl':        round(float(exp), 2) if exp is not None else None,
        })
    return pd.DataFrame(rows)


def summarize_strategy(combo: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for strategy, grp in combo.groupby('strategy', sort=False):
        total         = len(grp)
        never_pos     = int((~grp['any_positive_window']).sum())
        has_pos       = int(grp['any_positive_window'].sum())
        verdict       = 'DELETE' if never_pos == total else ('WEAK' if never_pos > 0 else 'KEEP')
        worst_row     = grp.sort_values('fail_strict_ratio', ascending=False).iloc[0]
        best_row      = grp.sort_values('fail_strict_ratio').iloc[0]
        rows.append({
            'strategy':               strategy,
            'verdict':                verdict,
            'total_combos':           total,
            'combos_any_positive':    has_pos,
            'combos_never_positive':  never_pos,
            'avg_fail_strict_ratio':  round(float(grp['fail_strict_ratio'].mean()), 4),
            'worst_combo':            f"{worst_row['variant']}/{worst_row['timeframe']}",
            'best_combo':             f"{best_row['variant']}/{best_row['timeframe']}",
        })
    return pd.DataFrame(rows)


# ─── Reporting ────────────────────────────────────────────────────────────────

def render_markdown(combo: pd.DataFrame, strategy_summary: pd.DataFrame, window_sizes: list[int]) -> str:
    lines: list[str] = []
    lines += [
        '# Rolling Window 분석 결과',
        '',
        f'- 데이터 범위: {BACKTEST_START} ~ {BACKTEST_END}',
        f'- Window sizes: {window_sizes} 개월 (월별 시프트)',
        f'- 대상 조합: {len(combo)} 개',
        f'- 실패 기준: `fail_strict` = 구간 누적 PnL ≤ $0  |  `fail_meaningful` = < ${FAIL_MEANINGFUL:,.0f}',
        '',
    ]

    # Tier 1: strategy verdict
    lines += [
        '## Tier 1 — Strategy 종합 판정',
        '',
        '| Strategy | 판정 | 조합 수 | 양수 구간 有 | 양수 구간 無 | avg fail% | 최악 조합 | 최선 조합 |',
        '|---|---|---|---|---|---|---|---|',
    ]
    v_label = {'DELETE': '🗑 DELETE', 'WEAK': '⚠ WEAK', 'KEEP': '✅ KEEP'}
    for _, r in strategy_summary.iterrows():
        lines.append(
            f"| `{r['strategy']}` | {v_label.get(r['verdict'], r['verdict'])} "
            f"| {r['total_combos']} | {r['combos_any_positive']} | {r['combos_never_positive']} "
            f"| {r['avg_fail_strict_ratio']:.1%} | {r['worst_combo']} | {r['best_combo']} |"
        )
    lines.append('')

    # Tier 2: matrix
    lines += [
        '## Tier 2 — (variant, TF) 조합별 "양수 구간 존재" 매트릭스',
        '',
        '`✓` = 어느 구간에서든 최소 1회 양수 수익  `✗` = 모든 구간에서 손실 또는 0',
        '',
    ]
    pivot = combo.pivot_table(
        index='strategy', columns=['variant', 'timeframe'],
        values='any_positive_window', aggfunc='first',
    )
    cols = [f"{v}/{tf}" for v, tf in pivot.columns]
    lines.append('| Strategy | ' + ' | '.join(cols) + ' |')
    lines.append('|---|' + '---|' * len(cols))
    for strat, row in pivot.iterrows():
        cells = ['✓' if v else '✗' for v in row]
        lines.append(f"| `{strat}` | " + ' | '.join(cells) + ' |')
    lines.append('')

    # Tier 3: detail table (fail_strict_ratio > 0)
    has_fail = combo[combo['fail_strict_ratio'] > 0].sort_values(
        ['strategy', 'fail_strict_ratio'], ascending=[True, False]
    )
    lines += [
        '## Tier 3 — Fail 발생 조합 상세',
        '',
        '| Strategy | Variant | TF | Fail% | Meaningful fail% | 최악 구간 | 최악 PnL | 최선 PnL | 전체기간 PnL |',
        '|---|---|---|---|---|---|---|---|---|',
    ]
    for _, r in has_fail.iterrows():
        mf_ratio = r['fail_meaningful_count'] / r['total_windows']
        fp = f"${r['full_period_pnl']:,.0f}" if r['full_period_pnl'] is not None else 'N/A'
        lines.append(
            f"| `{r['strategy']}` | {r['variant']} | {r['timeframe']} "
            f"| {r['fail_strict_ratio']:.1%} | {mf_ratio:.1%} "
            f"| {r['worst_window_start']}({r['worst_window_size']}m) "
            f"| ${r['worst_window_pnl']:,.0f} | ${r['best_window_pnl']:,.0f} | {fp} |"
        )
    lines.append('')
    return '\n'.join(lines)


def plot_heatmap(
    detail: pd.DataFrame, strategy: str, tf: str, variant: str, out_dir: Path
) -> Path | None:
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
    except ImportError:
        print("  [WARN] matplotlib 없음. heatmap 생략.", file=sys.stderr)
        return None

    grp = detail[
        (detail['strategy'] == strategy) &
        (detail['timeframe'] == tf) &
        (detail['variant']   == variant)
    ].copy()
    if grp.empty:
        return None

    pivot = grp.pivot_table(
        index='window_size_months', columns='start_month',
        values='net_pnl', aggfunc='first',
    ).sort_index()

    vals = pivot.values.astype(float)
    finite = vals[np.isfinite(vals)]
    abs_max = max(abs(finite).max(), 1.0) if len(finite) > 0 else 1.0

    fig, ax = plt.subplots(figsize=(max(16, len(pivot.columns) // 3), 4))
    norm = mcolors.TwoSlopeNorm(vmin=-abs_max, vcenter=0, vmax=abs_max)
    im = ax.imshow(vals, aspect='auto', cmap='RdBu', norm=norm, interpolation='nearest')

    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([f"{s}m" for s in pivot.index])
    x_labels = list(pivot.columns)
    step = max(1, len(x_labels) // 20)
    ax.set_xticks(range(0, len(x_labels), step))
    ax.set_xticklabels(x_labels[::step], rotation=45, ha='right', fontsize=7)

    plt.colorbar(im, ax=ax, label='Net PnL ($)')
    ax.set_title(f'{strategy} / {tf} / {variant}  —  Rolling Window PnL Heatmap')
    ax.set_xlabel('Window start month')
    ax.set_ylabel('Window size (months)')
    plt.tight_layout()

    out_path = out_dir / f'heatmap_{strategy}_{tf}_{variant}.png'
    plt.savefig(out_path, dpi=100)
    plt.close(fig)
    return out_path


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global BACKTEST_START, BACKTEST_END, FAIL_MEANINGFUL

    parser = argparse.ArgumentParser(
        description='7-strategies rolling window PnL analysis',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--variants',             nargs='+', default=VARIANTS_1X)
    parser.add_argument('--timeframes',           nargs='+', default=TIMEFRAMES)
    parser.add_argument('--window-sizes',         nargs='+', type=int, default=WINDOW_SIZES_DEFAULT)
    parser.add_argument('--start',                default=BACKTEST_START)
    parser.add_argument('--end',                  default=BACKTEST_END)
    parser.add_argument('--cap',                  type=float, default=MARGIN_CAP_RATIO,
                        help='거래당 잔고 사용 최대 비율 (0.95 = 잔고의 95%까지 margin)')
    parser.add_argument('--threshold-meaningful', type=float, default=FAIL_MEANINGFUL)
    parser.add_argument('--step-months',          type=int, default=WINDOW_STEP_DEFAULT,
                        help='윈도우 시프트 단위 개월 수 (기본: 3개월, 분기 단위)')
    parser.add_argument('--skip-plots',           action='store_true')
    parser.add_argument('--heatmap-all',          action='store_true', help='fail 여부 무관 모든 조합에 heatmap')
    args = parser.parse_args()

    BACKTEST_START  = args.start
    BACKTEST_END    = args.end
    FAIL_MEANINGFUL = args.threshold_meaningful

    safe_variants = [v for v in args.variants if '_x2' not in v and '_x3' not in v]
    if len(safe_variants) != len(args.variants):
        print(f"[WARN] 레버리지 variants 제외: {set(args.variants) - set(safe_variants)}", file=sys.stderr)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    n_total = len(STRATEGIES) * len(safe_variants) * len(args.timeframes)
    _m = pd.period_range(start=args.start, end=args.end, freq='M')
    win_counts = [max(0, len(range(s - 1, len(_m), args.step_months))) for s in args.window_sizes]
    n_windows  = sum(win_counts)
    print(
        f"[1/5] Building {n_total} combos × {n_windows} windows/combo = "
        f"{n_total * n_windows:,} evaluations..."
    )
    detail = build_windows_detail(
        safe_variants, args.timeframes, args.window_sizes,
        args.start, args.end, cap=args.cap,
        step=args.step_months,
    )
    if detail.empty:
        print("[ERROR] 로드된 데이터 없음. 경로 확인 필요.", file=sys.stderr)
        sys.exit(1)
    print(f"  → {len(detail):,} rows")

    print("[2/5] Summarizing combos...")
    combo_summary = summarize_combo(detail)

    print("[3/5] Summarizing strategies...")
    strategy_summary = summarize_strategy(combo_summary)

    print("[4/5] Writing CSVs...")
    detail.to_csv(OUT_DIR / 'windows_detail.csv', index=False)
    combo_summary.to_csv(OUT_DIR / 'combo_summary.csv', index=False)
    strategy_summary.to_csv(OUT_DIR / 'strategy_verdict.csv', index=False)
    print(f"  → {OUT_DIR}")

    print("[5/5] Rendering Markdown report...")
    md = render_markdown(combo_summary, strategy_summary, args.window_sizes)
    (OUT_DIR / 'strategy_verdict.md').write_text(md, encoding='utf-8')

    if not args.skip_plots:
        target = combo_summary if args.heatmap_all else combo_summary[combo_summary['fail_strict_ratio'] > 0]
        print(f"  Generating heatmaps for {len(target)} combos...")
        for _, r in target.iterrows():
            p = plot_heatmap(detail, r['strategy'], r['timeframe'], r['variant'], OUT_DIR)
            if p:
                print(f"    {p.name}")

    # ─── Console summary ──────────────────────────────────────────────────────
    print('\n' + '=' * 62)
    print('  전략 종합 판정')
    print('=' * 62)
    for _, r in strategy_summary.sort_values('avg_fail_strict_ratio', ascending=False).iterrows():
        bar  = '█' * int(r['avg_fail_strict_ratio'] * 20)
        mark = {'DELETE': '🗑 ', 'WEAK': '⚠ ', 'KEEP': '✅ '}.get(r['verdict'], '   ')
        print(f"  {mark}{r['verdict']:<7}  {r['strategy']:<25}  fail {r['avg_fail_strict_ratio']:5.1%}  {bar}")
    print('=' * 62)
    deletes = strategy_summary[strategy_summary['verdict'] == 'DELETE']['strategy'].tolist()
    weaks   = strategy_summary[strategy_summary['verdict'] == 'WEAK']['strategy'].tolist()
    print(f"\n  DELETE 후보: {deletes or '없음'}")
    print(f"  WEAK   후보: {weaks or '없음'}")
    print(f"\n  전체 결과: {OUT_DIR / 'strategy_verdict.md'}")


if __name__ == '__main__':
    main()
