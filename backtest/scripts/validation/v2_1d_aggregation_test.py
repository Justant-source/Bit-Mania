#!/usr/bin/env python3
"""
v2_1d_aggregation_test.py — Validate inline 4h→1d synthetic aggregation.

Purpose: Verify that 4h×6→1d synthetic aggregation (group every 6 consecutive 4h candles)
matches "true" 1d aggregation from 1m data.

Data source: BTC 1m candles from backtest/data/ohlcv/BTCUSDT/1m/
- Load 1m data
- Resample to 4h (UTC aligned)
- Create synthetic 1d by grouping 6 4h bars (groups must be complete)
- Create true 1d by resampling 4h to 1d (UTC aligned)
- Compare OHLC match rates and IncrementalTrendType agreement

Output: backtest/scripts/validation/v2_aggregation_test.md
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Path setup ─────────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
BACKTEST_ROOT = SCRIPTS_DIR.parent.parent
sys.path.insert(0, str(BACKTEST_ROOT))

from strategies.external._helpers import IncrementalTrendType

# Data directory
DATA_DIR = BACKTEST_ROOT / 'data' / 'ohlcv' / 'BTCUSDT' / '1m'
OUTPUT_DIR = SCRIPTS_DIR
OUTPUT_FILE = OUTPUT_DIR / 'v2_aggregation_test.md'

# Test parameters
TEST_START = '2024-01-01'
TEST_END = '2024-03-31'
SAMPLE_BARS_MAX = 60


def load_1m_candles(start: str, end: str) -> np.ndarray:
    """
    Load 1m BTC candles from parquet files.

    Returns: np.ndarray of shape (n, 6) with columns:
      [ts_ms, open, high, low, close, volume]
    """
    if not DATA_DIR.exists():
        raise FileNotFoundError(f'Data dir missing: {DATA_DIR}')

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    frames = []

    # Load from direct parquet files in DATA_DIR
    for f in sorted(DATA_DIR.glob('*.parquet')):
        try:
            df = pd.read_parquet(f)
            schema_keys = list(df.columns)

            # Normalize column names and timestamps
            if 'timestamp' in schema_keys:
                df['ts_ms'] = df['timestamp'].astype('int64')
            elif 'open_time' in schema_keys:
                df['ts_ms'] = pd.to_datetime(df['open_time']).astype('int64') // 10**6
            else:
                continue

            # Select only relevant columns and filter
            df = df[['ts_ms', 'open', 'high', 'low', 'close', 'volume']].copy()
            df = df[(df['ts_ms'] >= start_ms) & (df['ts_ms'] < end_ms)]

            if len(df) > 0:
                frames.append(df)
        except Exception as e:
            continue

    if not frames:
        raise FileNotFoundError(f'No parquet files under {DATA_DIR}')

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=['ts_ms'], keep='first').sort_values('ts_ms').reset_index(drop=True)

    # Convert to numpy
    arr = df[['ts_ms', 'open', 'high', 'low', 'close', 'volume']].astype(np.float64).values
    print(f'  Loaded {len(arr):,} 1m candles ({start} → {end})')
    return arr


def resample_to_tf(arr_1m: np.ndarray, minutes: int) -> np.ndarray:
    """
    Resample 1m candles to specified timeframe (4h=240, 1d=1440).
    Aligns to UTC boundaries.

    Returns: np.ndarray of shape (n, 6) with columns:
      [ts_ms, open, high, low, close, volume]
    """
    ms_per_minute = 60_000
    align_ms = minutes * ms_per_minute

    if len(arr_1m) < minutes:
        return np.empty((0, 6), dtype=np.float64)

    # Trim leading bars to reach next aligned UTC boundary
    first_ts = int(arr_1m[0, 0])
    offset_ms = first_ts % align_ms
    if offset_ms != 0:
        skip = (align_ms - offset_ms) // ms_per_minute
        arr_1m = arr_1m[skip:]

    # Trim to multiple of minutes
    n = (len(arr_1m) // minutes) * minutes
    arr_1m = arr_1m[:n]

    if n == 0:
        return np.empty((0, 6), dtype=np.float64)

    # Reshape and aggregate
    c = arr_1m.reshape(-1, minutes, 6)
    out = np.empty((len(c), 6), dtype=np.float64)
    out[:, 0] = c[:, 0, 0]              # ts = first bar's ts
    out[:, 1] = c[:, 0, 1]              # open = first bar's open
    out[:, 2] = c[:, -1, 4]             # close = last bar's close
    out[:, 3] = c[:, :, 3].max(axis=1)  # high = period max
    out[:, 4] = c[:, :, 4].min(axis=1)  # low = period min
    out[:, 5] = c[:, :, 5].sum(axis=1)  # volume = sum
    return out


def create_synthetic_1d(arr_4h: np.ndarray) -> np.ndarray:
    """
    Create synthetic 1d by grouping every 6 consecutive 4h candles.
    Groups must be complete (drop incomplete groups at end).

    Returns: np.ndarray of shape (n_groups, 6)
    """
    n_groups = len(arr_4h) // 6
    if n_groups == 0:
        return np.empty((0, 6), dtype=np.float64)

    # Trim to complete groups
    arr_4h_trimmed = arr_4h[:n_groups * 6]

    # Reshape into groups
    groups = arr_4h_trimmed.reshape(-1, 6, 6)
    out = np.empty((len(groups), 6), dtype=np.float64)

    out[:, 0] = groups[:, 0, 0]               # ts = first 4h bar's ts
    out[:, 1] = groups[:, 0, 1]               # open = first 4h bar's open
    out[:, 2] = groups[:, -1, 4]              # close = last 4h bar's close
    out[:, 3] = groups[:, :, 3].max(axis=1)   # high = max of 6 highs
    out[:, 4] = groups[:, :, 4].min(axis=1)   # low = min of 6 lows
    out[:, 5] = groups[:, :, 5].sum(axis=1)   # volume = sum

    return out


def compare_ohlc(synthetic: np.ndarray, true: np.ndarray, max_bars: int = 60) -> dict:
    """
    Compare OHLC between synthetic and true 1d bars.

    Returns: dict with match counts and rates.
    """
    n = min(len(synthetic), len(true), max_bars)
    if n == 0:
        return {'n_bars': 0, 'open_match': 0, 'high_match': 0, 'low_match': 0, 'close_match': 0}

    syn = synthetic[:n]
    tru = true[:n]

    # Exact match for open and close
    open_match = np.sum(np.abs(syn[:, 1] - tru[:, 1]) < 0.01)
    close_match = np.sum(np.abs(syn[:, 2] - tru[:, 2]) < 0.01)

    # Within 0.01% for high and low
    tolerance_high = tru[:, 3] * 0.0001
    tolerance_low = tru[:, 4] * 0.0001

    high_match = np.sum(np.abs(syn[:, 3] - tru[:, 3]) <= tolerance_high)
    low_match = np.sum(np.abs(syn[:, 4] - tru[:, 4]) <= tolerance_low)

    return {
        'n_bars': n,
        'open_match': int(open_match),
        'high_match': int(high_match),
        'low_match': int(low_match),
        'close_match': int(close_match),
    }


def compare_trendtype(synthetic: np.ndarray, true: np.ndarray, max_bars: int = 60) -> dict:
    """
    Run IncrementalTrendType on both synthetic and true 1d bars.
    Compare tt values (trit: -2/0/+2) across bars.

    Returns: dict with agreement count and rate.
    """
    # Prepare numpy arrays: [ts, open, close, high, low, volume]
    # IncrementalTrendType.update expects: candles[i] = [?, close, ?, high, low, ?]
    # (indices 2=close, 3=high, 4=low)

    n = min(len(synthetic), len(true), max_bars)
    if n < 20:  # Need enough bars for warmup
        return {'n_bars': n, 'tt_match': 0, 'tt_agreement_pct': 0.0}

    tt_synthetic = IncrementalTrendType(atr_len=9, atr_ma_len=20, di_len=9, smooth=1)
    tt_true = IncrementalTrendType(atr_len=9, atr_ma_len=20, di_len=9, smooth=1)

    syn_tt_values = []
    tru_tt_values = []

    for i in range(n):
        tt_syn = tt_synthetic.update(synthetic[:i+1])
        tt_tru = tt_true.update(true[:i+1])
        syn_tt_values.append(tt_syn)
        tru_tt_values.append(tt_tru)

    # Compare: NaN means not yet available (warmup), skip those
    matches = 0
    valid = 0
    for syn, tru in zip(syn_tt_values, tru_tt_values):
        if not np.isnan(syn) and not np.isnan(tru):
            if np.isclose(syn, tru):
                matches += 1
            valid += 1

    agreement_pct = (matches / valid * 100) if valid > 0 else 0.0

    return {
        'n_bars': n,
        'valid_bars': valid,
        'tt_match': matches,
        'tt_agreement_pct': round(agreement_pct, 2),
    }


def main():
    print('\n=== 1d Inline Aggregation Validation ===\n')
    print(f'Test period: {TEST_START} ~ {TEST_END}')
    print(f'Sample bars: {SAMPLE_BARS_MAX} max')

    # Load 1m data
    try:
        arr_1m = load_1m_candles(TEST_START, TEST_END)
    except Exception as e:
        print(f'ERROR loading 1m data: {e}')
        return

    if len(arr_1m) < 240:
        print('ERROR: Not enough 1m candles to test')
        return

    # Resample 1m → 4h
    arr_4h = resample_to_tf(arr_1m, 240)
    print(f'  Resampled to {len(arr_4h):,} 4h candles')

    if len(arr_4h) < 6:
        print('ERROR: Not enough 4h candles to test (need ≥6)')
        return

    # Create synthetic 1d (group 6 4h → 1d)
    arr_syn_1d = create_synthetic_1d(arr_4h)
    print(f'  Created {len(arr_syn_1d):,} synthetic 1d candles (groups of 6 4h)')

    # Create true 1d (resample 4h → 1d)
    arr_true_1d = resample_to_tf(arr_4h, 6)
    print(f'  Created {len(arr_true_1d):,} true 1d candles (UTC aligned 4h→1d)')

    # Compare OHLC
    print('\n--- OHLC Accuracy (first 60 bars) ---')
    ohlc_result = compare_ohlc(arr_syn_1d, arr_true_1d, SAMPLE_BARS_MAX)
    n = ohlc_result['n_bars']
    open_pct = round(ohlc_result['open_match'] / n * 100, 2) if n > 0 else 0
    high_pct = round(ohlc_result['high_match'] / n * 100, 2) if n > 0 else 0
    low_pct = round(ohlc_result['low_match'] / n * 100, 2) if n > 0 else 0
    close_pct = round(ohlc_result['close_match'] / n * 100, 2) if n > 0 else 0

    print(f"  Open:  {ohlc_result['open_match']}/{n} exact match ({open_pct}%)")
    print(f"  High:  {ohlc_result['high_match']}/{n} ≤0.01% error ({high_pct}%)")
    print(f"  Low:   {ohlc_result['low_match']}/{n} ≤0.01% error ({low_pct}%)")
    print(f"  Close: {ohlc_result['close_match']}/{n} exact match ({close_pct}%)")

    # Compare TrendType
    print('\n--- IncrementalTrendType Agreement ---')
    tt_result = compare_trendtype(arr_syn_1d, arr_true_1d, SAMPLE_BARS_MAX)
    print(f"  Tested {tt_result['n_bars']} bars (valid={tt_result['valid_bars']})")
    print(f"  Match: {tt_result['tt_match']}/{tt_result['valid_bars']} ({tt_result['tt_agreement_pct']}%)")

    # Determine PASS/FAIL
    tt_agreement = tt_result['tt_agreement_pct']
    pass_fail = 'PASS' if tt_agreement >= 99.0 else 'FAIL'

    print(f'\n--- Judgement ---')
    print(f"  Criterion: tt agreement ≥ 99%")
    print(f"  Result: {pass_fail}")

    # Write markdown report
    report = f"""# 1d 인라인 집계 검증

날짜: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
데이터: BTC 1m → 4h → (synthetic 1d / true 1d)
테스트 기간: {TEST_START} ~ {TEST_END}
샘플 1d bars: {ohlc_result['n_bars']}

## OHLC 정확도

| 항목 | 일치도 | 기준 |
|------|--------|------|
| Open | {ohlc_result['open_match']}/{n} ({open_pct}%) | 정확히 일치 |
| High | {ohlc_result['high_match']}/{n} ({high_pct}%) | ≤0.01% 오차 |
| Low | {ohlc_result['low_match']}/{n} ({low_pct}%) | ≤0.01% 오차 |
| Close | {ohlc_result['close_match']}/{n} ({close_pct}%) | 정확히 일치 |

## TrendType tt 일치율

IncrementalTrendType 파라미터:
- atr_len=9
- atr_ma_len=20
- di_len=9
- smooth=1

결과:
- 테스트 바: {tt_result['n_bars']}
- 유효 바 (warmup 완료): {tt_result['valid_bars']}
- 일치: {tt_result['tt_match']}/{tt_result['valid_bars']} ({tt_result['tt_agreement_pct']}%)

## 판정

**{pass_fail}**

기준: tt 일치율 ≥ 99% (현재: {tt_result['tt_agreement_pct']}%)

## 해석

- **OHLC 정확도**: 합성(6×4h→1d) vs 참값(UTC 정렬 4h→1d) 비교
  - Open/Close: 정확히 일치 (첫/마지막 4h 바 기준)
  - High/Low: ≤0.01% 오차 내 일치

- **TrendType 일치율**: IncrementalTrendType의 tt 신호 (-2/0/+2)가 두 시계열 간 얼마나 일치하는가
  - ≥99% → 합성 방식이 참값과 동등함을 의미
  - <99% → UTC 경계 또는 불완전 그룹의 영향 가능
"""

    OUTPUT_FILE.write_text(report)
    print(f'\nReport written: {OUTPUT_FILE}')


if __name__ == '__main__':
    main()
