#!/usr/bin/env python3
"""
Bybit BTCUSDT USDT-perp 펀딩비 히스토리 수집 (walk-backwards pagination)
- 저장: backtest/data/funding/BTCUSDT_8h.parquet
- 컬럼: timestamp (ms, int64), funding_rate (float64)
- 기간: 2018-04-01 ~ 2026-04-30 (Bybit 시작 전 구간은 0.0으로 fill)
- Bybit 실제 데이터: 2020-04 근방부터 시작
- 페이지네이션: walk-backwards (endTime 감소, 최신 → 과거)
"""
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

BASE_URL = 'https://api.bybit.com'
SYMBOL = 'BTCUSDT'
# 스크립트 위치: backtest/scripts/data/fetch_bybit_funding_history.py
# 목표 위치: backtest/data/funding/BTCUSDT_8h.parquet
SCRIPT_DIR = Path(__file__).resolve().parent  # .../backtest/scripts/data
OUT_PATH = SCRIPT_DIR.parents[1] / 'data' / 'funding' / f'{SYMBOL}_8h.parquet'
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# 목표 기간
FILL_START = datetime(2018, 4, 1, tzinfo=timezone.utc)
DATA_START = datetime(2019, 9, 1, tzinfo=timezone.utc)  # Bybit 실제 데이터 시작 추정
DATA_END = datetime(2026, 4, 30, 23, 59, 59, tzinfo=timezone.utc)


def _ms(dt: datetime) -> int:
    """Convert datetime to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


def fetch_all() -> list:
    """
    Fetch all funding rates using walk-backwards pagination.
    Strategy: fetch from endTime, then use oldest timestamp as new endTime.
    API doesn't provide nextPageCursor, so we track the oldest timestamp manually.
    """
    records = []
    current_end_ms = _ms(DATA_END)
    start_ms = _ms(DATA_START)
    request_count = 0
    total_unique = set()

    while True:
        params = {
            'category': 'linear',
            'symbol': SYMBOL,
            'endTime': current_end_ms,
            'limit': 200,
        }

        resp = requests.get(
            f'{BASE_URL}/v5/market/funding/history',
            params=params,
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get('retCode') != 0:
            raise RuntimeError(f"API error: {data}")

        result = data['result']
        items = result.get('list', [])
        request_count += 1

        if not items:
            break

        # Process items: newest first in response
        for item in items:
            ts = int(item['fundingRateTimestamp'])
            if ts < start_ms:
                # We've gone past the data start boundary
                break

            key = (ts, item['fundingRate'])
            if key not in total_unique:
                records.append({
                    'timestamp': ts,
                    'funding_rate': float(item['fundingRate'])
                })
                total_unique.add(key)

        # Check if we hit the start boundary
        if items and int(items[-1]['fundingRateTimestamp']) < start_ms:
            print(f'  Reached DATA_START boundary')
            break

        # Move to next iteration: fetch up to oldest timestamp - 1
        oldest_ts = int(items[-1]['fundingRateTimestamp'])
        current_end_ms = oldest_ts - 1000  # 1 second before

        if current_end_ms < start_ms:
            break

        # Log progress every 10 requests
        if request_count % 10 == 0:
            newest_ts = int(items[0]['fundingRateTimestamp'])
            print(f'  Request {request_count}: {len(records):,} unique records (newest: {datetime.fromtimestamp(newest_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d")}, oldest: {datetime.fromtimestamp(oldest_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d")})')

        time.sleep(0.1)  # Rate limiting

    print(f'  Total requests: {request_count}')
    return records


def main():
    print('Bybit 펀딩비 수집 시작 (walk-backwards pagination)...\n')

    records = fetch_all()
    print(f'\nAPI 수집: {len(records):,} 건')

    if not records:
        print('ERROR: 수집 데이터 없음')
        return

    df = pd.DataFrame(records).sort_values('timestamp').reset_index(drop=True)
    df['timestamp'] = df['timestamp'].astype('int64')
    df['funding_rate'] = df['funding_rate'].astype('float64')

    # 중복 제거
    df = df.drop_duplicates('timestamp').reset_index(drop=True)

    # Bybit 미상장 구간 (FILL_START ~ 첫 실제 데이터 이전) : 8h 간격으로 0.0 fill
    if len(df) > 0:
        first_ts = df['timestamp'].iloc[0]
        fill_start_ms = _ms(FILL_START)

        if fill_start_ms < first_ts:
            fill_ts_list = list(range(fill_start_ms, first_ts, 8 * 3600 * 1000))
            fill_df = pd.DataFrame({
                'timestamp': fill_ts_list,
                'funding_rate': 0.0
            })
            df = pd.concat([fill_df, df], ignore_index=True)
            print(f'Zero-fill added: {len(fill_ts_list):,} rows before first data')

    df = (
        df.sort_values('timestamp')
        .drop_duplicates('timestamp')
        .reset_index(drop=True)
    )
    df.to_parquet(OUT_PATH, index=False)

    file_size_kb = OUT_PATH.stat().st_size / 1024
    print(f'\n저장: {OUT_PATH}')
    print(f'총 {len(df):,} rows  ({file_size_kb:.1f} KB)')

    start_date = pd.to_datetime(df['timestamp'].iloc[0], unit='ms')
    end_date = pd.to_datetime(df['timestamp'].iloc[-1], unit='ms')
    print(f'기간: {start_date} ~ {end_date}')

    nonzero_count = (df['funding_rate'] != 0).sum()
    zero_count = len(df) - nonzero_count
    print(f'Non-zero: {nonzero_count:,}  |  Zero-filled: {zero_count:,}')

    if nonzero_count > 0:
        avg_funding = df[df['funding_rate'] != 0]['funding_rate'].mean() * 100
        print(f'평균 funding_rate (non-zero): {avg_funding:.4f}% / 8h')


if __name__ == '__main__':
    main()
