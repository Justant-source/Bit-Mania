#!/usr/bin/env python3
"""
Binance BTCUSDT 1m OHLC 수집
- 저장: backtest/data/ohlcv/BTCUSDT/1m/YYYY-MM.parquet
- 컬럼: timestamp (ms, int64), open, high, low, close, volume (float64)
- 기간: 2017-08-17 ~ 2026-04-30
- 월별 파티션 (이미 존재하는 월은 스킵)
"""
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import calendar

OUT_DIR = Path(__file__).parents[3] / 'data' / 'ohlcv' / 'BTCUSDT' / '1m'
OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = 'https://api.binance.com/api/v3/klines'
SYMBOL   = 'BTCUSDT'
INTERVAL = '1m'
LIMIT    = 1000

# 기간: 2017-08-17 ~ 2026-04-30
START_YEAR, START_MONTH = 2017, 8
END_YEAR,   END_MONTH   = 2026, 4

def month_range_ms(year: int, month: int):
    """월의 시작~끝 timestamp(ms) 반환"""
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    last_day = calendar.monthrange(year, month)[1]
    end   = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

def fetch_month(year: int, month: int) -> pd.DataFrame:
    start_ms, end_ms = month_range_ms(year, month)
    rows = []
    cur_start = start_ms
    while cur_start <= end_ms:
        params = dict(
            symbol=SYMBOL, interval=INTERVAL,
            startTime=cur_start, endTime=end_ms, limit=LIMIT
        )
        resp = requests.get(BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        rows.extend(data)
        last_ts = int(data[-1][0])
        if last_ts >= end_ms or len(data) < LIMIT:
            break
        cur_start = last_ts + 60_000  # +1분
        time.sleep(0.05)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[
        'timestamp','open','high','low','close','volume',
        'close_time','quote_vol','num_trades','taker_base','taker_quote','ignore'
    ])
    return df[['timestamp','open','high','low','close','volume']].astype({
        'timestamp': 'int64', 'open': 'float64', 'high': 'float64',
        'low': 'float64', 'close': 'float64', 'volume': 'float64',
    })

def iter_months():
    y, m = START_YEAR, START_MONTH
    while (y, m) <= (END_YEAR, END_MONTH):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def main():
    months = list(iter_months())
    print(f'총 {len(months)} 개월 다운로드 시작...')
    done, skip = 0, 0
    for y, m in months:
        out = OUT_DIR / f'{y:04d}-{m:02d}.parquet'
        if out.exists():
            skip += 1
            continue
        df = fetch_month(y, m)
        if df.empty:
            print(f'  {y}-{m:02d}: 데이터 없음 (스킵)')
            continue
        df.to_parquet(out, index=False)
        done += 1
        print(f'  {y}-{m:02d}: {len(df):,} 행 → {out.name}')
    total = sum(pd.read_parquet(OUT_DIR/f).shape[0] for f in sorted(OUT_DIR.glob('*.parquet')))
    print(f'\n완료: 신규 {done}개월, 스킵 {skip}개월')
    print(f'총 누적 캔들: {total:,}')

if __name__ == '__main__':
    main()
