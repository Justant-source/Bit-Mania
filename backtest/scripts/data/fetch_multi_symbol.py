"""B2용: Bybit 6종 심볼 6년치 펀딩비 + OHLCV 수집.

Track B 백테스트 인프라: Multi-symbol FA 데이터 집계.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from coinalyze_client import CoinalyzeClient

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOGEUSDT", "1000PEPEUSDT"]
EXCHANGE = "BYBIT"
START = datetime(2020, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 5, 1, tzinfo=timezone.utc)

DB_DSN = "postgresql://cryptoengine:cryptoengine@postgres:5432/cryptoengine"


def fetch_and_store(client: CoinalyzeClient):
    """Fetch 6 symbols' funding rates and OHLCV, store in PostgreSQL."""
    conn = psycopg2.connect(DB_DSN)
    cursor = conn.cursor()

    for symbol in SYMBOLS:
        print(f"=== {symbol} ===")

        print("  Fetching funding rate history (6 years, 8h interval)...")
        all_funding = []
        cur = START
        while cur < END:
            chunk_end = min(cur + pd.Timedelta(days=180), END)
            df = client.fetch_funding_rate_history(symbol, EXCHANGE, cur, chunk_end, "8h")
            all_funding.append(df)
            cur = chunk_end
        funding_df = pd.concat(all_funding, ignore_index=True).drop_duplicates()
        print(f"  → {len(funding_df)} funding records")

        records = [
            (row["exchange"].lower(), row["symbol"], row["timestamp"], row["rate"])
            for _, row in funding_df.iterrows()
        ]
        execute_values(
            cursor,
            """INSERT INTO historical_funding_rates
               (exchange, symbol, timestamp, rate)
               VALUES %s
               ON CONFLICT (exchange, symbol, timestamp) DO NOTHING""",
            records,
        )
        conn.commit()

        print("  Fetching OHLCV 1h (6 years)...")
        all_ohlcv = []
        cur = START
        while cur < END:
            chunk_end = min(cur + pd.Timedelta(days=90), END)
            df = client.fetch_ohlcv_history(symbol, EXCHANGE, cur, chunk_end, "1h")
            all_ohlcv.append(df)
            cur = chunk_end
        ohlcv_df = pd.concat(all_ohlcv, ignore_index=True).drop_duplicates()
        print(f"  → {len(ohlcv_df)} OHLCV records")

        records = [
            (row["exchange"].lower(), row["symbol"], "1h", row["timestamp"],
             row["open"], row["high"], row["low"], row["close"], row["volume"])
            for _, row in ohlcv_df.iterrows()
        ]
        execute_values(
            cursor,
            """INSERT INTO historical_ohlcv
               (exchange, symbol, interval, timestamp, open, high, low, close, volume)
               VALUES %s
               ON CONFLICT (exchange, symbol, interval, timestamp) DO NOTHING""",
            records,
        )
        conn.commit()

    cursor.close()
    conn.close()
    print("=== DONE ===")


if __name__ == "__main__":
    client = CoinalyzeClient()
    fetch_and_store(client)
