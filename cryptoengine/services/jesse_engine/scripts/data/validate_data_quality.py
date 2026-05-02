"""수집된 데이터의 품질 검증.

Track B 백테스트: 펀딩비 + OHLCV 데이터 정합성 확인.
"""
import psycopg2
import pandas as pd

DB_DSN = "postgresql://cryptoengine:cryptoengine@postgres:5432/cryptoengine"


def validate():
    """Validate funding rate and OHLCV data coverage, gaps, and anomalies."""
    conn = psycopg2.connect(DB_DSN)

    print("=" * 80)
    print("=== Funding Rate Coverage ===")
    print("=" * 80)
    df = pd.read_sql("""
        SELECT exchange, symbol,
               MIN(timestamp) AS start_date,
               MAX(timestamp) AS end_date,
               COUNT(*) AS records,
               ROUND(COUNT(*) * 100.0 / NULLIF(EXTRACT(epoch FROM (MAX(timestamp) - MIN(timestamp))) / 28800, 0), 2) AS coverage_pct
        FROM historical_funding_rates
        GROUP BY exchange, symbol
        ORDER BY exchange, symbol
    """, conn)
    print(df.to_string(index=False))

    print("\n" + "=" * 80)
    print("=== Funding Rate Gaps (>16h, indicates missing data) ===")
    print("=" * 80)
    df = pd.read_sql("""
        WITH gaps AS (
          SELECT exchange, symbol, timestamp,
                 EXTRACT(epoch FROM (timestamp - LAG(timestamp) OVER (PARTITION BY exchange, symbol ORDER BY timestamp))) / 3600 AS hours_gap
          FROM historical_funding_rates
        )
        SELECT exchange, symbol, COUNT(*) AS gap_count,
               ROUND(MAX(hours_gap)::numeric, 2) AS max_gap_hours,
               ROUND(AVG(hours_gap)::numeric, 2) AS avg_gap_hours
        FROM gaps
        WHERE hours_gap > 16
        GROUP BY exchange, symbol
        ORDER BY gap_count DESC
    """, conn)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("  (none)")

    print("\n" + "=" * 80)
    print("=== Funding Rate Anomalies (rate outside [-0.01, 0.01]) ===")
    print("=" * 80)
    df = pd.read_sql("""
        SELECT exchange, symbol, COUNT(*) AS anomaly_count,
               ROUND(MIN(rate)::numeric, 8) AS min_rate,
               ROUND(MAX(rate)::numeric, 8) AS max_rate
        FROM historical_funding_rates
        WHERE rate < -0.01 OR rate > 0.01
        GROUP BY exchange, symbol
        ORDER BY anomaly_count DESC
    """, conn)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("  (none)")

    print("\n" + "=" * 80)
    print("=== OHLCV Coverage ===")
    print("=" * 80)
    df = pd.read_sql("""
        SELECT exchange, symbol, interval,
               MIN(timestamp) AS start_date,
               MAX(timestamp) AS end_date,
               COUNT(*) AS records
        FROM historical_ohlcv
        GROUP BY exchange, symbol, interval
        ORDER BY exchange, symbol, interval
    """, conn)
    print(df.to_string(index=False))

    print("\n" + "=" * 80)
    print("=== OHLCV Gaps (>2 hours for 1h interval) ===")
    print("=" * 80)
    df = pd.read_sql("""
        WITH gaps AS (
          SELECT exchange, symbol, interval, timestamp,
                 EXTRACT(epoch FROM (timestamp - LAG(timestamp) OVER (PARTITION BY exchange, symbol, interval ORDER BY timestamp))) / 3600 AS hours_gap
          FROM historical_ohlcv
          WHERE interval = '1h'
        )
        SELECT exchange, symbol, COUNT(*) AS gap_count,
               ROUND(MAX(hours_gap)::numeric, 2) AS max_gap_hours
        FROM gaps
        WHERE hours_gap > 2
        GROUP BY exchange, symbol
        ORDER BY gap_count DESC
    """, conn)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("  (none)")

    print("\n" + "=" * 80)
    print("=== Data Completeness Summary ===")
    print("=" * 80)
    df = pd.read_sql("""
        SELECT
            (SELECT COUNT(*) FROM historical_funding_rates) AS funding_rate_total,
            (SELECT COUNT(DISTINCT exchange || symbol) FROM historical_funding_rates) AS funding_exchange_symbol_pairs,
            (SELECT COUNT(*) FROM historical_ohlcv) AS ohlcv_total,
            (SELECT COUNT(DISTINCT exchange || symbol || interval) FROM historical_ohlcv) AS ohlcv_exchange_symbol_interval_combos
    """, conn)
    print(df.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    validate()
