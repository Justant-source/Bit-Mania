#!/usr/bin/env python3
"""
Comprehensive test for environment fixes:
1. feature_engineering.py OI column fix
2. macro_indicators table creation and data insertion
3. ohlcv_1m_longterm table creation
4. oi_collector and macro_data_collector script imports
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

from tests.backtest.optimization.feature_engineering import (
    load_open_interest,
    load_etf_flow,
    build_features,
)
from tests.backtest.core import make_pool


async def test_oi_loading():
    """Test that OI can be loaded correctly with fixed column name."""
    print("\n=== TEST 1: Open Interest Loading ===")
    pool = await make_pool()

    try:
        start = datetime(2026, 4, 9, tzinfo=timezone.utc)
        end = datetime(2026, 4, 11, tzinfo=timezone.utc)

        df = await load_open_interest(pool, "BTCUSDT", start, end)

        if df.empty:
            print("✓ OI loading works (empty result expected for test data)")
            return True
        else:
            print(f"✓ OI loading works: {len(df)} rows loaded")
            print(f"  Columns: {df.columns.tolist()}")
            return True

    except Exception as e:
        print(f"✗ OI loading failed: {e}")
        return False
    finally:
        await pool.close()


async def test_etf_loading():
    """Test that ETF flow can be loaded."""
    print("\n=== TEST 2: ETF Flow Loading ===")
    pool = await make_pool()

    try:
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 3, 31, tzinfo=timezone.utc)

        df = await load_etf_flow(pool, start, end)

        if df.empty:
            print("✓ ETF Flow loading works (empty result for date range)")
            return True
        else:
            print(f"✓ ETF Flow loading works: {len(df)} days of data loaded")
            return True

    except Exception as e:
        print(f"✗ ETF Flow loading failed: {e}")
        return False
    finally:
        await pool.close()


async def test_macro_indicators_table():
    """Test that macro_indicators table exists and can store data."""
    print("\n=== TEST 3: macro_indicators Table ===")
    pool = await make_pool()

    try:
        async with pool.acquire() as conn:
            # Check table exists
            exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'macro_indicators'
                )
                """
            )

            if not exists:
                print("✗ macro_indicators table does not exist")
                return False

            print("✓ macro_indicators table exists")

            # Check that we can query it
            count = await conn.fetchval("SELECT COUNT(*) FROM macro_indicators")
            print(f"✓ macro_indicators contains {count} records")

            # List indicators
            indicators = await conn.fetch(
                "SELECT DISTINCT indicator FROM macro_indicators ORDER BY indicator"
            )
            if indicators:
                ind_names = [r['indicator'] for r in indicators]
                print(f"✓ Available indicators: {', '.join(ind_names)}")

            return True

    except Exception as e:
        print(f"✗ macro_indicators check failed: {e}")
        return False
    finally:
        await pool.close()


async def test_ohlcv_1m_longterm_table():
    """Test that ohlcv_1m_longterm table exists."""
    print("\n=== TEST 4: ohlcv_1m_longterm Table ===")
    pool = await make_pool()

    try:
        async with pool.acquire() as conn:
            # Check table exists
            exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'ohlcv_1m_longterm'
                )
                """
            )

            if not exists:
                print("✗ ohlcv_1m_longterm table does not exist")
                return False

            print("✓ ohlcv_1m_longterm table exists")

            # Check schema
            cols = await conn.fetch(
                """
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = 'ohlcv_1m_longterm'
                ORDER BY ordinal_position
                """
            )

            expected_cols = ['id', 'exchange', 'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            actual_cols = [c['column_name'] for c in cols]

            if actual_cols == expected_cols:
                print(f"✓ Schema is correct: {', '.join(expected_cols)}")
            else:
                print(f"✗ Schema mismatch. Expected: {expected_cols}, Got: {actual_cols}")
                return False

            return True

    except Exception as e:
        print(f"✗ ohlcv_1m_longterm check failed: {e}")
        return False
    finally:
        await pool.close()


async def test_collector_imports():
    """Test that collector scripts can be imported."""
    print("\n=== TEST 5: Collector Script Imports ===")

    try:
        import tests.backtest.analysis.oi_collector
        print("✓ oi_collector imports successfully")
    except Exception as e:
        print(f"✗ oi_collector import failed: {e}")
        return False

    try:
        import tests.backtest.analysis.macro_data_collector
        print("✓ macro_data_collector imports successfully")
    except Exception as e:
        print(f"✗ macro_data_collector import failed: {e}")
        return False

    return True


async def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("ENVIRONMENT FIXES VERIFICATION TEST")
    print("="*60)

    results = []

    results.append(("OI Loading", await test_oi_loading()))
    results.append(("ETF Flow", await test_etf_loading()))
    results.append(("macro_indicators", await test_macro_indicators_table()))
    results.append(("ohlcv_1m_longterm", await test_ohlcv_1m_longterm_table()))
    results.append(("Collector Imports", await test_collector_imports()))

    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status:10s} {test_name}")

    all_passed = all(r[1] for r in results)

    print("="*60)
    if all_passed:
        print("✓ ALL TESTS PASSED - Environment is correctly configured!")
        return 0
    else:
        print("✗ SOME TESTS FAILED - Review errors above")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
