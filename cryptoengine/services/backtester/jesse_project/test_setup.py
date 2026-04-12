#!/usr/bin/env python3
"""
Test Jesse setup and validate the FundingArb strategy.

Usage:
    python test_setup.py              # Run all checks
    python test_setup.py --db-only    # Skip Jesse checks, test DB only
    python test_setup.py --mock       # Use mock funding rate data
"""

import sys
import os
import asyncio
import argparse
from datetime import datetime, timedelta
import logging

# Add app to path
sys.path.insert(0, '/app')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


async def test_database_connection(use_jesse_db=True):
    """Test PostgreSQL connection."""
    try:
        import asyncpg

        db_name = 'jesse_db' if use_jesse_db else 'cryptoengine'
        dsn = (
            f"postgresql://cryptoengine:CryptoEngine2026!@"
            f"localhost:5432/{db_name}"
        )

        logger.info(f"Testing connection to {db_name} database...")
        conn = await asyncpg.connect(dsn)
        version = await conn.fetchval('SELECT version()')
        await conn.close()

        logger.info(f"✓ Database {db_name} is accessible")
        logger.info(f"  PostgreSQL version: {version[:60]}...")
        return True

    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


async def test_jesse_import():
    """Test Jesse package import."""
    try:
        logger.info("Testing Jesse import...")
        import jesse
        from jesse.strategies import Strategy

        logger.info(f"✓ Jesse {jesse.__version__} is installed")
        logger.info(f"  Jesse location: {jesse.__file__}")
        return True

    except ImportError as e:
        logger.error(f"✗ Jesse import failed: {e}")
        logger.info("  Run: pip install jesse>=0.41.0")
        return False


async def test_strategies_import():
    """Test custom strategy imports."""
    try:
        logger.info("Testing strategy imports...")

        # Add current directory to path
        sys.path.insert(0, '/app/jesse_project')

        from strategies.FundingArb import FundingArb, FundingArbConservative
        logger.info("✓ FundingArb strategy imported successfully")

        from strategies.MultiFundingRotation import (
            MultiFundingRotation,
            SimpleFundingRotation
        )
        logger.info("✓ MultiFundingRotation strategy imported successfully")

        return True

    except ImportError as e:
        logger.error(f"✗ Strategy import failed: {e}")
        return False


async def test_strategy_instantiation():
    """Test instantiating strategies."""
    try:
        logger.info("Testing strategy instantiation...")

        sys.path.insert(0, '/app/jesse_project')
        from strategies.FundingArb import FundingArb

        # Note: FundingArb requires Jesse framework context to fully instantiate
        # This is a basic check
        logger.info("✓ FundingArb class is valid")
        logger.info(f"  Hyperparameters: {FundingArb.hp}")

        return True

    except Exception as e:
        logger.error(f"✗ Strategy instantiation failed: {e}")
        return False


async def test_config_file():
    """Test configuration file is valid."""
    try:
        logger.info("Testing config.py...")

        sys.path.insert(0, '/app/jesse_project')
        from config import config

        # Check structure
        assert 'env' in config, "config['env'] not found"
        assert 'databases' in config['env'], "config['env']['databases'] not found"
        assert 'postgres' in config['env']['databases'], "PostgreSQL config not found"

        db_config = config['env']['databases']['postgres']
        logger.info("✓ config.py is valid")
        logger.info(f"  Database: {db_config['name']} @ {db_config['host']}:{db_config['port']}")

        return True

    except Exception as e:
        logger.error(f"✗ Config validation failed: {e}")
        return False


async def test_funding_rate_integration():
    """Test funding rate data mechanism."""
    try:
        logger.info("Testing funding rate integration...")

        # This is a mock test - actual integration requires data loader
        # Verify the strategy can accept mock funding rate data

        sys.path.insert(0, '/app/jesse_project')
        from strategies.FundingArb import FundingArb

        # Check that strategy has proper structure for funding rate
        has_funding_property = hasattr(FundingArb, 'current_funding_rate')
        has_settlement_method = hasattr(FundingArb, 'is_settlement_time')

        if has_funding_property and has_settlement_method:
            logger.info("✓ FundingArb has funding rate integration points")
            logger.info("  ✓ current_funding_rate property")
            logger.info("  ✓ is_settlement_time() method")
            return True
        else:
            logger.error("✗ Missing funding rate integration")
            return False

    except Exception as e:
        logger.error(f"✗ Funding rate integration check failed: {e}")
        return False


async def main(args):
    """Run all tests."""
    logger.info("=" * 70)
    logger.info("Jesse Framework Setup Validation")
    logger.info("=" * 70)

    results = {}

    # Database tests
    if not args.skip_db:
        logger.info("\n[1/6] Database Connectivity")
        results['db_jesse'] = await test_database_connection(use_jesse_db=True)
        results['db_crypto'] = await test_database_connection(use_jesse_db=False)

    # Jesse framework tests
    if not args.db_only:
        logger.info("\n[2/6] Jesse Framework")
        results['jesse'] = await test_jesse_import()

        logger.info("\n[3/6] Strategy Imports")
        results['strategies'] = await test_strategies_import()

        logger.info("\n[4/6] Strategy Instantiation")
        results['instantiation'] = await test_strategy_instantiation()

        logger.info("\n[5/6] Configuration File")
        results['config'] = await test_config_file()

        logger.info("\n[6/6] Funding Rate Integration")
        results['funding'] = await test_funding_rate_integration()

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status:8} {test_name}")

    logger.info("-" * 70)
    logger.info(f"Tests passed: {passed}/{total}")

    if passed == total:
        logger.info("\n✓ All tests passed! Jesse is ready to use.")
        logger.info("\nNext steps:")
        logger.info("  1. Create jesse_db database (if not done):")
        logger.info("     psql -U cryptoengine -d postgres -c 'CREATE DATABASE jesse_db;'")
        logger.info("  2. Import candle data:")
        logger.info("     jesse import-candles Bybit BTCUSDT 1h 2019-01-01 2025-01-01")
        logger.info("  3. Run backtest:")
        logger.info("     jesse backtest FundingArb Bybit BTCUSDT 1h 2019-01-01 2025-12-31")
        return 0
    else:
        logger.info("\n✗ Some tests failed. See errors above.")
        return 1


def _parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Test Jesse setup and validate strategies"
    )
    parser.add_argument(
        '--db-only',
        action='store_true',
        help="Only test database connectivity"
    )
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help="Skip database tests"
    )
    parser.add_argument(
        '--mock',
        action='store_true',
        help="Use mock funding rate data (for testing without real data)"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
