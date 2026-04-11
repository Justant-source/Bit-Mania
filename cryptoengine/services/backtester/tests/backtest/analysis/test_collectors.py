#!/usr/bin/env python3
"""Quick test to verify collector scripts work."""

import asyncio
import sys

sys.path.insert(0, "/app")

async def test_macro_collector():
    """Test macro data collector import."""
    try:
        import tests.backtest.analysis.macro_data_collector as mdc
        print("✓ macro_data_collector imports successfully")
        return True
    except Exception as e:
        print(f"✗ macro_data_collector import failed: {e}")
        return False


async def test_oi_collector():
    """Test OI collector import."""
    try:
        import tests.backtest.analysis.oi_collector as oic
        print("✓ oi_collector imports successfully")
        return True
    except Exception as e:
        print(f"✗ oi_collector import failed: {e}")
        return False


async def test_feature_engineering():
    """Test feature engineering import."""
    try:
        import tests.backtest.optimization.feature_engineering as fe
        print("✓ feature_engineering imports successfully")
        return True
    except Exception as e:
        print(f"✗ feature_engineering import failed: {e}")
        return False


async def main():
    """Run all tests."""
    results = await asyncio.gather(
        test_macro_collector(),
        test_oi_collector(),
        test_feature_engineering(),
    )

    if all(results):
        print("\n✓ All collectors imported successfully!")
        return 0
    else:
        print("\n✗ Some collectors failed to import")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
