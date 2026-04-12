"""
Phase 7.4 — Unit Tests for FundingTracker.

Run:
    python scripts/test_funding_pnl.py

Expected: 3 tests PASS, 0 FAIL
"""

import sys
import os
import tempfile
import unittest
from pathlib import Path

# Allow importing from parent
sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_funding_parquet(rows: list[dict]) -> str:
    """Create a temp parquet file with given funding data rows."""
    import polars as pl
    df = pl.DataFrame(rows)
    tmp = tempfile.mktemp(suffix=".parquet")
    df.write_parquet(tmp)
    return tmp


class TestFundingTimeDetection(unittest.TestCase):
    """Test 1: 8h funding interval detection."""

    def setUp(self):
        from strategies.utils.funding_pnl import FundingTracker
        # Dummy parquet to satisfy constructor
        self._path = _make_funding_parquet([
            {"timestamp_ms": 1704067200000, "funding_rate": 0.0001},  # 2024-01-01 00:00 UTC
            {"timestamp_ms": 1704096000000, "funding_rate": 0.0002},  # 2024-01-01 08:00 UTC
            {"timestamp_ms": 1704124800000, "funding_rate": -0.0001}, # 2024-01-01 16:00 UTC
        ])
        self.tracker = FundingTracker(self._path)

    def tearDown(self):
        Path(self._path).unlink(missing_ok=True)

    def test_is_funding_time_exact_hour(self):
        """UTC 00:00, 08:00, 16:00 must be detected as funding time."""
        # 2024-01-01 00:00:00 UTC in ms
        ts_00h = 1704067200000
        ts_08h = 1704096000000
        ts_16h = 1704124800000
        ts_non = 1704078000000  # 03:00 UTC — not a funding time

        self.assertTrue(self.tracker._is_funding_time(ts_00h), "00:00 UTC must be funding time")
        self.assertTrue(self.tracker._is_funding_time(ts_08h), "08:00 UTC must be funding time")
        self.assertTrue(self.tracker._is_funding_time(ts_16h), "16:00 UTC must be funding time")
        self.assertFalse(self.tracker._is_funding_time(ts_non), "03:00 UTC must NOT be funding time")


class TestFundingPnLSign(unittest.TestCase):
    """Test 2: P&L sign convention (long pays positive rate, short receives)."""

    def setUp(self):
        self._path = _make_funding_parquet([
            {"timestamp_ms": 1704067200000, "funding_rate": 0.0001},  # +0.01% at 00:00 UTC
        ])
        from strategies.utils.funding_pnl import FundingTracker
        self.tracker = FundingTracker(self._path)

    def tearDown(self):
        Path(self._path).unlink(missing_ok=True)

    def test_long_pays_positive_funding(self):
        """Long position PAYS when funding rate is positive → negative P&L."""
        pnl = self.tracker.calculate_pnl(
            position_direction=1,      # long
            notional=10_000.0,         # $10,000 position
            timestamp_ms=1704067200000, # funding time
        )
        self.assertLess(pnl, 0, "Long should PAY (negative P&L) when rate > 0")
        self.assertAlmostEqual(pnl, -1.0, places=4,
            msg="Expected -$1.00 (10000 × 0.0001)")

    def test_short_receives_positive_funding(self):
        """Short position RECEIVES when funding rate is positive → positive P&L."""
        pnl = self.tracker.calculate_pnl(
            position_direction=-1,     # short
            notional=10_000.0,
            timestamp_ms=1704067200000,
        )
        self.assertGreater(pnl, 0, "Short should RECEIVE (positive P&L) when rate > 0")
        self.assertAlmostEqual(pnl, 1.0, places=4,
            msg="Expected +$1.00 (10000 × 0.0001)")


class TestFundingPnLZeroOutsideFundingTime(unittest.TestCase):
    """Test 3: No P&L at non-funding timestamps."""

    def setUp(self):
        self._path = _make_funding_parquet([
            {"timestamp_ms": 1704067200000, "funding_rate": 0.0001},
        ])
        from strategies.utils.funding_pnl import FundingTracker
        self.tracker = FundingTracker(self._path)

    def tearDown(self):
        Path(self._path).unlink(missing_ok=True)

    def test_zero_pnl_at_non_funding_time(self):
        """P&L must be 0.0 at any non-funding timestamp."""
        # 2024-01-01 03:00 UTC — not a funding settlement
        ts_non_funding = 1704078000000
        pnl = self.tracker.calculate_pnl(
            position_direction=1,
            notional=10_000.0,
            timestamp_ms=ts_non_funding,
        )
        self.assertEqual(pnl, 0.0, "P&L must be 0.0 at non-funding timestamps")


if __name__ == "__main__":
    print("=" * 60)
    print("Phase 7.4 — FundingTracker Unit Tests")
    print("=" * 60)
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestFundingTimeDetection))
    suite.addTests(loader.loadTestsFromTestCase(TestFundingPnLSign))
    suite.addTests(loader.loadTestsFromTestCase(TestFundingPnLZeroOutsideFundingTime))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
