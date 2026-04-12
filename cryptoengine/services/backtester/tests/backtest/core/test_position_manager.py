"""
Unit tests for position management safety.
Tests that dict mutation during iteration is handled correctly.

Run: python -m pytest tests/backtest/core/test_position_manager.py -v
"""
import pytest
from copy import deepcopy


class SimplePositionManager:
    """Minimal position manager to test mutation safety."""

    def __init__(self):
        self.positions = {}  # symbol -> {size, entry_price, entry_bar}

    def open(self, symbol: str, size: float, entry_price: float, bar_idx: int):
        self.positions[symbol] = {
            "size": size,
            "entry_price": entry_price,
            "entry_bar": bar_idx,
        }

    def close_matching(self, predicate) -> list[str]:
        """Safely close all positions matching predicate.

        SAFE: Uses list(positions.keys()) to avoid RuntimeError on concurrent modification.
        """
        to_close = [s for s in self.positions if predicate(s, self.positions[s])]
        for symbol in to_close:
            del self.positions[symbol]
        return to_close

    def close_all(self) -> list[str]:
        """Close all positions."""
        closed = list(self.positions.keys())
        self.positions.clear()
        return closed


def test_multi_symbol_simultaneous_close():
    """3 symbols, all should close at same bar → all must be closed."""
    pm = SimplePositionManager()
    pm.open("BTCUSDT", 0.1, 50000, bar_idx=0)
    pm.open("ETHUSDT", 1.0, 3000, bar_idx=0)
    pm.open("SOLUSDT", 10.0, 100, bar_idx=0)

    closed = pm.close_all()

    assert len(pm.positions) == 0, "All positions must be closed"
    assert set(closed) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}, "All symbols must be in closed list"


def test_partial_close_during_iteration():
    """Only 2/3 positions should close, 1 remains."""
    pm = SimplePositionManager()
    pm.open("BTCUSDT", 0.1, 50000, bar_idx=0)
    pm.open("ETHUSDT", 1.0, 3000, bar_idx=0)
    pm.open("SOLUSDT", 10.0, 100, bar_idx=0)

    # Close only BTC and ETH
    closed = pm.close_matching(lambda s, p: s in {"BTCUSDT", "ETHUSDT"})

    assert len(pm.positions) == 1, "Only 1 position should remain"
    assert "SOLUSDT" in pm.positions, "SOLUSDT position must remain"
    assert "BTCUSDT" not in pm.positions
    assert "ETHUSDT" not in pm.positions
    assert set(closed) == {"BTCUSDT", "ETHUSDT"}


def test_no_positions_to_close():
    """Closing from empty dict should not raise."""
    pm = SimplePositionManager()
    closed = pm.close_all()
    assert closed == []
    assert len(pm.positions) == 0


def test_open_after_close():
    """Can reopen a position after closing."""
    pm = SimplePositionManager()
    pm.open("BTCUSDT", 0.1, 50000, bar_idx=0)
    pm.close_all()

    # Reopen same symbol
    pm.open("BTCUSDT", 0.2, 55000, bar_idx=10)
    assert len(pm.positions) == 1
    assert pm.positions["BTCUSDT"]["size"] == 0.2
    assert pm.positions["BTCUSDT"]["entry_price"] == 55000


def test_position_data_integrity():
    """Position data should not be mutated by iteration."""
    pm = SimplePositionManager()
    pm.open("BTCUSDT", 0.1, 50000, bar_idx=0)
    pm.open("ETHUSDT", 1.0, 3000, bar_idx=5)

    original_btc = deepcopy(pm.positions["BTCUSDT"])

    # Close only ETH
    pm.close_matching(lambda s, p: s == "ETHUSDT")

    # BTC position data should be unchanged
    assert pm.positions["BTCUSDT"] == original_btc, "BTC position data should be unmodified"


def test_concurrent_modification_safety():
    """Verify the pattern prevents RuntimeError on concurrent modification."""
    pm = SimplePositionManager()
    pm.open("BTCUSDT", 0.1, 50000, bar_idx=0)
    pm.open("ETHUSDT", 1.0, 3000, bar_idx=0)
    pm.open("SOLUSDT", 10.0, 100, bar_idx=0)

    # This should not raise RuntimeError: dictionary changed size during iteration
    try:
        closed = pm.close_matching(lambda s, p: True)
        assert len(closed) == 3, "All positions should be closed"
        assert len(pm.positions) == 0, "No positions should remain"
    except RuntimeError as e:
        pytest.fail(f"Concurrent modification should be safe, but got RuntimeError: {e}")


def test_selective_close_multiple_rounds():
    """Verify multiple close operations in sequence work correctly."""
    pm = SimplePositionManager()

    # Round 1: Open 5 positions
    for i, sym in enumerate(["BTC", "ETH", "SOL", "ARB", "OP"]):
        pm.open(f"{sym}USDT", 0.1, 50000 + i * 1000, bar_idx=0)

    assert len(pm.positions) == 5

    # Round 2: Close 2 positions
    closed1 = pm.close_matching(lambda s, p: s in {"BTCUSDT", "ETHUSDT"})
    assert len(closed1) == 2
    assert len(pm.positions) == 3

    # Round 3: Close another 1
    closed2 = pm.close_matching(lambda s, p: s == "SOLUSDT")
    assert len(closed2) == 1
    assert len(pm.positions) == 2

    # Round 4: Close remaining
    closed3 = pm.close_matching(lambda s, p: True)
    assert len(closed3) == 2
    assert len(pm.positions) == 0
