"""Unit tests for grid trading calculations.

Covers:
  - Grid price level generation (arithmetic and geometric)
  - Spacing validation and clamping
  - Quantity calculation per grid level
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


# ---------------------------------------------------------------------------
# GridCalculator — self-contained for test isolation
# ---------------------------------------------------------------------------

class GridCalculator:
    """Calculate grid levels, spacing, and quantities.

    Mirrors logic from grid-trading strategy configuration.
    """

    def __init__(
        self,
        *,
        grid_type: str = "geometric",
        levels_above: int = 10,
        levels_below: int = 10,
        spacing_pct: float = 0.5,
        min_spacing_pct: float = 0.2,
        max_spacing_pct: float = 2.0,
        order_size_usd: float = 200.0,
    ) -> None:
        self.grid_type = grid_type
        self.levels_above = levels_above
        self.levels_below = levels_below
        self.spacing_pct = self._clamp_spacing(spacing_pct, min_spacing_pct, max_spacing_pct)
        self.min_spacing_pct = min_spacing_pct
        self.max_spacing_pct = max_spacing_pct
        self.order_size_usd = order_size_usd

    @staticmethod
    def _clamp_spacing(spacing: float, lo: float, hi: float) -> float:
        return max(lo, min(spacing, hi))

    def generate_grid_prices(self, mid_price: float) -> list[float]:
        """Generate grid price levels around mid_price.

        Returns prices sorted ascending: [lowest, ..., mid_price, ..., highest].
        """
        if mid_price <= 0:
            raise ValueError("mid_price must be positive")

        prices: list[float] = []

        if self.grid_type == "geometric":
            factor = 1 + self.spacing_pct / 100.0
            for i in range(self.levels_below, 0, -1):
                prices.append(mid_price / (factor ** i))
            prices.append(mid_price)
            for i in range(1, self.levels_above + 1):
                prices.append(mid_price * (factor ** i))
        else:
            # Arithmetic
            step = mid_price * self.spacing_pct / 100.0
            for i in range(self.levels_below, 0, -1):
                prices.append(mid_price - step * i)
            prices.append(mid_price)
            for i in range(1, self.levels_above + 1):
                prices.append(mid_price + step * i)

        # Filter out non-positive prices
        prices = [p for p in prices if p > 0]
        return sorted(prices)

    def validate_spacing(self, prices: list[float]) -> bool:
        """Check that all adjacent price gaps are within [min, max] spacing."""
        if len(prices) < 2:
            return True
        for i in range(1, len(prices)):
            gap_pct = (prices[i] - prices[i - 1]) / prices[i - 1] * 100
            if gap_pct < self.min_spacing_pct or gap_pct > self.max_spacing_pct:
                return False
        return True

    def calculate_quantities(
        self, prices: list[float], total_capital: float | None = None
    ) -> list[float]:
        """Calculate base-currency quantity for each grid level.

        Each level gets ``order_size_usd / price`` units.
        If ``total_capital`` is given, scale down to fit budget.
        """
        quantities = [self.order_size_usd / p for p in prices if p > 0]

        if total_capital is not None:
            total_notional = sum(self.order_size_usd for _ in prices)
            if total_notional > total_capital:
                scale = total_capital / total_notional
                quantities = [q * scale for q in quantities]

        return quantities

    @property
    def total_grid_levels(self) -> int:
        return self.levels_above + self.levels_below + 1  # +1 for mid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGridPriceGeneration:
    def test_geometric_grid_symmetry(self):
        calc = GridCalculator(levels_above=5, levels_below=5, spacing_pct=0.5)
        prices = calc.generate_grid_prices(65000.0)
        assert len(prices) == 11

        mid_idx = 5
        assert prices[mid_idx] == pytest.approx(65000.0)

        # Symmetry check: ratio above/below mid should be equal
        ratio_above = prices[mid_idx + 1] / prices[mid_idx]
        ratio_below = prices[mid_idx] / prices[mid_idx - 1]
        assert ratio_above == pytest.approx(ratio_below, rel=1e-10)

    def test_arithmetic_grid_equal_spacing(self):
        calc = GridCalculator(
            grid_type="arithmetic",
            levels_above=5,
            levels_below=5,
            spacing_pct=0.5,
        )
        prices = calc.generate_grid_prices(65000.0)
        assert len(prices) == 11

        # Verify equal spacing
        diffs = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        for d in diffs:
            assert d == pytest.approx(diffs[0], rel=1e-10)

    def test_grid_sorted_ascending(self):
        calc = GridCalculator(levels_above=10, levels_below=10)
        prices = calc.generate_grid_prices(65000.0)
        assert prices == sorted(prices)

    def test_grid_all_positive(self):
        calc = GridCalculator(levels_above=5, levels_below=5, spacing_pct=1.0)
        prices = calc.generate_grid_prices(100.0)
        assert all(p > 0 for p in prices)

    def test_invalid_mid_price(self):
        calc = GridCalculator()
        with pytest.raises(ValueError):
            calc.generate_grid_prices(0.0)
        with pytest.raises(ValueError):
            calc.generate_grid_prices(-100.0)

    def test_single_level_above_below(self):
        calc = GridCalculator(levels_above=1, levels_below=1, spacing_pct=1.0)
        prices = calc.generate_grid_prices(65000.0)
        assert len(prices) == 3

    def test_zero_levels(self):
        calc = GridCalculator(levels_above=0, levels_below=0)
        prices = calc.generate_grid_prices(65000.0)
        assert len(prices) == 1
        assert prices[0] == 65000.0


class TestSpacingValidation:
    def test_valid_geometric_spacing(self):
        calc = GridCalculator(
            levels_above=5,
            levels_below=5,
            spacing_pct=0.5,
            min_spacing_pct=0.2,
            max_spacing_pct=2.0,
        )
        prices = calc.generate_grid_prices(65000.0)
        assert calc.validate_spacing(prices)

    def test_spacing_clamped_to_minimum(self):
        calc = GridCalculator(
            spacing_pct=0.1,
            min_spacing_pct=0.2,
            max_spacing_pct=2.0,
        )
        assert calc.spacing_pct == 0.2

    def test_spacing_clamped_to_maximum(self):
        calc = GridCalculator(
            spacing_pct=5.0,
            min_spacing_pct=0.2,
            max_spacing_pct=2.0,
        )
        assert calc.spacing_pct == 2.0

    def test_spacing_within_range_unchanged(self):
        calc = GridCalculator(
            spacing_pct=1.0,
            min_spacing_pct=0.2,
            max_spacing_pct=2.0,
        )
        assert calc.spacing_pct == 1.0


class TestQuantityCalculation:
    def test_quantity_per_level(self):
        calc = GridCalculator(order_size_usd=200.0)
        prices = [64000.0, 65000.0, 66000.0]
        quantities = calc.calculate_quantities(prices)
        assert len(quantities) == 3
        for p, q in zip(prices, quantities):
            assert q == pytest.approx(200.0 / p)

    def test_quantity_with_capital_constraint(self):
        calc = GridCalculator(order_size_usd=200.0, levels_above=5, levels_below=5)
        prices = calc.generate_grid_prices(65000.0)
        total_needed = 200.0 * len(prices)  # 11 * 200 = 2200
        total_capital = 1000.0  # less than needed

        quantities = calc.calculate_quantities(prices, total_capital=total_capital)
        total_notional = sum(q * p for q, p in zip(quantities, prices))
        assert total_notional <= total_capital + 1.0  # allow rounding

    def test_quantity_no_capital_constraint(self):
        calc = GridCalculator(order_size_usd=100.0)
        prices = [65000.0]
        quantities = calc.calculate_quantities(prices)
        assert quantities[0] == pytest.approx(100.0 / 65000.0)

    def test_total_grid_levels_property(self):
        calc = GridCalculator(levels_above=10, levels_below=10)
        assert calc.total_grid_levels == 21
