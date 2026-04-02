"""Grid level calculator with arithmetic and geometric spacing.

Computes a list of grid levels (price, quantity, side) given a center
price, ATR value, number of grids, and total capital.  Supports dynamic
recalculation when the price drifts away from the grid center.
"""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple

import numpy as np
import structlog

logger = structlog.get_logger()

# Minimum allowed spacing between adjacent grid levels (0.2%)
MIN_SPACING_PCT = 0.002


class SpacingMode(str, Enum):
    ARITHMETIC = "arithmetic"
    GEOMETRIC = "geometric"


class GridLevel(NamedTuple):
    """Single grid level definition."""

    price: float
    quantity: float
    side: str  # "buy" or "sell"


class GridCalculator:
    """Compute and manage grid level placement."""

    def __init__(
        self,
        spacing_mode: SpacingMode = SpacingMode.ARITHMETIC,
        min_spacing_pct: float = MIN_SPACING_PCT,
    ) -> None:
        self.spacing_mode = spacing_mode
        self.min_spacing_pct = min_spacing_pct
        self._log = logger.bind(component="grid_calculator")

    def calculate_grid(
        self,
        center_price: float,
        atr: float,
        num_grids: int,
        total_capital: float,
    ) -> list[GridLevel]:
        """Calculate grid levels around *center_price*.

        Parameters
        ----------
        center_price:
            Current mid-price used as the grid center.
        atr:
            ATR(14) value used to determine the range width.
        num_grids:
            Total number of grid levels (split evenly buy/sell).
        total_capital:
            Capital allocated to the grid strategy.

        Returns
        -------
        list[GridLevel]
            Ordered list of (price, quantity, side) tuples.
        """
        if center_price <= 0 or atr <= 0 or num_grids < 2 or total_capital <= 0:
            self._log.warning(
                "invalid_grid_params",
                center_price=center_price,
                atr=atr,
                num_grids=num_grids,
                total_capital=total_capital,
            )
            return []

        range_width = atr * 3
        lower_bound = center_price - range_width
        upper_bound = center_price + range_width

        # Enforce minimum spacing
        min_spacing = center_price * self.min_spacing_pct
        max_possible_grids = int((upper_bound - lower_bound) / min_spacing)
        if num_grids > max_possible_grids and max_possible_grids > 0:
            self._log.info(
                "reducing_grid_count",
                requested=num_grids,
                max_possible=max_possible_grids,
            )
            num_grids = max_possible_grids

        levels = self._generate_levels(lower_bound, upper_bound, num_grids)

        # Per-level capital allocation (uniform)
        capital_per_level = total_capital / num_grids

        grid: list[GridLevel] = []
        for price in levels:
            if price <= 0:
                continue
            quantity = capital_per_level / price
            side = "buy" if price < center_price else "sell"
            grid.append(GridLevel(price=round(price, 2), quantity=quantity, side=side))

        self._log.info(
            "grid_calculated",
            center=center_price,
            range_width=range_width,
            num_levels=len(grid),
            lower=round(lower_bound, 2),
            upper=round(upper_bound, 2),
        )
        return grid

    def _generate_levels(
        self, lower: float, upper: float, num_grids: int
    ) -> list[float]:
        """Generate price levels using the configured spacing mode."""
        if self.spacing_mode == SpacingMode.GEOMETRIC:
            return self._geometric_levels(lower, upper, num_grids)
        return self._arithmetic_levels(lower, upper, num_grids)

    @staticmethod
    def _arithmetic_levels(lower: float, upper: float, num_grids: int) -> list[float]:
        """Evenly spaced levels between lower and upper bounds."""
        return list(np.linspace(lower, upper, num_grids))

    @staticmethod
    def _geometric_levels(lower: float, upper: float, num_grids: int) -> list[float]:
        """Geometrically spaced levels (tighter near center)."""
        if lower <= 0:
            lower = 0.01
        return list(np.geomspace(lower, upper, num_grids))

    def should_recalculate(
        self, current_price: float, center_price: float, range_width: float
    ) -> bool:
        """Return True if the price has drifted enough to warrant a grid reset.

        A recalculation is triggered when the price has moved more than 50%
        of the range width away from the original center.
        """
        drift = abs(current_price - center_price)
        threshold = range_width * 0.5
        return drift > threshold
