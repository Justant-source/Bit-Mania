"""Grid state tracker — monitors order fills and manages grid lifecycle.

Maintains a mapping of grid levels to their order state and detects fills
so that opposite-direction orders can be created automatically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger()

MAX_OPEN_ORDERS = 60


class LevelStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    CANCELLED = "cancelled"


@dataclass
class GridLevelState:
    """Tracks a single grid level's order."""

    price: float
    side: str  # "buy" or "sell"
    quantity: float
    order_id: str | None = None
    status: LevelStatus = LevelStatus.PENDING


@dataclass
class GridStateTracker:
    """Track all grid levels and detect fills.

    Attributes
    ----------
    levels : dict[str, GridLevelState]
        Mapping of level_id -> level state.
    center_price : float
        Original center price when the grid was created.
    range_width : float
        Half-width of the grid range (ATR * 3).
    max_open_orders : int
        Maximum number of simultaneously open orders.
    """

    levels: dict[str, GridLevelState] = field(default_factory=dict)
    center_price: float = 0.0
    range_width: float = 0.0
    max_open_orders: int = MAX_OPEN_ORDERS

    def __post_init__(self) -> None:
        self._log = logger.bind(component="grid_state_tracker")
        self._fill_callbacks: list[Any] = []

    # ── level management ───────────────────────────────────────────────

    def set_grid(
        self,
        levels: list[dict[str, Any]],
        center_price: float,
        range_width: float,
    ) -> None:
        """Replace the current grid with a new set of levels.

        Parameters
        ----------
        levels:
            List of dicts with keys: level_id, price, side, quantity.
        center_price:
            The grid's center price.
        range_width:
            Half-width of the range (ATR * 3).
        """
        self.levels.clear()
        self.center_price = center_price
        self.range_width = range_width

        for lvl in levels:
            level_id = lvl["level_id"]
            self.levels[level_id] = GridLevelState(
                price=lvl["price"],
                side=lvl["side"],
                quantity=lvl["quantity"],
            )

        self._log.info(
            "grid_set",
            num_levels=len(self.levels),
            center_price=center_price,
            range_width=range_width,
        )

    def register_order(self, level_id: str, order_id: str) -> None:
        """Associate an exchange order_id with a grid level."""
        if level_id in self.levels:
            self.levels[level_id].order_id = order_id
            self.levels[level_id].status = LevelStatus.OPEN
            self._log.debug("order_registered", level_id=level_id, order_id=order_id)

    def mark_filled(self, level_id: str) -> GridLevelState | None:
        """Mark a level as filled and return its state for opposite order creation."""
        level = self.levels.get(level_id)
        if level is None:
            self._log.warning("fill_unknown_level", level_id=level_id)
            return None

        level.status = LevelStatus.FILLED
        self._log.info(
            "level_filled",
            level_id=level_id,
            price=level.price,
            side=level.side,
        )
        return level

    def mark_cancelled(self, level_id: str) -> None:
        """Mark a level as cancelled."""
        if level_id in self.levels:
            self.levels[level_id].status = LevelStatus.CANCELLED

    # ── fill detection ─────────────────────────────────────────────────

    def detect_fills(self, filled_order_ids: set[str]) -> list[GridLevelState]:
        """Compare open order IDs against a set of filled IDs.

        Returns list of newly filled levels that need opposite orders.
        """
        newly_filled: list[GridLevelState] = []

        for level_id, level in self.levels.items():
            if (
                level.status == LevelStatus.OPEN
                and level.order_id is not None
                and level.order_id in filled_order_ids
            ):
                filled_level = self.mark_filled(level_id)
                if filled_level is not None:
                    newly_filled.append(filled_level)

        return newly_filled

    def get_opposite_side(self, side: str) -> str:
        """Return the opposite trade direction."""
        return "sell" if side == "buy" else "buy"

    # ── queries ────────────────────────────────────────────────────────

    @property
    def open_order_count(self) -> int:
        """Number of currently open orders."""
        return sum(1 for l in self.levels.values() if l.status == LevelStatus.OPEN)

    @property
    def filled_count(self) -> int:
        """Number of filled levels."""
        return sum(1 for l in self.levels.values() if l.status == LevelStatus.FILLED)

    def can_place_more_orders(self) -> bool:
        """Check if we are below the max open order limit."""
        return self.open_order_count < self.max_open_orders

    def should_reset_grid(self, current_price: float) -> bool:
        """Return True if the price has drifted >50% of range from center."""
        if self.range_width <= 0:
            return False
        drift = abs(current_price - self.center_price)
        return drift > (self.range_width * 0.5)

    def get_open_order_ids(self) -> list[str]:
        """Return all order IDs that are currently open."""
        return [
            l.order_id
            for l in self.levels.values()
            if l.status == LevelStatus.OPEN and l.order_id is not None
        ]

    def reset(self) -> None:
        """Clear all grid state."""
        self.levels.clear()
        self.center_price = 0.0
        self.range_width = 0.0
        self._log.info("grid_state_reset")
