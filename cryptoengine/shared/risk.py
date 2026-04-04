"""Risk-management calculation utilities."""

from __future__ import annotations

import math
from typing import Sequence


def calculate_drawdown(equity_curve: Sequence[float]) -> tuple[float, float]:
    """Return (max_drawdown, current_drawdown) from an equity curve.

    Drawdown values are negative (e.g. -0.05 means -5%).
    Returns (0.0, 0.0) for empty or constant curves.
    """
    if len(equity_curve) < 2:
        return 0.0, 0.0

    peak = equity_curve[0]
    max_dd = 0.0

    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (val - peak) / peak if peak != 0 else 0.0
        if dd < max_dd:
            max_dd = dd

    current_peak = max(equity_curve)
    current_dd = (equity_curve[-1] - current_peak) / current_peak if current_peak != 0 else 0.0

    return max_dd, current_dd


def calculate_sharpe(
    returns: Sequence[float],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 365,
) -> float:
    """Annualised Sharpe ratio.

    Parameters
    ----------
    returns : daily (or per-period) simple returns.
    risk_free_rate : annualised risk-free rate.
    periods_per_year : 365 for daily, 252 for trading-day, etc.
    """
    if len(returns) < 2:
        return 0.0

    n = len(returns)
    mean_r = sum(returns) / n
    rf_per_period = risk_free_rate / periods_per_year
    excess = mean_r - rf_per_period

    variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0.0

    if std == 0:
        return 0.0

    return (excess / std) * math.sqrt(periods_per_year)


def calculate_sortino(
    returns: Sequence[float],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 365,
) -> float:
    """Annualised Sortino ratio (downside-deviation only)."""
    if len(returns) < 2:
        return 0.0

    n = len(returns)
    mean_r = sum(returns) / n
    rf_per_period = risk_free_rate / periods_per_year
    excess = mean_r - rf_per_period

    downside_sq = [r ** 2 for r in returns if r < 0]
    if not downside_sq:
        return float("inf") if excess > 0 else 0.0

    downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq))
    if downside_dev == 0:
        return 0.0

    return (excess / downside_dev) * math.sqrt(periods_per_year)


def position_size_calculator(
    equity: float,
    risk_per_trade: float,
    entry_price: float,
    stop_loss_price: float,
    fee_rate: float = 0.00055,
) -> float:
    """Calculate position size (in base units) given fixed-fractional risk.

    Parameters
    ----------
    equity : total account equity.
    risk_per_trade : fraction of equity to risk (e.g. 0.01 = 1%).
    entry_price : expected fill price.
    stop_loss_price : stop-loss price.
    fee_rate : round-trip fee as a fraction of notional.

    Returns
    -------
    Position size in base-currency units (always >= 0).
    """
    if entry_price <= 0 or stop_loss_price <= 0:
        return 0.0

    risk_amount = equity * risk_per_trade
    price_risk = abs(entry_price - stop_loss_price)

    if price_risk == 0:
        return 0.0

    # Account for round-trip fees
    fee_per_unit = entry_price * fee_rate * 2  # open + close
    effective_risk_per_unit = price_risk + fee_per_unit

    size = risk_amount / effective_risk_per_unit
    return max(size, 0.0)


def leverage_check(
    requested_leverage: float,
    max_leverage: float = 10.0,
    equity: float = 0.0,
    notional: float = 0.0,
) -> tuple[bool, float]:
    """Validate requested leverage and clamp to max.

    Returns
    -------
    (is_allowed, effective_leverage)
    """
    if requested_leverage <= 0:
        return False, 0.0

    effective = min(requested_leverage, max_leverage)

    # If equity and notional are provided, verify implied leverage
    if equity > 0 and notional > 0:
        implied = notional / equity
        effective = min(effective, max_leverage)
        if implied > max_leverage:
            return False, implied

    return True, effective
