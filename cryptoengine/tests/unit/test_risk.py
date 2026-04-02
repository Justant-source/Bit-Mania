"""Unit tests for risk-management utilities.

Covers:
  - Drawdown calculation (max and current)
  - Sharpe ratio (annualized)
  - Sortino ratio
  - Position sizing (fixed-fractional)
  - Leverage checks
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.risk import (
    calculate_drawdown,
    calculate_sharpe,
    calculate_sortino,
    leverage_check,
    position_size_calculator,
)


class TestDrawdown:
    def test_empty_curve(self):
        max_dd, cur_dd = calculate_drawdown([])
        assert max_dd == 0.0
        assert cur_dd == 0.0

    def test_single_point(self):
        max_dd, cur_dd = calculate_drawdown([100.0])
        assert max_dd == 0.0
        assert cur_dd == 0.0

    def test_constant_curve(self):
        max_dd, cur_dd = calculate_drawdown([100.0, 100.0, 100.0])
        assert max_dd == 0.0
        assert cur_dd == 0.0

    def test_monotonic_increase(self):
        max_dd, cur_dd = calculate_drawdown([100.0, 110.0, 120.0])
        assert max_dd == 0.0
        assert cur_dd == 0.0

    def test_simple_drawdown(self):
        curve = [100.0, 110.0, 100.0, 90.0, 95.0]
        max_dd, cur_dd = calculate_drawdown(curve)
        # Peak=110, trough=90 => dd = (90-110)/110 = -0.1818...
        assert max_dd == pytest.approx(-0.1818, abs=0.001)
        # Current: peak=110, last=95 => dd = (95-110)/110 = -0.1363...
        assert cur_dd == pytest.approx(-0.1364, abs=0.001)

    def test_drawdown_is_negative(self):
        curve = [100.0, 90.0, 80.0]
        max_dd, cur_dd = calculate_drawdown(curve)
        assert max_dd < 0
        assert cur_dd < 0

    def test_recovery_resets_current_drawdown(self):
        curve = [100.0, 90.0, 100.0, 110.0]
        max_dd, cur_dd = calculate_drawdown(curve)
        assert cur_dd == 0.0
        assert max_dd == pytest.approx(-0.1, abs=0.001)


class TestSharpe:
    def test_empty_returns(self):
        assert calculate_sharpe([]) == 0.0

    def test_single_return(self):
        assert calculate_sharpe([0.01]) == 0.0

    def test_all_positive_returns(self):
        returns = [0.01, 0.02, 0.01, 0.015]
        sharpe = calculate_sharpe(returns)
        assert sharpe > 0

    def test_all_negative_returns(self):
        returns = [-0.01, -0.02, -0.01, -0.015]
        sharpe = calculate_sharpe(returns)
        assert sharpe < 0

    def test_zero_variance_returns(self):
        returns = [0.01, 0.01, 0.01, 0.01]
        sharpe = calculate_sharpe(returns)
        assert sharpe == 0.0

    def test_annualization_factor(self):
        returns = [0.01, 0.02, -0.005, 0.015]
        # Different periods should give different Sharpe values
        sharpe_daily = calculate_sharpe(returns, periods_per_year=365)
        sharpe_hourly = calculate_sharpe(returns, periods_per_year=8760)
        # Hourly annualization should give higher value
        assert sharpe_hourly > sharpe_daily

    def test_risk_free_rate(self):
        returns = [0.001, 0.001, 0.001, 0.001, 0.001]
        sharpe_zero_rf = calculate_sharpe(returns, risk_free_rate=0.0)
        sharpe_high_rf = calculate_sharpe(returns, risk_free_rate=0.10)
        # Higher risk-free rate reduces excess return
        assert sharpe_zero_rf >= sharpe_high_rf


class TestSortino:
    def test_empty_returns(self):
        assert calculate_sortino([]) == 0.0

    def test_all_positive_no_downside(self):
        returns = [0.01, 0.02, 0.03]
        sortino = calculate_sortino(returns)
        assert sortino == float("inf")  # no downside deviation

    def test_mixed_returns(self):
        returns = [0.01, -0.02, 0.015, -0.01, 0.02]
        sortino = calculate_sortino(returns)
        assert isinstance(sortino, float)

    def test_sortino_higher_than_sharpe_for_positive_skew(self):
        returns = [0.03, 0.02, 0.01, -0.005, 0.025]
        sharpe = calculate_sharpe(returns)
        sortino = calculate_sortino(returns)
        # Sortino should be higher when returns are positively skewed
        assert sortino >= sharpe


class TestPositionSizing:
    def test_basic_position_size(self):
        size = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,  # 1%
            entry_price=65000.0,
            stop_loss_price=64000.0,
        )
        # risk_amount = 100, price_risk = 1000, fee_per_unit = 65000*0.0006*2 = 78
        # effective_risk = 1000 + 78 = 1078
        # size = 100 / 1078 ≈ 0.0928
        assert size > 0
        assert size < 1.0  # less than 1 BTC

    def test_zero_stop_distance(self):
        size = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=65000.0,
            stop_loss_price=65000.0,
        )
        assert size == 0.0

    def test_zero_entry_price(self):
        size = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=0.0,
            stop_loss_price=64000.0,
        )
        assert size == 0.0

    def test_larger_risk_per_trade(self):
        size_1pct = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=65000.0,
            stop_loss_price=64000.0,
        )
        size_2pct = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.02,
            entry_price=65000.0,
            stop_loss_price=64000.0,
        )
        assert size_2pct == pytest.approx(size_1pct * 2, rel=0.01)

    def test_negative_prices_return_zero(self):
        size = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=-100.0,
            stop_loss_price=64000.0,
        )
        assert size == 0.0

    def test_fee_rate_impact(self):
        size_low_fee = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=65000.0,
            stop_loss_price=64000.0,
            fee_rate=0.0001,
        )
        size_high_fee = position_size_calculator(
            equity=10000.0,
            risk_per_trade=0.01,
            entry_price=65000.0,
            stop_loss_price=64000.0,
            fee_rate=0.001,
        )
        # Higher fees -> smaller position
        assert size_high_fee < size_low_fee


class TestLeverageCheck:
    def test_within_limits(self):
        allowed, effective = leverage_check(5.0, max_leverage=10.0)
        assert allowed is True
        assert effective == 5.0

    def test_exceeds_max(self):
        allowed, effective = leverage_check(15.0, max_leverage=10.0)
        assert allowed is True
        assert effective == 10.0  # clamped

    def test_zero_leverage(self):
        allowed, effective = leverage_check(0.0)
        assert allowed is False
        assert effective == 0.0

    def test_negative_leverage(self):
        allowed, effective = leverage_check(-5.0)
        assert allowed is False

    def test_implied_leverage_exceeds_max(self):
        allowed, implied = leverage_check(
            5.0, max_leverage=3.0, equity=1000.0, notional=5000.0
        )
        assert allowed is False
        assert implied == 5.0

    def test_implied_leverage_within_max(self):
        allowed, effective = leverage_check(
            3.0, max_leverage=5.0, equity=1000.0, notional=2000.0
        )
        assert allowed is True
