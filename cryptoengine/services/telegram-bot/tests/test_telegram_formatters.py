"""Unit tests for telegram-bot formatters.py (C-2).

Tests:
- format_pnl() output format and key inclusion (including T-4 fields)
- format_daily_report() field presence and formatting
- format_alert() for each alert type
- format_position()
- compute_sharpe_annualized() and compute_max_drawdown()
- _safe_float() edge cases (0, negative, None, bad input)
- Extreme value handling throughout
"""

from __future__ import annotations

import math
import sys
import os

# Allow running tests directly from the service directory without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from formatters import (
    _safe_float,
    compute_max_drawdown,
    compute_sharpe_annualized,
    format_alert,
    format_daily_report,
    format_pnl,
    format_position,
)


# ── _safe_float ───────────────────────────────────────────────────────────────

class TestSafeFloat:
    def test_none_returns_default(self):
        assert _safe_float(None) == 0.0

    def test_none_custom_default(self):
        assert _safe_float(None, -1.0) == -1.0

    def test_zero(self):
        assert _safe_float(0) == 0.0

    def test_negative(self):
        assert _safe_float(-123.45) == pytest.approx(-123.45)

    def test_string_float(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_bad_string(self):
        assert _safe_float("not_a_number") == 0.0

    def test_large_value(self):
        assert _safe_float(1e18) == pytest.approx(1e18)

    def test_bool_true(self):
        # Python bool is a subclass of int; True → 1.0
        assert _safe_float(True) == pytest.approx(1.0)


# ── compute_sharpe_annualized ─────────────────────────────────────────────────

class TestComputeSharpe:
    def test_empty_returns_zero(self):
        assert compute_sharpe_annualized([]) == 0.0

    def test_single_value_returns_zero(self):
        assert compute_sharpe_annualized([0.01]) == 0.0

    def test_constant_returns_zero_stddev(self):
        # All identical returns → stddev = 0 → Sharpe = 0
        returns = [0.01] * 30
        assert compute_sharpe_annualized(returns) == 0.0

    def test_positive_returns(self):
        # Consistent positive daily returns → positive Sharpe
        returns = [0.001] * 5 + [0.002] * 5  # slight variation
        sharpe = compute_sharpe_annualized(returns)
        assert sharpe > 0

    def test_mixed_returns(self):
        returns = [0.01, -0.01, 0.02, -0.005, 0.015]
        sharpe = compute_sharpe_annualized(returns)
        assert isinstance(sharpe, float)
        assert not math.isnan(sharpe)

    def test_annualization_factor(self):
        # With known mean and std, verify annualization (√252)
        returns = [0.01, 0.02]
        mean = sum(returns) / 2
        import math as _math
        var = sum((r - mean) ** 2 for r in returns) / 1
        std = _math.sqrt(var)
        expected = (mean / std) * _math.sqrt(252)
        result = compute_sharpe_annualized(returns)
        assert result == pytest.approx(expected)


# ── compute_max_drawdown ──────────────────────────────────────────────────────

class TestComputeMaxDrawdown:
    def test_empty_returns_zero(self):
        assert compute_max_drawdown([]) == 0.0

    def test_single_value_returns_zero(self):
        assert compute_max_drawdown([1000.0]) == 0.0

    def test_monotone_increase_no_dd(self):
        equities = [100.0, 110.0, 120.0, 130.0]
        assert compute_max_drawdown(equities) == 0.0

    def test_simple_drawdown(self):
        # peak=100, trough=80 → DD = 20%
        equities = [100.0, 90.0, 80.0, 95.0]
        dd = compute_max_drawdown(equities)
        assert dd == pytest.approx(20.0)

    def test_multiple_drawdowns(self):
        # Two drawdowns; the larger one should be returned
        equities = [100.0, 90.0, 95.0, 110.0, 80.0]
        # From 110 to 80 = 27.27% > from 100 to 90 = 10%
        dd = compute_max_drawdown(equities)
        assert dd == pytest.approx((110.0 - 80.0) / 110.0 * 100, rel=1e-4)

    def test_zero_peak_skipped(self):
        equities = [0.0, 10.0, 8.0]
        # First peak is 0 → skipped; peak becomes 10, trough 8 → 20%
        dd = compute_max_drawdown(equities)
        assert dd == pytest.approx(20.0)


# ── format_pnl ────────────────────────────────────────────────────────────────

class TestFormatPnl:
    def _base_portfolio(self, **overrides) -> dict:
        base = {
            "total_equity": 10000.0,
            "unrealized_pnl": 50.0,
            "realized_pnl_today": 30.0,
            "daily_drawdown": 0.5,
            "weekly_drawdown": 1.2,
        }
        base.update(overrides)
        return base

    def test_contains_equity(self):
        msg = format_pnl(self._base_portfolio())
        assert "10,000.00" in msg

    def test_contains_unrealized(self):
        msg = format_pnl(self._base_portfolio())
        assert "Unrealized" in msg

    def test_contains_realized(self):
        msg = format_pnl(self._base_portfolio())
        assert "Realized" in msg

    def test_positive_total_emoji(self):
        msg = format_pnl(self._base_portfolio())
        # total = 80 > 0 → 📈
        assert "\U0001f4c8" in msg

    def test_negative_total_emoji(self):
        msg = format_pnl(self._base_portfolio(unrealized_pnl=-100.0, realized_pnl_today=-50.0))
        # total = -150 < 0 → 📉
        assert "\U0001f4c9" in msg

    def test_daily_drawdown_shown(self):
        msg = format_pnl(self._base_portfolio(daily_drawdown=3.14))
        assert "3.14" in msg

    def test_kill_switch_shown_when_active(self):
        msg = format_pnl(self._base_portfolio(kill_switch_triggered=True))
        assert "KILL SWITCH" in msg

    def test_kill_switch_hidden_when_inactive(self):
        msg = format_pnl(self._base_portfolio(kill_switch_triggered=False))
        assert "KILL SWITCH" not in msg

    def test_none_values_handled(self):
        # All None should not raise; defaults to 0.0
        portfolio = {
            "total_equity": None,
            "unrealized_pnl": None,
            "realized_pnl_today": None,
            "daily_drawdown": None,
            "weekly_drawdown": None,
        }
        msg = format_pnl(portfolio)
        assert "Portfolio Summary" in msg

    def test_zero_equity(self):
        msg = format_pnl(self._base_portfolio(total_equity=0.0))
        assert "0.00" in msg

    def test_sharpe_and_monthly_dd_included(self):
        """T-4: sharpe_30d and monthly_max_dd should appear in output."""
        portfolio = self._base_portfolio(sharpe_30d=2.35, monthly_max_dd=4.52)
        msg = format_pnl(portfolio)
        assert "Sharpe" in msg
        assert "2.35" in msg
        assert "4.52" in msg

    def test_sharpe_absent_when_not_provided(self):
        """T-4: Sharpe line should not appear when keys are missing."""
        msg = format_pnl(self._base_portfolio())
        assert "Sharpe" not in msg

    def test_strategy_breakdown_shown(self):
        portfolio = self._base_portfolio(strategies=[
            {"strategy_id": "funding_arb", "current_pnl": 25.0, "position_count": 2},
        ])
        msg = format_pnl(portfolio)
        assert "funding_arb" in msg
        assert "25.00" in msg


# ── format_daily_report ───────────────────────────────────────────────────────

class TestFormatDailyReport:
    def _base_report(self, **overrides) -> dict:
        base = {
            "date": "2026-04-06",
            "total_pnl": 120.5,
            "total_trades": 10,
            "win_rate": 70.0,
            "sharpe_ratio": 3.58,
            "max_drawdown": 1.23,
            "ending_equity": 10200.0,
            "total_fees": 5.0,
            "funding_earned": 45.0,
        }
        base.update(overrides)
        return base

    def test_contains_date(self):
        msg = format_daily_report(self._base_report())
        assert "2026-04-06" in msg

    def test_contains_pnl(self):
        msg = format_daily_report(self._base_report())
        assert "120.50" in msg or "120" in msg

    def test_contains_sharpe(self):
        msg = format_daily_report(self._base_report())
        assert "3.58" in msg

    def test_contains_max_drawdown(self):
        msg = format_daily_report(self._base_report())
        assert "1.23" in msg

    def test_contains_funding(self):
        msg = format_daily_report(self._base_report())
        assert "45.00" in msg or "Funding" in msg

    def test_negative_pnl_emoji(self):
        msg = format_daily_report(self._base_report(total_pnl=-50.0))
        assert "\U0001f4c9" in msg

    def test_strategy_breakdown_included(self):
        report = self._base_report(strategy_breakdown=[
            {"strategy_id": "funding_arb", "pnl": 80.0, "trades": 7},
        ])
        msg = format_daily_report(report)
        assert "funding_arb" in msg

    def test_zero_trades(self):
        msg = format_daily_report(self._base_report(total_trades=0, win_rate=0.0))
        assert "0" in msg

    def test_zero_funding(self):
        msg = format_daily_report(self._base_report(funding_earned=0.0))
        assert "0.00" in msg


# ── format_position ───────────────────────────────────────────────────────────

class TestFormatPosition:
    def _base_position(self, **overrides) -> dict:
        base = {
            "symbol": "BTCUSDT",
            "side": "long",
            "size": 0.01,
            "entry_price": 70000.0,
            "leverage": 5,
            "unrealized_pnl": 150.0,
            "liquidation_price": 55000.0,
        }
        base.update(overrides)
        return base

    def test_symbol_present(self):
        msg = format_position(self._base_position())
        assert "BTCUSDT" in msg

    def test_long_green_emoji(self):
        msg = format_position(self._base_position(side="long"))
        assert "\U0001f7e2" in msg

    def test_short_red_emoji(self):
        msg = format_position(self._base_position(side="short"))
        assert "\U0001f534" in msg

    def test_positive_pnl_check_emoji(self):
        msg = format_position(self._base_position(unrealized_pnl=10.0))
        assert "\u2705" in msg

    def test_negative_pnl_cross_emoji(self):
        msg = format_position(self._base_position(unrealized_pnl=-5.0))
        assert "\u274c" in msg

    def test_leverage_shown(self):
        msg = format_position(self._base_position(leverage=5))
        assert "5x" in msg

    def test_zero_size(self):
        msg = format_position(self._base_position(size=0.0))
        assert "0.0000" in msg


# ── format_alert ─────────────────────────────────────────────────────────────

class TestFormatAlert:
    def test_entry_alert(self):
        data = {
            "strategy_id": "funding_arb",
            "symbol": "BTCUSDT",
            "side": "buy",
            "quantity": 0.01,
            "filled_price": 70000.0,
            "fee": 0.35,
        }
        msg = format_alert("entry", data)
        assert "BTCUSDT" in msg
        assert "funding_arb" in msg
        assert "New Position" in msg

    def test_exit_alert_profit(self):
        data = {
            "strategy_id": "funding_arb",
            "symbol": "BTCUSDT",
            "filled_price": 71000.0,
            "realized_pnl": 50.0,
            "hold_duration": "2h 15m",
        }
        msg = format_alert("exit", data)
        assert "Position Closed" in msg
        assert "\u2705" in msg  # profit emoji

    def test_exit_alert_loss(self):
        data = {"realized_pnl": -25.0}
        msg = format_alert("exit", data)
        assert "\u274c" in msg

    def test_funding_alert(self):
        data = {
            "symbol": "BTCUSDT",
            "rate": 0.0001,
            "payment": 5.0,
            "next_funding_time": "08:00 UTC",
        }
        msg = format_alert("funding", data)
        assert "Funding" in msg
        assert "BTCUSDT" in msg

    def test_kill_switch_level4(self):
        data = {
            "level": 4,
            "trigger_reason": "max_drawdown",
            "daily_drawdown": 5.0,
            "weekly_drawdown": 8.0,
        }
        msg = format_alert("kill_switch", data)
        assert "KILL SWITCH" in msg
        assert "Level 4" in msg
        assert "EMERGENCY" in msg

    def test_anomaly_warning(self):
        data = {
            "severity": "warning",
            "component": "execution-engine",
            "anomaly_type": "order_lag",
            "details": "Fill latency > 5s",
        }
        msg = format_alert("anomaly", data)
        assert "Anomaly" in msg
        assert "execution-engine" in msg

    def test_anomaly_critical(self):
        data = {"severity": "critical", "component": "redis", "anomaly_type": "disconnect", "details": "timeout"}
        msg = format_alert("anomaly", data)
        assert "\U0001f6a8" in msg

    def test_generic_unknown_type(self):
        data = {"type": "custom_event", "message": "something happened"}
        msg = format_alert("unknown_type", data)
        assert "Alert" in msg

    def test_empty_data(self):
        """All formatters must handle empty dict without raising."""
        for alert_type in ("entry", "exit", "funding", "kill_switch", "anomaly"):
            msg = format_alert(alert_type, {})
            assert isinstance(msg, str)
            assert len(msg) > 0

    def test_none_values_in_data(self):
        """Formatters must handle None values gracefully."""
        data = {"symbol": None, "payment": None, "rate": None}
        msg = format_alert("funding", data)
        assert isinstance(msg, str)
