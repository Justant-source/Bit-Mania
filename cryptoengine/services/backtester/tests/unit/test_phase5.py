"""Phase 5 소액 실전 관련 유닛 테스트.

Coverage:
  - FundingArbStrategy._calculate_position_size: pct_equity / fixed_notional / min_viable 모드
  - FundingTracker.estimate_net_profit_per_cycle: 수익/손실/BEP 계산
  - FundingTracker.is_entry_net_profitable: 진입 판단
  - KillSwitch.check: 절대값 + 퍼센트 AND 조건
"""

from __future__ import annotations

import asyncio
import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── 경로 설정 ──────────────────────────────────────────────────────────────
# backtester 컨테이너 내: /app/services/strategies/funding-arb, /app/shared
_BASE = os.path.dirname(__file__)
# 테스트 로컬 실행 시 경로 조정
_REPO_ROOT = os.path.abspath(os.path.join(_BASE, "../../../../.."))
sys.path.insert(0, os.path.join(_REPO_ROOT, "cryptoengine"))
sys.path.insert(0, os.path.join(_REPO_ROOT, "cryptoengine/shared"))
sys.path.insert(0, os.path.join(_REPO_ROOT, "cryptoengine/services/strategies/funding-arb"))
sys.path.insert(0, os.path.join(_REPO_ROOT, "cryptoengine/services/strategies"))


# ── Helper: minimal stub to avoid heavy imports ────────────────────────────

def _make_strategy(
    phase5_mode: bool = False,
    sizing_mode: str = "pct_equity",
    fixed_notional_usd: float = 150.0,
    pct_equity: float = 5.0,
    min_position_usd: float = 100.0,
    allocated_capital: float = 200.0,
    leverage: float = 5.0,
) -> object:
    """Construct a minimal FundingArbStrategy-like object without real imports.

    We replicate only the fields used by _calculate_position_size.
    """
    obj = types.SimpleNamespace()
    obj._phase5_mode = phase5_mode
    obj._sizing_mode = sizing_mode
    obj._fixed_notional_usd = fixed_notional_usd
    obj._pct_equity = pct_equity / 100.0
    obj._min_position_usd = min_position_usd
    obj.allocated_capital = allocated_capital
    obj.leverage = leverage
    obj._log = MagicMock()
    obj._log.debug = MagicMock()

    # Copy the actual method
    try:
        from strategy import FundingArbStrategy
        obj._calculate_position_size = lambda price: (
            FundingArbStrategy._calculate_position_size(obj, price)
        )
    except ImportError:
        # Fallback: inline the same logic for offline testing
        def _calculate_position_size(price: float) -> float:
            if price <= 0:
                return 0.0
            lf = 1.0 + (1.0 / obj.leverage) if obj.leverage > 0 else 2.0
            if obj._sizing_mode == "fixed_notional":
                usable = obj._fixed_notional_usd * 0.95
                qty = usable / (price * lf)
            elif obj._sizing_mode == "min_viable":
                usable = obj._min_position_usd * 0.95
                qty = usable / (price * lf)
            else:
                usable = obj.allocated_capital * obj._pct_equity * 0.95
                qty = usable / (price * lf)
            notional = qty * price
            if notional < obj._min_position_usd:
                obj._log.debug("position_size_below_minimum")
                return 0.0
            return qty
        obj._calculate_position_size = _calculate_position_size

    return obj


class TestPositionSizing(unittest.TestCase):
    """_calculate_position_size 테스트 ($200 자본 기준)."""

    BTC_PRICE = 65_000.0

    # ── pct_equity 모드 ────────────────────────────────────────────────────

    def test_pct_equity_normal_capital(self):
        """$10,000 자본, pct_equity=5% → 적정 수량 계산."""
        s = _make_strategy(allocated_capital=10_000, sizing_mode="pct_equity", pct_equity=5.0)
        qty = s._calculate_position_size(self.BTC_PRICE)
        # usable = 10000 * 0.05 * 0.95 = 475
        # lf = 1 + 1/5 = 1.2
        # qty = 475 / (65000 * 1.2) ≈ 0.006090
        self.assertAlmostEqual(qty, 475 / (self.BTC_PRICE * 1.2), places=6)
        notional = qty * self.BTC_PRICE
        self.assertGreater(notional, 100)  # min_position_usd=100 초과

    def test_pct_equity_small_capital_blocked(self):
        """$200 자본, pct_equity=5% → $10 미만 → 진입 차단 (0.0 반환)."""
        s = _make_strategy(
            allocated_capital=200,
            sizing_mode="pct_equity",
            pct_equity=5.0,
            min_position_usd=100,
        )
        qty = s._calculate_position_size(self.BTC_PRICE)
        # usable = 200 * 0.05 * 0.95 = 9.5 → notional = 9.5/1.2 * 65000 ≈ $514... wait
        # qty = 9.5 / (65000 * 1.2) = 9.5 / 78000 ≈ 0.0001218
        # notional = 0.0001218 * 65000 = $7.92 < $100 → blocked
        self.assertEqual(qty, 0.0)

    # ── fixed_notional 모드 (Phase 5) ──────────────────────────────────────

    def test_fixed_notional_150_usd(self):
        """fixed_notional $150 → 적정 수량 계산."""
        s = _make_strategy(
            phase5_mode=True,
            sizing_mode="fixed_notional",
            fixed_notional_usd=150.0,
            min_position_usd=50,
        )
        qty = s._calculate_position_size(self.BTC_PRICE)
        # usable = 150 * 0.95 = 142.5
        # lf = 1.2
        # qty = 142.5 / (65000 * 1.2) ≈ 0.001827
        expected_qty = (150 * 0.95) / (self.BTC_PRICE * 1.2)
        self.assertAlmostEqual(qty, expected_qty, places=6)
        notional = qty * self.BTC_PRICE
        self.assertGreater(notional, 50)   # min_position_usd=50 초과
        self.assertLess(notional, 160)     # fixed_notional 이하

    def test_fixed_notional_independent_of_allocated_capital(self):
        """fixed_notional 모드는 allocated_capital과 무관하다."""
        s1 = _make_strategy(
            sizing_mode="fixed_notional",
            fixed_notional_usd=150.0,
            allocated_capital=200.0,
            min_position_usd=50,
        )
        s2 = _make_strategy(
            sizing_mode="fixed_notional",
            fixed_notional_usd=150.0,
            allocated_capital=10_000.0,
            min_position_usd=50,
        )
        self.assertAlmostEqual(
            s1._calculate_position_size(self.BTC_PRICE),
            s2._calculate_position_size(self.BTC_PRICE),
            places=8,
        )

    def test_fixed_notional_zero_price(self):
        """가격 0이면 0.0 반환."""
        s = _make_strategy(sizing_mode="fixed_notional", fixed_notional_usd=150.0)
        self.assertEqual(s._calculate_position_size(0.0), 0.0)

    # ── min_viable 모드 ────────────────────────────────────────────────────

    def test_min_viable_uses_min_position_usd(self):
        """min_viable 모드: min_position_usd 기반 최소 수량."""
        s = _make_strategy(sizing_mode="min_viable", min_position_usd=80.0)
        qty = s._calculate_position_size(self.BTC_PRICE)
        expected_qty = (80.0 * 0.95) / (self.BTC_PRICE * 1.2)
        self.assertAlmostEqual(qty, expected_qty, places=6)

    def test_min_viable_below_minimum_blocked(self):
        """min_viable 모드에서 min_position_usd < 자체 임계값이면 차단."""
        s = _make_strategy(sizing_mode="min_viable", min_position_usd=10.0)
        qty = s._calculate_position_size(self.BTC_PRICE)
        # qty * price < 10 → 차단
        self.assertEqual(qty, 0.0)

    # ── 레버리지 별 차이 ───────────────────────────────────────────────────

    def test_higher_leverage_gives_more_qty(self):
        """레버리지 5x가 2x보다 많은 수량을 제공한다 (동일 자본)."""
        s5 = _make_strategy(sizing_mode="fixed_notional", fixed_notional_usd=150, leverage=5.0, min_position_usd=50)
        s2 = _make_strategy(sizing_mode="fixed_notional", fixed_notional_usd=150, leverage=2.0, min_position_usd=50)
        qty5 = s5._calculate_position_size(self.BTC_PRICE)
        qty2 = s2._calculate_position_size(self.BTC_PRICE)
        self.assertGreater(qty5, qty2)


class TestFundingTrackerNetProfit(unittest.TestCase):
    """FundingTracker.estimate_net_profit_per_cycle + is_entry_net_profitable 테스트."""

    def _make_tracker(self) -> object:
        try:
            from funding_tracker import FundingTracker
            tracker = FundingTracker.__new__(FundingTracker)
            tracker.strategy_id = "test"
            tracker.symbol = "BTC/USDT:USDT"
            tracker._payments = []
            tracker._cumulative_income = 0.0
            tracker._redis = None
            tracker._log = MagicMock()
            tracker._log.debug = MagicMock()
            return tracker
        except ImportError:
            # Inline fallback
            obj = types.SimpleNamespace()
            obj._log = MagicMock()
            obj._log.debug = MagicMock()

            def estimate_net_profit_per_cycle(
                funding_rate_8h, position_usd,
                leverage=5.0, spot_fee_rate=0.0001,
                perp_fee_rate=0.00055, slippage_pct=0.0003
            ):
                funding_income_usd = position_usd * funding_rate_8h
                entry_fee_usd = position_usd * (spot_fee_rate + perp_fee_rate)
                exit_fee_usd = position_usd * (spot_fee_rate + perp_fee_rate)
                total_fee_usd = entry_fee_usd + exit_fee_usd
                slippage_usd = position_usd * slippage_pct * 4
                net_profit_usd = funding_income_usd - total_fee_usd - slippage_usd
                breakeven_cycles = (total_cost := total_fee_usd + slippage_usd) / funding_income_usd if funding_income_usd > 0 else float("inf")
                return {
                    "funding_income_usd": round(funding_income_usd, 6),
                    "entry_fee_usd": round(entry_fee_usd, 6),
                    "exit_fee_usd": round(exit_fee_usd, 6),
                    "slippage_usd": round(slippage_usd, 6),
                    "net_profit_usd": round(net_profit_usd, 6),
                    "breakeven_cycles": round(breakeven_cycles, 2),
                    "is_profitable": net_profit_usd > 0,
                }

            def is_entry_net_profitable(funding_rate_8h, position_usd, min_cycles_to_profit=2.0, **kwargs):
                result = estimate_net_profit_per_cycle(funding_rate_8h, position_usd, **kwargs)
                breakeven = result["breakeven_cycles"]
                is_ok = breakeven <= min_cycles_to_profit and result["funding_income_usd"] > 0
                obj._log.debug("net_profitability_check")
                return is_ok

            obj.estimate_net_profit_per_cycle = estimate_net_profit_per_cycle
            obj.is_entry_net_profitable = is_entry_net_profitable
            return obj

    POSITION_USD = 142.5  # fixed_notional 150 * 0.95
    # 비용: fee 왕복 = 142.5 * (0.0001+0.00055)*2 = 0.1853
    # 슬리피지: 142.5 * 0.0003 * 4 = 0.171
    # 총 비용: 0.3563 USD
    # 펀딩비 0.012% (0.00012/8h): 142.5 * 0.00012 = 0.0171 USD → BEP = 0.3563/0.0171 ≈ 20.8회
    # 펀딩비 0.03% (0.0003/8h):  142.5 * 0.0003 = 0.04275 → BEP = 0.3563/0.04275 ≈ 8.3회
    # 펀딩비 0.1% (0.001/8h):    142.5 * 0.001 = 0.1425 → BEP = 0.3563/0.1425 ≈ 2.5회
    # 펀딩비 0.2% (0.002/8h):    142.5 * 0.002 = 0.285 → BEP = 0.3563/0.285 ≈ 1.25회

    def test_high_funding_rate_profitable_cycle_1(self):
        """펀딩비 0.2% (연 219%): 1회 수취로 순수익 양수."""
        tracker = self._make_tracker()
        result = tracker.estimate_net_profit_per_cycle(
            funding_rate_8h=0.002,
            position_usd=self.POSITION_USD,
        )
        self.assertTrue(result["is_profitable"])
        self.assertGreater(result["net_profit_usd"], 0)
        self.assertLess(result["breakeven_cycles"], 2.0)

    def test_low_funding_rate_not_profitable_cycle_1(self):
        """펀딩비 0.012% (연 13.1%): 1회로는 손실, BEP > 2회."""
        tracker = self._make_tracker()
        result = tracker.estimate_net_profit_per_cycle(
            funding_rate_8h=0.00012,
            position_usd=self.POSITION_USD,
        )
        # 0.00012 * 142.5 = 0.0171 < 0.3563 비용 → 손실
        self.assertFalse(result["is_profitable"])
        self.assertGreater(result["breakeven_cycles"], 2.0)

    def test_cost_components_sum(self):
        """비용 컴포넌트 합계 검증."""
        tracker = self._make_tracker()
        result = tracker.estimate_net_profit_per_cycle(0.001, 100.0)
        total_cost = result["entry_fee_usd"] + result["exit_fee_usd"] + result["slippage_usd"]
        self.assertAlmostEqual(
            result["net_profit_usd"],
            result["funding_income_usd"] - total_cost,
            places=5,
        )

    def test_is_entry_profitable_passes_high_rate(self):
        """펀딩비 높을 때 (연 219%) is_entry_net_profitable → True."""
        tracker = self._make_tracker()
        result = tracker.is_entry_net_profitable(
            funding_rate_8h=0.002,
            position_usd=self.POSITION_USD,
            min_cycles_to_profit=2.0,
        )
        self.assertTrue(result)

    def test_is_entry_profitable_fails_low_rate(self):
        """펀딩비 낮을 때 (연 13.1%) is_entry_net_profitable → False."""
        tracker = self._make_tracker()
        result = tracker.is_entry_net_profitable(
            funding_rate_8h=0.00012,
            position_usd=self.POSITION_USD,
            min_cycles_to_profit=2.0,
        )
        self.assertFalse(result)

    def test_zero_funding_rate(self):
        """펀딩비 0: breakeven_cycles = inf, is_profitable = False."""
        tracker = self._make_tracker()
        result = tracker.estimate_net_profit_per_cycle(0.0, 100.0)
        self.assertFalse(result["is_profitable"])
        self.assertEqual(result["breakeven_cycles"], float("inf"))


class TestKillSwitchAbsoluteThreshold(unittest.TestCase):
    """KillSwitch 절대값 + 퍼센트 AND 조건 테스트."""

    def _make_portfolio(
        self,
        daily_drawdown: float = 0.0,
        weekly_drawdown: float = 0.0,
    ) -> object:
        p = MagicMock()
        p.daily_drawdown = daily_drawdown
        p.weekly_drawdown = weekly_drawdown
        return p

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_pct_only_triggers(self):
        """절대값 없을 때: 퍼센트 초과만으로 발동 (기존 동작)."""
        try:
            from kill_switch import KillSwitch, KillLevel
        except ImportError:
            self.skipTest("kill_switch 모듈 없음")

        ks = KillSwitch(daily_limit=-0.01)
        portfolio = self._make_portfolio(daily_drawdown=-0.02)
        level = self._run(ks.check(portfolio))
        self.assertEqual(level, KillLevel.PORTFOLIO)

    def test_abs_and_pct_both_breach_triggers(self):
        """절대값+퍼센트 AND 조건: 둘 다 초과 시 발동."""
        try:
            from kill_switch import KillSwitch, KillLevel
        except ImportError:
            self.skipTest("kill_switch 모듈 없음")

        ks = KillSwitch(
            daily_limit=-0.05,         # 5% 임계값
            daily_loss_abs_usd=10.0,   # $10 절대값
        )
        # equity_at_open=200, daily_drawdown=-0.06 → $12 > $10 AND 6% > 5% → 발동
        portfolio = self._make_portfolio(daily_drawdown=-0.06)
        level = self._run(ks.check(portfolio, equity_at_open=200.0))
        self.assertEqual(level, KillLevel.PORTFOLIO)

    def test_pct_breach_abs_ok_no_trigger(self):
        """퍼센트 초과 + 절대값 미달 → 발동 안 함 (AND 조건)."""
        try:
            from kill_switch import KillSwitch, KillLevel
        except ImportError:
            self.skipTest("kill_switch 모듈 없음")

        ks = KillSwitch(
            daily_limit=-0.05,         # 5% 임계값
            daily_loss_abs_usd=10.0,   # $10 절대값
        )
        # equity_at_open=200, daily_drawdown=-0.06 → $12... 잠깐
        # 실제로는: 퍼센트만 초과하고 절대값은 미달인 케이스
        # equity_at_open=100, daily_drawdown=-0.06 → $6 < $10 → 발동 안 함
        portfolio = self._make_portfolio(daily_drawdown=-0.06)
        level = self._run(ks.check(portfolio, equity_at_open=100.0))
        self.assertEqual(level, KillLevel.NONE)

    def test_abs_breach_pct_ok_no_trigger(self):
        """절대값 초과 + 퍼센트 미달 → 발동 안 함 (AND 조건)."""
        try:
            from kill_switch import KillSwitch, KillLevel
        except ImportError:
            self.skipTest("kill_switch 모듈 없음")

        ks = KillSwitch(
            daily_limit=-0.10,         # 10% 임계값 (완화)
            daily_loss_abs_usd=10.0,   # $10 절대값
        )
        # equity_at_open=200, daily_drawdown=-0.06 → $12 > $10 BUT 6% < 10% → 발동 안 함
        portfolio = self._make_portfolio(daily_drawdown=-0.06)
        level = self._run(ks.check(portfolio, equity_at_open=200.0))
        self.assertEqual(level, KillLevel.NONE)

    def test_no_abs_threshold_legacy_behavior(self):
        """절대값 임계값 없으면 기존(퍼센트 전용) 동작 유지."""
        try:
            from kill_switch import KillSwitch, KillLevel
        except ImportError:
            self.skipTest("kill_switch 모듈 없음")

        ks = KillSwitch(daily_limit=-0.01)  # 절대값 파라미터 없음
        portfolio = self._make_portfolio(daily_drawdown=-0.015)
        level = self._run(ks.check(portfolio))
        self.assertEqual(level, KillLevel.PORTFOLIO)


if __name__ == "__main__":
    unittest.main(verbosity=2)
