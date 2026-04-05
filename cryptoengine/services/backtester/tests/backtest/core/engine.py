"""core.engine — 백테스트 기본 엔진 클래스.

사용법:
    from core.engine import FAEngine, SimpleBacktester

FAEngine: FA (펀딩비 차익거래) 델타뉴트럴 백테스트 엔진.
    - fa_capital_ratio: FA에 할당할 자본 비율 (기본 0.50)
    - leverage:         레버리지 (기본 2.0)
    - reinvest_ratio:   수익 중 BTC 현물 재투자 비율 (기본 0.0)
    - 펀딩비 3회 연속 양수 → 진입, 3회 연속 역전 → 청산

SimpleBacktester: 범용 이벤트 기반 백테스터 (시그널 함수 주입 방식).
    - signal_fn(ohlcv_row, state) -> "buy" | "sell" | "close" | None
"""
from __future__ import annotations

import math
from typing import Any, Callable

import pandas as pd

from .metrics import mdd, profit_factor, safe_float, sharpe


# ── FAEngine ──────────────────────────────────────────────────────────────────

class FAEngine:
    """FA short_hold 변형 백테스트 엔진.

    파라미터:
        ohlcv           OHLCV DataFrame (index=ts)
        funding         펀딩비 DataFrame (index=ts, col=rate)
        initial_capital 초기 자본 USDT
        fa_capital_ratio FA에 사용할 자본 비율 (0.0~1.0)
        leverage        선물 레버리지 배수
        reinvest_ratio  수익의 현물 BTC 재투자 비율 (0.0=없음)
        fee_rate        수수료율 (기본 0.00055 = Bybit taker)
        min_funding_rate 진입 최소 펀딩비 임계값 (기본 0.0001 = 0.01%)
        consecutive_intervals 진입 필요 연속 구간 수 (기본 3)
        max_hold_bars   최대 보유 바 수 (기본 168 = 7일)
    """

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float = 10_000.0,
        fa_capital_ratio: float = 0.50,
        leverage: float = 2.0,
        reinvest_ratio: float = 0.0,
        fee_rate: float = 0.00055,
        min_funding_rate: float = 0.0001,
        consecutive_intervals: int = 3,
        max_hold_bars: int = 168,
    ) -> None:
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._capital  = initial_capital
        self._equity   = initial_capital
        self._fa_ratio = fa_capital_ratio
        self._leverage = leverage
        self._reinv    = reinvest_ratio
        self._fee      = fee_rate
        self._min_rate = min_funding_rate
        self._consec   = consecutive_intervals
        self._max_hold = max_hold_bars

        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[dict]        = []
        self._position: dict | None     = None
        self._spot_btc: float           = 0.0
        self._total_reinvested: float   = 0.0

    # ── 실행 ──────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        """6년 (혹은 지정 기간) 전체 백테스트 실행.

        Returns:
            dict: 지표 딕셔너리 (total_profit_pct, sharpe_ratio, max_drawdown_pct, ...)
        """
        bars  = self._ohlcv.reset_index()
        n     = len(bars)
        pos_c = neg_c = 0

        for idx in range(20, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding(bar)
            ts      = bar["ts"]

            # 정산 시각 판단 (8h 주기: 00:00 / 08:00 / 16:00 UTC)
            is_settle = self._is_settlement(ts, idx)

            # 보유 중 펀딩비 정산
            if self._position and is_settle:
                self._settle_funding(funding)

            # 포지션 없음 → 진입 판단
            if self._position is None:
                if is_settle:
                    if funding >= self._min_rate:
                        pos_c += 1; neg_c = 0
                    elif funding <= -self._min_rate:
                        neg_c += 1; pos_c = 0
                    else:
                        pos_c = neg_c = 0

                    if pos_c >= self._consec:
                        self._open(bar, "sell", idx)
                        pos_c = 0
                    elif neg_c >= self._consec:
                        self._open(bar, "buy", idx)
                        neg_c = 0
            else:
                # 포지션 있음 → 청산 판단
                direction = self._position["funding_direction"]
                bars_held = idx - self._position["entry_idx"]
                rev = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)

                if is_settle:
                    if rev:
                        self._position["rev_cnt"] = self._position.get("rev_cnt", 0) + 1
                    else:
                        self._position["rev_cnt"] = 0
                    if self._position["rev_cnt"] >= 3 or bars_held >= self._max_hold:
                        self._close(bar)
                        pos_c = neg_c = 0
                elif bars_held >= self._max_hold:
                    self._close(bar)
                    pos_c = neg_c = 0

            self._equity_curve.append(self._equity)

        if self._position:
            self._close(bars.iloc[-1])
            if self._equity_curve:
                self._equity_curve[-1] = self._equity

        return self._build_result()

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _is_settlement(self, ts, idx: int) -> bool:
        try:
            t = pd.Timestamp(ts)
            if t.tzinfo is None:
                t = t.tz_localize("UTC")
            return (t.hour % 8 == 0) and (t.minute == 0)
        except Exception:
            return (idx % 8 == 0)

    def _get_funding(self, bar) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        try:
            ts   = pd.Timestamp(bar["ts"])
            mask = self._funding.index <= ts
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    def _settle_funding(self, funding: float) -> None:
        direction = self._position["funding_direction"]
        notional  = self._position["notional"]
        net       = notional * funding * direction
        self._equity += net
        self._position["funding_acc"] = self._position.get("funding_acc", 0.0) + net

    def _open(self, bar, side: str, idx: int) -> None:
        entry    = float(bar["close"])
        fa_cap   = self._equity * self._fa_ratio
        notional = fa_cap * self._leverage
        fee      = notional * self._fee
        self._equity -= fee
        self._position = {
            "side":             side,
            "entry_price":      entry,
            "notional":         notional,
            "entry_ts":         bar["ts"],
            "entry_idx":        idx,
            "fee_paid":         fee,
            "funding_direction": 1 if side == "sell" else -1,
            "funding_acc":      0.0,
            "rev_cnt":          0,
        }

    def _close(self, bar) -> None:
        if not self._position:
            return
        notional   = self._position["notional"]
        exit_price = float(bar["close"])
        fee_exit   = notional * self._fee
        self._equity -= fee_exit
        pnl = (
            self._position.get("funding_acc", 0.0)
            - self._position.get("fee_paid", 0.0)
            - fee_exit
        )
        # 수익 재투자
        if pnl > 0 and self._reinv > 0 and exit_price > 0:
            amt = pnl * self._reinv
            self._spot_btc += amt / exit_price
            self._total_reinvested += amt
            self._equity -= amt

        self._trades.append({
            "pnl":        pnl,
            "entry_ts":   str(self._position.get("entry_ts", "")),
            "close_ts":   str(bar["ts"]),
            "entry_price": self._position["entry_price"],
            "exit_price":  exit_price,
            "notional":    notional,
        })
        self._position = None

    def _build_result(self) -> dict:
        total = self._equity - self._capital
        eq    = pd.Series(self._equity_curve)
        pnls  = [t["pnl"] for t in self._trades]
        wins  = [p for p in pnls if p > 0]

        final_price  = float(self._ohlcv["close"].iloc[-1])
        spot_value   = self._spot_btc * final_price
        portfolio_eq = self._equity + spot_value

        return {
            "initial_capital":    self._capital,
            "final_equity":       round(self._equity, 4),
            "total_profit":       round(total, 4),
            "total_profit_pct":   round(total / self._capital * 100, 4),
            "portfolio_ret_pct":  round((portfolio_eq - self._capital) / self._capital * 100, 4),
            "spot_btc":           round(self._spot_btc, 8),
            "spot_value":         round(spot_value, 4),
            "total_reinvested":   round(self._total_reinvested, 4),
            "sharpe_ratio":       round(sharpe(eq), 4),
            "max_drawdown_pct":   round(mdd(eq), 4),
            "profit_factor":      safe_float(profit_factor(pnls), 0.0),
            "win_rate":           round(len(wins) / len(pnls) * 100, 2) if pnls else 0.0,
            "total_trades":       len(self._trades),
            "trades":             self._trades,
            "equity_curve":       self._equity_curve,
        }


# ── SimpleBacktester ──────────────────────────────────────────────────────────

class SimpleBacktester:
    """시그널 함수 주입 방식 범용 백테스터.

    사용법:
        def my_signal(idx, bars, state):
            # 반환: "buy" | "sell" | "close" | None
            ...

        bt = SimpleBacktester(ohlcv, initial_capital=10000, fee_rate=0.00055)
        result = bt.run(my_signal)

    Args:
        ohlcv:          OHLCV DataFrame
        initial_capital 초기 자본
        fee_rate:       수수료율
        position_ratio: 자본 중 포지션 비율 (기본 0.95)
    """

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        initial_capital: float = 10_000.0,
        fee_rate: float = 0.00055,
        position_ratio: float = 0.95,
    ) -> None:
        self._ohlcv    = ohlcv
        self._capital  = initial_capital
        self._equity   = initial_capital
        self._fee      = fee_rate
        self._pos_rat  = position_ratio
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[dict] = []
        self._position: dict | None = None

    def run(
        self,
        signal_fn: Callable[[int, pd.DataFrame, dict], str | None],
    ) -> dict:
        """백테스트 실행.

        Args:
            signal_fn: (idx, bars, state) → "buy" | "sell" | "close" | None

        Returns:
            결과 dict
        """
        bars  = self._ohlcv.reset_index()
        n     = len(bars)
        state: dict[str, Any] = {}

        for idx in range(20, n):
            signal = signal_fn(idx, bars, state)
            bar    = bars.iloc[idx]
            price  = float(bar["close"])

            if self._position is None:
                if signal in ("buy", "sell"):
                    size = (self._equity * self._pos_rat) / price
                    fee  = price * size * self._fee
                    self._equity -= fee
                    self._position = {
                        "side":        signal,
                        "entry_price": price,
                        "size":        size,
                        "entry_ts":    str(bar.get("ts", idx)),
                        "fee_in":      fee,
                    }
            else:
                # 청산
                should_close = (signal == "close") or (
                    (self._position["side"] == "buy"  and signal == "sell") or
                    (self._position["side"] == "sell" and signal == "buy")
                )
                if should_close:
                    size      = self._position["size"]
                    fee_out   = price * size * self._fee
                    self._equity -= fee_out
                    entry     = self._position["entry_price"]
                    if self._position["side"] == "buy":
                        pnl = (price - entry) * size - self._position["fee_in"] - fee_out
                    else:
                        pnl = (entry - price) * size - self._position["fee_in"] - fee_out
                    self._equity += pnl
                    self._trades.append({
                        "pnl":        pnl,
                        "entry_ts":   self._position["entry_ts"],
                        "close_ts":   str(bar.get("ts", idx)),
                        "entry_price": entry,
                        "exit_price":  price,
                        "side":        self._position["side"],
                    })
                    self._position = None

            self._equity_curve.append(self._equity)

        # 미청산 강제 종료
        if self._position:
            last = bars.iloc[-1]
            price = float(last["close"])
            size  = self._position["size"]
            fee   = price * size * self._fee
            self._equity -= fee
            entry = self._position["entry_price"]
            if self._position["side"] == "buy":
                pnl = (price - entry) * size - self._position["fee_in"] - fee
            else:
                pnl = (entry - price) * size - self._position["fee_in"] - fee
            self._equity += pnl
            self._trades.append({
                "pnl": pnl,
                "entry_ts": self._position["entry_ts"],
                "close_ts": str(last.get("ts", n)),
                "entry_price": entry,
                "exit_price": price,
                "side": self._position["side"],
            })
            if self._equity_curve:
                self._equity_curve[-1] = self._equity

        return self._build_result()

    def _build_result(self) -> dict:
        total = self._equity - self._capital
        eq    = pd.Series(self._equity_curve)
        pnls  = [t["pnl"] for t in self._trades]
        wins  = [p for p in pnls if p > 0]

        return {
            "initial_capital":  self._capital,
            "final_equity":     round(self._equity, 4),
            "total_profit":     round(total, 4),
            "total_profit_pct": round(total / self._capital * 100, 4),
            "sharpe_ratio":     round(sharpe(eq), 4),
            "max_drawdown_pct": round(mdd(eq), 4),
            "profit_factor":    safe_float(profit_factor(pnls), 0.0),
            "win_rate":         round(len(wins) / len(pnls) * 100, 2) if pnls else 0.0,
            "total_trades":     len(self._trades),
            "trades":           self._trades,
            "equity_curve":     self._equity_curve,
        }
