"""bt_volatility_squeeze_simple.py — 간단한 테스트 버전"""
from __future__ import annotations

import sys
sys.path.insert(0, "/app")

import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd
import numpy as np

from tests.backtest.core import (
    load_ohlcv,
    sharpe, mdd, cagr, safe_float,
    make_pool, save_result,
)

from squeeze_indicator import compute_squeeze

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOL = "BTCUSDT"
START_DT = datetime(2023, 4, 1, tzinfo=timezone.utc)
END_DT = datetime(2026, 4, 10, tzinfo=timezone.utc)
INITIAL_CAPITAL = 5_000.0

MAKER_FEE = 0.0002
TAKER_FEE = 0.00055
SLIPPAGE = 0.0003


class SimpleVSBacktester:
    """간단한 변동성 스퀴즈 백테스터 - HMM 필터 없음."""

    def __init__(self, df_1d: pd.DataFrame, initial_capital: float = 5_000.0):
        self.df_1d = df_1d.copy()
        self.initial_capital = initial_capital
        self.equity = initial_capital
        self.equity_curve = [initial_capital]
        self.trades = []
        self.position = None

    def _compute_indicators(self):
        """지표 계산."""
        self.df_1d = compute_squeeze(self.df_1d, bb_std=2.0, kc_atr_mult=1.5)
        self.df_1d["volume_ma20"] = self.df_1d["volume"].rolling(20).mean()

    def run(self) -> dict:
        """백테스트 실행."""
        self._compute_indicators()
        bars_1d = self.df_1d.reset_index()
        n = len(bars_1d)

        for idx in range(20, n):
            bar = bars_1d.iloc[idx]

            # 포지션 보유 중
            if self.position:
                exit_signal = self._check_exit(bar)
                if exit_signal:
                    self._close_position(bar)
                continue

            # 신규 진입
            entry_signal = self._check_entry(bar)
            if entry_signal:
                self._open_position(bar, entry_signal)

        # 미청산 포지션
        if self.position and len(bars_1d) > 0:
            last_bar = bars_1d.iloc[-1]
            self._close_position(last_bar)

        return self._calculate_metrics()

    def _check_entry(self, bar) -> bool:
        """진입 조건."""
        close = bar["close"]
        volume = bar["volume"]
        volume_ma20 = bar["volume_ma20"]
        squeeze_off = bar["squeeze_off"]
        bb_upper = bar["bb_upper"]
        bb_lower = bar["bb_lower"]
        rsi = bar["rsi"]

        if pd.isna(squeeze_off) or not squeeze_off:
            return False
        if pd.isna(volume_ma20) or volume < volume_ma20 * 1.5:
            return False

        # 롱 또는 숏
        if close > bb_upper and rsi >= 50:
            return "long"
        if close < bb_lower and rsi <= 50:
            return "short"
        return False

    def _open_position(self, bar, pos_type: str):
        """포지션 진입."""
        close = bar["close"]
        atr = bar["atr"]

        if pos_type == "long":
            entry_price = close * (1 + SLIPPAGE)
            stop_loss = entry_price - atr * 2.0
            tp_target = bar["bb_upper"]
        else:
            entry_price = close * (1 - SLIPPAGE)
            stop_loss = entry_price + atr * 2.0
            tp_target = bar["bb_lower"]

        btc_size = (self.equity * 0.25) / entry_price
        notional = btc_size * entry_price

        # 수수료
        fee = notional * 0.00065
        self.equity -= fee

        self.position = {
            "ts": bar["ts"],
            "type": pos_type,
            "entry_price": entry_price,
            "size_btc": btc_size,
            "notional": notional,
            "stop_loss": stop_loss,
            "tp_target": tp_target,
        }

        logger.info(f"진입: {pos_type.upper()} @ {entry_price:.2f} (BTC {btc_size:.6f})")

    def _check_exit(self, bar) -> bool:
        """청산 조건."""
        if not self.position:
            return False

        close = bar["close"]
        ptype = self.position["type"]
        sl = self.position["stop_loss"]
        tp = self.position["tp_target"]

        if ptype == "long" and close <= sl:
            return True
        if ptype == "long" and close >= tp:
            return True
        if ptype == "short" and close >= sl:
            return True
        if ptype == "short" and close <= tp:
            return True

        return False

    def _close_position(self, bar):
        """청산."""
        if not self.position:
            return

        close = bar["close"]
        ptype = self.position["type"]
        entry_price = self.position["entry_price"]
        size_btc = self.position["size_btc"]
        notional = self.position["notional"]
        entry_ts = self.position["ts"]

        if ptype == "long":
            pnl = size_btc * (close - entry_price)
        else:
            pnl = size_btc * (entry_price - close)

        fee = (size_btc * close) * 0.00055
        pnl -= fee

        self.position = None
        self.equity += pnl

        self.trades.append({
            "entry_ts": entry_ts,
            "exit_ts": bar["ts"],
            "type": ptype,
            "entry_price": entry_price,
            "exit_price": close,
            "size_btc": size_btc,
            "pnl": pnl,
        })

        self.equity_curve.append(self.equity)
        logger.info(f"청산: {ptype.upper()} @ {close:.2f}, PnL: {pnl:.2f} USD")

    def _calculate_metrics(self) -> dict:
        """지표 계산."""
        equity_series = pd.Series(self.equity_curve)
        total_return = (self.equity - self.initial_capital) / self.initial_capital
        total_return_pct = total_return * 100

        n_days = (END_DT - START_DT).days
        n_years = n_days / 365.25

        return {
            "total_return_pct": safe_float(total_return_pct),
            "cagr_pct": safe_float(cagr(total_return_pct, n_years)),
            "sharpe_ratio": safe_float(sharpe(equity_series, periods_per_year=252)),
            "max_drawdown_pct": safe_float(mdd(equity_series)),
            "final_equity": safe_float(self.equity),
            "trade_count": len(self.trades),
            "win_rate": safe_float(
                len([t for t in self.trades if t["pnl"] > 0]) / len(self.trades) * 100
                if self.trades else 0.0
            ),
        }


async def main():
    """메인."""
    pool = await make_pool()

    logger.info("데이터 로드 중...")
    df_1d = await load_ohlcv(pool, SYMBOL, "1d", START_DT, END_DT)
    await pool.close()

    if df_1d.empty:
        logger.error("데이터 로드 실패")
        return

    logger.info(f"데이터 로드 완료: {len(df_1d)}개 바")

    bt = SimpleVSBacktester(df_1d, INITIAL_CAPITAL)
    metrics = bt.run()

    logger.info(f"\n=== 결과 ===")
    logger.info(f"거래 수: {metrics['trade_count']}")
    logger.info(f"총 수익률: {metrics['total_return_pct']:.2f}%")
    logger.info(f"CAGR: {metrics['cagr_pct']:.2f}%")
    logger.info(f"Sharpe: {metrics['sharpe_ratio']:.3f}")
    logger.info(f"MDD: {metrics['max_drawdown_pct']:.2f}%")
    logger.info(f"승률: {metrics['win_rate']:.2f}%")


if __name__ == "__main__":
    asyncio.run(main())
