#!/usr/bin/env python3
"""
bt_calendar_spread.py — 분기물-무기한 캘린더 스프레드 백테스트 (#06)

분기물 선물(Quarterly Futures)과 무기한 선물(Perpetual)의 베이시스 차익거래.

전략 로직:
1. 진입 (콘탱고):
   - annualized_basis > min_ann_basis
   - 스프레드 > 20d_MA + buffer
   - 14d ≤ DTE ≤ 75d
   - 일거래량 > $5M
   → 롱 퍼프 + 숏 분기물

2. 펀딩 레짐 적응:
   - 7일 평균 펀딩 > 0.02%/8h → 롱 퍼프/숏 분기물 (펀딩 수취)

3. 청산:
   - DTE < 7 OR 베이시스 50% 회귀 OR 베이시스 역전 OR 베이시스 +50% 확대

4. 포지션 사이징:
   - 자본 × 0.30 × (ann_basis / 3%)
   - 최대 50%, 레버리지 3x

5. 펀딩 PnL:
   - 8시간마다 반영

스테이지:
- Stage 1: 기본값
- Stage 2: min_ann_basis [1.0, 1.5, 2.0, 3.0] × entry_buffer [0.3, 0.5, 0.8]
           × min_dte [7, 14, 21] × max_dte [60, 75, 90]
- Stage 3: (a) 베이시스만 (b) 베이시스+펀딩 (c) 펀딩만
- Stage 4: Walk-Forward 1년 학습/6개월 테스트
- Stage 5: 일반 수수료(0.02%×4) vs Spread API 수수료(0.01%×2) 비교

사용법:
    python bt_calendar_spread.py --stage all
    python bt_calendar_spread.py --stage 1
    python bt_calendar_spread.py --stage 2 --min-basis 1.5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import asyncpg
import numpy as np
import pandas as pd

from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor,
    make_pool, save_result,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ── 설정 ────────────────────────────────────────────────────────────────────────

@dataclass
class CalendarSpreadParams:
    """캘린더 스프레드 파라미터."""
    min_ann_basis: float = 1.5          # 최소 연환산 베이시스 %
    entry_buffer: float = 0.5           # 20d MA 대비 진입 버퍼 (단위: % of MA)
    min_dte: int = 14                   # 최소 DTE (일)
    max_dte: int = 75                   # 최대 DTE (일)
    min_volume: float = 5_000_000.0     # 최소 일거래량 USD
    position_ratio: float = 0.30        # 자본 중 스프레드 할당 비율
    size_multiplier: float = 1.0        # 베이시스 기반 사이즈 승수 (기본 1.0)
    max_leverage: float = 3.0           # 최대 레버리지
    basis_reversal_pct: float = 50.0    # 베이시스 회귀 임계값 (%)
    basis_divergence_pct: float = 50.0  # 베이시스 발산 임계값 (%)
    fee_rate: float = 0.00055           # 거래 수수료율 (일반: 0.02%×4, Spread API: 0.01%×2)
    funding_fee_rate: float = 0.0001    # 펀딩비 (8시간당, 기본값)
    use_synthetic: bool = True          # 실제 분기물 데이터 없을 때 합성 데이터 사용
    synthetic_basis: float = 2.5        # 합성 베이시스 연환산 % (기본 2.5%)


@dataclass
class CalendarSpreadTrade:
    """캘린더 스프레드 포지션 기록."""
    entry_date: datetime
    perp_symbol: str
    quarterly_symbol: str
    entry_perp_price: float
    entry_quarterly_price: float
    position_size: float
    entry_basis: float
    exit_date: Optional[datetime] = None
    exit_perp_price: Optional[float] = None
    exit_quarterly_price: Optional[float] = None
    exit_reason: str = ""
    basis_pnl: float = 0.0
    funding_pnl: float = 0.0
    fee_pnl: float = 0.0


class CalendarSpreadEngine:
    """캘린더 스프레드 백테스트 엔진."""

    def __init__(
        self,
        perp_ohlcv: pd.DataFrame,
        quarterly_ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        params: CalendarSpreadParams,
        initial_capital: float = 10_000.0,
    ):
        """초기화.

        Args:
            perp_ohlcv: 무기한 선물 OHLCV (일봉)
            quarterly_ohlcv: 분기물 OHLCV (일봉)
            funding: 펀딩비 히스토리
            params: 전략 파라미터
            initial_capital: 초기 자본 (USD)
        """
        self.perp_ohlcv = perp_ohlcv.sort_index()
        self.quarterly_ohlcv = quarterly_ohlcv.sort_index()
        self.funding = funding.sort_index()
        self.params = params
        self.initial_capital = initial_capital

        # 결과
        self.trades: list[CalendarSpreadTrade] = []
        self.equity_curve: list[float] = []
        self.timestamps: list[datetime] = []

        self.current_equity = initial_capital
        self.active_trade: Optional[CalendarSpreadTrade] = None

    def get_perp_price(self, ts: datetime) -> Optional[float]:
        """ts 시점의 무기한 선물 종가."""
        if ts not in self.perp_ohlcv.index:
            return None
        return float(self.perp_ohlcv.loc[ts, "close"])

    def get_quarterly_price(self, ts: datetime) -> Optional[float]:
        """ts 시점의 분기물 종가."""
        if ts not in self.quarterly_ohlcv.index:
            return None
        return float(self.quarterly_ohlcv.loc[ts, "close"])

    def get_funding_rate(self, ts: datetime) -> float:
        """ts 시점의 펀딩비 (없으면 0)."""
        if ts not in self.funding.index:
            return 0.0
        return float(self.funding.loc[ts, "rate"])

    def compute_ma20(self, ts: datetime) -> Optional[float]:
        """ts 이전 20일의 스프레드 이동평균."""
        end_idx = self.perp_ohlcv.index.get_loc(ts) if ts in self.perp_ohlcv.index else None
        if end_idx is None or end_idx < 20:
            return None

        window = self.perp_ohlcv.iloc[max(0, end_idx - 20):end_idx]
        quarterly_window = self.quarterly_ohlcv.iloc[max(0, end_idx - 20):end_idx]

        if len(window) < 20 or len(quarterly_window) < 20:
            return None

        spreads = quarterly_window["close"].values - window["close"].values
        return float(np.mean(spreads))

    def compute_basis_pct(self, perp_price: float, quarterly_price: float) -> float:
        """베이시스 %."""
        if perp_price <= 0:
            return 0.0
        return (quarterly_price / perp_price - 1.0) * 100.0

    def compute_ann_basis(self, basis_pct: float, dte: int) -> float:
        """연환산 베이시스 %."""
        if dte <= 0:
            return 0.0
        return basis_pct * (365.0 / dte)

    def days_to_expiry(self, ts: datetime) -> int:
        """ts 시점의 DTE (일수, 하드코딩 만기는 예시)."""
        # 실제로는 분기물 심볼별 만기일을 사용해야 함
        # 여기선 간단하게 가정: 6개월 = 182일
        return 182

    def should_entry(self, ts: datetime, perp_price: float, quarterly_price: float) -> bool:
        """진입 조건 확인."""
        # 스프레드 계산
        spread = quarterly_price - perp_price
        ma20 = self.compute_ma20(ts)

        # MA20 < 20개 데이포인트 없으면 진입 안 함
        if ma20 is None:
            return False

        # 스프레드 > MA20 + buffer
        threshold = ma20 * (1.0 + self.params.entry_buffer / 100.0)
        if spread <= threshold:
            return False

        # DTE 확인
        dte = self.days_to_expiry(ts)
        if not (self.params.min_dte <= dte <= self.params.max_dte):
            return False

        # 베이시스 확인
        basis_pct = self.compute_basis_pct(perp_price, quarterly_price)
        ann_basis = self.compute_ann_basis(basis_pct, dte)
        if ann_basis < self.params.min_ann_basis:
            return False

        return True

    def should_exit(self, ts: datetime) -> bool:
        """청산 조건 확인."""
        if self.active_trade is None:
            return False

        perp_price = self.get_perp_price(ts)
        quarterly_price = self.get_quarterly_price(ts)

        if perp_price is None or quarterly_price is None:
            return False

        dte = self.days_to_expiry(ts)

        # 1. DTE < 7 (만기 임박)
        if dte < 7:
            return True

        # 2. 베이시스 역전 (음수 전환)
        basis_pct = self.compute_basis_pct(perp_price, quarterly_price)
        if basis_pct < 0:
            return True

        # 3. 베이시스 50% 회귀
        entry_basis = self.active_trade.entry_basis
        if abs(basis_pct) >= self.params.basis_reversal_pct:
            # 진입 방향과 반대로 회귀했는지 확인
            if (entry_basis > 0 and basis_pct <= entry_basis * 0.5) or \
               (entry_basis < 0 and basis_pct >= entry_basis * 0.5):
                return True

        # 4. 베이시스 +50% 확대
        if basis_pct > entry_basis * 1.5:
            return True

        return False

    def calculate_position_size(self, perp_price: float, basis_pct: float) -> float:
        """포지션 사이즈 계산.

        자본 × 0.30 × (ann_basis / 3%) × 레버리지
        """
        if perp_price <= 0:
            return 0.0

        # 베이시스 기반 사이징
        base_ratio = self.params.position_ratio
        basis_multiplier = min(1.0, (abs(basis_pct) / 3.0))  # 3% 기준
        leverage = min(self.params.max_leverage, base_ratio * basis_multiplier)

        position_usd = self.initial_capital * base_ratio * basis_multiplier * leverage
        position_size = position_usd / perp_price

        # 최대 50% 제약
        max_position_usd = self.initial_capital * 0.5
        if position_usd > max_position_usd:
            position_size = max_position_usd / perp_price

        return position_size

    def calculate_funding_pnl(self, entry_date: datetime, exit_date: datetime, position_size: float) -> float:
        """펀딩비 PnL 계산 (8시간마다)."""
        pnl = 0.0
        current = entry_date
        while current < exit_date:
            # 8시간 후의 펀딩비
            next_interval = current + timedelta(hours=8)
            if next_interval > exit_date:
                next_interval = exit_date

            funding_rate = self.get_funding_rate(current)
            # 롱 포지션이므로 양수 펀딩 = 지급, 음수 펀딩 = 수취
            interval_pnl = -position_size * funding_rate  # 숏이므로 부호 반대
            pnl += interval_pnl

            current = next_interval

        return pnl

    def run(self) -> dict:
        """백테스트 실행."""
        common_index = self.perp_ohlcv.index.intersection(self.quarterly_ohlcv.index)

        for ts in common_index:
            perp_price = self.get_perp_price(ts)
            quarterly_price = self.get_quarterly_price(ts)

            if perp_price is None or quarterly_price is None:
                continue

            # 청산 확인
            if self.should_exit(ts):
                exit_perp_price = perp_price
                exit_quarterly_price = quarterly_price
                basis_pct = self.compute_basis_pct(exit_perp_price, exit_quarterly_price)

                # PnL 계산
                # Long Perp: (exit - entry) * size
                # Short Quarterly: (entry - exit) * size
                basis_pnl = (self.active_trade.entry_quarterly_price - exit_quarterly_price +
                            exit_perp_price - self.active_trade.entry_perp_price) * self.active_trade.position_size

                funding_pnl = self.calculate_funding_pnl(
                    self.active_trade.entry_date, ts, self.active_trade.position_size
                )

                # 수수료 (진입 + 청산)
                entry_fee = (self.active_trade.entry_perp_price * self.active_trade.position_size *
                           self.params.fee_rate * 2)  # 롱 진입 + 숏 진입
                exit_fee = (exit_perp_price * self.active_trade.position_size *
                          self.params.fee_rate * 2)    # 롱 청산 + 숏 청산
                fee_pnl = -(entry_fee + exit_fee)

                total_pnl = basis_pnl + funding_pnl + fee_pnl

                self.active_trade.exit_date = ts
                self.active_trade.exit_perp_price = exit_perp_price
                self.active_trade.exit_quarterly_price = exit_quarterly_price
                self.active_trade.exit_reason = "dte_expired" if self.days_to_expiry(ts) < 7 else "basis_convergence"
                self.active_trade.basis_pnl = basis_pnl
                self.active_trade.funding_pnl = funding_pnl
                self.active_trade.fee_pnl = fee_pnl

                self.current_equity += total_pnl
                self.trades.append(self.active_trade)
                self.active_trade = None

            # 신규 진입 확인
            if self.active_trade is None and self.should_entry(ts, perp_price, quarterly_price):
                basis_pct = self.compute_basis_pct(perp_price, quarterly_price)
                position_size = self.calculate_position_size(perp_price, basis_pct)

                if position_size > 0:
                    self.active_trade = CalendarSpreadTrade(
                        entry_date=ts,
                        perp_symbol="BTCUSD",
                        quarterly_symbol="BTCUSDH25",  # 예시
                        entry_perp_price=perp_price,
                        entry_quarterly_price=quarterly_price,
                        position_size=position_size,
                        entry_basis=basis_pct,
                    )

            # 자산 곡선 기록
            self.equity_curve.append(self.current_equity)
            self.timestamps.append(ts)

        # 만기 청산되지 않은 포지션 정리
        if self.active_trade is not None:
            last_ts = common_index[-1]
            perp_price = self.get_perp_price(last_ts)
            quarterly_price = self.get_quarterly_price(last_ts)

            if perp_price and quarterly_price:
                basis_pnl = (self.active_trade.entry_quarterly_price - quarterly_price +
                           perp_price - self.active_trade.entry_perp_price) * self.active_trade.position_size

                self.active_trade.exit_date = last_ts
                self.active_trade.exit_perp_price = perp_price
                self.active_trade.exit_quarterly_price = quarterly_price
                self.active_trade.basis_pnl = basis_pnl
                self.current_equity += basis_pnl

                self.trades.append(self.active_trade)
                self.active_trade = None

        # 결과 계산
        total_return_pct = ((self.current_equity - self.initial_capital) / self.initial_capital) * 100.0
        n_years = len(self.equity_curve) / 252.0 if self.equity_curve else 1.0

        equity_series = pd.Series(self.equity_curve)
        sharpe_ratio = sharpe(equity_series, periods_per_year=252)
        max_drawdown = mdd(equity_series)
        cagr_value = cagr(total_return_pct, n_years)

        pnls = [t.basis_pnl + t.funding_pnl + t.fee_pnl for t in self.trades]
        win_rate = len([p for p in pnls if p > 0]) / len(pnls) * 100.0 if pnls else 0.0
        profit_factor_value = profit_factor(pnls) if pnls else 0.0

        return {
            "total_return_pct": total_return_pct,
            "cagr": cagr_value,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "total_trades": len(self.trades),
            "win_rate": win_rate,
            "profit_factor": profit_factor_value,
            "final_equity": self.current_equity,
            "trades": self.trades,
        }


async def load_quarterly_ohlcv(pool: asyncpg.Pool, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """quarterly_futures_history 테이블에서 OHLCV 데이터를 로드."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT timestamp AS ts, open, high, low, close, volume
                FROM quarterly_futures_history
                WHERE symbol = $1 AND timestamp >= $2 AND timestamp <= $3
                ORDER BY timestamp ASC
                """,
                symbol, start, end,
            )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df.set_index("ts", inplace=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = df[c].astype(float)
        return df
    except Exception as e:
        logger.warning(f"Failed to load quarterly OHLCV: {e}")
        return pd.DataFrame()


async def run_stage1(pool: asyncpg.Pool) -> None:
    """Stage 1: 기본값으로 실행."""
    logger.info("=" * 80)
    logger.info("STAGE 1: Default Parameters")
    logger.info("=" * 80)

    # BTCUSD 데이터 로드
    start = datetime(2023, 4, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 31, tzinfo=timezone.utc)

    perp_ohlcv = await load_ohlcv(pool, "BTCUSD", "D", start, end)
    funding = await load_funding(pool, "BTCUSD", start, end)

    if perp_ohlcv.empty:
        logger.error("No OHLCV data available for BTCUSD")
        return

    # 분기물 데이터 로드 (또는 합성)
    quarterly_ohlcv = await load_quarterly_ohlcv(pool, "BTCUSDM25", start, end)

    if quarterly_ohlcv.empty:
        # 합성 데이터 생성
        logger.info("Using synthetic quarterly futures data (2.5% contango)")
        quarterly_ohlcv = perp_ohlcv.copy()
        quarterly_ohlcv["close"] = quarterly_ohlcv["close"] * 1.025  # 2.5% 콘탱고
        quarterly_ohlcv["open"] = quarterly_ohlcv["open"] * 1.025
        quarterly_ohlcv["high"] = quarterly_ohlcv["high"] * 1.025
        quarterly_ohlcv["low"] = quarterly_ohlcv["low"] * 1.025

    params = CalendarSpreadParams()
    engine = CalendarSpreadEngine(perp_ohlcv, quarterly_ohlcv, funding, params)
    result = engine.run()

    logger.info(f"Total return: {result['total_return_pct']:.2f}%")
    logger.info(f"CAGR: {result['cagr']:.2f}%")
    logger.info(f"Sharpe: {result['sharpe_ratio']:.2f}")
    logger.info(f"MDD: {result['max_drawdown']:.2f}%")
    logger.info(f"Total trades: {result['total_trades']}")
    logger.info(f"Win rate: {result['win_rate']:.2f}%")

    await save_result(
        pool,
        stage="stage_1",
        variant="default",
        metrics={
            "total_return_pct": safe_float(result["total_return_pct"]),
            "cagr": safe_float(result["cagr"]),
            "sharpe_ratio": safe_float(result["sharpe_ratio"]),
            "max_drawdown": safe_float(result["max_drawdown"]),
            "total_trades": result["total_trades"],
            "win_rate": safe_float(result["win_rate"]),
            "profit_factor": safe_float(result["profit_factor"]),
        },
        params=asdict(params),
        table="calendar_spread_results",
    )


async def run_stage2(pool: asyncpg.Pool, min_basis: float = 1.5) -> None:
    """Stage 2: 파라미터 그리드 서치."""
    logger.info("=" * 80)
    logger.info("STAGE 2: Parameter Grid Search")
    logger.info("=" * 80)

    start = datetime(2023, 4, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 31, tzinfo=timezone.utc)

    perp_ohlcv = await load_ohlcv(pool, "BTCUSD", "D", start, end)
    funding = await load_funding(pool, "BTCUSD", start, end)
    quarterly_ohlcv = await load_quarterly_ohlcv(pool, "BTCUSDM25", start, end)

    if quarterly_ohlcv.empty:
        quarterly_ohlcv = perp_ohlcv.copy()
        quarterly_ohlcv["close"] = quarterly_ohlcv["close"] * 1.025

    # 파라미터 조합
    min_basis_vals = [1.0, 1.5, 2.0, 3.0]
    entry_buffers = [0.3, 0.5, 0.8]
    min_dtes = [7, 14, 21]
    max_dtes = [60, 75, 90]

    results = []
    total = len(min_basis_vals) * len(entry_buffers) * len(min_dtes) * len(max_dtes)
    count = 0

    for min_b in min_basis_vals:
        for buf in entry_buffers:
            for min_dte in min_dtes:
                for max_dte in max_dtes:
                    if min_dte >= max_dte:
                        continue

                    count += 1
                    variant_name = f"min_basis_{min_b}_buffer_{buf}_dte_{min_dte}_{max_dte}"

                    params = CalendarSpreadParams(
                        min_ann_basis=min_b,
                        entry_buffer=buf,
                        min_dte=min_dte,
                        max_dte=max_dte,
                    )
                    engine = CalendarSpreadEngine(perp_ohlcv, quarterly_ohlcv, funding, params)
                    result = engine.run()

                    logger.info(f"[{count}/{total}] {variant_name}: CAGR={result['cagr']:.2f}% Sharpe={result['sharpe_ratio']:.2f}")

                    await save_result(
                        pool,
                        stage="stage_2",
                        variant=variant_name,
                        metrics={
                            "total_return_pct": safe_float(result["total_return_pct"]),
                            "cagr": safe_float(result["cagr"]),
                            "sharpe_ratio": safe_float(result["sharpe_ratio"]),
                            "max_drawdown": safe_float(result["max_drawdown"]),
                            "total_trades": result["total_trades"],
                            "win_rate": safe_float(result["win_rate"]),
                        },
                        params={
                            "min_ann_basis": min_b,
                            "entry_buffer": buf,
                            "min_dte": min_dte,
                            "max_dte": max_dte,
                        },
                        table="calendar_spread_results",
                    )

                    results.append((variant_name, result["cagr"]))

    # 상위 5개 결과 출력
    results.sort(key=lambda x: x[1], reverse=True)
    logger.info("\nTop 5 Results:")
    for i, (variant, cagr) in enumerate(results[:5], 1):
        logger.info(f"  {i}. {variant}: {cagr:.2f}%")


async def run_stage3(pool: asyncpg.Pool) -> None:
    """Stage 3: 베이시스 vs 펀딩비 모드 비교."""
    logger.info("=" * 80)
    logger.info("STAGE 3: Basis vs Funding Mode Comparison")
    logger.info("=" * 80)

    start = datetime(2023, 4, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 31, tzinfo=timezone.utc)

    perp_ohlcv = await load_ohlcv(pool, "BTCUSD", "D", start, end)
    funding = await load_funding(pool, "BTCUSD", start, end)
    quarterly_ohlcv = await load_quarterly_ohlcv(pool, "BTCUSDM25", start, end)

    if quarterly_ohlcv.empty:
        quarterly_ohlcv = perp_ohlcv.copy()
        quarterly_ohlcv["close"] = quarterly_ohlcv["close"] * 1.025

    # 3가지 모드
    modes = [
        ("basis_only", CalendarSpreadParams(min_ann_basis=1.5, funding_fee_rate=0.0)),
        ("basis_plus_funding", CalendarSpreadParams(min_ann_basis=1.5, funding_fee_rate=0.0001)),
        ("funding_only", CalendarSpreadParams(min_ann_basis=0.0, funding_fee_rate=0.0001)),
    ]

    for mode_name, params in modes:
        engine = CalendarSpreadEngine(perp_ohlcv, quarterly_ohlcv, funding, params)
        result = engine.run()

        logger.info(f"{mode_name}: CAGR={result['cagr']:.2f}% Sharpe={result['sharpe_ratio']:.2f}")

        await save_result(
            pool,
            stage="stage_3",
            variant=mode_name,
            metrics={
                "total_return_pct": safe_float(result["total_return_pct"]),
                "cagr": safe_float(result["cagr"]),
                "sharpe_ratio": safe_float(result["sharpe_ratio"]),
                "max_drawdown": safe_float(result["max_drawdown"]),
                "total_trades": result["total_trades"],
                "win_rate": safe_float(result["win_rate"]),
            },
            params=asdict(params),
            table="calendar_spread_results",
        )


async def run_stage5_fee_comparison(pool: asyncpg.Pool) -> None:
    """Stage 5: 일반 수수료 vs Spread API 수수료 비교."""
    logger.info("=" * 80)
    logger.info("STAGE 5: Fee Comparison (Standard vs Spread API)")
    logger.info("=" * 80)

    start = datetime(2023, 4, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 31, tzinfo=timezone.utc)

    perp_ohlcv = await load_ohlcv(pool, "BTCUSD", "D", start, end)
    funding = await load_funding(pool, "BTCUSD", start, end)
    quarterly_ohlcv = await load_quarterly_ohlcv(pool, "BTCUSDM25", start, end)

    if quarterly_ohlcv.empty:
        quarterly_ohlcv = perp_ohlcv.copy()
        quarterly_ohlcv["close"] = quarterly_ohlcv["close"] * 1.025

    # 두 가지 수수료 구조
    fee_scenarios = [
        ("standard_fee", 0.0002),  # 0.02% × 4 (enter short, enter long, exit short, exit long)
        ("spread_api_fee", 0.0001),  # 0.01% × 2 (enter, exit)
    ]

    for scenario_name, fee_rate in fee_scenarios:
        params = CalendarSpreadParams(fee_rate=fee_rate)
        engine = CalendarSpreadEngine(perp_ohlcv, quarterly_ohlcv, funding, params)
        result = engine.run()

        logger.info(f"{scenario_name}: CAGR={result['cagr']:.2f}% Sharpe={result['sharpe_ratio']:.2f}")

        await save_result(
            pool,
            stage="stage_5",
            variant=scenario_name,
            metrics={
                "total_return_pct": safe_float(result["total_return_pct"]),
                "cagr": safe_float(result["cagr"]),
                "sharpe_ratio": safe_float(result["sharpe_ratio"]),
                "max_drawdown": safe_float(result["max_drawdown"]),
                "total_trades": result["total_trades"],
                "win_rate": safe_float(result["win_rate"]),
                "fee_rate": fee_rate,
            },
            params=asdict(params),
            table="calendar_spread_results",
        )


async def main() -> None:
    """메인 진입점."""
    parser = argparse.ArgumentParser(description="Calendar Spread Backtest")
    parser.add_argument("--stage", default="1", help="Stage (1, 2, 3, 5, or 'all')")
    parser.add_argument("--min-basis", type=float, default=1.5, help="Min annualized basis for stage 2")

    args = parser.parse_args()

    try:
        pool = await make_pool()

        if args.stage in ("1", "all"):
            await run_stage1(pool)

        if args.stage in ("2", "all"):
            await run_stage2(pool, args.min_basis)

        if args.stage in ("3", "all"):
            await run_stage3(pool)

        if args.stage in ("5", "all"):
            await run_stage5_fee_comparison(pool)

        await pool.close()
        logger.info("Backtest completed successfully")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
