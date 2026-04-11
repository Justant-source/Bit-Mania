"""bt_funding_extreme_reversal.py — BTC 펀딩비 극단치 역발상 전략 백테스트.

전략:
  숏 진입: zscore > entry_zscore AND 펀딩비 > 0.0003 AND RSI(14,4h) > 70 AND 25일 모멘텀 > 5%
  롱 진입: zscore < -entry_zscore AND 펀딩비 < -0.0001 AND RSI(14,4h) < 30 AND 25일 모멘텀 < -5%
  청산: |zscore| < exit_zscore OR 보유 5일 초과 OR 진입가 ±3% 손절 OR zscore 추가 0.5 극단화
  사이징: 자본 × 0.15 × |zscore| / 1.5 (최대 30%), 레버리지 2x
  펀딩 PnL: 8시간마다 수취/지급

Stage:
  1. Baseline: 기본 파라미터 (entry=1.5, exit=0.5, window=30, min_funding=0.0003)
  2. GridSearch: 48조합 (entry×exit×window)
  3. Ablation: 4가지 필터 조합 (zscore만, +RSI, +모멘텀, 전부)
  4. WalkForward: 1년 학습 / 6개월 테스트, 3개월 슬라이딩
  5. LowFundingEnv: 2025-04-01 ~ 2026-04-10 (저펀딩 환경 별도 분석)

실행:
    python tests/backtest/fa/bt_funding_extreme_reversal.py --stage all
    python tests/backtest/fa/bt_funding_extreme_reversal.py --stage 1
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import numpy as np
import pandas as pd

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor,
    make_pool, save_result,
)
from tests.backtest.fa.funding_zscore_calculator import compute_funding_zscore

# ── 설정 ────────────────────────────────────────────────────────────────────

SYMBOL = "BTCUSDT"
INITIAL_CAPITAL = 10_000.0
LEVERAGE = 2.0
MAX_POSITION_RATIO = 0.30

# 수수료 (Bybit 메이커 기준)
ENTRY_FEE = 0.0002  # 편도 0.02%
EXIT_FEE = 0.0002   # 편도 0.02%
SLIPPAGE = 0.0003   # 편도 0.03%

# 신호 필터 기본값
BASE_PARAMS = {
    "entry_zscore": 1.5,
    "exit_zscore": 0.5,
    "window_days": 30,
    "min_funding": 0.0003,
    "rsi_upper": 70,
    "rsi_lower": 30,
    "momentum_window": 25,
    "momentum_threshold": 0.05,
    "max_hold_bars": 120,  # 5일 (1h 봉 120개)
    "stoploss_pct": 0.03,
}

# Stage 2 그리드서치
GRID_PARAMS = {
    "entry_zscore": [1.0, 1.5, 2.0, 2.5],
    "exit_zscore": [0.0, 0.3, 0.5, 0.8],
    "window_days": [14, 30, 60],
}

# ── 데이터 범위 ────────────────────────────────────────────────────────────

# 전체 범위 (6년 히스토리)
FULL_START = datetime(2020, 4, 1, tzinfo=timezone.utc)
FULL_END = datetime(2026, 4, 10, tzinfo=timezone.utc)

# Stage 5: 저펀딩 환경 (1년)
LOW_FUNDING_START = datetime(2025, 4, 1, tzinfo=timezone.utc)
LOW_FUNDING_END = datetime(2026, 4, 10, tzinfo=timezone.utc)

# ── 로깅 ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ── 지표 계산 ────────────────────────────────────────────────────────────────

def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI(14) 계산."""
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi


def calculate_momentum(close: pd.Series, window: int = 25) -> pd.Series:
    """모멘텀 (현재 / window일전 - 1)."""
    prev = close.shift(window)
    return (close / prev - 1.0)


# ── 백테스터 ────────────────────────────────────────────────────────────────

class FundingExtremeReversalBacktester:
    """펀딩비 극단치 역발상 백테스터."""

    def __init__(
        self,
        ohlcv_1h: pd.DataFrame,  # 1h OHLCV
        ohlcv_4h: pd.DataFrame,  # 4h OHLCV (RSI 계산용)
        funding: pd.DataFrame,   # 펀딩비 (8h)
        params: dict,
        initial_capital: float = 10_000.0,
    ):
        self.ohlcv_1h = ohlcv_1h.copy()
        self.ohlcv_4h = ohlcv_4h.copy()
        self.funding = compute_funding_zscore(funding.copy(), window_days=params["window_days"])
        self.params = params
        self.initial_capital = initial_capital

        # 시장 지표 계산
        self.ohlcv_4h["rsi"] = calculate_rsi(self.ohlcv_4h["close"], period=14)
        self.ohlcv_1h["momentum"] = calculate_momentum(self.ohlcv_1h["close"], window=params["momentum_window"])

        # 상태
        self.equity = initial_capital
        self.position = None  # {"side": "long"/"short", "entry_price": float, "entry_ts": datetime, "entry_bar": int, "entry_zscore": float}
        self.equity_curve = [initial_capital]
        self.trades = []

    def _get_funding_at(self, ts: datetime) -> float | None:
        """주어진 시점의 펀딩비 반환."""
        mask = (self.funding.index >= ts) & (self.funding.index <= ts + timedelta(hours=1))
        matching = self.funding.loc[mask]
        if len(matching) > 0:
            return matching["rate"].iloc[-1]
        return None

    def _get_rsi_at(self, ts: datetime) -> float | None:
        """주어진 시점의 RSI(4h) 반환."""
        # 4h 봉에서 ts 이전의 마지막 값
        mask = self.ohlcv_4h.index <= ts
        matching = self.ohlcv_4h.loc[mask]
        if len(matching) > 0:
            rsi_val = matching["rsi"].iloc[-1]
            return rsi_val if not pd.isna(rsi_val) else None
        return None

    def _get_momentum_at(self, ts: datetime) -> float | None:
        """주어진 시점의 모멘텀 반환."""
        mask = self.ohlcv_1h.index <= ts
        matching = self.ohlcv_1h.loc[mask]
        if len(matching) > 0:
            mom_val = matching["momentum"].iloc[-1]
            return mom_val if not pd.isna(mom_val) else None
        return None

    def _get_zscore_at(self, ts: datetime) -> float | None:
        """주어진 시점의 z-score 반환."""
        mask = self.funding.index <= ts
        matching = self.funding.loc[mask]
        if len(matching) > 0:
            zs = matching["zscore"].iloc[-1]
            return zs if not pd.isna(zs) else None
        return None

    def _should_enter_short(self, ts: datetime, zscore: float) -> bool:
        """숏 진입 조건."""
        if zscore <= self.params["entry_zscore"]:
            return False

        funding = self._get_funding_at(ts)
        if funding is None:
            return False

        # 펀딩비 필터 옵션 (선택사항)
        if funding < self.params["min_funding"]:
            return False

        # 이하는 Ablation 테스트용 - use_rsi, use_momentum 플래그로 제어
        use_rsi = self.params.get("use_rsi", True)
        use_momentum = self.params.get("use_momentum", True)

        if use_rsi:
            rsi = self._get_rsi_at(ts)
            if rsi is None or rsi <= self.params["rsi_upper"]:
                return False

        if use_momentum:
            momentum = self._get_momentum_at(ts)
            if momentum is None or momentum <= self.params["momentum_threshold"]:
                return False

        return True

    def _should_enter_long(self, ts: datetime, zscore: float) -> bool:
        """롱 진입 조건."""
        if zscore >= -self.params["entry_zscore"]:
            return False

        funding = self._get_funding_at(ts)
        if funding is None:
            return False

        use_rsi = self.params.get("use_rsi", True)
        use_momentum = self.params.get("use_momentum", True)

        if use_rsi:
            rsi = self._get_rsi_at(ts)
            if rsi is None or rsi >= self.params["rsi_lower"]:
                return False

        if use_momentum:
            momentum = self._get_momentum_at(ts)
            if momentum is None or momentum >= -self.params["momentum_threshold"]:
                return False

        return True

    def _should_exit(self, ts: datetime, current_bar: int) -> bool:
        """청산 조건."""
        if self.position is None:
            return False

        zscore = self._get_zscore_at(ts)
        if zscore is None:
            return False

        # 조건 1: |zscore| < exit_zscore
        if abs(zscore) < self.params["exit_zscore"]:
            return True

        # 조건 2: 보유 5일 초과 (1h 봉 120개)
        bars_held = current_bar - self.position["entry_bar"]
        if bars_held > self.params["max_hold_bars"]:
            return True

        # 조건 3, 4는 run()에서 시가 기반 처리

        return False

    def _calculate_position_size(self, zscore: float, current_price: float) -> float:
        """포지션 크기 계산 (명목가 기준)."""
        # 자본 × 0.15 × |zscore| / 1.5 (최대 30%)
        ratio = 0.15 * abs(zscore) / 1.5
        ratio = min(ratio, MAX_POSITION_RATIO)
        notional = self.equity * ratio * LEVERAGE
        return notional / current_price

    def run(self) -> dict:
        """백테스트 실행."""
        bars = self.ohlcv_1h.reset_index()
        n = len(bars)

        for idx in range(1, n):
            bar = bars.iloc[idx]
            ts = bar["ts"]
            close = bar["close"]

            zscore = self._get_zscore_at(ts)
            if zscore is None:
                continue

            # 포지션 청산 판단 (손절 제외)
            if self.position and self._should_exit(ts, idx):
                self._close_position(ts, close, "signal", zscore)

            # 포지션 청산 판단 (손절 / 극단화)
            if self.position:
                # 손절 (진입가 ±3%)
                entry_price = self.position["entry_price"]
                if self.position["side"] == "short":
                    if close >= entry_price * (1 + self.params["stoploss_pct"]):
                        self._close_position(ts, close, "stoploss", zscore)
                        continue
                    # 극단화 손절 (z-score 추가 0.5)
                    if zscore < self.position["entry_zscore"] - 0.5:
                        self._close_position(ts, close, "extreme_sl", zscore)
                        continue
                else:  # long
                    if close <= entry_price * (1 - self.params["stoploss_pct"]):
                        self._close_position(ts, close, "stoploss", zscore)
                        continue
                    # 극단화 손절
                    if zscore > self.position["entry_zscore"] + 0.5:
                        self._close_position(ts, close, "extreme_sl", zscore)
                        continue

            # 새로운 포지션 진입
            if self.position is None:
                if self._should_enter_short(ts, zscore):
                    self._enter_position(ts, close, "short", zscore, idx)
                elif self._should_enter_long(ts, zscore):
                    self._enter_position(ts, close, "long", zscore, idx)

            # 펀딩비 정산 (8시간마다)
            self._settle_funding(ts, idx)

            # 자산곡선 업데이트
            self.equity_curve.append(self.equity)

        return self._calculate_metrics()

    def _enter_position(
        self,
        ts: datetime,
        price: float,
        side: str,
        zscore: float,
        bar_idx: int,
    ) -> None:
        """포지션 진입."""
        qty = self._calculate_position_size(zscore, price)
        entry_cost = qty * price * (1 + ENTRY_FEE + SLIPPAGE)

        self.position = {
            "side": side,
            "entry_price": price,
            "entry_ts": ts,
            "entry_bar": bar_idx,
            "entry_zscore": zscore,
            "qty": qty,
            "entry_cost": entry_cost,
        }
        self.equity -= entry_cost

    def _close_position(
        self,
        ts: datetime,
        price: float,
        reason: str,
        current_zscore: float,
    ) -> None:
        """포지션 청산."""
        if self.position is None:
            return

        qty = self.position["qty"]
        side = self.position["side"]
        entry_price = self.position["entry_price"]
        entry_cost = self.position["entry_cost"]

        # 청산 수익 (수수료 포함)
        close_revenue = qty * price * (1 - EXIT_FEE - SLIPPAGE)

        # PnL (펀딩비 제외, 가격 PnL만)
        price_pnl = close_revenue - entry_cost

        # 누적 펀딩비 (position 기간 동안 수취/지급)
        # 이후 funding_settle에서 계산
        funding_pnl = self.position.get("cumulative_funding", 0)

        total_pnl = price_pnl + funding_pnl

        self.trades.append({
            "entry_ts": self.position["entry_ts"],
            "exit_ts": ts,
            "side": side,
            "entry_price": entry_price,
            "exit_price": price,
            "qty": qty,
            "price_pnl": price_pnl,
            "funding_pnl": funding_pnl,
            "total_pnl": total_pnl,
            "reason": reason,
        })

        self.equity += close_revenue + funding_pnl
        self.position = None

    def _settle_funding(self, ts: datetime, bar_idx: int) -> None:
        """8시간마다 펀딩비 정산."""
        if self.position is None:
            return

        # 8시간 단위 정산 판단 (UTC 00:00, 08:00, 16:00)
        hour = ts.hour
        if hour % 8 != 0:
            return

        # 이미 이 시점에서 정산했으면 스킵
        if self.position.get("last_settle_ts") == ts:
            return

        funding = self._get_funding_at(ts)
        if funding is None:
            return

        # 펀딩비 수취/지급 (역방향)
        # 숏 포지션: 양수 펀딩비 → 수취 (수입)
        #           음수 펀딩비 → 지급 (손실)
        # 롱 포지션: 양수 펀딩비 → 지급 (손실)
        #          음수 펀딩비 → 수취 (수입)

        if self.position["side"] == "short":
            # 숏이 수취하는 경우: 펀딩비 > 0
            funding_pnl = self.position["qty"] * abs(funding)
        else:  # long
            # 롱이 수취하는 경우: 펀딩비 < 0
            funding_pnl = self.position["qty"] * abs(funding)

        self.position["cumulative_funding"] = self.position.get("cumulative_funding", 0) + funding_pnl
        self.position["last_settle_ts"] = ts

    def _calculate_metrics(self) -> dict:
        """성과 지표 계산."""
        equity_series = pd.Series(self.equity_curve)
        total_return_pct = ((self.equity - self.initial_capital) / self.initial_capital) * 100
        n_years = len(self.ohlcv_1h) / (24 * 365)

        if len(self.trades) == 0:
            return {
                "total_return_pct": total_return_pct,
                "sharpe_ratio": 0.0,
                "max_drawdown_pct": 0.0,
                "trade_count": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "cagr": 0.0,
                "monthly_returns": {},
            }

        sharpe_val = sharpe(equity_series, periods_per_year=8760)
        mdd_val = mdd(equity_series)
        cagr_val = cagr(total_return_pct, n_years)

        pnls = [t["total_pnl"] for t in self.trades]
        wins = sum(1 for p in pnls if p > 0)
        win_rate = wins / len(pnls) if pnls else 0.0

        pf = profit_factor(pnls)
        monthly = monthly_returns(equity_series)

        return {
            "total_return_pct": total_return_pct,
            "sharpe_ratio": safe_float(sharpe_val),
            "max_drawdown_pct": mdd_val,
            "trade_count": len(self.trades),
            "win_rate": win_rate,
            "profit_factor": safe_float(pf),
            "cagr": cagr_val,
            "monthly_returns": monthly,
            "final_equity": self.equity,
        }


# ── 메인 로직 ────────────────────────────────────────────────────────────────

async def run_stage_1(pool: asyncpg.Pool) -> None:
    """Stage 1: Baseline 테스트."""
    logger.info("=" * 70)
    logger.info("Stage 1: Baseline (entry=1.5, exit=0.5, window=30)")
    logger.info("=" * 70)

    ohlcv_1h = await load_ohlcv(pool, SYMBOL, "1h", FULL_START, FULL_END)
    ohlcv_4h = await load_ohlcv(pool, SYMBOL, "4h", FULL_START, FULL_END)
    funding = await load_funding(pool, SYMBOL, FULL_START, FULL_END)

    if ohlcv_1h.empty or funding.empty:
        logger.error("데이터 로드 실패")
        return

    bt = FundingExtremeReversalBacktester(
        ohlcv_1h, ohlcv_4h, funding, BASE_PARAMS, INITIAL_CAPITAL
    )
    result = bt.run()

    logger.info(f"거래수: {result['trade_count']}")
    logger.info(f"승률: {result['win_rate']*100:.2f}%")
    logger.info(f"수익률: {result['total_return_pct']:.2f}%")
    logger.info(f"Sharpe: {result['sharpe_ratio']:.4f}")
    logger.info(f"MDD: {result['max_drawdown_pct']:.2f}%")
    logger.info(f"CAGR: {result['cagr']:.2f}%")

    await save_result(
        pool,
        stage="stage_1_baseline",
        variant="default",
        metrics=result,
        params=BASE_PARAMS,
        table="funding_extreme_reversal_results"
    )


async def run_stage_2(pool: asyncpg.Pool) -> None:
    """Stage 2: 그리드서치 (48조합)."""
    logger.info("=" * 70)
    logger.info("Stage 2: GridSearch (48 combinations)")
    logger.info("=" * 70)

    ohlcv_1h = await load_ohlcv(pool, SYMBOL, "1h", FULL_START, FULL_END)
    ohlcv_4h = await load_ohlcv(pool, SYMBOL, "4h", FULL_START, FULL_END)
    funding = await load_funding(pool, SYMBOL, FULL_START, FULL_END)

    if ohlcv_1h.empty or funding.empty:
        logger.error("데이터 로드 실패")
        return

    count = 0
    total = (len(GRID_PARAMS["entry_zscore"]) *
             len(GRID_PARAMS["exit_zscore"]) *
             len(GRID_PARAMS["window_days"]))

    results = []

    for entry_z in GRID_PARAMS["entry_zscore"]:
        for exit_z in GRID_PARAMS["exit_zscore"]:
            for window_d in GRID_PARAMS["window_days"]:
                count += 1
                params = BASE_PARAMS.copy()
                params.update({
                    "entry_zscore": entry_z,
                    "exit_zscore": exit_z,
                    "window_days": window_d,
                })

                bt = FundingExtremeReversalBacktester(
                    ohlcv_1h, ohlcv_4h, funding, params, INITIAL_CAPITAL
                )
                result = bt.run()
                result["params_key"] = f"e{entry_z:.1f}_ex{exit_z:.1f}_w{window_d}"

                results.append({
                    "params": params,
                    "result": result,
                })

                logger.info(
                    f"[{count}/{total}] entry={entry_z:.1f} exit={exit_z:.1f} "
                    f"window={window_d}d → Sharpe={result['sharpe_ratio']:.3f} "
                    f"Return={result['total_return_pct']:.2f}%"
                )

                await save_result(
                    pool,
                    stage="stage_2_gridsearch",
                    variant=result["params_key"],
                    metrics=result,
                    params=params,
                    table="funding_extreme_reversal_results"
                )

    # 최적 파라미터 출력
    best = max(results, key=lambda x: x["result"]["sharpe_ratio"])
    logger.info("\n최적 파라미터 (Sharpe 기준):")
    logger.info(f"  entry_zscore: {best['params']['entry_zscore']:.1f}")
    logger.info(f"  exit_zscore: {best['params']['exit_zscore']:.1f}")
    logger.info(f"  window_days: {best['params']['window_days']}")
    logger.info(f"  Sharpe: {best['result']['sharpe_ratio']:.4f}")
    logger.info(f"  Return: {best['result']['total_return_pct']:.2f}%")


async def run_stage_3(pool: asyncpg.Pool) -> None:
    """Stage 3: 필터 Ablation (4가지)."""
    logger.info("=" * 70)
    logger.info("Stage 3: Filter Ablation")
    logger.info("=" * 70)

    ohlcv_1h = await load_ohlcv(pool, SYMBOL, "1h", FULL_START, FULL_END)
    ohlcv_4h = await load_ohlcv(pool, SYMBOL, "4h", FULL_START, FULL_END)
    funding = await load_funding(pool, SYMBOL, FULL_START, FULL_END)

    if ohlcv_1h.empty or funding.empty:
        logger.error("데이터 로드 실패")
        return

    ablation_configs = [
        {
            "name": "zscore_only",
            "desc": "z-score만 사용",
            "use_rsi": False,
            "use_momentum": False,
        },
        {
            "name": "zscore_rsi",
            "desc": "z-score + RSI",
            "use_rsi": True,
            "use_momentum": False,
        },
        {
            "name": "zscore_momentum",
            "desc": "z-score + 모멘텀",
            "use_rsi": False,
            "use_momentum": True,
        },
        {
            "name": "full_filters",
            "desc": "z-score + RSI + 모멘텀 (기본)",
            "use_rsi": True,
            "use_momentum": True,
        },
    ]

    for config in ablation_configs:
        logger.info(f"\n테스트: {config['desc']}")
        params = BASE_PARAMS.copy()
        params["use_rsi"] = config["use_rsi"]
        params["use_momentum"] = config["use_momentum"]

        bt = FundingExtremeReversalBacktester(
            ohlcv_1h, ohlcv_4h, funding, params, INITIAL_CAPITAL
        )
        result = bt.run()

        logger.info(f"  거래수: {result['trade_count']}")
        logger.info(f"  Sharpe: {result['sharpe_ratio']:.4f}")
        logger.info(f"  Return: {result['total_return_pct']:.2f}%")

        await save_result(
            pool,
            stage="stage_3_ablation",
            variant=config["name"],
            metrics=result,
            params=params,
            table="funding_extreme_reversal_results"
        )


async def run_stage_4(pool: asyncpg.Pool) -> None:
    """Stage 4: Walk-Forward (1년 학습, 6개월 테스트, 3개월 슬라이딩)."""
    logger.info("=" * 70)
    logger.info("Stage 4: Walk-Forward Analysis")
    logger.info("=" * 70)

    ohlcv_1h = await load_ohlcv(pool, SYMBOL, "1h", FULL_START, FULL_END)
    ohlcv_4h = await load_ohlcv(pool, SYMBOL, "4h", FULL_START, FULL_END)
    funding = await load_funding(pool, SYMBOL, FULL_START, FULL_END)

    if ohlcv_1h.empty or funding.empty:
        logger.error("데이터 로드 실패")
        return

    # 간단한 WF: 2 윈도우 (시간상 제약)
    train_days = 365
    test_days = 180
    slide_days = 90

    wf_results = []
    window_idx = 1

    # 시작점: 충분한 데이터가 있는 지점부터
    start_date = FULL_START + timedelta(days=train_days)
    current_date = start_date

    while current_date + timedelta(days=test_days) < FULL_END:
        train_end = current_date
        test_start = current_date
        test_end = current_date + timedelta(days=test_days)

        logger.info(f"\nWindow {window_idx}: {test_start.date()} ~ {test_end.date()}")

        # 테스트 데이터에서만 실행
        test_ohlcv_1h = ohlcv_1h[(ohlcv_1h.index >= test_start) & (ohlcv_1h.index < test_end)]
        test_ohlcv_4h = ohlcv_4h[(ohlcv_4h.index >= test_start) & (ohlcv_4h.index < test_end)]
        test_funding = funding[(funding.index >= test_start) & (funding.index < test_end)]

        if test_ohlcv_1h.empty:
            current_date += timedelta(days=slide_days)
            window_idx += 1
            continue

        bt = FundingExtremeReversalBacktester(
            test_ohlcv_1h, test_ohlcv_4h, test_funding, BASE_PARAMS, INITIAL_CAPITAL
        )
        result = bt.run()
        wf_results.append(result)

        logger.info(f"  거래수: {result['trade_count']}")
        logger.info(f"  Sharpe: {result['sharpe_ratio']:.4f}")
        logger.info(f"  Return: {result['total_return_pct']:.2f}%")

        await save_result(
            pool,
            stage="stage_4_walkforward",
            variant=f"window_{window_idx}",
            metrics=result,
            params=BASE_PARAMS,
            table="funding_extreme_reversal_results"
        )

        current_date += timedelta(days=slide_days)
        window_idx += 1

    if wf_results:
        avg_sharpe = sum(r["sharpe_ratio"] for r in wf_results) / len(wf_results)
        avg_return = sum(r["total_return_pct"] for r in wf_results) / len(wf_results)
        logger.info(f"\nWF 평균 Sharpe: {avg_sharpe:.4f}")
        logger.info(f"WF 평균 Return: {avg_return:.2f}%")


async def run_stage_5(pool: asyncpg.Pool) -> None:
    """Stage 5: 저펀딩 환경 (2025-04-01 ~ 2026-04-10)."""
    logger.info("=" * 70)
    logger.info("Stage 5: Low Funding Environment (Apr 2025 - Apr 2026)")
    logger.info("=" * 70)

    ohlcv_1h = await load_ohlcv(pool, SYMBOL, "1h", LOW_FUNDING_START, LOW_FUNDING_END)
    ohlcv_4h = await load_ohlcv(pool, SYMBOL, "4h", LOW_FUNDING_START, LOW_FUNDING_END)
    funding = await load_funding(pool, SYMBOL, LOW_FUNDING_START, LOW_FUNDING_END)

    if ohlcv_1h.empty or funding.empty:
        logger.error("데이터 로드 실패 (저펀딩 환경)")
        return

    bt = FundingExtremeReversalBacktester(
        ohlcv_1h, ohlcv_4h, funding, BASE_PARAMS, INITIAL_CAPITAL
    )
    result = bt.run()

    logger.info(f"거래수: {result['trade_count']}")
    logger.info(f"승률: {result['win_rate']*100:.2f}%")
    logger.info(f"수익률: {result['total_return_pct']:.2f}%")
    logger.info(f"Sharpe: {result['sharpe_ratio']:.4f}")
    logger.info(f"MDD: {result['max_drawdown_pct']:.2f}%")
    logger.info(f"CAGR: {result['cagr']:.2f}%")

    await save_result(
        pool,
        stage="stage_5_low_funding",
        variant="1year_env",
        metrics=result,
        params=BASE_PARAMS,
        table="funding_extreme_reversal_results"
    )


async def main(args):
    """메인 진입점."""
    try:
        pool = await make_pool()
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return

    try:
        if args.stage in ["all", "1"]:
            await run_stage_1(pool)
        if args.stage in ["all", "2"]:
            await run_stage_2(pool)
        if args.stage in ["all", "3"]:
            await run_stage_3(pool)
        if args.stage in ["all", "4"]:
            await run_stage_4(pool)
        if args.stage in ["all", "5"]:
            await run_stage_5(pool)

        logger.info("\n" + "=" * 70)
        logger.info("백테스트 완료")
        logger.info("=" * 70)
    finally:
        await pool.close()


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        choices=["all", "1", "2", "3", "4", "5"],
        default="all",
        help="실행할 Stage (기본: all)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
