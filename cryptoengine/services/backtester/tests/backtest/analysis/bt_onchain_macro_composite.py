"""analysis/bt_onchain_macro_composite.py
온체인 매크로 복합 신호 전략 (#08) 백테스트.

전략 개요:
  7개 지표(MVRV Z-Score, aSOPR, 거래소순유출, 공포탐욕, DXY, ETF플로우, 펀딩비)
  복합 점수화(0~7점)로 사이클 극단에서만 초저빈도 매매.

지표 점수화 (각 0 또는 1점, 최대 7점):
  1. MVRV Z-Score < 1.5 (저평가)
  2. aSOPR < 1.0 (투매, 데이터 없으면 스킵 → 최대 점수 6점)
  3. 거래소 순유출 7d MA 음수 (홀딩 증가)
  4. 공포탐욕 < 25 (극단적 공포)
  5. DXY 5d 변화 ≤ +0.3% (드물게, 역상관 사용)
  6. ETF 5d 누적 순유입 > $200M (자금 유입)
  7. 펀딩비 30d MA ≤ 0 (약세 신호)

진입/청산:
  - 풀포지션 롱: score ≥ entry_full (기본 5)
  - 반포지션 롱: score == entry_half (기본 4)
  - 청산 규칙: 점수 < exit_level (기본 2)
  - 익절: +30% → 50% 청산, +50% → 추가 25%
  - 손절: -8%
  - 시간청산: 90일

스테이지:
  Stage 1: Baseline (기본값 단일 실행)
  Stage 2: 임계값 그리드서치 (18조합)
  Stage 3: Ablation (7개 지표 제거 변형)
  Stage 4: 단일 지표 비교
  Stage 5: Walk-Forward (1.5년 학습 / 1년 테스트, 2개 폴드)

실행:
    docker compose --profile backtest run --rm backtester \\
      python tests/backtest/analysis/bt_onchain_macro_composite.py --stage all
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float, monthly_returns,
    make_pool, save_result,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

UTC = timezone.utc
SYMBOL = "BTCUSDT"
TIMEFRAME = "1d"
INITIAL_CAPITAL = 10_000.0


# ── 7개 지표 로드 ──────────────────────────────────────────────────────────────

async def load_onchain_metrics(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """onchain_metrics 테이블에서 데이터 로드.

    Returns:
        DataFrame (index=date, columns=[price, market_cap, realized_cap, mvrv, asopr, exchange_netflow])
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, price_usd, market_cap_usd, realized_cap_usd, mvrv,
                   mvrv_zscore, asopr, exchange_netflow_usd
            FROM onchain_metrics
            WHERE asset = 'BTC' AND date >= $1 AND date <= $2
            ORDER BY date ASC
            """,
            start.date(),
            end.date(),
        )
    if not rows:
        return pd.DataFrame()

    # asyncpg Record를 dict로 변환, DECIMAL → float
    data = []
    for row in rows:
        d = dict(row)
        # DECIMAL 타입을 float로 변환
        for k, v in d.items():
            if v is not None and hasattr(v, '__float__'):
                d[k] = float(v)
        data.append(d)

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    # MVRV 계산 (DB에 없으면 직접 계산)
    if "market_cap_usd" in df.columns and "realized_cap_usd" in df.columns:
        df["mvrv"] = (df["market_cap_usd"] / df["realized_cap_usd"]).fillna(1.0)

    # MVRV Z-Score 재계산 (DB에 4yr MA 부족하면)
    if "mvrv_zscore" in df.columns and df["mvrv_zscore"].isna().all():
        if "mvrv" in df.columns:
            ma_4yr = df["mvrv"].rolling(window=4*365, min_periods=365).mean()
            std_4yr = df["mvrv"].rolling(window=4*365, min_periods=365).std()
            std_4yr = std_4yr.replace(0, 0.001)
            df["mvrv_zscore"] = (df["mvrv"] - ma_4yr) / std_4yr

    return df


async def load_fear_greed(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> pd.Series:
    """fear_greed_history 테이블에서 데이터 로드.

    Returns:
        Series (index=date, value=FG value 0~100)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, value
            FROM fear_greed_history
            WHERE date >= $1 AND date <= $2
            ORDER BY date ASC
            """,
            start.date(),
            end.date(),
        )
    if not rows:
        return pd.Series()

    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["value"] = df["value"].astype(float)
    return df["value"]


async def load_etf_flow(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> pd.Series:
    """etf_flow_history 테이블에서 ETF 플로우 로드.

    Returns:
        Series (index=date, value=total_flow_usd)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, total_flow_usd
            FROM etf_flow_history
            WHERE date >= $1 AND date <= $2
            ORDER BY date ASC
            """,
            start.date(),
            end.date(),
        )
    if not rows:
        return pd.Series()

    data = []
    for row in rows:
        d = dict(row)
        if d.get("total_flow_usd") is not None:
            d["total_flow_usd"] = float(d["total_flow_usd"])
        data.append(d)

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df["total_flow_usd"]


# ── 복합 점수화 로직 ──────────────────────────────────────────────────────────────

def compute_composite_signals(
    ohlcv: pd.DataFrame,
    onchain: pd.DataFrame,
    fear_greed: pd.Series,
    etf_flow: pd.Series,
    funding: pd.DataFrame,
) -> pd.DataFrame:
    """7개 지표 점수화.

    Returns:
        DataFrame (index=date, columns=[score, signal_1, signal_2, ...signal_7])
        signal_*: 각 지표의 0/1 점수
    """
    signals = pd.DataFrame(index=ohlcv.index)

    # 공통 인덱스
    common_idx = ohlcv.index.intersection(onchain.index) if len(onchain) > 0 else ohlcv.index

    # 신호 1: MVRV Z-Score < 1.5
    if len(onchain) > 0:
        mvrv_zs = onchain.reindex(common_idx)["mvrv_zscore"]
        signals["signal_1"] = (mvrv_zs < 1.5).astype(int)
    else:
        signals["signal_1"] = 0

    # 신호 2: aSOPR < 1.0 (데이터 거의 없으므로 0으로 처리, 점수 0)
    signals["signal_2"] = 0

    # 신호 3: 거래소 순유출 7d MA 음수
    if len(onchain) > 0:
        netflow = onchain.reindex(common_idx)["exchange_netflow_usd"]
        netflow_ma7 = netflow.rolling(window=7, min_periods=1).mean()
        signals["signal_3"] = (netflow_ma7 < 0).astype(int)
    else:
        signals["signal_3"] = 0

    # 신호 4: 공포탐욕 < 25
    if len(fear_greed) > 0:
        fg_reindex = fear_greed.reindex(common_idx, method="ffill")
        signals["signal_4"] = (fg_reindex < 25).astype(int)
    else:
        signals["signal_4"] = 0

    # 신호 5: DXY 5d 변화 ≤ +0.3% (간접 추정: BTC 5d 낙폭 ≥ -1%)
    # (DXY 상승 = BTC 하락 역상관, BTC 5d 낙폭이 작으면 DXY 상승 적음)
    close_5d_change = ohlcv["close"].pct_change(5)  # 5일 변화
    signals["signal_5"] = (close_5d_change <= -0.01).astype(int)  # 더 보수적으로

    # 신호 6: ETF 5d 누적 순유입 > $200M
    if len(etf_flow) > 0:
        # etf_flow는 Series (index=date, value=total_flow_usd)
        etf_reindex = etf_flow.reindex(common_idx, fill_value=0)
        etf_5d_sum = etf_reindex.rolling(window=5, min_periods=1).sum()
        signals["signal_6"] = (etf_5d_sum > 200_000_000).astype(int)
    else:
        signals["signal_6"] = 0

    # 신호 7: 펀딩비 30d MA ≤ 0
    if len(funding) > 0:
        funding_reindex = funding.reindex(common_idx, fill_value=0)["rate"]
        funding_ma30 = funding_reindex.rolling(window=30, min_periods=1).mean()
        signals["signal_7"] = (funding_ma30 <= 0).astype(int)
    else:
        signals["signal_7"] = 0

    # 복합 점수 (aSOPR 데이터 없으므로 최대 6점)
    signals["score"] = signals[["signal_1", "signal_3", "signal_4", "signal_5", "signal_6", "signal_7"]].sum(axis=1)

    return signals.fillna(0).astype(int)


# ── 백테스트 엔진 ──────────────────────────────────────────────────────────────

class OnChainMacroBacktester:
    """온체인 매크로 복합 신호 전략 백테스터."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        signals: pd.DataFrame,
        initial_capital: float = 10_000,
        entry_full: int = 5,
        entry_half: int = 4,
        exit_level: int = 2,
        tp1_pct: float = 0.30,
        tp1_reduce: float = 0.50,
        tp2_pct: float = 0.50,
        tp2_reduce: float = 0.25,
        sl_pct: float = -0.08,
        max_hold_days: int = 90,
        fee_rate: float = 0.00055,
    ):
        self.ohlcv = ohlcv
        self.signals = signals
        self.initial_capital = initial_capital
        self.entry_full = entry_full
        self.entry_half = entry_half
        self.exit_level = exit_level
        self.tp1_pct = tp1_pct
        self.tp1_reduce = tp1_reduce
        self.tp2_pct = tp2_pct
        self.tp2_reduce = tp2_reduce
        self.sl_pct = sl_pct
        self.max_hold_days = max_hold_days
        self.fee_rate = fee_rate

        self.equity = initial_capital
        self.equity_curve = []
        self.trades = []
        self.position = None  # {"size": BTC, "entry_price": float, "entry_date": timestamp, "reduce_tp1": bool}

    def run(self) -> dict[str, Any]:
        """백테스트 실행."""
        for i, (date, row) in enumerate(self.ohlcv.iterrows()):
            score = self.signals.loc[date, "score"] if date in self.signals.index else 0
            close = row["close"]

            # 포지션 관리
            if self.position is not None:
                self._manage_position(date, close)

            # 진입 신호
            if self.position is None:
                if score >= self.entry_full:
                    self._enter_position(date, close, size=1.0)
                elif score == self.entry_half:
                    self._enter_position(date, close, size=0.5)

            self.equity_curve.append(self.equity)

        # 포지션 정리
        if self.position is not None and len(self.ohlcv) > 0:
            last_close = self.ohlcv.iloc[-1]["close"]
            self._close_position(self.ohlcv.index[-1], last_close, reason="end")

        return self._compute_metrics()

    def _enter_position(self, date: datetime, price: float, size: float = 1.0):
        """진입."""
        btc_size = (self.equity * size) / price * (1 - self.fee_rate)
        self.position = {
            "size": btc_size,
            "entry_price": price,
            "entry_date": date,
            "reduce_tp1": False,
        }

    def _manage_position(self, date: datetime, close: float):
        """포지션 관리 (익절/손절/시간청산)."""
        if self.position is None:
            return

        pnl_pct = (close - self.position["entry_price"]) / self.position["entry_price"]
        days_held = (date - self.position["entry_date"]).days

        # 손절
        if pnl_pct <= self.sl_pct:
            self._close_position(date, close, reason="stoploss")
            return

        # 익절 1
        if not self.position["reduce_tp1"] and pnl_pct >= self.tp1_pct:
            reduce_size = self.position["size"] * self.tp1_reduce
            self.equity += reduce_size * close * (1 - self.fee_rate)
            self.position["size"] -= reduce_size
            self.position["reduce_tp1"] = True

        # 익절 2
        if pnl_pct >= self.tp2_pct:
            reduce_size = self.position["size"] * self.tp2_reduce
            self.equity += reduce_size * close * (1 - self.fee_rate)
            self.position["size"] -= reduce_size

        # 시간청산
        if days_held >= self.max_hold_days:
            self._close_position(date, close, reason="timeout")
            return

    def _close_position(self, date: datetime, price: float, reason: str = "signal"):
        """청산."""
        if self.position is None:
            return

        sale_value = self.position["size"] * price * (1 - self.fee_rate)
        pnl = sale_value - (self.position["size"] * self.position["entry_price"])
        pnl_pct = pnl / (self.position["size"] * self.position["entry_price"]) if self.position["size"] > 0 else 0

        self.trades.append({
            "entry_date": self.position["entry_date"],
            "exit_date": date,
            "entry_price": self.position["entry_price"],
            "exit_price": price,
            "size": self.position["size"],
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "reason": reason,
        })

        self.equity = sale_value
        self.position = None

    def _compute_metrics(self) -> dict[str, Any]:
        """성과 지표 계산."""
        equity_series = pd.Series(self.equity_curve, index=self.ohlcv.index)
        total_return_pct = (self.equity - self.initial_capital) / self.initial_capital * 100

        # 연기간 계산
        start_date = self.ohlcv.index[0]
        end_date = self.ohlcv.index[-1]
        n_years = (end_date - start_date).days / 365.25

        return {
            "total_return_pct": safe_float(total_return_pct),
            "cagr_pct": safe_float(cagr(total_return_pct, n_years) if n_years > 0 else 0),
            "sharpe_ratio": safe_float(sharpe(equity_series, periods_per_year=365)),
            "max_drawdown_pct": safe_float(mdd(equity_series)),
            "trade_count": len(self.trades),
            "win_rate": safe_float(
                sum(1 for t in self.trades if t["pnl_pct"] > 0) / len(self.trades) * 100
                if len(self.trades) > 0 else 0
            ),
            "avg_win_pct": safe_float(
                np.mean([t["pnl_pct"] * 100 for t in self.trades if t["pnl_pct"] > 0])
                if any(t["pnl_pct"] > 0 for t in self.trades) else 0
            ),
            "avg_loss_pct": safe_float(
                np.mean([t["pnl_pct"] * 100 for t in self.trades if t["pnl_pct"] < 0])
                if any(t["pnl_pct"] < 0 for t in self.trades) else 0
            ),
            "final_equity": safe_float(self.equity),
        }


# ── 메인 ──────────────────────────────────────────────────────────────────────

async def run_stage1_baseline(pool: asyncpg.Pool) -> dict[str, Any]:
    """Stage 1: Baseline."""
    logger.info("=== Stage 1: Baseline ===")

    # 3년 데이터 (2023-01-01 ~ 2026-04-11)
    start = datetime(2023, 1, 1, tzinfo=UTC)
    end = datetime.now(UTC)

    ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start, end)
    onchain = await load_onchain_metrics(pool, start, end)
    fear_greed = await load_fear_greed(pool, start, end)
    etf_flow = await load_etf_flow(pool, start, end)
    funding = await load_funding(pool, SYMBOL, start, end)

    if len(ohlcv) == 0:
        logger.warning("OHLCV 데이터 없음")
        return {}

    logger.info(f"데이터 범위: {ohlcv.index[0]} ~ {ohlcv.index[-1]} ({len(ohlcv)} 일자)")
    logger.info(f"온체인 메트릭: {len(onchain)}, 공포탐욕: {len(fear_greed)}, ETF 플로우: {len(etf_flow)}")

    signals = compute_composite_signals(ohlcv, onchain, fear_greed, etf_flow, funding)

    # 신호 통계
    signal_counts = signals["score"].value_counts().sort_index(ascending=False)
    logger.info(f"신호 분포:\n{signal_counts}")

    backtester = OnChainMacroBacktester(ohlcv, signals)
    metrics = backtester.run()

    logger.info(f"거래수: {metrics['trade_count']}, 승률: {metrics['win_rate']:.1f}%")
    logger.info(f"CAGR: {metrics['cagr_pct']:.2f}%, Sharpe: {metrics['sharpe_ratio']:.2f}")

    return metrics


async def run_stage2_grid(pool: asyncpg.Pool) -> list[dict]:
    """Stage 2: 임계값 그리드서치."""
    logger.info("=== Stage 2: Grid Search (18조합) ===")

    start = datetime(2023, 1, 1, tzinfo=UTC)
    end = datetime.now(UTC)

    ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start, end)
    onchain = await load_onchain_metrics(pool, start, end)
    fear_greed = await load_fear_greed(pool, start, end)
    etf_flow = await load_etf_flow(pool, start, end)
    funding = await load_funding(pool, SYMBOL, start, end)

    if len(ohlcv) == 0:
        return []

    signals = compute_composite_signals(ohlcv, onchain, fear_greed, etf_flow, funding)

    results = []
    for entry_full in [4, 5, 6]:
        for entry_half in [3, 4]:
            for exit_level in [2, 3, 4]:
                if exit_level >= entry_half:
                    continue

                backtester = OnChainMacroBacktester(
                    ohlcv, signals,
                    entry_full=entry_full,
                    entry_half=entry_half,
                    exit_level=exit_level,
                )
                metrics = backtester.run()

                variant_name = f"entry_full_{entry_full}_half_{entry_half}_exit_{exit_level}"
                metrics["variant"] = variant_name
                results.append(metrics)

                logger.info(f"{variant_name}: {metrics['trade_count']} 거래, CAGR {metrics['cagr_pct']:.2f}%")

    return results


async def run_stage3_ablation(pool: asyncpg.Pool) -> dict[str, dict]:
    """Stage 3: Ablation (지표 1개씩 제거)."""
    logger.info("=== Stage 3: Ablation ===")

    start = datetime(2023, 1, 1, tzinfo=UTC)
    end = datetime.now(UTC)

    ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start, end)
    onchain = await load_onchain_metrics(pool, start, end)
    fear_greed = await load_fear_greed(pool, start, end)
    etf_flow = await load_etf_flow(pool, start, end)
    funding = await load_funding(pool, SYMBOL, start, end)

    if len(ohlcv) == 0:
        return {}

    signals_full = compute_composite_signals(ohlcv, onchain, fear_greed, etf_flow, funding)

    results = {}

    # Full baseline
    bt = OnChainMacroBacktester(ohlcv, signals_full)
    results["full"] = bt.run()

    # 각 신호 제거
    for signal_idx in range(1, 7):
        signals_ablated = signals_full.copy()
        signals_ablated[f"signal_{signal_idx}"] = 0
        signals_ablated["score"] = signals_ablated[["signal_1", "signal_3", "signal_4", "signal_5", "signal_6", "signal_7"]].sum(axis=1)

        bt = OnChainMacroBacktester(ohlcv, signals_ablated)
        metric = bt.run()
        results[f"without_signal_{signal_idx}"] = metric

        logger.info(f"신호_{signal_idx} 제거: {metric['trade_count']} 거래, CAGR {metric['cagr_pct']:.2f}%")

    return results


async def run_stage4_single_indicator(pool: asyncpg.Pool) -> dict[str, dict]:
    """Stage 4: 단일 지표 비교."""
    logger.info("=== Stage 4: Single Indicator ===")

    start = datetime(2023, 1, 1, tzinfo=UTC)
    end = datetime.now(UTC)

    ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start, end)
    onchain = await load_onchain_metrics(pool, start, end)
    fear_greed = await load_fear_greed(pool, start, end)

    if len(ohlcv) == 0:
        return {}

    # 타임존 정규화 (UTC-aware)
    if len(ohlcv.index) > 0 and ohlcv.index.tz is not None:
        ohlcv.index = ohlcv.index.tz_convert('UTC')
    elif len(ohlcv.index) > 0 and ohlcv.index.tz is None:
        ohlcv.index = ohlcv.index.tz_localize('UTC')

    results = {}

    # MVRV 단독
    if len(onchain) > 0:
        if len(onchain.index) > 0 and onchain.index.tz is None:
            onchain.index = onchain.index.tz_localize('UTC')
        mvrv_zs = onchain["mvrv_zscore"].reindex(ohlcv.index, method="ffill")
        signals_mvrv = pd.DataFrame(index=ohlcv.index)
        signals_mvrv["score"] = (mvrv_zs < 1.5).astype(int) * 5

        bt = OnChainMacroBacktester(ohlcv, signals_mvrv, entry_full=5, entry_half=4, exit_level=4)
        results["mvrv_only"] = bt.run()
        logger.info(f"MVRV 단독: {results['mvrv_only']['trade_count']} 거래")

    # 공포탐욕 단독
    if len(fear_greed) > 0:
        if len(fear_greed.index) > 0 and fear_greed.index.tz is None:
            fear_greed.index = fear_greed.index.tz_localize('UTC')
        fg = fear_greed.reindex(ohlcv.index, method="ffill")
        signals_fg = pd.DataFrame(index=ohlcv.index)
        signals_fg["score"] = (fg < 25).astype(int) * 5

        bt = OnChainMacroBacktester(ohlcv, signals_fg, entry_full=5, entry_half=4, exit_level=4)
        results["fear_greed_only"] = bt.run()
        logger.info(f"공포탐욕 단독: {results['fear_greed_only']['trade_count']} 거래")

    # 복합 (이미 계산함)
    signals_full = compute_composite_signals(ohlcv, onchain, fear_greed, await load_etf_flow(pool, start, end), await load_funding(pool, SYMBOL, start, end))
    bt = OnChainMacroBacktester(ohlcv, signals_full)
    results["composite"] = bt.run()
    logger.info(f"복합 신호: {results['composite']['trade_count']} 거래")

    return results


async def run_stage5_walkforward(pool: asyncpg.Pool) -> dict[str, dict]:
    """Stage 5: Walk-Forward (1.5년 학습 / 1년 테스트, 2개 폴드)."""
    logger.info("=== Stage 5: Walk-Forward ===")

    start = datetime(2023, 1, 1, tzinfo=UTC)
    end = datetime.now(UTC)

    # 폴드 정의
    fold1_train_end = datetime(2024, 6, 30, tzinfo=UTC)
    fold1_test_end = datetime(2025, 6, 30, tzinfo=UTC)

    results = {}

    for fold_idx in [1]:
        if fold_idx == 1:
            train_start, train_end = start, fold1_train_end
            test_start, test_end = fold1_train_end + timedelta(days=1), fold1_test_end
        else:
            break  # 2개 폴드만 (데이터 부족)

        logger.info(f"Fold {fold_idx}: train {train_start.date()} ~ {train_end.date()}, "
                   f"test {test_start.date()} ~ {test_end.date()}")

        # 데이터 로드
        ohlcv_test = await load_ohlcv(pool, SYMBOL, TIMEFRAME, test_start, test_end)
        onchain_test = await load_onchain_metrics(pool, test_start, test_end)
        fear_greed_test = await load_fear_greed(pool, test_start, test_end)
        etf_flow_test = await load_etf_flow(pool, test_start, test_end)
        funding_test = await load_funding(pool, SYMBOL, test_start, test_end)

        if len(ohlcv_test) == 0:
            continue

        signals_test = compute_composite_signals(ohlcv_test, onchain_test, fear_greed_test, etf_flow_test, funding_test)

        bt = OnChainMacroBacktester(ohlcv_test, signals_test)
        metric = bt.run()

        results[f"fold_{fold_idx}_oos"] = metric
        logger.info(f"Fold {fold_idx} OOS: {metric['trade_count']} 거래, CAGR {metric['cagr_pct']:.2f}%")

    return results


async def main():
    parser = argparse.ArgumentParser(description="온체인 매크로 복합 신호 전략 백테스트")
    parser.add_argument("--stage", choices=["1", "2", "3", "4", "5", "all"], default="all", help="실행할 스테이지")
    args = parser.parse_args()

    pool = await make_pool()

    try:
        all_results = {}

        if args.stage in ["1", "all"]:
            all_results["stage_1"] = await run_stage1_baseline(pool)
            await save_result(pool, "stage_1", "baseline", all_results["stage_1"], {})

        if args.stage in ["2", "all"]:
            results_2 = await run_stage2_grid(pool)
            for res in results_2:
                variant = res.pop("variant")
                await save_result(pool, "stage_2", variant, res, {})

        if args.stage in ["3", "all"]:
            results_3 = await run_stage3_ablation(pool)
            for variant, metric in results_3.items():
                await save_result(pool, "stage_3", variant, metric, {})

        if args.stage in ["4", "all"]:
            results_4 = await run_stage4_single_indicator(pool)
            for variant, metric in results_4.items():
                await save_result(pool, "stage_4", variant, metric, {})

        if args.stage in ["5", "all"]:
            results_5 = await run_stage5_walkforward(pool)
            for variant, metric in results_5.items():
                await save_result(pool, "stage_5", variant, metric, {})

        logger.info("=== 완료 ===")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
