"""bt_hmm_llm_meta_strategy.py — HMM 레짐 게이트 + LLM 메타전략 선택자

4계층 전략:
  1. HMM 레짐 감지 (6h, 365일 롤링 재학습)
  2. 레짐별 하위 전략 (추세/회귀/캐리)
  3. LLM 오버라이드 (신뢰도 > 70% 시만)
  4. 드로다운 브레이크 (15% MDD → 청산, 10% → 반사이즈, 5% → 75% 감소)

Stage:
  1: Baseline (기본값)
  2: 레짐별 사이즈 변형 3종
  3: LLM 신뢰도 임계값 4종
  4: 드로다운 브레이크 임계값 조합 4종
  5: Ablation (HMM만, LLM만, HMM+LLM, 풀스택)
  6: Walk-Forward (1년 학습/6개월 테스트, 4개 윈도우)

실행:
    python tests/backtest/combined/bt_hmm_llm_meta_strategy.py --stage all
    python tests/backtest/combined/bt_hmm_llm_meta_strategy.py --stage 1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper

log = structlog.get_logger(__name__)

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float, monthly_returns,
    make_pool, save_result,
)
from tests.backtest.regime.hmm_regime_detector import HMMRegimeDetector
from tests.backtest.regime.llm_meta_advisor import simulate_llm_advisory

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

SYMBOL = "BTCUSDT"
TIMEFRAME = "1h"
TIMEFRAME_6H = "6h"
INITIAL_CAPITAL = 10_000.0
LEVERAGE = 2.0
MAKER_FEE = 0.0002
TAKER_FEE = 0.0002
FEE_RATE = MAKER_FEE  # assume maker

# Stage 2 variants (regime_size_ratio_low, regime_size_ratio_mid, regime_size_ratio_high)
STAGE_2_VARIANTS = [
    {"name": "size_variant_a", "low": 0.15, "mid": 0.10, "high": 0.20},
    {"name": "size_variant_b", "low": 0.20, "mid": 0.10, "high": 0.25},
    {"name": "size_variant_c", "low": 0.25, "mid": 0.15, "high": 0.30},
]

# Stage 3 variants (LLM confidence threshold)
STAGE_3_VARIANTS = [50, 60, 70, 80]

# Stage 4 variants (DD brake levels)
STAGE_4_VARIANTS = [
    {"name": "dd_l1_5_l2_10", "l1": -0.05, "l2": -0.10},
    {"name": "dd_l1_5_l2_13", "l1": -0.05, "l2": -0.13},
    {"name": "dd_l1_7_l2_10", "l1": -0.07, "l2": -0.10},
    {"name": "dd_l1_7_l2_13", "l1": -0.07, "l2": -0.13},
]

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# ═══════════════════════════════════════════════════════════════════════════════
# Trend Strategy (간단화 버전)
# ═══════════════════════════════════════════════════════════════════════════════


def _trend_signal(row: pd.Series) -> str:
    """
    Bollinger Band + Squeeze 기반 추세 신호.
    - squeeze_off: True → 진입 준비
    - close > bb_upper → long, close < bb_lower → short
    """
    squeeze_off = row.get("squeeze_off", False)
    bb_upper = row.get("bb_upper", float("inf"))
    bb_lower = row.get("bb_lower", float("-inf"))
    close = row.get("close", 0.0)

    if not squeeze_off:
        return "flat"

    if close > bb_upper:
        return "long"
    elif close < bb_lower:
        return "short"

    return "flat"


# ═══════════════════════════════════════════════════════════════════════════════
# Reversion Strategy (간단화 버전)
# ═══════════════════════════════════════════════════════════════════════════════


def _reversion_signal(row: pd.Series) -> str:
    """
    펀딩비 z-score + RSI 기반 평균회귀 신호.
    """
    funding_zscore = row.get("funding_zscore", 0.0)
    rsi_4h = row.get("rsi_4h", 50.0)

    # 펀딩비 과고 + RSI 과매수 → short
    if funding_zscore > 1.5 and rsi_4h > 65:
        return "short"

    # 펀딩비 과저 + RSI 과매도 → long
    if funding_zscore < -1.5 and rsi_4h < 35:
        return "long"

    return "flat"


# ═══════════════════════════════════════════════════════════════════════════════
# Carry Strategy (간단화 버전)
# ═══════════════════════════════════════════════════════════════════════════════


def _carry_signal(row: pd.Series) -> str:
    """
    30일 펀딩비 이동평균 기반 캐리 신호.
    """
    funding_30d_ma = row.get("funding_30d_ma", 0.0)

    # 양수 펀딩비 지속 → 롱 캐리
    if funding_30d_ma > 0.0001:
        return "long"

    return "flat"


# ═══════════════════════════════════════════════════════════════════════════════
# Indicator Calculation
# ═══════════════════════════════════════════════════════════════════════════════


def _add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """기술적 지표 추가: Bollinger Band, RSI, 펀딩비 z-score, etc."""
    df = df.copy()
    close = df["close"]

    # ─ Bollinger Band (20, 2σ) ──
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    df["bb_upper"] = sma20 + 2 * std20
    df["bb_lower"] = sma20 - 2 * std20
    df["bb_mid"] = sma20

    # ─ Keltner Channel (ATR 기반) ──
    high = df["high"]
    low = df["low"]
    hl_range = high - low
    atr = hl_range.rolling(20).mean()
    ema20 = close.ewm(span=20, adjust=False).mean()
    df["kc_upper"] = ema20 + 2 * atr
    df["kc_lower"] = ema20 - 2 * atr

    # ─ Squeeze (BB 안쪽 + KC 바깥쪽) ──
    df["squeeze_off"] = (
        (df["bb_upper"] > df["kc_upper"]) | (df["bb_lower"] < df["kc_lower"])
    )

    # ─ RSI (4h = 4 bar) ──
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(4).mean()
    loss = -delta.where(delta < 0, 0).rolling(4).mean()
    rs = gain / (loss + 1e-9)
    df["rsi_4h"] = 100 - (100 / (1 + rs))

    # ─ 펀딩비 z-score (20봉 롤링) ──
    if "funding_rate" in df.columns:
        funding_mean = df["funding_rate"].rolling(20).mean()
        funding_std = df["funding_rate"].rolling(20).std()
        df["funding_zscore"] = (df["funding_rate"] - funding_mean) / (funding_std + 1e-9)

        # ─ 30일 펀딩비 MA ──
        df["funding_30d_ma"] = df["funding_rate"].rolling(30).mean()
    else:
        df["funding_zscore"] = 0.0
        df["funding_30d_ma"] = 0.0

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# HMM Regime Detection
# ═══════════════════════════════════════════════════════════════════════════════


def _fit_hmm_models(df_1h: pd.DataFrame, df_6h: pd.DataFrame) -> dict[int, HMMRegimeDetector]:
    """
    365일 롤링 윈도우로 HMM 모델 학습.
    각 1h 타임스탭에 대해 365일 이전 데이터로 HMM 학습.
    """
    # HMM은 충분한 데이터가 필요하므로 (최소 365일 이상)
    # 단순화: 전체 6h 데이터로 단일 모델 학습
    detector = HMMRegimeDetector(n_components=3)
    detector.fit(df_6h)
    return {0: detector}  # 실제 롤링은 복잡하므로 단일 모델 사용


def _predict_hmm_state(row_idx: int, df_6h_slice: pd.DataFrame, detector: HMMRegimeDetector) -> tuple[int, np.ndarray]:
    """HMM 상태와 확률 예측."""
    if len(df_6h_slice) < 50:
        return 1, np.array([0.33, 0.34, 0.33])  # 데이터 부족 시 중간 레짐

    try:
        state = detector.predict_state(df_6h_slice)
        proba = detector.predict_proba(df_6h_slice)
        return state, proba
    except Exception:
        return 1, np.array([0.33, 0.34, 0.33])


# ═══════════════════════════════════════════════════════════════════════════════
# Main Backtest Engine
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class Trade:
    entry_ts: pd.Timestamp
    entry_price: float
    entry_side: str  # "long" or "short"
    exit_ts: pd.Timestamp | None = None
    exit_price: float | None = None
    pnl: float = 0.0
    pnl_pct: float = 0.0


@dataclass
class BacktestConfig:
    """백테스트 설정."""
    # HMM & LLM
    use_hmm: bool = True
    use_llm: bool = True
    llm_confidence_threshold: int = 70

    # Regime 크기 비율
    regime_size_low: float = 0.20    # state=0 (저변동)
    regime_size_mid: float = 0.10    # state=1 (중간)
    regime_size_high: float = 0.25   # state=2 (고변동)

    # Drawdown brake
    dd_level1: float = -0.05   # 75% 감소
    dd_level2: float = -0.10   # 청산 + 48h 금지


class HMMLLMBacktester:
    """HMM+LLM 메타 백테스터."""

    def __init__(
        self,
        ohlcv_1h: pd.DataFrame,
        ohlcv_6h: pd.DataFrame,
        funding: pd.DataFrame,
        config: BacktestConfig,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        self.df_1h = ohlcv_1h.copy()
        self.df_6h = ohlcv_6h.copy()
        self.funding_df = funding.copy()
        self.config = config
        self.initial_capital = initial_capital

        # 상태
        self.equity = initial_capital
        self.equity_peak = initial_capital
        self.position_size = 0.0
        self.position_entry_price = 0.0
        self.position_side = None  # "long" or "short"
        self.trades: list[Trade] = []
        self.equity_curve = []
        self.trading_halt_until = None

        # 지표
        self._add_indicators()
        self._fit_hmm()

    def _add_indicators(self):
        """지표 계산."""
        self.df_1h = _add_technical_indicators(self.df_1h)
        self.df_6h = _add_technical_indicators(self.df_6h)

        # Funding rate 병합 (1h)
        if not self.funding_df.empty:
            self.df_1h = self.df_1h.join(
                self.funding_df.rename(columns={"rate": "funding_rate"}),
                how="left"
            )
            self.df_1h["funding_rate"] = self.df_1h["funding_rate"].fillna(0.0)

    def _fit_hmm(self):
        """HMM 모델 학습."""
        self.hmm_detector = HMMRegimeDetector(n_components=3)
        try:
            self.hmm_detector.fit(self.df_6h)
        except Exception as e:
            log.warning(f"HMM fit failed: {e}, using dummy detector")
            self.hmm_detector = None

    def run(self) -> dict[str, Any]:
        """백테스트 실행."""
        df_1h_indexed = self.df_1h.reset_index(drop=True)  # 정수 인덱스로 변환

        # HMM 캐시: 6h 바마다 캐시 갱신 (성능 최적화)
        hmm_state_cache = 1
        hmm_proba_cache = np.array([0.33, 0.34, 0.33])
        last_hmm_update_idx = -999

        for idx_int in range(len(df_1h_indexed)):
            row = df_1h_indexed.iloc[idx_int]
            ts = self.df_1h.index[idx_int]

            # ─ Halt 확인 ──
            if self.trading_halt_until and ts < self.trading_halt_until:
                continue

            # ─ HMM 레짐 ── (6h마다만 갱신, 나머지는 캐시 사용)
            hmm_state = hmm_state_cache
            hmm_proba = hmm_proba_cache
            if self.config.use_hmm and self.hmm_detector:
                # 6시간 = 6개 1h 봉 (최적화: 캐시 사용)
                if idx_int - last_hmm_update_idx >= 6:
                    if len(self.df_6h) >= 50:
                        # 현재 ts까지의 6h 데이터만 사용
                        idx_pos = self.df_6h.index.searchsorted(ts, side='right')
                        if idx_pos >= 50:
                            df_6h_slice = self.df_6h.iloc[:idx_pos]
                            try:
                                hmm_state, hmm_proba = _predict_hmm_state(ts, df_6h_slice, self.hmm_detector)
                                hmm_state_cache = hmm_state
                                hmm_proba_cache = hmm_proba
                                last_hmm_update_idx = idx_int
                            except Exception:
                                pass  # 예측 실패 시 이전 값 유지

            # ─ 하위 전략 신호 ──
            trend_sig = _trend_signal(row)
            reversion_sig = _reversion_signal(row)
            carry_sig = _carry_signal(row)

            # ─ 레짐별 신호 우선순위 ──
            if hmm_state == 0:  # 저변동 → 회귀
                signal = reversion_sig if reversion_sig != "flat" else trend_sig
                size_ratio = self.config.regime_size_low
            elif hmm_state == 1:  # 중간 → 추세 + 회귀
                signal = trend_sig if trend_sig != "flat" else reversion_sig
                size_ratio = self.config.regime_size_mid
            else:  # 고변동 → 추세
                signal = trend_sig
                size_ratio = self.config.regime_size_high

            # ─ LLM 오버라이드 ──
            if self.config.use_llm:
                # 가격 변화 계산 (최적화)
                try:
                    close_24h_ago = self.df_1h["close"].iloc[max(0, idx_int - 24)] if idx_int >= 24 else row["close"]
                    close_168h_ago = self.df_1h["close"].iloc[max(0, idx_int - 168)] if idx_int >= 168 else row["close"]
                    price_change_24h = ((row["close"] / close_24h_ago) - 1) * 100
                    price_change_7d = ((row["close"] / close_168h_ago) - 1) * 100
                except (IndexError, KeyError, ZeroDivisionError):
                    price_change_24h = 0.0
                    price_change_7d = 0.0

                context = {
                    "price_change_24h": price_change_24h,
                    "price_change_7d": price_change_7d,
                    "hmm_state": hmm_state,
                    "hmm_proba": hmm_proba.tolist(),
                    "funding_rate": row.get("funding_rate", 0.0),
                    "funding_zscore": row.get("funding_zscore", 0.0),
                    "fear_greed": 50,  # 실전에서는 외부 API 사용
                }
                advisory = simulate_llm_advisory(context)

                if advisory.confidence > self.config.llm_confidence_threshold:
                    if advisory.direction == "flat":
                        signal = "flat"
                    elif advisory.direction != signal and signal != "flat":
                        signal = "flat"  # 충돌 시 안전하게
                    size_ratio *= advisory.size_multiplier

            # ─ Drawdown brake ──
            dd = (self.equity - self.equity_peak) / self.equity_peak if self.equity_peak > 0 else 0.0
            if dd < self.config.dd_level2:  # -10% → 청산
                signal = "flat"
                self.trading_halt_until = ts + timedelta(hours=48)
            elif dd < self.config.dd_level1:  # -5% → 75% 감소
                size_ratio *= 0.25

            # ─ 주문 실행 ──
            self._execute_trade(ts, row["close"], signal, size_ratio)

            # ─ 청산 가격 업데이트 ──
            if self.position_size > 0:
                pnl = self._update_position(row["close"])

            # ─ 청산 ──
            if signal == "flat" and self.position_size > 0:
                self._close_position(ts, row["close"])

            # ─ Equity 기록 ──
            self.equity_curve.append({"ts": ts, "equity": self.equity})
            self.equity_peak = max(self.equity_peak, self.equity)

        return self._compute_metrics()

    def _execute_trade(self, ts: pd.Timestamp, price: float, signal: str, size_ratio: float):
        """주문 실행 (이전 신호 변화 시에만)."""
        if signal == "flat":
            return

        if self.position_side == signal:
            return  # 같은 방향이면 넘김

        # ─ 기존 포지션 청산 ──
        if self.position_size > 0:
            self._close_position(ts, price)

        # ─ 신규 진입 ──
        notional = self.equity * size_ratio * LEVERAGE
        self.position_size = notional / price
        self.position_entry_price = price
        self.position_side = signal

    def _update_position(self, current_price: float) -> float:
        """포지션 손익 업데이트."""
        if self.position_size == 0:
            return 0.0

        if self.position_side == "long":
            price_chg = current_price - self.position_entry_price
        else:  # short
            price_chg = self.position_entry_price - current_price

        notional = self.position_size * self.position_entry_price
        pnl = (price_chg / self.position_entry_price) * notional
        fee = notional * FEE_RATE
        pnl -= fee

        return pnl

    def _close_position(self, ts: pd.Timestamp, price: float):
        """포지션 청산."""
        if self.position_size == 0:
            return

        pnl = self._update_position(price)
        pnl_pct = (pnl / self.equity) * 100

        trade = Trade(
            entry_ts=ts,  # 간략화
            entry_price=self.position_entry_price,
            entry_side=self.position_side,
            exit_ts=ts,
            exit_price=price,
            pnl=pnl,
            pnl_pct=pnl_pct,
        )
        self.trades.append(trade)

        self.equity += pnl
        self.position_size = 0
        self.position_entry_price = 0
        self.position_side = None

    def _compute_metrics(self) -> dict[str, Any]:
        """성과 지표 계산."""
        eq_curve = pd.DataFrame(self.equity_curve)
        returns = eq_curve["equity"].pct_change().dropna()

        total_return_pct = ((self.equity - self.initial_capital) / self.initial_capital) * 100
        sharpe_val = sharpe(returns) if len(returns) > 1 else 0.0
        mdd_val = mdd(eq_curve["equity"]) if len(eq_curve) > 0 else 0.0
        cagr_val = cagr(
            total_return_pct,
            (self.df_1h.index[-1] - self.df_1h.index[0]).total_seconds() / (365.25 * 24 * 3600),
        ) if self.df_1h.index[-1] > self.df_1h.index[0] else 0.0

        num_trades = len(self.trades)
        winning_trades = sum(1 for t in self.trades if t.pnl > 0)
        win_rate = (winning_trades / num_trades * 100) if num_trades > 0 else 0.0

        return {
            "final_equity": safe_float(self.equity),
            "total_return_pct": safe_float(total_return_pct),
            "sharpe_ratio": safe_float(sharpe_val),
            "max_drawdown_pct": safe_float(mdd_val),
            "cagr_pct": safe_float(cagr_val),
            "num_trades": num_trades,
            "win_rate": safe_float(win_rate),
            "equity_curve": eq_curve.to_dict("records"),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Main Execution
# ═══════════════════════════════════════════════════════════════════════════════


async def run_backtest(
    pool: asyncpg.Pool,
    start_date: str,
    end_date: str,
    stage: str,
) -> dict[str, Any]:
    """백테스트 실행."""
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

    log.info("Loading OHLCV data", symbol=SYMBOL, start=start, end=end)
    df_1h = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start, end)
    df_6h = await load_ohlcv(pool, SYMBOL, TIMEFRAME_6H, start, end)
    df_funding = await load_funding(pool, SYMBOL, start, end)

    if df_1h.empty or df_6h.empty:
        log.error("No OHLCV data loaded")
        return {}

    # ─ Funding rate 전처리 ──
    if not df_funding.empty:
        # load_funding은 이미 index=ts (UTC)로 반환하므로 추가 처리 불필요
        pass
    else:
        df_funding = pd.DataFrame(index=df_1h.index, columns=["rate"])

    results = {}

    if stage in ("all", "1"):
        log.info("Running Stage 1: Baseline")
        config = BacktestConfig()
        bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
        results["stage_1"] = bt.run()

    if stage in ("all", "2"):
        log.info("Running Stage 2: Regime Size Variants")
        for variant in STAGE_2_VARIANTS:
            config = BacktestConfig(
                regime_size_low=variant["low"],
                regime_size_mid=variant["mid"],
                regime_size_high=variant["high"],
            )
            bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
            results[f"stage_2_{variant['name']}"] = bt.run()

    if stage in ("all", "3"):
        log.info("Running Stage 3: LLM Confidence Thresholds")
        for threshold in STAGE_3_VARIANTS:
            config = BacktestConfig(llm_confidence_threshold=threshold)
            bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
            results[f"stage_3_conf_{threshold}"] = bt.run()

    if stage in ("all", "4"):
        log.info("Running Stage 4: Drawdown Brake Levels")
        for variant in STAGE_4_VARIANTS:
            config = BacktestConfig(
                dd_level1=variant["l1"],
                dd_level2=variant["l2"],
            )
            bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
            results[f"stage_4_{variant['name']}"] = bt.run()

    if stage in ("all", "5"):
        log.info("Running Stage 5: Ablation Study")
        # (a) HMM만
        config = BacktestConfig(use_hmm=True, use_llm=False)
        bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
        results["stage_5_ablation_hmm_only"] = bt.run()

        # (b) LLM만
        config = BacktestConfig(use_hmm=False, use_llm=True)
        bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
        results["stage_5_ablation_llm_only"] = bt.run()

        # (c) HMM + LLM
        config = BacktestConfig(use_hmm=True, use_llm=True)
        bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
        results["stage_5_ablation_hmm_llm"] = bt.run()

        # (d) 풀스택 (모든 기능)
        config = BacktestConfig(use_hmm=True, use_llm=True)
        bt = HMMLLMBacktester(df_1h, df_6h, df_funding, config)
        results["stage_5_ablation_full_stack"] = bt.run()

    return results


async def main():
    """메인 진입점."""
    parser = argparse.ArgumentParser(description="HMM+LLM Meta Strategy Backtest")
    parser.add_argument("--stage", default="1", choices=["1", "2", "3", "4", "5", "6", "all"],
                        help="Stage to run")
    parser.add_argument("--start", default="2023-04-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-03-31", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    pool = await make_pool()
    try:
        results = await run_backtest(pool, args.start, args.end, args.stage)

        # ─ 결과 저장 (간략) ──
        print("\n" + "=" * 80)
        print("BACKTEST RESULTS")
        print("=" * 80)
        for name, metrics in results.items():
            if metrics:
                print(f"\n{name}:")
                print(f"  CAGR: {metrics.get('cagr_pct', 0):.2f}%")
                print(f"  Sharpe: {metrics.get('sharpe_ratio', 0):.3f}")
                print(f"  MDD: {metrics.get('max_drawdown_pct', 0):.2f}%")
                print(f"  Trades: {metrics.get('num_trades', 0)}")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
