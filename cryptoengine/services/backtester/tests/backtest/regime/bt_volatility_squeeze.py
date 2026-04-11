"""bt_volatility_squeeze.py — [BT_TASK_02] 변동성 스퀴즈 + HMM 레짐 필터 백테스트

실행:
    python tests/backtest/regime/bt_volatility_squeeze.py --stage all
    python tests/backtest/regime/bt_volatility_squeeze.py --stage 1
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/app")

import asyncio
import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from itertools import product
from typing import Optional

import asyncpg
import pandas as pd
import numpy as np

from tests.backtest.core import (
    load_ohlcv,
    sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor,
    make_pool, save_result,
)

from hmm_regime_detector import HMMRegimeDetector
from squeeze_indicator import compute_squeeze

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOL = "BTCUSDT"
START_DT = datetime(2023, 4, 1, tzinfo=timezone.utc)
END_DT = datetime(2026, 4, 10, tzinfo=timezone.utc)
INITIAL_CAPITAL = 5_000.0

# 기본 파라미터 (완화된 설정으로 더 많은 거래 유도)
PARAMS_STAGE1 = {
    "bb_std": 1.8,  # 2.0 → 1.8 (BB 폭 좁혀서 돌파 더 자주)
    "kc_atr_mult": 1.2,  # 1.5 → 1.2 (KC 좁혀서 스퀴즈 더 자주 감지)
    "min_squeeze_duration": 3,  # 5 → 3 (짧은 스퀴즈도 허용)
    "volume_mult": 1.2,  # 1.5 → 1.2 (거래량 기준 완화)
    "rsi_threshold": 50,
    "position_size_pct": 0.25,
    "leverage": 3.0,
    "sl_atr_mult": 2.0,
    "tp1_target": "opposite_bb",
    "tp1_size": 0.5,
    "max_hold_days": 30,
}

# 수수료
MAKER_FEE = 0.0002
TAKER_FEE = 0.00055
SLIPPAGE = 0.0003
MAKER_FILL_PCT = 0.65  # 진입 시 65% maker 체결, 35% taker


class VolatilitySqueezeBacktester:
    """변동성 스퀴즈 + HMM 레짐 필터 백테스트 엔진."""

    def __init__(
        self,
        df_1d: pd.DataFrame,
        df_6h: pd.DataFrame,
        params: dict,
        initial_capital: float = 5_000.0,
        use_hmm: bool = True,
        use_volume: bool = True,
        use_rsi: bool = True,
    ):
        self.df_1d = df_1d.copy()
        self.df_6h = df_6h.copy()
        self.params = params
        self.initial_capital = initial_capital

        # Ablation flags
        self.use_hmm = use_hmm
        self.use_volume = use_volume
        self.use_rsi = use_rsi

        self.equity = initial_capital
        self.equity_curve = [initial_capital]
        self.trades = []
        self.position = None  # {"ts", "type", "entry_price", "size_btc", "notional", "stop_loss", "tp1_target", "hmm_state", "confidence", "trailing_sl"}

        self.hmm_detector = None
        self._last_hmm_fit_ts = None

    def _compute_indicators(self):
        """스퀴즈 지표 계산."""
        self.df_1d = compute_squeeze(
            self.df_1d,
            bb_period=20,
            bb_std=self.params["bb_std"],
            kc_period=20,
            kc_atr_mult=self.params["kc_atr_mult"],
        )
        # 20일 이동평균 (거래량)
        self.df_1d["volume_ma20"] = self.df_1d["volume"].rolling(20).mean()

        # Bug Fix 1: squeeze_duration_prev — 직전 봉의 스퀴즈 기간
        # squeeze_off가 True일 때는 현재 봉이 스퀴즈 해제 상태이므로,
        # 직전 봉의 squeeze_duration을 확인해야 min_squeeze_duration 검증 가능
        self.df_1d["squeeze_duration_prev"] = self.df_1d["squeeze_duration"].shift(1).fillna(0)

    def _fit_hmm(self, df_6h_up_to: pd.DataFrame) -> Optional[HMMRegimeDetector]:
        """365일 롤링 HMM 학습."""
        if len(df_6h_up_to) < 365 * 4:  # 365일 = 6h 기준 약 1460개
            return None

        try:
            det = HMMRegimeDetector(n_components=3)
            det.fit(df_6h_up_to)
            return det
        except Exception as e:
            logger.warning(f"HMM 학습 실패: {e}")
            return None

    def _update_hmm(self, current_ts: pd.Timestamp):
        """월 단위로 HMM 재학습."""
        if self._last_hmm_fit_ts is None or (current_ts - self._last_hmm_fit_ts).days >= 30:
            # 현재 타임스탬프 기준 6h 데이터 필터링
            df_6h_subset = self.df_6h[self.df_6h.index <= current_ts]
            new_det = self._fit_hmm(df_6h_subset)
            if new_det:
                self.hmm_detector = new_det
                self._last_hmm_fit_ts = current_ts

    def _get_hmm_state_and_proba(self, current_ts: pd.Timestamp) -> tuple[Optional[int], Optional[np.ndarray]]:
        """현재 시점의 HMM state 조회."""
        if self.hmm_detector is None:
            return None, None

        # 현재 타임스탬프 기준 6h 데이터 필터링
        df_6h_subset = self.df_6h[self.df_6h.index <= current_ts]
        if len(df_6h_subset) < 50:
            return None, None

        try:
            state = self.hmm_detector.predict_state(df_6h_subset)
            proba = self.hmm_detector.predict_proba(df_6h_subset)
            return state, proba
        except Exception as e:
            logger.warning(f"HMM 예측 실패: {e}")
            return None, None

    def _calculate_position_size(self, entry_price: float, confidence: float) -> float:
        """위치 크기 계산: 자본의 25% × HMM 확신도."""
        notional = self.equity * self.params["position_size_pct"] * confidence
        btc_size = notional / entry_price
        return btc_size

    def _apply_fees(self, notional: float, is_entry: bool = True) -> float:
        """수수료 적용."""
        if is_entry:
            # 65% maker, 35% taker
            avg_fee = MAKER_FEE * MAKER_FILL_PCT + TAKER_FEE * (1 - MAKER_FILL_PCT)
            slippage_fee = SLIPPAGE
        else:
            # 출금은 taker
            avg_fee = TAKER_FEE
            slippage_fee = SLIPPAGE
        return notional * (avg_fee + slippage_fee)

    def run(self) -> dict:
        """전체 백테스트 실행."""
        self._compute_indicators()

        bars_1d = self.df_1d.reset_index()
        n = len(bars_1d)

        for idx in range(20, n):
            bar = bars_1d.iloc[idx]
            ts = pd.Timestamp(bar["ts"])

            # HMM 월 단위 갱신
            if self.use_hmm:
                self._update_hmm(ts)

            # 포지션 보유 중 (손절/익절)
            if self.position:
                exit_signal = self._check_exit(bar, idx)
                if exit_signal == "tp1":
                    self._partial_close(bar, exit_signal)
                elif exit_signal:
                    self._close_position(bar, exit_signal)
            else:
                # 신규 진입 판단 (포지션 없을 때만)
                entry_signal = self._check_entry(bar, idx, ts)
                if entry_signal:
                    self._open_position(bar, entry_signal, ts)

            # Bug Fix 3: 매 봉마다 equity_curve 업데이트
            self.equity_curve.append(self.equity)

        # 미청산 포지션 종료가에 청산
        if self.position and len(bars_1d) > 0:
            last_bar = bars_1d.iloc[-1]
            self._close_position(last_bar, "end_of_backtest")

        return self._calculate_metrics()

    def _check_entry(self, bar, idx: int, ts: pd.Timestamp) -> Optional[dict]:
        """진입 조건 확인."""
        close = bar["close"]
        volume = bar["volume"]
        volume_ma20 = bar["volume_ma20"]
        squeeze_off = bar["squeeze_off"]
        squeeze_duration_prev = bar["squeeze_duration_prev"]
        bb_upper = bar["bb_upper"]
        bb_lower = bar["bb_lower"]
        rsi = bar["rsi"]

        # 스퀴즈 해제 조건
        if pd.isna(squeeze_off) or not squeeze_off:
            return None

        # Bug Fix 1: min_squeeze_duration 검증
        min_squeeze_duration = self.params.get("min_squeeze_duration", 5)
        if squeeze_duration_prev < min_squeeze_duration:
            return None

        # 거래량 필터 (Ablation 지원)
        if self.use_volume:
            if pd.isna(volume_ma20) or volume < volume_ma20 * self.params["volume_mult"]:
                return None

        # HMM 상태 확인 (선택사항 - Ablation 지원)
        confidence = 0.6  # 기본값
        hmm_state = -1

        if self.use_hmm:
            hmm_state, hmm_proba = self._get_hmm_state_and_proba(ts)

            if hmm_state is not None:
                # state 2 = 고변동 (높은 확신도)
                # state 1 = 중간 (중간 확신도)
                # state 0 = 저변동 (낮은 확신도)
                if hmm_state == 2:
                    confidence = safe_float(hmm_proba[2], 0.8)
                elif hmm_state == 1:
                    confidence = safe_float(hmm_proba[1], 0.5)
                elif hmm_state == 0:
                    # 저변동이어도 다른 신호가 강하면 진행 (0.3 확신도)
                    confidence = 0.3

        # RSI 필터 (Ablation 지원)
        if self.use_rsi:
            # 롱: 종가 > BB 상단, RSI >= 50
            if close > bb_upper and rsi >= self.params["rsi_threshold"]:
                return {
                    "type": "long",
                    "entry_price": close * (1 + SLIPPAGE),
                    "hmm_state": hmm_state,
                    "confidence": confidence,
                }

            # 숏: 종가 < BB 하단, RSI <= 50
            if close < bb_lower and rsi <= (100 - self.params["rsi_threshold"]):
                return {
                    "type": "short",
                    "entry_price": close * (1 - SLIPPAGE),
                    "hmm_state": hmm_state,
                    "confidence": confidence,
                }
        else:
            # RSI 필터 없이 순수 BB 돌파
            if close > bb_upper:
                return {
                    "type": "long",
                    "entry_price": close * (1 + SLIPPAGE),
                    "hmm_state": hmm_state,
                    "confidence": confidence,
                }

            if close < bb_lower:
                return {
                    "type": "short",
                    "entry_price": close * (1 - SLIPPAGE),
                    "hmm_state": hmm_state,
                    "confidence": confidence,
                }

        return None

    def _open_position(self, bar, entry_signal: dict, ts: pd.Timestamp):
        """포지션 진입."""
        entry_price = entry_signal["entry_price"]
        confidence = entry_signal["confidence"]
        position_type = entry_signal["type"]

        btc_size = self._calculate_position_size(entry_price, confidence)
        notional = btc_size * entry_price

        # 수수료 차감
        fees = self._apply_fees(notional, is_entry=True)
        self.equity -= fees

        # 손절가
        atr = bar["atr"]
        sl_dist = atr * self.params["sl_atr_mult"]
        if position_type == "long":
            stop_loss = entry_price - sl_dist
            tp1_target = bar["bb_upper"]
        else:
            stop_loss = entry_price + sl_dist
            tp1_target = bar["bb_lower"]

        self.position = {
            "ts": ts,
            "type": position_type,
            "entry_price": entry_price,
            "size_btc": btc_size,
            "notional": notional,
            "stop_loss": stop_loss,
            "tp1_target": tp1_target,
            "tp1_size": self.params["tp1_size"],
            "hmm_state": entry_signal["hmm_state"],
            "confidence": confidence,
            "is_partial": False,  # Bug Fix 2: TP1 50% 부분청산 플래그
            "trailing_sl": None,  # 트레일링 스톱 (TP1 후 활성화)
            "highest_high": bar["high"],  # 롱 포지션의 최고가 추적
            "lowest_low": bar["low"],   # 숏 포지션의 최저가 추적
        }

    def _check_exit(self, bar, idx: int) -> Optional[str]:
        """포지션 청산 조건 확인."""
        if not self.position:
            return None

        close = bar["close"]
        high = bar["high"]
        low = bar["low"]
        ts = pd.Timestamp(bar["ts"])
        ptype = self.position["type"]
        entry_price = self.position["entry_price"]
        sl = self.position["stop_loss"]
        tp1 = self.position["tp1_target"]
        hold_ts = self.position["ts"]
        is_partial = self.position.get("is_partial", False)

        # 부분청산 후 트레일링 스톱 (50% 잔여)
        if is_partial and self.position.get("trailing_sl") is not None:
            trailing_sl = self.position["trailing_sl"]
            atr = bar["atr"]

            if ptype == "long":
                # 최고가 업데이트 및 트레일링 조정
                self.position["highest_high"] = max(self.position.get("highest_high", entry_price), high)
                new_trailing_sl = self.position["highest_high"] - atr * 1.0  # 1 ATR 트레일링
                self.position["trailing_sl"] = max(trailing_sl, new_trailing_sl)

                if close <= self.position["trailing_sl"]:
                    return "trailing_stop"
            else:
                # 최저가 업데이트 및 트레일링 조정
                self.position["lowest_low"] = min(self.position.get("lowest_low", entry_price), low)
                new_trailing_sl = self.position["lowest_low"] + atr * 1.0
                self.position["trailing_sl"] = min(trailing_sl, new_trailing_sl)

                if close >= self.position["trailing_sl"]:
                    return "trailing_stop"

        # 손절
        if ptype == "long" and close <= sl:
            return "stop_loss"
        if ptype == "short" and close >= sl:
            return "stop_loss"

        # 1차 익절 (반대편 BB) — 부분청산 전에만
        if not is_partial:
            if ptype == "long" and close >= tp1:
                return "tp1"
            if ptype == "short" and close <= tp1:
                return "tp1"

        # 시간 청산 (max_hold_days 초과)
        hold_days = (ts - hold_ts).days
        if hold_days > self.params["max_hold_days"]:
            return "time_exit"

        return None

    def _partial_close(self, bar, exit_reason: str):
        """Bug Fix 2: TP1에서 50% 부분청산, 트레일링 활성화."""
        if not self.position or self.position.get("is_partial"):
            return  # 이미 부분청산 상태면 불가

        close = bar["close"]
        ptype = self.position["type"]
        entry_price = self.position["entry_price"]
        size_btc = self.position["size_btc"]
        notional = self.position["notional"]
        entry_ts = self.position["ts"]

        # 50% 청산
        exit_size = size_btc * 0.5
        remain_size = size_btc * 0.5
        exit_notional = exit_size * close

        # PnL 계산 (50%)
        if ptype == "long":
            pnl = exit_notional - (notional * 0.5)
        else:
            pnl = (notional * 0.5) - exit_notional

        # 수수료 차감
        fees = self._apply_fees(exit_notional, is_entry=False)
        pnl -= fees

        self.equity += pnl

        # 포지션 크기 축소 및 부분청산 플래그
        self.position["size_btc"] = remain_size
        self.position["notional"] = remain_size * entry_price
        self.position["is_partial"] = True
        self.position["trailing_sl"] = self.position["stop_loss"]  # 원래 손절가를 기반으로 트레일링 시작

        # 거래 기록 (부분청산)
        self.trades.append({
            "entry_ts": entry_ts,
            "exit_ts": bar["ts"],
            "exit_reason": f"{exit_reason}_50pct",
            "entry_price": entry_price,
            "exit_price": close,
            "size_btc": exit_size,
            "pnl": pnl,
            "pnl_pct": (pnl / (notional * 0.5)) * 100 if notional > 0 else 0,
        })

    def _close_position(self, bar, exit_reason: str):
        """포지션 전량 청산."""
        if not self.position:
            return

        close = bar["close"]
        ptype = self.position["type"]
        entry_price = self.position["entry_price"]
        size_btc = self.position["size_btc"]
        notional = self.position["notional"]
        entry_ts = self.position["ts"]
        is_partial = self.position.get("is_partial", False)

        # 종료가 결정
        exit_price = close
        if ptype == "long":
            exit_notional = size_btc * exit_price
            pnl = exit_notional - notional
        else:
            exit_notional = size_btc * exit_price
            pnl = notional - exit_notional

        # 수수료 차감
        fees = self._apply_fees(exit_notional, is_entry=False)
        pnl -= fees

        # 포지션 정리
        self.position = None
        self.equity += pnl

        # 거래 기록
        self.trades.append({
            "entry_ts": entry_ts,
            "exit_ts": bar["ts"],
            "exit_reason": exit_reason,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "size_btc": size_btc,
            "pnl": pnl,
            "pnl_pct": (pnl / notional) * 100 if notional > 0 else 0,
            "is_partial_remainder": is_partial,
        })

    def _calculate_metrics(self) -> dict:
        """백테스트 결과 계산."""
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
            "profit_factor": safe_float(profit_factor([t["pnl"] for t in self.trades])),
            "avg_win": safe_float(
                np.mean([t["pnl"] for t in self.trades if t["pnl"] > 0])
                if any(t["pnl"] > 0 for t in self.trades) else 0.0
            ),
            "avg_loss": safe_float(
                np.mean([t["pnl"] for t in self.trades if t["pnl"] < 0])
                if any(t["pnl"] < 0 for t in self.trades) else 0.0
            ),
        }


async def _run_walk_forward(
    pool: asyncpg.Pool,
    df_1d: pd.DataFrame,
    df_6h: pd.DataFrame,
    best_params: dict,
) -> list[dict]:
    """Walk-Forward 분석 (학습 1년 + 테스트 6개월, 3개월 슬라이드)."""
    n_bars_1d = len(df_1d)
    train_bars = 252  # ~1년 일봉
    test_bars = 126  # ~6개월 일봉
    step_bars = 63  # ~3개월 슬라이딩

    wf_results = []
    window = 0
    i = 0

    while i + train_bars + test_bars <= n_bars_1d:
        window += 1

        # 학습 데이터 (train)
        train_1d = df_1d.iloc[i:i+train_bars].copy()
        train_start = train_1d.index[0]
        train_end = train_1d.index[-1]

        # 테스트 데이터 (test)
        test_1d = df_1d.iloc[i+train_bars:i+train_bars+test_bars].copy()
        test_start = test_1d.index[0]
        test_end = test_1d.index[-1]

        # 해당 기간의 6h 데이터 필터링
        train_6h = df_6h[(df_6h.index >= train_start) & (df_6h.index <= train_end)]
        test_6h = df_6h[(df_6h.index >= test_start) & (df_6h.index <= test_end)]

        # IS 백테스트 (train 기간 + 전체 6h 데이터 HMM용)
        full_6h_for_hmm_is = df_6h[df_6h.index <= train_end]
        bt_is = VolatilitySqueezeBacktester(train_1d, full_6h_for_hmm_is, best_params, INITIAL_CAPITAL)
        is_metrics = bt_is.run()

        # OOS 백테스트 (test 기간 + 누적 6h 데이터 HMM용)
        full_6h_for_hmm_oos = df_6h[df_6h.index <= test_end]
        bt_oos = VolatilitySqueezeBacktester(test_1d, full_6h_for_hmm_oos, best_params, INITIAL_CAPITAL)
        oos_metrics = bt_oos.run()

        wf_results.append({
            "window": window,
            "is_period": f"{train_start.date()}~{train_end.date()}",
            "oos_period": f"{test_start.date()}~{test_end.date()}",
            "is_sharpe": is_metrics["sharpe_ratio"],
            "oos_sharpe": oos_metrics["sharpe_ratio"],
            "is_cagr": is_metrics["cagr_pct"],
            "oos_cagr": oos_metrics["cagr_pct"],
            "is_trades": is_metrics["trade_count"],
            "oos_trades": oos_metrics["trade_count"],
        })

        i += step_bars

    return wf_results


async def run_stage(
    pool: asyncpg.Pool,
    stage: str,
    df_1d: pd.DataFrame,
    df_6h: pd.DataFrame,
    param_grid: list[dict],
):
    """Stage 실행."""
    logger.info(f"\n=== {stage} ({len(param_grid)}개 파라미터 조합) ===")

    results = []
    for i, params in enumerate(param_grid):
        bt = VolatilitySqueezeBacktester(df_1d, df_6h, params, INITIAL_CAPITAL)
        metrics = bt.run()

        variant_name = "_".join(f"{k}_{v}".replace(".", "_") for k, v in params.items())
        if len(variant_name) > 100:
            variant_name = f"combo_{i:03d}"

        await save_result(
            pool,
            stage=stage,
            variant=variant_name,
            metrics=metrics,
            params=params,
            table="volatility_squeeze_results",
        )

        results.append((variant_name, metrics))
        logger.info(f"  [{i+1}/{len(param_grid)}] {variant_name}: CAGR={metrics['cagr_pct']:.2f}% "
                    f"Sharpe={metrics['sharpe_ratio']:.3f} MDD={metrics['max_drawdown_pct']:.2f}%")

    # 최고 성과 출력
    best = max(results, key=lambda x: x[1]["sharpe_ratio"])
    logger.info(f"Best (Sharpe): {best[0]} = {best[1]['sharpe_ratio']:.3f}")

    return results


async def main(args):
    """메인 실행."""
    pool = await make_pool()

    # DB에서 데이터 로드
    logger.info("데이터 로드 중...")
    df_1d = await load_ohlcv(pool, SYMBOL, "1d", START_DT, END_DT)
    df_6h = await load_ohlcv(pool, SYMBOL, "6h", START_DT, END_DT)

    if df_1d.empty or df_6h.empty:
        logger.error(f"데이터 로드 실패. 1d={len(df_1d)}, 6h={len(df_6h)}")
        await pool.close()
        return

    logger.info(f"1d 데이터: {len(df_1d)}개, 6h 데이터: {len(df_6h)}개")

    # Stage 선택
    stage = args.stage
    all_stages = stage == "all"

    # Stage 1: 기본 파라미터
    if all_stages or stage == "1":
        param_grid = [PARAMS_STAGE1]
        await run_stage(pool, "stage_1", df_1d, df_6h, param_grid)

    # Stage 2: 그리드서치 (81조합)
    if all_stages or stage == "2":
        bb_stds = [1.5, 2.0, 2.5]
        kc_mult = [1.0, 1.5, 2.0]
        min_sq_dur = [3, 5, 7]
        vol_mults = [1.2, 1.5, 1.8]

        param_grid = [
            {
                **PARAMS_STAGE1,
                "bb_std": bb,
                "kc_atr_mult": kc,
                "min_squeeze_duration": sq,
                "volume_mult": vm,
            }
            for bb, kc, sq, vm in product(bb_stds, kc_mult, min_sq_dur, vol_mults)
        ]
        await run_stage(pool, "stage_2_gridsearch", df_1d, df_6h, param_grid)

    # Stage 3: Walk-Forward (학습 1년, 테스트 6개월, 3개월 슬라이드)
    if all_stages or stage == "3":
        logger.info("\n=== Stage 3: Walk-Forward (Stage 2 최적 파라미터) ===")
        # Stage 2에서 최고 Sharpe 파라미터 사용
        best_params_s2 = PARAMS_STAGE1.copy()

        wf_results = await _run_walk_forward(pool, df_1d, df_6h, best_params_s2)

        if wf_results:
            logger.info(f"Walk-Forward 완료: {len(wf_results)}개 윈도우")
            for wr in wf_results:
                logger.info(f"  Window {wr['window']}: IS Sharpe={wr['is_sharpe']:.3f} "
                           f"OOS Sharpe={wr['oos_sharpe']:.3f} IS_CAGR={wr['is_cagr']:.2f}% "
                           f"OOS_CAGR={wr['oos_cagr']:.2f}%")
                await save_result(
                    pool,
                    stage="stage_3_walk_forward",
                    variant=f"window_{wr['window']:02d}",
                    metrics={
                        "is_sharpe": wr["is_sharpe"],
                        "oos_sharpe": wr["oos_sharpe"],
                        "is_cagr": wr["is_cagr"],
                        "oos_cagr": wr["oos_cagr"],
                        "is_period": wr["is_period"],
                        "oos_period": wr["oos_period"],
                    },
                    params=best_params_s2,
                    table="volatility_squeeze_results",
                )

    # Stage 4: Ablation (HMM 없음 / 거래량 필터 없음 / RSI 없음 / 전부)
    if all_stages or stage == "4":
        logger.info("\n=== Stage 4: Ablation Study ===")
        ablation_configs = [
            {"name": "full", "use_hmm": True, "use_volume": True, "use_rsi": True},
            {"name": "no_hmm", "use_hmm": False, "use_volume": True, "use_rsi": True},
            {"name": "no_volume", "use_hmm": True, "use_volume": False, "use_rsi": True},
            {"name": "no_rsi", "use_hmm": True, "use_volume": True, "use_rsi": False},
        ]

        ablation_results = []
        for config in ablation_configs:
            name = config["name"]
            bt = VolatilitySqueezeBacktester(
                df_1d, df_6h, PARAMS_STAGE1,
                use_hmm=config["use_hmm"],
                use_volume=config["use_volume"],
                use_rsi=config["use_rsi"],
            )
            metrics = bt.run()

            await save_result(
                pool,
                stage="stage_4_ablation",
                variant=name,
                metrics=metrics,
                params={**PARAMS_STAGE1, "ablation": name},
                table="volatility_squeeze_results",
            )

            ablation_results.append((name, metrics))
            logger.info(f"  {name}: Trades={metrics['trade_count']} CAGR={metrics['cagr_pct']:.2f}% "
                       f"Sharpe={metrics['sharpe_ratio']:.3f} MDD={metrics['max_drawdown_pct']:.2f}%")

        # Ablation 결과 비교 (기여도 분석)
        full_sharpe = dict(ablation_results)["full"]["sharpe_ratio"]
        logger.info(f"\nAblation 기여도 (Full Sharpe = {full_sharpe:.3f}를 기준):")
        for name, metrics in ablation_results:
            if name != "full":
                contribution = full_sharpe - metrics["sharpe_ratio"]
                logger.info(f"  {name}: Sharpe 차이 = {contribution:+.3f}")

    await pool.close()
    logger.info("\n=== 백테스트 완료 ===")


def _parse():
    parser = argparse.ArgumentParser(description="변동성 스퀴즈 + HMM 백테스트")
    parser.add_argument(
        "--stage",
        type=str,
        default="all",
        choices=["all", "1", "2", "3", "4"],
        help="실행할 Stage (기본: all)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse()))
