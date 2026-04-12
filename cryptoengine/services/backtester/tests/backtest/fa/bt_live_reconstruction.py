"""bt_live_reconstruction.py — Task 9.5: 실거래 손실 재구성 분석

실거래 로그 없이 백테스트로 -24.15% 손실 원인을 역추론한다.

시뮬레이션 시나리오:
  A. 정상 FA (델타뉴트럴)         : 실거래가 이 전략대로 됐다면 예상 수익
  B. 선물 Short 단독 (헷지 누락)  : 현물 Long 없이 선물 Short만 실행됐다면
  C. Kill Switch 조기 발동        : 손실 임계값 도달 후 포지션 청산·재진입 없음
  D. DCA 혼합 계좌               : FA + DCA 전략이 같은 계좌에서 실행됐다면

실행:
    docker compose --profile backtest run --rm backtester \\
      python tests/backtest/fa/bt_live_reconstruction.py
"""
from __future__ import annotations

import asyncio
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import structlog

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float,
    make_pool, FAEngine,
)

log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
INITIAL_CAPITAL = 10_000.0
TAKER_FEE       = 0.00055
MAINT_MARGIN    = 0.005    # 0.5% 유지증거금 비율

# 실거래 추정 기간 (2024년 전체 — ETF 전환 구간)
START_DATE = "2024-01-01"
END_DATE   = "2024-12-31"

# FA 파라미터 (fa80_lev5_r30)
FA_PARAMS = dict(
    fa_capital_ratio    = 0.80,
    leverage            = 5.0,
    reinvest_ratio      = 0.30,
    fee_rate            = TAKER_FEE,
    min_funding_rate    = 0.0001,
    consecutive_intervals = 3,
    max_hold_bars       = 168,
)

# Kill Switch 임계값 (Phase 4/5 설정 추정)
KILL_SWITCH_PCT = -0.10   # 자본 대비 -10% 시 발동

# DCA 파라미터 (Fear&Greed 기반)
DCA_INVEST_AMOUNT = 500.0   # 매 진입 $500
DCA_FEAR_THRESHOLD = 30     # F&G < 30 → 매수


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 A: 정상 FA 델타뉴트럴
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_a(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """정상 FA — 실거래가 의도대로 됐을 때 기대 성과."""
    engine = FAEngine(ohlcv, funding, INITIAL_CAPITAL, **FA_PARAMS)
    result = engine.run()
    return {
        "scenario": "A_normal_fa",
        "label":    "정상 FA (델타뉴트럴)",
        **_extract(result),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 B: 선물 Short 단독 (현물 헷지 누락)
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_b(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """선물 Short만 실행, 현물 Long 없음.

    델타 뉴트럴이 깨져 BTC 가격 상승 시 직접 손실 발생.
    진입·청산 시그널은 FA와 동일하나 P&L에 가격변동분 추가.
    """
    bars     = ohlcv.reset_index()
    equity   = INITIAL_CAPITAL
    position = None
    pos_c = neg_c = 0
    equity_curve = [equity]
    trades       = []

    for idx in range(20, len(bars)):
        bar      = bars.iloc[idx]
        price    = float(bar["close"])
        ts       = bar["ts"] if hasattr(bar["ts"], "hour") else pd.Timestamp(bar["ts"])
        is_settle = (ts.hour % 8 == 0) and (ts.minute == 0)

        # 펀딩비 조회
        fr = _get_funding(bar, funding)

        if position is None:
            if is_settle:
                if fr >= FA_PARAMS["min_funding_rate"]:
                    pos_c += 1; neg_c = 0
                else:
                    pos_c = max(0, pos_c - 1); neg_c = 0
                if pos_c >= FA_PARAMS["consecutive_intervals"]:
                    # 선물 Short 진입 (현물 없음)
                    fa_cap   = equity * FA_PARAMS["fa_capital_ratio"]
                    notional = fa_cap * FA_PARAMS["leverage"]
                    qty      = notional / price
                    fee_in   = notional * TAKER_FEE
                    equity  -= fee_in
                    position = {
                        "side":    "short",
                        "entry_price": price,
                        "qty":     qty,
                        "notional": notional,
                        "entry_idx": idx,
                        "rev_cnt": 0,
                        "funding_acc": 0.0,
                        "fee_paid": fee_in,
                    }
                    pos_c = 0
        else:
            # 펀딩비 수취 (Short이면 양수 펀딩 시 수취)
            if is_settle:
                fund_pnl = position["notional"] * fr * 1  # direction=+1 (short)
                equity += fund_pnl
                position["funding_acc"] += fund_pnl

                # 청산 판단
                rev = fr < 0
                if rev:
                    position["rev_cnt"] += 1
                else:
                    position["rev_cnt"] = 0

                bars_held = idx - position["entry_idx"]
                if position["rev_cnt"] >= 3 or bars_held >= FA_PARAMS["max_hold_bars"]:
                    # 선물 Short 청산: 가격 변동분 반영
                    entry_p  = position["entry_price"]
                    fee_out  = position["notional"] * TAKER_FEE
                    price_pnl = (entry_p - price) * position["qty"]  # short: 하락 시 이익
                    pnl      = price_pnl + position["funding_acc"] - position["fee_paid"] - fee_out
                    equity  += price_pnl - fee_out
                    trades.append({
                        "entry_price": entry_p, "exit_price": price,
                        "price_pnl": price_pnl, "funding_acc": position["funding_acc"],
                        "pnl": pnl,
                    })
                    position = None
                    pos_c = neg_c = 0

            # 청산 전 자본 소진 체크 (롱커버 불능)
            if position:
                unrealized = (position["entry_price"] - price) * position["qty"]
                if unrealized < -equity * 0.95:  # 강제 청산 근접
                    fee_out   = position["notional"] * TAKER_FEE
                    price_pnl = (position["entry_price"] - price) * position["qty"]
                    equity   += price_pnl - fee_out
                    equity    = max(equity, 0)
                    trades.append({
                        "entry_price": position["entry_price"], "exit_price": price,
                        "price_pnl": price_pnl, "funding_acc": position["funding_acc"],
                        "pnl": price_pnl + position["funding_acc"] - position["fee_paid"] - fee_out,
                        "liquidated": True,
                    })
                    position = None
                    pos_c = neg_c = 0
                    log.warning("강제청산(B시나리오)", price=price, equity=equity)

        equity_curve.append(equity)

    # 미청산 포지션 처리
    if position:
        bar       = bars.iloc[-1]
        price_end = float(bar["close"])
        price_pnl = (position["entry_price"] - price_end) * position["qty"]
        fee_out   = position["notional"] * TAKER_FEE
        equity   += price_pnl - fee_out
        trades.append({"pnl": price_pnl + position["funding_acc"] - position["fee_paid"] - fee_out})

    pnls = [t["pnl"] for t in trades]
    eq   = pd.Series(equity_curve)

    return {
        "scenario": "B_short_only",
        "label":    "선물 Short 단독 (헷지 누락)",
        "final_equity":      round(equity, 2),
        "total_profit_pct":  round((equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        "max_drawdown_pct":  round(mdd(eq), 2),
        "sharpe_ratio":      round(sharpe(eq), 4),
        "total_trades":      len(trades),
        "liquidations":      sum(1 for t in trades if t.get("liquidated")),
        "total_funding":     round(sum(t.get("funding_acc", 0) for t in trades), 2),
        "total_price_pnl":   round(sum(t.get("price_pnl", 0) for t in trades), 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 C: Kill Switch 조기 발동 후 미재진입
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_c(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """정상 FA로 시작하다 Kill Switch 발동 후 재진입 안 함.

    Kill Switch 기준: 일별 손실 -10% 또는 누적 MDD -20% 초과.
    발동 후 포지션 전량 청산, 그 이후 거래 없음.
    """
    bars     = ohlcv.reset_index()
    equity   = INITIAL_CAPITAL
    peak     = INITIAL_CAPITAL
    position = None
    pos_c = neg_c = 0
    equity_curve = [equity]
    trades       = []
    ks_triggered = None
    ks_equity    = None

    for idx in range(20, len(bars)):
        bar    = bars.iloc[idx]
        price  = float(bar["close"])
        ts     = bar["ts"] if hasattr(bar["ts"], "hour") else pd.Timestamp(bar["ts"])
        is_settle = (ts.hour % 8 == 0) and (ts.minute == 0)
        fr     = _get_funding(bar, funding)

        # Kill Switch 체크 (누적 MDD -20%)
        peak = max(peak, equity)
        drawdown = (equity - peak) / peak
        if ks_triggered is None and drawdown <= -0.20:
            ks_triggered = ts
            ks_equity    = equity
            # 포지션 강제 청산
            if position:
                fee_out  = position["notional"] * TAKER_FEE
                equity  -= fee_out
                trades.append({"pnl": position.get("funding_acc", 0) - position["fee_paid"] - fee_out,
                                "ks": True})
                position = None
            log.info("Kill Switch 발동", ts=str(ts), equity=equity, drawdown=f"{drawdown:.1%}")

        if ks_triggered:
            equity_curve.append(equity)
            continue

        if position is None:
            if is_settle:
                if fr >= FA_PARAMS["min_funding_rate"]:
                    pos_c += 1; neg_c = 0
                else:
                    pos_c = 0
                if pos_c >= FA_PARAMS["consecutive_intervals"]:
                    fa_cap   = equity * FA_PARAMS["fa_capital_ratio"]
                    notional = fa_cap * FA_PARAMS["leverage"]
                    fee_in   = notional * TAKER_FEE
                    equity  -= fee_in
                    position = {
                        "notional": notional, "entry_price": price, "entry_idx": idx,
                        "rev_cnt": 0, "funding_acc": 0.0, "fee_paid": fee_in,
                        "funding_direction": 1,
                    }
                    pos_c = 0
        else:
            if is_settle:
                fund_pnl = position["notional"] * fr * position["funding_direction"]
                equity  += fund_pnl
                position["funding_acc"] += fund_pnl
                rev = fr < 0
                position["rev_cnt"] = position["rev_cnt"] + 1 if rev else 0
                bars_held = idx - position["entry_idx"]
                if position["rev_cnt"] >= 3 or bars_held >= FA_PARAMS["max_hold_bars"]:
                    fee_out  = position["notional"] * TAKER_FEE
                    equity  -= fee_out
                    pnl      = position["funding_acc"] - position["fee_paid"] - fee_out
                    trades.append({"pnl": pnl})
                    position = None
                    pos_c = neg_c = 0

        equity_curve.append(equity)

    eq   = pd.Series(equity_curve)
    pnls = [t["pnl"] for t in trades]

    return {
        "scenario": "C_kill_switch",
        "label":    "Kill Switch 조기 발동 (재진입 없음)",
        "final_equity":      round(equity, 2),
        "total_profit_pct":  round((equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        "max_drawdown_pct":  round(mdd(eq), 2),
        "sharpe_ratio":      round(sharpe(eq), 4),
        "total_trades":      len(trades),
        "ks_triggered_at":   str(ks_triggered) if ks_triggered else "미발동",
        "ks_equity":         round(ks_equity, 2) if ks_equity else None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 D: DCA 혼합 계좌 (FA + 적응형 DCA 공동 자본)
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_d(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """FA + DCA가 같은 $10K 계좌에서 자본 경합.

    DCA: BTC 가격 하락 구간(-5%/주) 자동 매수 → 계좌 소진 위험
    2024년 하반기 BTC 상승 전 하락 구간에서 DCA 손실이 계좌 잠식 가능.
    """
    bars       = ohlcv.reset_index()
    equity     = INITIAL_CAPITAL
    fa_equity  = INITIAL_CAPITAL * 0.60   # FA: 60%
    dca_equity = INITIAL_CAPITAL * 0.40   # DCA: 40%
    dca_btc    = 0.0
    position   = None
    pos_c      = 0
    equity_curve = [equity]
    trades       = []
    dca_trades   = []

    prev_price   = float(bars.iloc[0]["close"])

    for idx in range(20, len(bars)):
        bar    = bars.iloc[idx]
        price  = float(bar["close"])
        ts     = bar["ts"] if hasattr(bar["ts"], "hour") else pd.Timestamp(bar["ts"])
        is_settle = (ts.hour % 8 == 0) and (ts.minute == 0)
        fr     = _get_funding(bar, funding)

        # DCA: 주간 -5% 이상 하락 시 매수 ($200)
        weekly_ret = (price - prev_price) / prev_price
        if idx % (7 * 24) == 0:  # 주간 기준
            if weekly_ret < -0.05 and dca_equity >= DCA_INVEST_AMOUNT:
                bought_btc = DCA_INVEST_AMOUNT / price
                dca_btc   += bought_btc
                dca_equity -= DCA_INVEST_AMOUNT
                dca_trades.append({"buy_price": price, "qty": bought_btc})
            prev_price = price

        # FA 로직 (fa_equity 사용)
        if position is None:
            if is_settle:
                if fr >= FA_PARAMS["min_funding_rate"]:
                    pos_c += 1
                else:
                    pos_c = 0
                if pos_c >= FA_PARAMS["consecutive_intervals"]:
                    notional = fa_equity * 0.80 * FA_PARAMS["leverage"]
                    fee_in   = notional * TAKER_FEE
                    fa_equity -= fee_in
                    position  = {
                        "notional": notional, "entry_price": price, "entry_idx": idx,
                        "rev_cnt": 0, "funding_acc": 0.0, "fee_paid": fee_in,
                        "funding_direction": 1,
                    }
                    pos_c = 0
        else:
            if is_settle:
                fund_pnl = position["notional"] * fr * position["funding_direction"]
                fa_equity += fund_pnl
                position["funding_acc"] += fund_pnl
                rev = fr < 0
                position["rev_cnt"] = position["rev_cnt"] + 1 if rev else 0
                bars_held = idx - position["entry_idx"]
                if position["rev_cnt"] >= 3 or bars_held >= FA_PARAMS["max_hold_bars"]:
                    fee_out   = position["notional"] * TAKER_FEE
                    fa_equity -= fee_out
                    pnl       = position["funding_acc"] - position["fee_paid"] - fee_out
                    trades.append({"pnl": pnl})
                    position  = None
                    pos_c     = 0

        # 포트폴리오 합산
        dca_value = dca_btc * price + dca_equity
        equity    = fa_equity + dca_value
        equity_curve.append(equity)

    # DCA 최종 청산가 기준 평가
    dca_unrealized = dca_btc * float(bars.iloc[-1]["close"])
    eq   = pd.Series(equity_curve)
    pnls = [t["pnl"] for t in trades]

    final_price = float(bars.iloc[-1]["close"])
    final_dca   = dca_btc * final_price + dca_equity

    return {
        "scenario": "D_fa_plus_dca",
        "label":    "FA + DCA 혼합 계좌",
        "final_equity":      round(equity, 2),
        "total_profit_pct":  round((equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        "max_drawdown_pct":  round(mdd(eq), 2),
        "sharpe_ratio":      round(sharpe(eq), 4),
        "fa_trades":         len(trades),
        "dca_trades":        len(dca_trades),
        "dca_btc_held":      round(dca_btc, 6),
        "dca_final_value":   round(final_dca, 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────────────────

def _get_funding(bar, funding: pd.DataFrame) -> float:
    if funding is None or funding.empty:
        return 0.0001
    try:
        ts   = pd.Timestamp(bar["ts"])
        mask = funding.index <= ts
        if mask.any():
            return float(funding.loc[mask, "rate"].iloc[-1])
    except Exception:
        pass
    return 0.0001


def _extract(result: dict) -> dict:
    return {
        "final_equity":      round(result.get("final_equity", 0), 2),
        "total_profit_pct":  round(result.get("total_profit_pct", 0), 2),
        "max_drawdown_pct":  round(result.get("max_drawdown_pct", 0), 2),
        "sharpe_ratio":      round(result.get("sharpe_ratio", 0), 4),
        "total_trades":      result.get("total_trades", 0),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 E: 진입 방향 반전 버그 (Long 진입해야 할 때 Short, vice versa)
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_e(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """진입 방향이 반대로 코딩된 버그.

    FA 정상 로직: 펀딩 양수 → 선물 Short (펀딩 수취)
    버그 버전:    펀딩 양수 → 선물 Long (펀딩 지불)

    2024년처럼 BTC가 상승하면 Long 포지션은 가격 이익, 하지만 펀딩 지불.
    결과: 가격 변동 포함 시 이익/손실이 복잡하게 교차.
    단, delta-neutral 엔진 기준으로는 펀딩 방향만 반전됨 (가격 무관).
    """
    # delta-neutral 가정 하에 펀딩 방향만 반전
    engine = FAEngine(ohlcv, funding, INITIAL_CAPITAL, **FA_PARAMS)
    # 내부 funding_direction을 반전시키기 위해 min_funding_rate 음수 버전으로 우회
    # → funding <= -0.0001 일 때 진입 (정상은 >= 0.0001)
    # 실제로는 2024년 대부분 양수이므로 거래 0건
    bars     = ohlcv.reset_index()
    equity   = INITIAL_CAPITAL
    position = None
    neg_c    = 0
    equity_curve = [equity]
    trades   = []

    for idx in range(20, len(bars)):
        bar      = bars.iloc[idx]
        ts       = bar["ts"] if hasattr(bar["ts"], "hour") else pd.Timestamp(bar["ts"])
        is_settle = (ts.hour % 8 == 0) and (ts.minute == 0)
        fr       = _get_funding(bar, funding)

        if position is None:
            if is_settle:
                # 버그: 음수 펀딩일 때 진입 (정상과 반대)
                if fr <= -FA_PARAMS["min_funding_rate"]:
                    neg_c += 1
                else:
                    neg_c = 0
                if neg_c >= FA_PARAMS["consecutive_intervals"]:
                    notional = equity * FA_PARAMS["fa_capital_ratio"] * FA_PARAMS["leverage"]
                    fee_in   = notional * TAKER_FEE
                    equity  -= fee_in
                    position = {
                        "notional": notional, "entry_idx": idx,
                        "rev_cnt": 0, "funding_acc": 0.0, "fee_paid": fee_in,
                        "funding_direction": -1,  # 잘못된 방향: 지불
                    }
                    neg_c = 0
        else:
            if is_settle:
                fund_pnl = position["notional"] * fr * position["funding_direction"]
                equity  += fund_pnl
                position["funding_acc"] += fund_pnl
                rev = fr > 0  # 반전: 양수가 되면 청산
                position["rev_cnt"] = position["rev_cnt"] + 1 if rev else 0
                bars_held = idx - position["entry_idx"]
                if position["rev_cnt"] >= 3 or bars_held >= FA_PARAMS["max_hold_bars"]:
                    fee_out  = position["notional"] * TAKER_FEE
                    equity  -= fee_out
                    pnl      = position["funding_acc"] - position["fee_paid"] - fee_out
                    trades.append({"pnl": pnl})
                    position = None
                    neg_c    = 0

        equity_curve.append(equity)

    eq   = pd.Series(equity_curve)

    return {
        "scenario": "E_inverted_signal",
        "label":    "진입 신호 반전 버그",
        "final_equity":      round(equity, 2),
        "total_profit_pct":  round((equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        "max_drawdown_pct":  round(mdd(eq), 2),
        "sharpe_ratio":      round(sharpe(eq), 4),
        "total_trades":      len(trades),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 F: 기간 변형 — 2023년 실거래
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_f_2023(ohlcv_2023: pd.DataFrame, funding_2023: pd.DataFrame) -> dict:
    """2023년 전체를 실거래 기간으로 가정."""
    if ohlcv_2023.empty:
        return {"scenario": "F_2023", "label": "2023년 기간 FA", "total_profit_pct": None, "error": "데이터없음"}
    engine = FAEngine(ohlcv_2023, funding_2023, INITIAL_CAPITAL, **FA_PARAMS)
    result = engine.run()
    return {
        "scenario": "F_2023",
        "label":    "2023년 기간 정상 FA",
        **_extract(result),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 시나리오 G: 선물 Short 단독 + 낮은 레버리지 (1~2x)
# ──────────────────────────────────────────────────────────────────────────────

def run_scenario_g_low_lev(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """선물 Short 단독이지만 레버리지 1x (delta exposure 부분 완화).

    실전에서 레버리지 설정이 1x로 잘못됐거나 헷지 비율이 낮았을 경우.
    BTC +120% 상승 시 → Short 1x: -120% × 80% 자본비율 = -96%
    하지만 현물 BTC 보유분 가치 상승으로 일부 상쇄 → 순손실 약 -20~-30% 가능.
    """
    bars     = ohlcv.reset_index()
    equity   = INITIAL_CAPITAL
    # 현물 BTC 선구매 (초기 자본 20% → 시장 노출)
    initial_price = float(bars.iloc[0]["close"])
    spot_alloc    = INITIAL_CAPITAL * 0.20
    spot_btc      = spot_alloc / initial_price
    equity       -= spot_alloc  # 현물 매수에 자본 사용

    position = None
    pos_c    = 0
    equity_curve = [equity + spot_btc * initial_price]
    trades   = []

    for idx in range(20, len(bars)):
        bar   = bars.iloc[idx]
        price = float(bar["close"])
        ts    = bar["ts"] if hasattr(bar["ts"], "hour") else pd.Timestamp(bar["ts"])
        is_settle = (ts.hour % 8 == 0) and (ts.minute == 0)
        fr    = _get_funding(bar, funding)

        if position is None:
            if is_settle:
                if fr >= FA_PARAMS["min_funding_rate"]:
                    pos_c += 1
                else:
                    pos_c = 0
                if pos_c >= FA_PARAMS["consecutive_intervals"]:
                    # 선물 Short, 레버리지 1x만 (설정 오류)
                    notional = equity * FA_PARAMS["fa_capital_ratio"] * 1.0
                    fee_in   = notional * TAKER_FEE
                    equity  -= fee_in
                    position = {
                        "notional": notional, "entry_price": price, "entry_idx": idx,
                        "qty": notional / price,
                        "rev_cnt": 0, "funding_acc": 0.0, "fee_paid": fee_in,
                    }
                    pos_c = 0
        else:
            if is_settle:
                fund_pnl = position["notional"] * fr * 1
                equity  += fund_pnl
                position["funding_acc"] += fund_pnl
                rev = fr < 0
                position["rev_cnt"] = position["rev_cnt"] + 1 if rev else 0
                bars_held = idx - position["entry_idx"]
                if position["rev_cnt"] >= 3 or bars_held >= FA_PARAMS["max_hold_bars"]:
                    fee_out   = position["notional"] * TAKER_FEE
                    price_pnl = (position["entry_price"] - price) * position["qty"]
                    equity   += price_pnl - fee_out
                    trades.append({
                        "pnl": price_pnl + position["funding_acc"] - position["fee_paid"] - fee_out,
                        "price_pnl": price_pnl,
                    })
                    position = None
                    pos_c    = 0

        spot_value = spot_btc * price
        equity_curve.append(equity + spot_value)

    final_price   = float(bars.iloc[-1]["close"])
    spot_value    = spot_btc * final_price
    total_equity  = equity + spot_value
    eq = pd.Series(equity_curve)

    return {
        "scenario": "G_low_lev_partial_hedge",
        "label":    "선물Short 1x + 현물 20% 부분헷지",
        "final_equity":      round(total_equity, 2),
        "total_profit_pct":  round((total_equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2),
        "max_drawdown_pct":  round(mdd(eq), 2),
        "sharpe_ratio":      round(sharpe(eq), 4),
        "total_trades":      len(trades),
        "spot_final_value":  round(spot_value, 2),
        "total_price_pnl":   round(sum(t.get("price_pnl", 0) for t in trades), 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    log.info("실거래 재구성 백테스트 시작 (확장 시나리오)")

    pool = await make_pool()

    # 2024년 데이터
    s24 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    e24 = datetime(2024, 12, 31, 23, 59, tzinfo=timezone.utc)
    ohlcv_24   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, s24, e24)
    funding_24 = await load_funding(pool, SYMBOL, s24, e24)

    # 2023년 데이터
    s23 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    e23 = datetime(2023, 12, 31, 23, 59, tzinfo=timezone.utc)
    ohlcv_23   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, s23, e23)
    funding_23 = await load_funding(pool, SYMBOL, s23, e23)

    # 2023-2024 2년 데이터
    s_2yr = datetime(2023, 1, 1, tzinfo=timezone.utc)
    e_2yr = datetime(2024, 12, 31, 23, 59, tzinfo=timezone.utc)
    ohlcv_2yr   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, s_2yr, e_2yr)
    funding_2yr = await load_funding(pool, SYMBOL, s_2yr, e_2yr)

    await pool.close()

    log.info("2024 데이터", ohlcv=len(ohlcv_24), funding=len(funding_24),
             btc_change=f"{(float(ohlcv_24['close'].iloc[-1])/float(ohlcv_24['close'].iloc[0])-1)*100:.1f}%")
    log.info("2023 데이터", ohlcv=len(ohlcv_23), funding=len(funding_23))
    log.info("2yr 데이터", ohlcv=len(ohlcv_2yr), funding=len(funding_2yr))

    # ── 2024 시나리오 ──
    results_24 = []
    for fn in [run_scenario_a, run_scenario_b, run_scenario_c,
               run_scenario_d, run_scenario_e, run_scenario_g_low_lev]:
        r = fn(ohlcv_24, funding_24)
        r["period"] = "2024"
        results_24.append(r)

    # ── 2023 시나리오 ──
    r_f = run_scenario_f_2023(ohlcv_23, funding_23)
    r_f["period"] = "2023"

    # ── 2yr 정상 FA ──
    engine_2yr = FAEngine(ohlcv_2yr, funding_2yr, INITIAL_CAPITAL, **FA_PARAMS)
    res_2yr    = engine_2yr.run()
    r_2yr = {"scenario": "A_2yr", "label": "2023-2024 정상 FA (2년)",
              "period": "2023-24", **_extract(res_2yr)}

    ACTUAL_LOSS = -24.15

    # ── 전체 출력 ──
    print("\n" + "=" * 78)
    print(f"  실거래 재구성 분석  |  실거래 주장 손실: -24.15%  |  초기 자본: $10,000")
    print("=" * 78)

    all_results = results_24 + [r_f, r_2yr]
    print(f"{'시나리오':<35} {'기간':<8} {'수익률':>8} {'MDD':>8} {'거래수':>6} {'차이':>8}")
    print("-" * 78)
    for r in all_results:
        if r.get("error"):
            continue
        trades_n = r.get("total_trades") or r.get("fa_trades", 0)
        diff     = r["total_profit_pct"] - ACTUAL_LOSS
        flag     = " ←" if abs(diff) < 10 else ""
        print(f"{r['label']:<35} {r['period']:<8} {r['total_profit_pct']:>+7.2f}% "
              f"{r['max_drawdown_pct']:>7.2f}% {trades_n:>6} {diff:>+7.1f}%p{flag}")

    print("=" * 78)

    # ── 근접도 분석 ──
    valid = [r for r in all_results if r.get("total_profit_pct") is not None and not r.get("error")]
    diffs = sorted(valid, key=lambda r: abs(r["total_profit_pct"] - ACTUAL_LOSS))

    print("\n[근접도 순위 (실제 -24.15% 기준)]")
    for i, r in enumerate(diffs[:4], 1):
        d = r["total_profit_pct"] - ACTUAL_LOSS
        print(f"  {i}위: [{r['scenario']}] {r['label']} ({r['period']}) "
              f"→ {r['total_profit_pct']:+.2f}%  (차이 {d:+.1f}%p)")

    print("\n[결론]")
    top = diffs[0]
    if abs(top["total_profit_pct"] - ACTUAL_LOSS) < 8:
        print(f"  → {top['label']}이 -24.15%와 가장 근접 (차이 {abs(top['total_profit_pct']-ACTUAL_LOSS):.1f}%p)")
    else:
        print(f"  → 어떤 단일 시나리오도 -24.15%와 10%p 이내로 일치하지 않음")
        print(f"  → 가장 근접: {top['label']} ({top['total_profit_pct']:+.2f}%)")
        print(f"  → 복합 원인 또는 다른 기간 가능성 높음")


if __name__ == "__main__":
    asyncio.run(main())
