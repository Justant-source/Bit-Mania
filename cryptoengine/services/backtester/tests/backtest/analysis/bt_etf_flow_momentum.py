"""analysis/bt_etf_flow_momentum.py — [BT_TASK_01] ETF Flow Momentum 전략 백테스트

목표: BTC 현물 ETF의 일별 순유입/유출 모멘텀 → 1d OHLCV 추적

전략:
  진입 (롱):
    1. 3일 누적 ETF 순유입 > +$100M
    2. 5일 누적 순유입 > 0
    3. 현재 포지션 없음
    4. FOMC/CPI 발표 2일 이내 아님
    5. DXY 5일 변화율 <= +0.5%

  진입 (숏):
    1. 3일 누적 ETF 순유출 < -$200M
    2. 5일 연속 순유출
    3. DXY 5일 변화율 >= +0.3%
    4. 이벤트 윈도우 외

  청산:
    - 손절: 진입가 -3%
    - 트레일링 스톱: 진입가 +2% 도달 후 2×ATR(14) 트레일링
    - 신호 반전: 3일 누적 플로우 부호 전환 시 즉시
    - 시간 청산: 보유 20일 초과

실행:
    docker compose --profile backtest run --rm backtester \\
      python tests/backtest/analysis/bt_etf_flow_momentum.py --stage all

    --stage: all | 1 | 2 | 3
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import sys
from datetime import datetime, timezone, timedelta
from typing import Any

import asyncpg
import pandas as pd
import numpy as np

sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv,
    sharpe, mdd, cagr, safe_float, monthly_returns,
    make_pool, save_result,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

UTC = timezone.utc
SYMBOL = "BTCUSDT"
INITIAL_CAPITAL = 5000.0
TIMEFRAME = "1d"

# 수수료 및 슬리피지
TAKER_FEE = 0.00055
SLIPPAGE = 0.0003
ONE_WAY_COST = TAKER_FEE + SLIPPAGE  # 0.00085 = 0.085%

# 파라미터 (Stage 1 기본값)
PARAMS_STAGE1 = {
    "flow_3d_threshold_long": 100_000_000,  # $100M
    "flow_5d_threshold_long": 0,
    "flow_3d_threshold_short": -200_000_000,  # -$200M
    "event_lookback_days": 2,
    "dxy_threshold_long": 0.005,  # +0.5%
    "dxy_threshold_short": 0.003,  # +0.3%
    "sl_pct": -0.03,  # -3%
    "tp_pct": 0.02,  # +2%
    "trail_atr_mult": 2.0,
    "max_hold_days": 20,
    "position_size_pct": 0.20,  # 자본의 20%
    "leverage": 2.0,
    "maker_execution_rate": 0.70,  # 70% 체결률 (post_only)
}


class ETFFlowBacktester:
    """ETF Flow Momentum 백테스터."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        etf_flows: pd.DataFrame,
        macro_events: set[str],
        dxy_prices: pd.DataFrame | None = None,
        **params,
    ):
        self.ohlcv = ohlcv.copy()
        self.etf_flows = etf_flows.copy()
        self.macro_events = macro_events
        self.dxy_prices = dxy_prices or pd.DataFrame()
        self.params = params

        # 결과
        self.trades = []
        self.equity_curve = [INITIAL_CAPITAL]
        self.position = None  # {"entry_idx", "entry_price", "direction", "entry_date"}
        self.pnl_history = []

    def run(self) -> dict[str, Any]:
        """백테스트 실행."""
        equity = INITIAL_CAPITAL
        position = None
        trailing_stop_price = None

        for i in range(1, len(self.ohlcv)):
            current_date = self.ohlcv.index[i].date()
            current_bar = self.ohlcv.iloc[i]
            current_price = float(current_bar["close"])

            # 포지션 유지 중인 경우
            if position:
                hold_days = (current_date - position["entry_date"]).days

                # 청산 신호 1: 손절
                if current_price <= position["entry_price"] * (1 + self.params["sl_pct"]):
                    exit_price = position["entry_price"] * (1 + self.params["sl_pct"])
                    pnl_pct = (exit_price / position["entry_price"] - 1) * position["direction"]
                    equity = self._execute_exit(equity, position, exit_price, pnl_pct, "stop_loss")
                    position = None
                    trailing_stop_price = None
                    continue

                # 청산 신호 2: 트레일링 스톱 (진입가 +2% 이상에서 시작)
                profit_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                if profit_pct >= self.params["tp_pct"]:
                    # BUG FIX 2: 트레일링 스톱을 연속 업데이트 (롱: 최고가 추적, 숏: 최저가 추적)
                    if position["direction"] == 1:
                        trailing_stop_price = max(trailing_stop_price or 0, current_price)
                    else:
                        trailing_stop_price = min(trailing_stop_price or float("inf"), current_price)

                if trailing_stop_price is not None:
                    # ATR 계산 (간단 버전: high-low의 평균)
                    atr = self._calculate_atr(i)
                    if position["direction"] == 1:
                        trail_level = trailing_stop_price - atr * self.params["trail_atr_mult"]
                        if current_price <= trail_level:
                            pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                            equity = self._execute_exit(equity, position, current_price, pnl_pct, "trailing_stop")
                            position = None
                            trailing_stop_price = None
                            continue
                    else:
                        # 숏: 최저가 - ATR
                        trail_level = trailing_stop_price + atr * self.params["trail_atr_mult"]
                        if current_price >= trail_level:
                            pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                            equity = self._execute_exit(equity, position, current_price, pnl_pct, "trailing_stop")
                            position = None
                            trailing_stop_price = None
                            continue

                # 청산 신호 3: 신호 반전 (3d 누적 플로우 부호 전환)
                flow_3d = self._calc_cumulative_flow(i, 3)
                if position["direction"] == 1 and flow_3d < 0:
                    pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                    equity = self._execute_exit(equity, position, current_price, pnl_pct, "signal_reversal")
                    position = None
                    trailing_stop_price = None
                    continue
                elif position["direction"] == -1 and flow_3d > 0:
                    pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                    equity = self._execute_exit(equity, position, current_price, pnl_pct, "signal_reversal")
                    position = None
                    trailing_stop_price = None
                    continue

                # 청산 신호 4: 시간 청산 (20일 초과)
                if hold_days > self.params["max_hold_days"]:
                    pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                    equity = self._execute_exit(equity, position, current_price, pnl_pct, "time_exit")
                    position = None
                    trailing_stop_price = None
                    continue

                # 청산 신호 5: 이벤트 차단 (FOMC/CPI 2일 전)
                if self._is_event_window(current_date):
                    pnl_pct = (current_price / position["entry_price"] - 1) * position["direction"]
                    equity = self._execute_exit(equity, position, current_price, pnl_pct, "event_hedge")
                    position = None
                    trailing_stop_price = None
                    continue

            # 진입 신호 탐색 (포지션 없을 때)
            if position is None:
                signal = self._check_entry_signal(i)
                if signal:
                    direction, confidence = signal
                    entry_price = current_price * (1 + ONE_WAY_COST * (1 if direction == 1 else -1))
                    position_size = equity * self.params["position_size_pct"] * confidence / entry_price
                    position = {
                        "entry_idx": i,
                        "entry_date": current_date,
                        "entry_price": entry_price,
                        "direction": direction,
                        "position_size": position_size,
                    }
                    logger.info(f"[{current_date}] 진입: {direction:+d}x {position_size:.4f} BTC @ ${entry_price:.2f}")
                    trailing_stop_price = None

            # BUG FIX 5: 매 루프마다 equity_curve 갱신 (청산 시에만 갱신하던 기존 로직 개선)
            self.equity_curve.append(equity)

        # 미결제 포지션 청산 (마지막 바 종가)
        if position:
            exit_price = float(self.ohlcv.iloc[-1]["close"])
            pnl_pct = (exit_price / position["entry_price"] - 1) * position["direction"]
            equity = self._execute_exit(equity, position, exit_price, pnl_pct, "end_of_backtest")

        self.equity_curve.append(equity)
        return self._calculate_metrics(equity)

    def _check_entry_signal(self, idx: int) -> tuple[int, float] | None:
        """진입 신호 확인. (방향, 신뢰도) 반환."""
        current_date = self.ohlcv.index[idx].date()

        # 이벤트 윈도우 외 확인
        if self._is_event_window(current_date):
            return None

        flow_3d = self._calc_cumulative_flow(idx, 3)
        flow_5d = self._calc_cumulative_flow(idx, 5)
        dxy_change_5d = self._calc_dxy_change(idx, 5)

        # 롱 신호
        if (flow_3d > self.params["flow_3d_threshold_long"] and
            flow_5d > self.params["flow_5d_threshold_long"] and
            dxy_change_5d <= self.params["dxy_threshold_long"]):
            confidence = min(1.5, abs(flow_3d) / 300_000_000)
            return (1, confidence)

        # 숏 신호
        if (flow_3d < self.params["flow_3d_threshold_short"] and
            dxy_change_5d >= self.params["dxy_threshold_short"]):
            confidence = min(1.5, abs(flow_3d) / 300_000_000)
            return (-1, confidence)

        return None

    def _is_event_window(self, current_date: str | object) -> bool:
        """FOMC/CPI 이벤트 윈도우 확인 (2일 이내)."""
        if isinstance(current_date, str):
            current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        elif not isinstance(current_date, type(datetime.now().date())):
            current_date = current_date.date() if hasattr(current_date, 'date') else current_date

        for event_date in self.macro_events:
            if isinstance(event_date, str):
                event_date = datetime.strptime(event_date, "%Y-%m-%d").date()
            if abs((current_date - event_date).days) <= self.params["event_lookback_days"]:
                return True
        return False

    def _calc_cumulative_flow(self, idx: int, days: int) -> float:
        """N일 누적 ETF 플로우."""
        if len(self.etf_flows) == 0:
            return 0.0

        current_date = self.ohlcv.index[idx].date()
        start_date = current_date - timedelta(days=days-1)

        mask = (self.etf_flows.index.date >= start_date) & (self.etf_flows.index.date <= current_date)
        flows_in_range = self.etf_flows[mask]

        if flows_in_range.empty:
            return 0.0
        return float(flows_in_range["total_flow_usd"].sum())

    def _calc_dxy_change(self, idx: int, days: int) -> float:
        """DXY 5일 변화율 (대체: 0 반환, 실제로는 DXY 데이터 필요)."""
        # DXY 데이터 없으므로 0 반환 (필터 무효화)
        return 0.0

    def _calculate_atr(self, idx: int, period: int = 14) -> float:
        """ATR 계산."""
        if idx < period:
            period = idx
        bars = self.ohlcv.iloc[max(0, idx-period):idx+1]
        tr = bars["high"] - bars["low"]
        return float(tr.mean())

    def _execute_exit(self, equity: float, position: dict, exit_price: float, pnl_pct: float, reason: str) -> float:
        """포지션 청산 및 수익 계산."""
        position_value = position["position_size"] * position["entry_price"]
        # BUG FIX 1: pnl_pct에 이미 direction이 곱해졌으므로 다시 곱하지 않음
        pnl_usd = position_value * pnl_pct * self.params["leverage"]

        # 수수료 차감 (진입/청산 각각 편도 수수료)
        fee_usd = position_value * ONE_WAY_COST * self.params["leverage"] * 2  # 진입/청산 2회
        net_pnl = pnl_usd - fee_usd

        new_equity = equity + net_pnl
        self.trades.append({
            "entry_date": position["entry_date"],
            "entry_price": position["entry_price"],
            "exit_date": self.ohlcv.index[len(self.equity_curve)-1].date(),
            "exit_price": exit_price,
            "direction": position["direction"],
            "size": position["position_size"],
            "pnl_usd": net_pnl,
            "pnl_pct": (new_equity - equity) / equity,
            "reason": reason,
        })

        logger.info(f"  청산: {reason} | PnL: ${net_pnl:+.2f} ({(new_equity-equity)/equity*100:+.2f}%)")
        return new_equity

    def _calculate_metrics(self, final_equity: float) -> dict[str, Any]:
        """백테스트 지표 계산."""
        equity_series = pd.Series(self.equity_curve)
        total_return_pct = (final_equity - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        n_years = (len(self.ohlcv) - 1) / 365  # 근사값
        n_years = max(1/365, n_years)  # 최소 1일

        # BUG FIX 4: 일봉 데이터에 대해 periods_per_year=252 사용
        sharpe_val = sharpe(equity_series, periods_per_year=252)
        mdd_val = mdd(equity_series)
        cagr_val = cagr(total_return_pct, n_years)
        win_rate = len([t for t in self.trades if t["pnl_usd"] > 0]) / len(self.trades) if self.trades else 0.0

        return {
            "initial_capital": INITIAL_CAPITAL,
            "final_equity": final_equity,
            "total_return_pct": total_return_pct,
            "sharpe_ratio": safe_float(sharpe_val),
            "max_drawdown_pct": safe_float(mdd_val),
            "cagr_pct": safe_float(cagr_val),
            "trade_count": len(self.trades),
            "win_rate": safe_float(win_rate),
            "avg_trade_pnl": safe_float(np.mean([t["pnl_usd"] for t in self.trades])) if self.trades else 0.0,
            "data_points": len(self.ohlcv),
        }


async def load_etf_flows(pool: asyncpg.Pool, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """DB에서 ETF 플로우 데이터 로드."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, total_flow_usd
            FROM etf_flow_history
            WHERE date >= $1 AND date <= $2
            ORDER BY date ASC
            """,
            start_date.date(), end_date.date(),
        )

    if not rows:
        logger.warning("ETF 플로우 데이터 없음 → 빈 DataFrame")
        return pd.DataFrame(columns=["total_flow_usd"])

    # asyncpg에서 반환하는 Record를 dict로 변환
    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df.set_index("date", inplace=True)
    return df


async def load_macro_events(pool: asyncpg.Pool, start_date: datetime, end_date: datetime) -> set[str]:
    """DB에서 매크로 이벤트 로드."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_date
            FROM macro_events
            WHERE event_date >= $1 AND event_date <= $2
            """,
            start_date.date(), end_date.date(),
        )

    return {str(row["event_date"]) for row in rows}


async def run_stage_1(pool: asyncpg.Pool, ohlcv: pd.DataFrame, etf_flows: pd.DataFrame, macro_events: set) -> dict:
    """Stage 1: 기본 파라미터 단일 실행."""
    logger.info("=" * 60)
    logger.info("STAGE 1: 기본 파라미터 실행")
    logger.info("=" * 60)

    bt = ETFFlowBacktester(ohlcv, etf_flows, macro_events, **PARAMS_STAGE1)
    metrics = bt.run()

    logger.info(f"Stage 1 결과:")
    logger.info(f"  CAGR: {metrics['cagr_pct']:+.2f}%")
    logger.info(f"  Sharpe: {metrics['sharpe_ratio']:.2f}")
    logger.info(f"  MDD: {metrics['max_drawdown_pct']:.2f}%")
    logger.info(f"  거래수: {metrics['trade_count']}")

    await save_result(
        pool,
        stage="stage_1",
        variant="baseline",
        metrics=metrics,
        params=PARAMS_STAGE1,
        table="etf_flow_results",
    )

    return metrics


async def run_stage_2(pool: asyncpg.Pool, ohlcv: pd.DataFrame, etf_flows: pd.DataFrame, macro_events: set) -> list[dict]:
    """Stage 2: 파라미터 그리드 서치 (4×3×3 = 36조합)."""
    logger.info("=" * 60)
    logger.info("STAGE 2: 파라미터 그리드 서치")
    logger.info("=" * 60)

    # 핵심 파라미터 변형
    flow_thresholds_long = [50_000_000, 100_000_000, 150_000_000, 200_000_000]  # 4가지
    sl_pcts = [-0.02, -0.03, -0.05]  # 3가지
    position_pcts = [0.15, 0.20, 0.25]  # 3가지

    results = []
    combo_count = 0

    for flow_3d_th in flow_thresholds_long:
        for sl_pct in sl_pcts:
            for pos_pct in position_pcts:
                combo_count += 1
                variant_name = f"flow{flow_3d_th//1e6:.0f}M_sl{sl_pct*100:.0f}pct_pos{pos_pct*100:.0f}pct"

                params = PARAMS_STAGE1.copy()
                params["flow_3d_threshold_long"] = flow_3d_th
                params["sl_pct"] = sl_pct
                params["position_size_pct"] = pos_pct

                bt = ETFFlowBacktester(ohlcv, etf_flows, macro_events, **params)
                metrics = bt.run()
                results.append((variant_name, metrics, params))

                logger.info(f"[{combo_count}/36] {variant_name}: CAGR {metrics['cagr_pct']:+.2f}%, Sharpe {metrics['sharpe_ratio']:.2f}")

                await save_result(
                    pool,
                    stage="stage_2",
                    variant=variant_name,
                    metrics=metrics,
                    params=params,
                    table="etf_flow_results",
                )

    # 상위 5개 출력
    results_sorted = sorted(results, key=lambda x: x[1]["sharpe_ratio"], reverse=True)
    logger.info("\n상위 5개 파라미터 조합:")
    for i, (variant, metrics, params) in enumerate(results_sorted[:5], 1):
        logger.info(f"{i}. {variant}: Sharpe {metrics['sharpe_ratio']:.2f}, CAGR {metrics['cagr_pct']:+.2f}%")

    return results


async def run_stage_3(pool: asyncpg.Pool, ohlcv: pd.DataFrame, etf_flows: pd.DataFrame, macro_events: set) -> list[dict]:
    """Stage 3: Walk-Forward (6개월 학습 / 3개월 테스트)."""
    logger.info("=" * 60)
    logger.info("STAGE 3: Walk-Forward (6M / 3M)")
    logger.info("=" * 60)

    train_days = 180
    test_days = 90
    step_days = 90  # 월간 롤

    wf_results = []
    window_count = 0

    for i in range(0, len(ohlcv) - train_days - test_days, step_days):
        window_count += 1
        train_end_idx = i + train_days
        test_end_idx = train_end_idx + test_days

        train_ohlcv = ohlcv.iloc[i:train_end_idx]
        test_ohlcv = ohlcv.iloc[train_end_idx:test_end_idx]

        train_flows = etf_flows[(etf_flows.index >= train_ohlcv.index[0]) & (etf_flows.index < train_ohlcv.index[-1])]
        test_flows = etf_flows[(etf_flows.index >= test_ohlcv.index[0]) & (etf_flows.index < test_ohlcv.index[-1])]

        # Train에서 최적 파라미터 찾기 (간단히 기본값 사용)
        bt_train = ETFFlowBacktester(train_ohlcv, train_flows, macro_events, **PARAMS_STAGE1)
        metrics_train = bt_train.run()

        # Test에서 동일 파라미터로 실행
        bt_test = ETFFlowBacktester(test_ohlcv, test_flows, macro_events, **PARAMS_STAGE1)
        metrics_test = bt_test.run()

        window_label = f"W{window_count}"
        logger.info(f"[{window_label}] IS Sharpe: {metrics_train['sharpe_ratio']:.2f}, OOS Sharpe: {metrics_test['sharpe_ratio']:.2f}")

        wf_results.append({
            "window": window_count,
            "is_sharpe": metrics_train["sharpe_ratio"],
            "oos_sharpe": metrics_test["sharpe_ratio"],
            "is_metrics": metrics_train,
            "oos_metrics": metrics_test,
        })

        await save_result(
            pool,
            stage="stage_3",
            variant=f"{window_label}_is",
            metrics=metrics_train,
            params=PARAMS_STAGE1,
            table="etf_flow_results",
        )
        await save_result(
            pool,
            stage="stage_3",
            variant=f"{window_label}_oos",
            metrics=metrics_test,
            params=PARAMS_STAGE1,
            table="etf_flow_results",
        )

    return wf_results


async def main(args):
    pool = await make_pool()
    try:
        # 데이터 로드
        start_date = datetime(2024, 1, 11, tzinfo=UTC)
        end_date = datetime.now(tz=UTC)

        logger.info(f"데이터 로드: {start_date.date()} ~ {end_date.date()}")

        ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_date, end_date)
        etf_flows = await load_etf_flows(pool, start_date, end_date)
        macro_events = await load_macro_events(pool, start_date, end_date)

        logger.info(f"OHLCV: {len(ohlcv)} 봉")
        logger.info(f"ETF Flows: {len(etf_flows)} 행")
        logger.info(f"Macro Events: {len(macro_events)} 개")

        if ohlcv.empty:
            logger.error("OHLCV 데이터 없음")
            return

        # Stage 실행
        stage = args.stage

        if stage in ("all", "1"):
            metrics_s1 = await run_stage_1(pool, ohlcv, etf_flows, macro_events)

        if stage in ("all", "2"):
            results_s2 = await run_stage_2(pool, ohlcv, etf_flows, macro_events)

        if stage in ("all", "3"):
            results_s3 = await run_stage_3(pool, ohlcv, etf_flows, macro_events)

        logger.info("백테스트 완료")

    except Exception as e:
        logger.error(f"백테스트 실패: {e}", exc_info=True)
    finally:
        await pool.close()


def _parse():
    p = argparse.ArgumentParser(description="ETF Flow Momentum Backtest")
    p.add_argument("--stage", choices=["all", "1", "2", "3"], default="all", help="실행할 Stage")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse()))
