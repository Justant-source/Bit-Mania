"""bt_btc_eth_pair_v3.py — BTC/ETH 페어 트레이딩 v3: Kalman Filter + Copula 의존성

v2 (Engle-Granger) 실패 원인 분석 → v3 재설계:
- 문제: 공적분 안정도 30% (목표 60% 미달), ETF 후 BTC/ETH 변동성 분기
- 해법: Engle-Granger 공적분 포기, Kalman 필터 동적 헤지비율 + 순위 상관계수 기반

v3 특징:
1. Kalman Filter: 시변 베타 추정 (선형 회귀 beta에서 시계열 진화 모델로)
2. 순위 상관계수 (Spearman/Kendall): 공적분 없어도 순위 종속성 활용
3. 2분할 분석: ETF 전(2020-2023) vs ETF 후(2024+) 의존성 비교
4. Walk-Forward 검증: 6개월 윈도우로 Out-Of-Sample 성과 평가
"""
import sys
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timezone, timedelta
from itertools import product

import numpy as np
import pandas as pd
from scipy.stats import spearmanr, kendalltau
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
from statsmodels.tsa.stattools import adfuller

sys.path.insert(0, str(Path(__file__).parent.parent))
from core.loader import load_ohlcv
from core.db import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE
from core.metrics import sharpe, mdd, cagr, safe_float, monthly_returns, profit_factor
from core.constants import TAKER_FEE

warnings.filterwarnings('ignore')

# ========================
# Kalman Filter 구현
# ========================

def kalman_hedge_ratio(btc_prices: np.ndarray, eth_prices: np.ndarray, delta: float = 1e-5, R: float = 0.001):
    """Kalman Filter로 시변 헤지비율(beta) 추정

    상태 모델: beta_t = beta_{t-1} + w_t (random walk)
    관측 모델: btc_t = beta_t * eth_t + v_t

    Args:
        btc_prices: BTC 가격 배열 (길이 n)
        eth_prices: ETH 가격 배열 (길이 n)
        delta: 프로세스 노이즈 비율 (작을수록 안정적)
        R: 관측 노이즈 (수치 안정성)

    Returns:
        beta_t 시계열 (길이 n)
    """
    n = len(btc_prices)
    beta = np.zeros(n)
    P = np.zeros(n)  # 추정 오차 분산

    # 초기화: 첫 30개로 OLS 계산
    if n >= 30:
        eth_init = eth_prices[:30]
        btc_init = btc_prices[:30]
        beta[0] = np.polyfit(eth_init, btc_init, 1)[0]  # 단순 선형 회귀
    else:
        beta[0] = 1.0

    P[0] = 1.0
    Q = delta / (1.0 - delta)  # 프로세스 노이즈

    for t in range(1, n):
        # Predict
        beta_pred = beta[t-1]
        P_pred = P[t-1] + Q

        # Update
        eth_t = eth_prices[t]
        btc_t = btc_prices[t]

        # Kalman gain
        denom = eth_t**2 * P_pred + R
        if abs(denom) > 1e-10:
            K = P_pred * eth_t / denom
        else:
            K = 0.0

        # State & variance update
        innovation = btc_t - beta_pred * eth_t
        beta[t] = beta_pred + K * innovation
        P[t] = (1.0 - K * eth_t) * P_pred

        # 안정화: 극단값 방지
        if beta[t] < 0.1 or beta[t] > 5.0:
            beta[t] = np.clip(beta[t], 0.1, 5.0)

    return beta


def compute_copula_dependence(btc_returns: np.ndarray, eth_returns: np.ndarray, window: int = 60):
    """순위 기반 의존성 지수 계산 (Spearman & Kendall Tau)

    Copula의 핵심: 순위 상관계수는 분포 형태와 무관하게 의존성 구조를 포착

    Returns:
        Dict: {spearman_rho, kendall_tau, lower_tail_dep}
    """
    if len(btc_returns) < window:
        return {'spearman': 0.0, 'kendall': 0.0}

    btc_ret = btc_returns[-window:]
    eth_ret = eth_returns[-window:]

    # Spearman 순위 상관계수
    spearman_rho, _ = spearmanr(btc_ret, eth_ret)

    # Kendall Tau (하단 꼬리 의존성 포착에 더 민감)
    kendall_tau, _ = kendalltau(btc_ret, eth_ret)

    return {
        'spearman': float(np.nan_to_num(spearman_rho, 0.0)),
        'kendall': float(np.nan_to_num(kendall_tau, 0.0)),
    }


# ========================
# 백테스트 엔진 (v3)
# ========================

class KalmanPairTradingBacktester:
    def __init__(self, btc_ohlcv: pd.DataFrame, eth_ohlcv: pd.DataFrame,
                 capital: float = 10000, leverage: float = 2.0,
                 entry_zscore: float = 2.0, exit_zscore: float = 0.5,
                 rolling_window: int = 60, kalman_delta: float = 1e-5,
                 min_copula_rho: float = 0.3):
        self.btc = btc_ohlcv.copy()
        self.eth = eth_ohlcv.copy()
        self.capital = capital
        self.leverage = leverage
        self.entry_zscore = entry_zscore
        self.exit_zscore = exit_zscore
        self.rolling_window = rolling_window
        self.kalman_delta = kalman_delta
        self.min_copula_rho = min_copula_rho

        self.btc_fee = TAKER_FEE
        self.eth_fee = TAKER_FEE

        self.trades = []
        self.equity_curve = []
        self.positions = {}

    def backtest(self) -> dict:
        """Kalman Filter 기반 페어 트레이딩 백테스트"""
        equity = self.capital
        position = None

        # 로그 가격 (Kalman은 선형 가격 공간에서 작동)
        btc_prices = self.btc['close'].values
        eth_prices = self.eth['close'].values

        # 수익률 시계열 (Copula 의존성 계산용)
        btc_returns = np.log(btc_prices[1:] / btc_prices[:-1])
        eth_returns = np.log(eth_prices[1:] / eth_prices[:-1])

        for i in range(self.rolling_window, len(btc_prices)):
            ts = self.btc.index[i]
            btc_close = btc_prices[i]
            eth_close = eth_prices[i]

            # Kalman 헤지비율 계산 (최근 rolling_window 데이터)
            btc_slice = btc_prices[i - self.rolling_window:i]
            eth_slice = eth_prices[i - self.rolling_window:i]

            kalman_betas = kalman_hedge_ratio(btc_slice, eth_slice, delta=self.kalman_delta)
            beta = kalman_betas[-1]  # 현재 추정치
            beta_std = np.std(kalman_betas)  # 베타 변동성

            # 베타 유효성 확인
            if abs(beta) < 0.1 or abs(beta) > 10.0 or beta_std > 1.0:
                if position:
                    self._close_position(position, ts, btc_close, eth_close, equity, 'invalid_beta')
                    position = None
                continue

            # Copula 의존성 점검 (순위 상관계수)
            if i >= self.rolling_window:
                btc_ret_slice = btc_returns[max(0, i-1-self.rolling_window):i-1]
                eth_ret_slice = eth_returns[max(0, i-1-self.rolling_window):i-1]

                if len(btc_ret_slice) >= 10:
                    copula_dep = compute_copula_dependence(btc_ret_slice, eth_ret_slice, window=self.rolling_window)
                    spearman_rho = copula_dep['spearman']

                    # 의존성 부족하면 거래 금지
                    if spearman_rho < self.min_copula_rho:
                        if position:
                            self._close_position(position, ts, btc_close, eth_close, equity, 'low_copula_dep')
                            position = None
                        continue

            # Spread = BTC - beta * ETH (로그 공간)
            log_btc_slice = np.log(btc_slice)
            log_eth_slice = np.log(eth_slice)
            log_spread = log_btc_slice - beta * log_eth_slice

            spread_mean = log_spread.mean()
            spread_std = log_spread.std()

            if spread_std > 0:
                zscore = (log_spread[-1] - spread_mean) / spread_std
            else:
                zscore = 0.0

            # 진입: Long Spread (BTC 롱 + ETH 숏)
            if position is None and zscore < -self.entry_zscore:
                btc_notional = self.capital * 0.20 * self.leverage
                btc_qty = btc_notional / btc_close
                eth_notional = btc_notional / beta
                eth_qty = eth_notional / eth_close

                position = {
                    'side': 'long',
                    'btc_qty': btc_qty,
                    'eth_qty': eth_qty,
                    'entry_ts': ts,
                    'entry_btc_price': btc_close,
                    'entry_eth_price': eth_close,
                    'beta': beta,
                    'spearman_rho': spearman_rho if i >= self.rolling_window else 0.0,
                }
                self.positions[len(self.trades)] = position

            # 진입: Short Spread (BTC 숏 + ETH 롱)
            elif position is None and zscore > self.entry_zscore:
                btc_notional = self.capital * 0.20 * self.leverage
                btc_qty = btc_notional / btc_close
                eth_notional = btc_notional / beta
                eth_qty = eth_notional / eth_close

                position = {
                    'side': 'short',
                    'btc_qty': btc_qty,
                    'eth_qty': eth_qty,
                    'entry_ts': ts,
                    'entry_btc_price': btc_close,
                    'entry_eth_price': eth_close,
                    'beta': beta,
                    'spearman_rho': spearman_rho if i >= self.rolling_window else 0.0,
                }
                self.positions[len(self.trades)] = position

            # 청산 조건
            elif position:
                hold_days = (ts - position['entry_ts']).days
                should_close = False
                reason = ''

                # 익절
                if abs(zscore) < self.exit_zscore:
                    should_close = True
                    reason = 'profit_taking'
                # 극단값 (공적분 붕괴)
                elif abs(zscore) > 3.5:
                    should_close = True
                    reason = 'zscore_extreme'
                # 타임아웃
                elif hold_days >= 30:
                    should_close = True
                    reason = 'max_hold'

                if should_close:
                    equity = self._close_position(position, ts, btc_close, eth_close, equity, reason)
                    position = None

            # 자산 스냅샷
            if position:
                if position['side'] == 'long':
                    pnl = (btc_close - position['entry_btc_price']) * position['btc_qty'] + \
                          (position['entry_eth_price'] - eth_close) * position['eth_qty']
                else:
                    pnl = (position['entry_btc_price'] - btc_close) * position['btc_qty'] + \
                          (eth_close - position['entry_eth_price']) * position['eth_qty']
                equity = self.capital + pnl
            else:
                equity = self.capital

            self.equity_curve.append({
                'ts': ts,
                'equity': equity,
                'position': 'long' if position and position['side'] == 'long' else \
                           'short' if position and position['side'] == 'short' else None,
            })

        # 최종 청산
        if position and len(self.equity_curve) > 0:
            last_ts = self.equity_curve[-1]['ts']
            btc_close = btc_prices[-1]
            eth_close = eth_prices[-1]
            equity = self._close_position(position, last_ts, btc_close, eth_close, equity, 'end_of_period')

        return self._compute_metrics()

    def _close_position(self, position, ts, btc_close, eth_close, equity, reason):
        """포지션 청산"""
        entry_fee = position['btc_qty'] * position['entry_btc_price'] * self.btc_fee + \
                   position['eth_qty'] * position['entry_eth_price'] * self.eth_fee
        exit_fee = position['btc_qty'] * btc_close * self.btc_fee + \
                  position['eth_qty'] * eth_close * self.eth_fee

        if position['side'] == 'long':
            pnl = (btc_close - position['entry_btc_price']) * position['btc_qty'] + \
                  (position['entry_eth_price'] - eth_close) * position['eth_qty']
        else:
            pnl = (position['entry_btc_price'] - btc_close) * position['btc_qty'] + \
                  (eth_close - position['entry_eth_price']) * position['eth_qty']

        total_fees = entry_fee + exit_fee
        net_pnl = pnl - total_fees

        hold_days = (ts - position['entry_ts']).days

        self.trades.append({
            'entry_ts': position['entry_ts'],
            'exit_ts': ts,
            'side': position['side'],
            'hold_days': hold_days,
            'pnl': net_pnl,
            'reason': reason,
            'beta': position['beta'],
        })

        return self.capital + net_pnl

    def _compute_metrics(self) -> dict:
        """메트릭 계산"""
        if len(self.equity_curve) == 0:
            return self._empty_result()

        equity_df = pd.DataFrame(self.equity_curve)
        equity_df.set_index('ts', inplace=True)
        equity_series = equity_df['equity'] if isinstance(equity_df['equity'], pd.Series) else \
                        pd.Series(equity_df['equity'].values, index=equity_df.index)

        total_return = (equity_series.iloc[-1] - self.capital) / self.capital
        n_years = len(equity_df) / 365.0
        annual_return = cagr(total_return * 100, n_years) if n_years > 0 else 0

        return {
            'num_trades': len(self.trades),
            'total_return': float(total_return),
            'annual_return': float(annual_return),
            'sharpe': float(sharpe(equity_series, periods_per_year=252)),
            'mdd': float(mdd(equity_series)),
            'profit_factor': float(profit_factor([t['pnl'] for t in self.trades])) if self.trades else 0.0,
            'max_hold_days': max([t['hold_days'] for t in self.trades]) if self.trades else 0,
        }

    def _empty_result(self):
        return {
            'num_trades': 0,
            'total_return': 0.0,
            'annual_return': 0.0,
            'sharpe': 0.0,
            'mdd': 0.0,
            'profit_factor': 0.0,
            'max_hold_days': 0,
        }


# ========================
# Stage 분석
# ========================

async def run_stage_1_copula_analysis(btc: pd.DataFrame, eth: pd.DataFrame):
    """Stage 1: Copula 의존성 분석 (ETF 전/후 비교)"""
    print("\n=== Stage 1: Copula Dependence Analysis (Pre/Post ETF) ===\n")

    btc_returns = np.log(btc['close'].values[1:] / btc['close'].values[:-1])
    eth_returns = np.log(eth['close'].values[1:] / eth['close'].values[:-1])
    dates = btc.index[1:]

    # ETF 기준: 2024-01-01
    etf_date = pd.Timestamp('2024-01-01', tz='UTC')
    etf_idx = (dates >= etf_date).argmax()

    print(f"Data period: {dates[0].date()} ~ {dates[-1].date()}")
    print(f"ETF date (split): {etf_date.date()}")
    print(f"Pre-ETF: {dates[0].date()} ~ {dates[etf_idx-1].date()} ({etf_idx} days)")
    print(f"Post-ETF: {dates[etf_idx].date()} ~ {dates[-1].date()} ({len(dates)-etf_idx} days)\n")

    # 전체 기간
    spearman_all, _ = spearmanr(btc_returns, eth_returns)
    kendall_all, _ = kendalltau(btc_returns, eth_returns)

    # Pre-ETF
    btc_pre = btc_returns[:etf_idx]
    eth_pre = eth_returns[:etf_idx]
    spearman_pre, _ = spearmanr(btc_pre, eth_pre)
    kendall_pre, _ = kendalltau(btc_pre, eth_pre)

    # Post-ETF
    btc_post = btc_returns[etf_idx:]
    eth_post = eth_returns[etf_idx:]
    spearman_post, _ = spearmanr(btc_post, eth_post)
    kendall_post, _ = kendalltau(btc_post, eth_post)

    results = {
        'pre_etf': {
            'spearman': float(spearman_pre),
            'kendall': float(kendall_pre),
            'days': etf_idx,
        },
        'post_etf': {
            'spearman': float(spearman_post),
            'kendall': float(kendall_post),
            'days': len(dates) - etf_idx,
        },
        'overall': {
            'spearman': float(spearman_all),
            'kendall': float(kendall_all),
            'days': len(dates),
        }
    }

    print("=== 순위 상관계수 비교 ===")
    print(f"Pre-ETF  (2020-2023): Spearman={results['pre_etf']['spearman']:.4f}, "
          f"Kendall={results['pre_etf']['kendall']:.4f}")
    print(f"Post-ETF (2024+):     Spearman={results['post_etf']['spearman']:.4f}, "
          f"Kendall={results['post_etf']['kendall']:.4f}")
    print(f"Overall:             Spearman={results['overall']['spearman']:.4f}, "
          f"Kendall={results['overall']['kendall']:.4f}\n")

    # 결론
    pre_avg = (results['pre_etf']['spearman'] + results['pre_etf']['kendall']) / 2
    post_avg = (results['post_etf']['spearman'] + results['post_etf']['kendall']) / 2

    if pre_avg > 0.5 and post_avg > 0.5:
        print("✓ 강한 순위 의존성 지속 (공적분 없어도 페어 트레이딩 가능)")
    elif post_avg > 0.3:
        print("△ 중간 의존성 (신호 강화 필요)")
    else:
        print("✗ 의존성 부족 (페어 트레이딩 부적합)")

    return results


async def run_stage_2_kalman_baseline(btc: pd.DataFrame, eth: pd.DataFrame):
    """Stage 2: Kalman Baseline"""
    print("\n=== Stage 2: Baseline (Kalman delta=1e-5, entry_zscore=2.0) ===")

    tester = KalmanPairTradingBacktester(
        btc, eth,
        entry_zscore=2.0, exit_zscore=0.5, rolling_window=60,
        kalman_delta=1e-5, min_copula_rho=0.3
    )
    result = tester.backtest()

    print(f"Trades: {result['num_trades']}, Return: {result['total_return']*100:.2f}%, "
          f"Sharpe: {result['sharpe']:.2f}, MDD: {result['mdd']:.2f}%")

    return result


async def run_stage_3_gridsearch(btc: pd.DataFrame, eth: pd.DataFrame):
    """Stage 3: Parameter Grid Search"""
    print("\n=== Stage 3: Grid Search (Kalman parameters) ===")

    results = []

    entry_zscore_vals = [1.5, 2.0, 2.5]
    exit_zscore_vals = [0.3, 0.5, 0.8]
    kalman_delta_vals = [1e-6, 1e-5, 1e-4]

    total = len(entry_zscore_vals) * len(exit_zscore_vals) * len(kalman_delta_vals)
    count = 0

    for entry, exit_z, delta in product(entry_zscore_vals, exit_zscore_vals, kalman_delta_vals):
        count += 1
        if count % 9 == 0:
            print(f"  {count}/{total}...")

        tester = KalmanPairTradingBacktester(
            btc, eth,
            entry_zscore=entry, exit_zscore=exit_z,
            rolling_window=60, kalman_delta=delta, min_copula_rho=0.3
        )
        result = tester.backtest()
        result['params'] = {
            'entry_zscore': entry,
            'exit_zscore': exit_z,
            'kalman_delta': delta,
        }
        results.append(result)

    sorted_results = sorted(results, key=lambda x: x['sharpe'], reverse=True)

    print("\nTop 5 by Sharpe Ratio:")
    for i, r in enumerate(sorted_results[:5], 1):
        print(f"  {i}. Sharpe={r['sharpe']:.2f}, Return={r['total_return']*100:.2f}%, "
              f"Trades={r['num_trades']}, Params={r['params']}")

    return sorted_results


async def run_stage_4_walkforward(btc: pd.DataFrame, eth: pd.DataFrame):
    """Stage 4: Walk-Forward Validation"""
    print("\n=== Stage 4: Walk-Forward (6개월 윈도우, 3개월 슬라이드) ===")

    total_days = len(btc)
    train_days = int(180)  # 6개월
    test_days = int(180)   # 6개월

    wf_results = []
    start_idx = 0

    while start_idx + train_days + test_days <= total_days:
        train_end = start_idx + train_days
        test_end = train_end + test_days

        btc_train = btc.iloc[start_idx:train_end]
        eth_train = eth.iloc[start_idx:train_end]
        btc_test = btc.iloc[train_end:test_end]
        eth_test = eth.iloc[train_end:test_end]

        # IS
        tester_is = KalmanPairTradingBacktester(
            btc_train, eth_train,
            entry_zscore=2.0, exit_zscore=0.5, rolling_window=60,
            kalman_delta=1e-5, min_copula_rho=0.3
        )
        is_result = tester_is.backtest()

        # OOS
        tester_oos = KalmanPairTradingBacktester(
            btc_test, eth_test,
            entry_zscore=2.0, exit_zscore=0.5, rolling_window=60,
            kalman_delta=1e-5, min_copula_rho=0.3
        )
        oos_result = tester_oos.backtest()

        wf_results.append({
            'period': f"{btc.index[start_idx].date()} ~ {btc.index[test_end-1].date()}",
            'is_sharpe': is_result['sharpe'],
            'oos_sharpe': oos_result['sharpe'],
            'is_return': is_result['total_return'],
            'oos_return': oos_result['total_return'],
            'oos_trades': oos_result['num_trades'],
        })

        print(f"  Window {len(wf_results)}: IS Sharpe={is_result['sharpe']:.2f} → "
              f"OOS Sharpe={oos_result['sharpe']:.2f} (trades={oos_result['num_trades']})")

        start_idx += int(90)  # 3개월 슬라이드

    avg_oos_sharpe = np.mean([r['oos_sharpe'] for r in wf_results]) if wf_results else 0.0
    avg_oos_return = np.mean([r['oos_return'] for r in wf_results]) if wf_results else 0.0

    print(f"\nWalk-Forward Summary:")
    print(f"  Avg OOS Sharpe: {avg_oos_sharpe:.2f}")
    print(f"  Avg OOS Return: {avg_oos_return*100:.2f}%")

    return wf_results


async def main():
    parser = argparse.ArgumentParser(description='BTC/ETH Pair Trading v3 (Kalman + Copula)')
    parser.add_argument('--stage', default='all', choices=['all', '1', '2', '3', '4'],
                       help='실행 단계')
    args = parser.parse_args()

    print("\n=== BTC/ETH Pair Trading v3: Kalman Filter + Copula Dependence ===\n")

    # 데이터 로드
    start_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime(2026, 12, 31, tzinfo=timezone.utc)

    pool = await make_pool()
    try:
        btc = await load_ohlcv(pool, 'BTCUSDT', '1d', start_dt, end_dt)
        eth = await load_ohlcv(pool, 'ETHUSDT', '1d', start_dt, end_dt)
    finally:
        await pool.close()

    if btc is None or eth is None or len(btc) < 200:
        print("ERROR: Insufficient data")
        return

    print(f"Loaded {len(btc)} days of OHLCV (BTC/ETH)\n")

    results_all = {}

    if args.stage in ['all', '1']:
        results_all['stage_1'] = await run_stage_1_copula_analysis(btc, eth)

    if args.stage in ['all', '2']:
        results_all['stage_2'] = await run_stage_2_kalman_baseline(btc, eth)

    if args.stage in ['all', '3']:
        results_all['stage_3'] = await run_stage_3_gridsearch(btc, eth)

    if args.stage in ['all', '4']:
        results_all['stage_4'] = await run_stage_4_walkforward(btc, eth)

    print("\n=== Execution Complete ===\n")


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
