"""cointegration_tester.py — Engle-Granger 공적분 검정 도구

공적분 공존 기간 계산, 베타, 반감기, ADF 안정성 검증
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
from statsmodels.tsa.stattools import adfuller

# core 임포트
BACKTEST_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BACKTEST_DIR))
from core.loader import load_ohlcv
from core.db import make_pool


def engle_granger_test(y: pd.Series, x: pd.Series) -> dict:
    """Engle-Granger 2단계 공적분 검정

    Args:
        y: 피종속변수 시계열 (로그 가격)
        x: 독립변수 시계열 (로그 가격)

    Returns:
        {
            'beta': 헤지비율,
            'alpha': 절편,
            'residual': 잔차 시계열,
            'adf_stat': ADF 통계량,
            'adf_pvalue': ADF p-값 (낮을수록 정상),
            'half_life': OU 반감기 (일 단위),
            'is_cointegrated': p < 0.10 여부
        }
    """
    # Step 1: OLS 회귀로 베타 추정
    X = add_constant(x)
    model = OLS(y, X).fit()
    alpha = model.params.iloc[0]
    beta = model.params.iloc[1]
    residual = y - beta * x - alpha

    # Step 2: 잔차에 ADF 검정 (autolag='AIC' 사용)
    adf_result = adfuller(residual.dropna(), autolag='AIC')
    adf_stat, adf_pvalue = adf_result[0], adf_result[1]

    # Step 3: OU 프로세스 반감기 추정
    # Δz_t = ρ * z_{t-1} + ε_t
    residual_clean = residual.dropna()
    lag_res = residual_clean.shift(1).dropna()
    delta_res = residual_clean.diff().dropna()

    common_idx = lag_res.index.intersection(delta_res.index)
    if len(common_idx) > 2:
        ar_model = OLS(delta_res[common_idx], add_constant(lag_res[common_idx])).fit()
        rho = ar_model.params[1]
        # 반감기 = ln(2) / |ρ|
        half_life = np.log(2) / abs(rho) if rho < 0 else np.inf
    else:
        rho = np.nan
        half_life = np.inf

    return {
        'beta': float(beta),
        'alpha': float(alpha),
        'residual': residual,
        'adf_stat': float(adf_stat),
        'adf_pvalue': float(adf_pvalue),
        'half_life': float(half_life),
        'is_cointegrated': bool(adf_pvalue < 0.10),
    }


def compute_spread_zscore(btc: pd.Series, eth: pd.Series, window=60) -> pd.DataFrame:
    """롤링 윈도우로 z-score 계산

    Args:
        btc: BTC 로그 가격
        eth: ETH 로그 가격
        window: 롤링 윈도우 크기 (일)

    Returns:
        DataFrame: {timestamp, beta, spread, spread_mean, spread_std, zscore, adf_pvalue}
    """
    results = []
    dates = btc.index

    for i in range(window, len(btc)):
        end_idx = i
        start_idx = i - window

        btc_slice = btc.iloc[start_idx:end_idx]
        eth_slice = eth.iloc[start_idx:end_idx]

        test_result = engle_granger_test(btc_slice, eth_slice)
        beta = test_result['beta']
        residual = test_result['residual']
        adf_pvalue = test_result['adf_pvalue']

        # 현재 잔차 (spread)
        spread = btc.iloc[i] - beta * eth.iloc[i] - test_result['alpha']
        spread_mean = residual.mean()
        spread_std = residual.std()

        if spread_std > 0:
            zscore = (spread - spread_mean) / spread_std
        else:
            zscore = 0

        results.append({
            'timestamp': dates[i],
            'beta': beta,
            'spread': spread,
            'spread_mean': spread_mean,
            'spread_std': spread_std,
            'zscore': zscore,
            'adf_pvalue': adf_pvalue,
        })

    return pd.DataFrame(results)


async def main():
    parser = argparse.ArgumentParser(description='Engle-Granger 공적분 검정')
    parser.add_argument('--pair', default='BTCUSDT,ETHUSDT', help='심볼 쌍 (쉼표 구분)')
    parser.add_argument('--report-stability', action='store_true', help='월별 ADF 안정성 리포트')
    args = parser.parse_args()

    symbols = args.pair.split(',')
    if len(symbols) != 2:
        print("ERROR: 정확히 2개 심볼 필요 (예: BTCUSDT,ETHUSDT)")
        sys.exit(1)

    sym1, sym2 = symbols
    print(f"\n=== 공적분 검정: {sym1} vs {sym2} ===\n")

    # DB에서 데이터 로드 (전체 기간)
    from datetime import datetime, timezone
    start_dt = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime(2026, 12, 31, tzinfo=timezone.utc)

    pool = await make_pool()
    try:
        df1 = await load_ohlcv(pool, sym1, '1d', start_dt, end_dt)
        df2 = await load_ohlcv(pool, sym2, '1d', start_dt, end_dt)
    finally:
        await pool.close()

    if df1 is None or df2 is None:
        print(f"ERROR: 데이터 로드 실패 ({sym1} 또는 {sym2})")
        sys.exit(1)

    print(f"{sym1}: {len(df1)} rows, {df1.index.min()} ~ {df1.index.max()}")
    print(f"{sym2}: {len(df2)} rows, {df2.index.min()} ~ {df2.index.max()}\n")

    # 인덱스 정렬 및 교집합 취득
    df1 = df1.sort_index()
    df2 = df2.sort_index()
    common_dates = df1.index.intersection(df2.index)

    df1 = df1.loc[common_dates]
    df2 = df2.loc[common_dates]

    # 로그 가격
    log_btc = np.log(df1['close'])
    log_eth = np.log(df2['close'])

    print(f"공통 기간: {len(df1)} 일 ({df1.index.min().date()} ~ {df1.index.max().date()})\n")

    # 전체 기간 공적분 검정
    print("=== 전체 기간 Engle-Granger 검정 ===")
    eg_result = engle_granger_test(log_btc, log_eth)
    print(f"Beta (헤지비율): {eg_result['beta']:.6f}")
    print(f"Alpha (절편): {eg_result['alpha']:.6f}")
    print(f"ADF 통계량: {eg_result['adf_stat']:.6f}")
    print(f"ADF p-값: {eg_result['adf_pvalue']:.6f} {'✓ 정상' if eg_result['adf_pvalue'] < 0.10 else '✗ 비정상'}")
    print(f"반감기: {eg_result['half_life']:.2f} 일\n")

    # 안정성 리포트 생성
    if args.report_stability:
        print("=== 월별 안정성 분석 ===\n")
        monthly_stability = []

        for window_days in [60, 90, 120]:
            print(f"--- {window_days}일 롤링 윈도우 ---")

            zscore_df = compute_spread_zscore(log_btc, log_eth, window=window_days)
            zscore_df['year_month'] = pd.to_datetime(zscore_df['timestamp']).dt.to_period('M')

            monthly_stats = zscore_df.groupby('year_month').agg({
                'adf_pvalue': ['mean', 'median', 'min'],
                'zscore': ['std', 'mean'],
            }).round(6)

            # 안정 월 비율 (p < 0.10)
            stable_count = (zscore_df['adf_pvalue'] < 0.10).sum()
            total_count = len(zscore_df)
            stable_pct = 100.0 * stable_count / total_count if total_count > 0 else 0

            print(f"안정 비율: {stable_pct:.1f}% ({stable_count}/{total_count} 일)")
            print(f"ADF p-값 평균: {zscore_df['adf_pvalue'].mean():.6f}")
            print(f"ADF p-값 중앙값: {zscore_df['adf_pvalue'].median():.6f}")
            print(f"ADF p-값 최소값: {zscore_df['adf_pvalue'].min():.6f}")
            print()

            monthly_stability.append({
                'window': window_days,
                'stable_pct': stable_pct,
                'mean_adf_p': zscore_df['adf_pvalue'].mean(),
                'median_adf_p': zscore_df['adf_pvalue'].median(),
                'min_adf_p': zscore_df['adf_pvalue'].min(),
            })

        stability_df = pd.DataFrame(monthly_stability)
        print("=== 요약 ===")
        print(stability_df.to_string(index=False))

        # 안정성 기준
        print("\n=== 평가 ===")
        overall_stable = stability_df['stable_pct'].mean()
        if overall_stable >= 60:
            print(f"✓ 안정성 충분 (평균 {overall_stable:.1f}%) — 페어 트레이딩 적합")
        elif overall_stable >= 40:
            print(f"△ 안정성 중간 (평균 {overall_stable:.1f}%) — 임계값 완화 필요")
        else:
            print(f"✗ 안정성 부족 (평균 {overall_stable:.1f}%) — 페어 트레이딩 부적합")


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
