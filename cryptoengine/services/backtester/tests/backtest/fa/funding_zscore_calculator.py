"""funding_zscore_calculator.py — 펀딩비 z-score 계산기.

펀딩비의 평균과 표준편차를 기반으로 정상화된 점수를 계산하여
극단적 펀딩비 이벤트를 감지.

사용법:
    from funding_zscore_calculator import compute_funding_zscore

    df_funding = pd.DataFrame({
        'rate': [0.0001, 0.00015, -0.00005, ...],
        ...
    }, index=pd.DatetimeIndex([...]))

    result = compute_funding_zscore(df_funding, window_days=30)

검증 실행:
    python funding_zscore_calculator.py --validate
"""
import pandas as pd
import numpy as np


def compute_funding_zscore(df_funding: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
    """
    펀딩비 시계열에서 z-score와 극단 여부를 계산한다.

    Args:
        df_funding: 펀딩비 DataFrame (index=timestamp, columns=['rate'])
        window_days: 이동평균/표준편차 윈도우 (일 단위)
                    8시간 정산이므로 실제 봉은 window_days * 3

    Returns:
        DataFrame with columns:
            - rate: 원본 펀딩비
            - funding_ma: 이동평균
            - funding_std: 표준편차
            - zscore: (현재 - 평균) / 표준편차
            - is_extreme_high: zscore > +1.5 (고펀딩)
            - is_extreme_low: zscore < -1.5 (저펀딩 또는 음수)
    """
    window_bars = window_days * 3  # 8h당 1봉이므로 3 * window_days
    df = df_funding.copy()

    # 이동평균 (윈도우 절반 이상 데이터로도 계산 가능)
    df['funding_ma'] = df['rate'].rolling(
        window=window_bars,
        min_periods=max(1, window_bars // 2)
    ).mean()

    # 표준편차
    df['funding_std'] = df['rate'].rolling(
        window=window_bars,
        min_periods=max(1, window_bars // 2)
    ).std()

    # z-score (std=0 방지)
    df['zscore'] = (df['rate'] - df['funding_ma']) / df['funding_std'].replace(0, np.nan)

    # 극단 여부
    df['is_extreme_high'] = df['zscore'] > 1.5
    df['is_extreme_low'] = df['zscore'] < -1.5

    return df


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true",
                        help="검증 모드: 샘플 데이터로 통계 출력")
    args = parser.parse_args()

    if args.validate:
        print("펀딩 z-score 계산기 검증 모드")
        print("-" * 60)

        # 샘플 데이터 생성 (정규분포 + 약간의 극단값)
        np.random.seed(42)
        rates = np.random.normal(loc=0.00005, scale=0.00003, size=1000)
        # 극단값 추가 (10개)
        rates[100] = 0.0002  # 고펀딩
        rates[200] = -0.0001  # 저펀딩

        dates = pd.date_range('2024-01-01', periods=len(rates), freq='8h', tz='UTC')
        df = pd.DataFrame({'rate': rates}, index=dates)

        # z-score 계산
        result = compute_funding_zscore(df, window_days=30)

        # 통계 출력
        print(f"데이터 포인트 수: {len(result)}")
        print(f"평균 펀딩비: {result['rate'].mean():.8f}")
        print(f"표준편차: {result['rate'].std():.8f}")
        print(f"최대 펀딩비: {result['rate'].max():.8f}")
        print(f"최소 펀딩비: {result['rate'].min():.8f}")
        print()
        print(f"극단 고펀딩 이벤트: {result['is_extreme_high'].sum()} 회")
        print(f"극단 저펀딩 이벤트: {result['is_extreme_low'].sum()} 회")
        print()

        # 극단 이벤트 샘플 출력
        if result['is_extreme_high'].sum() > 0:
            print("극단 고펀딩 샘플:")
            extreme_high = result[result['is_extreme_high']].head(3)
            for idx, row in extreme_high.iterrows():
                print(f"  {idx}: rate={row['rate']:.8f}, zscore={row['zscore']:.2f}")

        print("\n✓ 검증 완료")
