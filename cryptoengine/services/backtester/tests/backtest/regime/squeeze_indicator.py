"""squeeze_indicator.py — 볼린저 밴드 + 켈트너 채널 스퀴즈 지표."""
import pandas as pd
import numpy as np


def compute_squeeze(
    df: pd.DataFrame,
    bb_period: int = 20,
    bb_std: float = 2.0,
    kc_period: int = 20,
    kc_atr_mult: float = 1.5,
) -> pd.DataFrame:
    """
    Bollinger Band + Keltner Channel 스퀴즈 지표 계산.

    Returns:
        DataFrame with columns:
            bb_upper, bb_lower, bb_width
            kc_upper, kc_lower
            in_squeeze: BB가 KC 안에 있는지
            squeeze_off: 스퀴즈 해제 (진입 트리거)
            squeeze_duration: 연속 스퀴즈 기간
            atr: ATR(14)
            rsi: RSI(14)
    """
    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # Bollinger Band (20일 기준)
    bb_mid = close.rolling(bb_period).mean()
    bb_std_val = close.rolling(bb_period).std()
    df["bb_upper"] = bb_mid + bb_std * bb_std_val
    df["bb_lower"] = bb_mid - bb_std * bb_std_val
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bb_mid

    # ATR (14일)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    # Keltner Channel (20일 기준)
    kc_mid = close.rolling(kc_period).mean()
    df["kc_upper"] = kc_mid + kc_atr_mult * df["atr"]
    df["kc_lower"] = kc_mid - kc_atr_mult * df["atr"]

    # 스퀴즈: BB가 KC 안에 있을 때
    df["in_squeeze"] = (df["bb_upper"] < df["kc_upper"]) & (df["bb_lower"] > df["kc_lower"])

    # squeeze_off: 직전 봉이 스퀴즈, 현재 봉이 아님
    df["squeeze_off"] = (~df["in_squeeze"]) & df["in_squeeze"].shift(1).fillna(False)

    # squeeze_duration: 연속 스퀴즈 기간
    duration = []
    cnt = 0
    for sq in df["in_squeeze"]:
        if sq:
            cnt += 1
        else:
            cnt = 0
        duration.append(cnt)
    df["squeeze_duration"] = duration

    # RSI(14)
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - 100 / (1 + rs)

    return df
