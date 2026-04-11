#!/usr/bin/env python3
"""
청산 캐스케이드 탐지기 (Cascade Detector)

BT_TASK_07: 1h OHLCV 기반 청산 캐스케이드 패턴 탐지

알려진 대형 캐스케이드 이벤트:
- 2024-08-05: Enko liquidation (BTC -5.5%)
- 2024-12-18: Fed hawkish (BTC -5%)
- 2025-02-03: Tariff shock (BTC -5%)
- 2025-04-07: Tariff escalation (BTC -3%)

탐지 조건:
1. 4h 누적 가격 변화 < -3% (롱 청산) 또는 > +3% (숏 청산)
2. 4h 거래량 > 24h 평균 × 2.0
3. OI 변화 < -10% (선택)
"""
import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

import pandas as pd
import numpy as np

from tests.backtest.core import make_pool
from tests.backtest.core.loader import load_ohlcv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KST = timezone(timedelta(hours=9))

# 알려진 대형 이벤트 (검증용)
KNOWN_EVENTS = [
    {"date": datetime(2024, 8, 5, tzinfo=KST), "name": "Enko liquidation", "severity": 0.055},
    {"date": datetime(2024, 12, 18, tzinfo=KST), "name": "Fed hawkish", "severity": 0.050},
    {"date": datetime(2025, 2, 3, tzinfo=KST), "name": "Tariff shock", "severity": 0.050},
    {"date": datetime(2025, 4, 7, tzinfo=KST), "name": "Tariff escalation", "severity": 0.030},
]


def detect_cascade(
    df_1h: pd.DataFrame,
    threshold_usd: float = 500_000_000,
    oi_drop_threshold: float = -0.10,
    price_change_threshold: float = -0.03,
) -> pd.DataFrame:
    """
    1h 봉 기반 청산 캐스케이드 탐지

    입력:
        df_1h: open_time, close, high, low, volume이 포함된 DataFrame (1h 타임프레임)
        threshold_usd: 추정 청산액 임계값 (추정값 기반)
        oi_drop_threshold: OI 하락률 임계값
        price_change_threshold: 가격 변화 임계값 (음수 = 하락)

    반환:
        DataFrame with columns:
            - cascade_time: 캐스케이드 탐지 시점
            - side: "long_squeeze" 또는 "short_squeeze"
            - severity_score: 0.0 ~ 1.0 (정규화)
            - price_at_detection: 탐지 시점 가격
            - volume_ratio: 거래량 비율
            - estimated_liquidation_usd: 추정 청산액
    """
    df = df_1h.sort_values("open_time").reset_index(drop=True)

    # 4h 롤링 윈도우
    df["close_4h_ago"] = df["close"].shift(4)
    df["high_4h"] = df["high"].rolling(4, min_periods=4).max()
    df["low_4h"] = df["low"].rolling(4, min_periods=4).min()
    df["price_change_4h"] = (df["close"] - df["close_4h_ago"]) / df["close_4h_ago"]
    df["volume_ma24"] = df["volume"].rolling(24, min_periods=1).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma24"]

    # OI 변화 (거래량으로 대체 추정, 실제 OI 없음)
    df["oi_proxy"] = (df["volume"] * df["close"]).rolling(4, min_periods=4).sum()
    df["oi_change_4h"] = df["oi_proxy"].pct_change(4)

    results = []

    for idx, row in df.iterrows():
        if idx < 24:  # 충분한 히스토리 필요
            continue

        price_change = row["price_change_4h"]
        volume_ratio = row["volume_ratio"]
        oi_change = row.get("oi_change_4h", 0) or 0

        # 하강 캐스케이드 (롱 청산)
        if price_change < price_change_threshold and volume_ratio > 2.0:
            estimated_liq = abs(price_change) * row["close"] * row["volume"] * 0.1
            severity = min(abs(price_change) / 0.10, 1.0)  # 10% 기준

            results.append({
                "cascade_time": row["open_time"],
                "side": "long_squeeze",
                "severity_score": severity,
                "price_at_detection": row["close"],
                "volume_ratio": volume_ratio,
                "estimated_liquidation_usd": estimated_liq,
                "price_change_4h_pct": price_change * 100,
                "oi_change_4h_pct": oi_change * 100,
            })

        # 상승 캐스케이드 (숏 청산)
        elif price_change > abs(price_change_threshold) and volume_ratio > 2.0:
            estimated_liq = abs(price_change) * row["close"] * row["volume"] * 0.1
            severity = min(price_change / 0.10, 1.0)

            results.append({
                "cascade_time": row["open_time"],
                "side": "short_squeeze",
                "severity_score": severity,
                "price_at_detection": row["close"],
                "volume_ratio": volume_ratio,
                "estimated_liquidation_usd": estimated_liq,
                "price_change_4h_pct": price_change * 100,
                "oi_change_4h_pct": oi_change * 100,
            })

    return pd.DataFrame(results)


def validate_against_known(cascades: pd.DataFrame) -> dict:
    """
    탐지된 캐스케이드를 알려진 이벤트와 매칭

    반환:
        {"matched": int, "total_known": int, "matching_rate": float, "details": [...]}
    """
    matched = []
    for known in KNOWN_EVENTS:
        # ±6시간 윈도우
        window_start = known["date"] - timedelta(hours=6)
        window_end = known["date"] + timedelta(hours=6)

        found = cascades[
            (cascades["cascade_time"] >= window_start) &
            (cascades["cascade_time"] <= window_end) &
            (cascades["side"] == "long_squeeze")  # 알려진 이벤트는 모두 가격 하락
        ]

        if not found.empty:
            best_match = found.loc[found["severity_score"].idxmax()]
            matched.append({
                "known_event": known["name"],
                "known_date": known["date"],
                "detected_date": best_match["cascade_time"],
                "delay_hours": (best_match["cascade_time"] - known["date"]).total_seconds() / 3600,
                "detected_severity": best_match["severity_score"],
                "expected_severity": known["severity"],
            })

    rate = len(matched) / len(KNOWN_EVENTS) if KNOWN_EVENTS else 0.0
    return {
        "matched": len(matched),
        "total_known": len(KNOWN_EVENTS),
        "matching_rate": rate,
        "details": matched,
    }


async def main():
    parser = argparse.ArgumentParser(description="청산 캐스케이드 탐지기")
    parser.add_argument("--validate", action="store_true", help="알려진 이벤트 검증")
    parser.add_argument("--threshold", type=float, default=500_000_000, help="추정 청산액 임계값")
    parser.add_argument("--start", type=str, default="2024-01-01", help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="종료일 (YYYY-MM-DD)")
    args = parser.parse_args()

    pool = await make_pool()

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=KST)
        end_date = (
            datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=KST)
            if args.end
            else datetime.now(tz=KST)
        )

        logger.info(f"OHLCV 로드: {start_date} ~ {end_date}")
        df_1h = await load_ohlcv(pool, "BTCUSDT", timeframe="1h", start=start_date, end=end_date)

        if df_1h.empty:
            logger.error("OHLCV 데이터 없음")
            return

        # 인덱스를 컬럼으로 변환
        df_1h = df_1h.reset_index()
        df_1h = df_1h.rename(columns={"ts": "open_time"})

        logger.info(f"로드된 봉: {len(df_1h)}개")

        cascades = detect_cascade(df_1h, threshold_usd=args.threshold)
        logger.info(f"탐지된 캐스케이드: {len(cascades)}개")

        if cascades.empty:
            logger.info("캐스케이드 신호 없음")
        else:
            print("\n=== 탐지된 캐스케이드 (상위 10건) ===")
            print(cascades.nlargest(10, "severity_score")[
                ["cascade_time", "side", "severity_score", "price_at_detection", "estimated_liquidation_usd"]
            ].to_string(index=False))

        if args.validate:
            validation = validate_against_known(cascades)
            print("\n=== 검증 결과 ===")
            print(f"알려진 이벤트 매칭: {validation['matched']}/{validation['total_known']} ({validation['matching_rate']:.1%})")
            if validation["details"]:
                validation_df = pd.DataFrame(validation["details"])
                print(validation_df.to_string(index=False))

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
