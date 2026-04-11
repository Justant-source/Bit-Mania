#!/usr/bin/env python3
"""
청산 캐스케이드 백테스트 - 빠른 검증 버전

1년 데이터로 빠르게 실행하여 결과 검증
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone

# 임포트
sys.path.insert(0, "/app")
from tests.backtest.core import load_ohlcv, make_pool
from tests.backtest.core.metrics import sharpe, mdd, cagr, profit_factor
from tests.backtest.analysis.cascade_detector import detect_cascade
from tests.backtest.stress.bt_liquidation_cascade import CascadeBacktester

KST = timezone(timedelta(hours=9))


async def main():
    pool = await make_pool()

    try:
        # 1년 데이터 로드 (빠른 테스트)
        start_time = datetime(2025, 4, 1, tzinfo=KST)
        end_time = datetime(2026, 4, 11, tzinfo=KST)

        print(f"데이터 로드: {start_time.date()} ~ {end_time.date()}")
        ohlcv_1h = await load_ohlcv(pool, "BTCUSDT", timeframe="1h", start=start_time, end=end_time)

        if ohlcv_1h.empty:
            print("데이터 없음")
            return

        # 인덱스 정리
        ohlcv_1h = ohlcv_1h.reset_index()
        ohlcv_1h = ohlcv_1h.rename(columns={"ts": "open_time"})
        print(f"로드됨: {len(ohlcv_1h)}개 봉\n")

        # 캐스케이드 탐지
        cascades = detect_cascade(ohlcv_1h)
        print(f"탐지된 캐스케이드: {len(cascades)}개")
        print(f"  - long_squeeze: {len(cascades[cascades['side']=='long_squeeze'])}개")
        print(f"  - short_squeeze: {len(cascades[cascades['side']=='short_squeeze'])}개\n")

        # Stage 1 실행
        print("=" * 60)
        print("Stage 1: Baseline 실행 중...")
        print("=" * 60)

        tester = CascadeBacktester(
            ohlcv_1h,
            initial_capital=10_000,
        )
        result = tester.run()

        print(f"\n결과:")
        print(f"  CAGR: {result['cagr_pct']:.2f}%")
        print(f"  Sharpe: {result['sharpe_ratio']:.2f}")
        print(f"  MDD: {result['max_drawdown_pct']:.2f}%")
        print(f"  Win Rate: {result['win_rate']:.2f}%")
        print(f"  Profit Factor: {result['profit_factor']:.2f}")
        print(f"  Trades: {result['num_trades']} (W:{result['num_wins']} L:{result['num_losses']})")
        print(f"  Final Capital: ${result['final_capital']:.0f}")

        # 합격 판정
        print(f"\n합격 기준:")
        print(f"  CAGR >= 25%: {'✅' if result['cagr_pct'] >= 25 else '❌'}")
        print(f"  Sharpe >= 1.0: {'✅' if result['sharpe_ratio'] >= 1.0 else '❌'}")
        print(f"  MDD <= 15%: {'✅' if abs(result['max_drawdown_pct']) <= 15 else '❌'}")
        print(f"  Win Rate >= 55%: {'✅' if result['win_rate'] >= 55 else '❌'}")
        print(f"  Profit Factor >= 1.5: {'✅' if result['profit_factor'] >= 1.5 else '❌'}")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
