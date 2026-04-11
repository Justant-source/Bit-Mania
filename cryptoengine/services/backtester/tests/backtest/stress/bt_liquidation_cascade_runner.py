#!/usr/bin/env python3
"""
청산 캐스케이드 백테스트 통합 실행기

최종 종합 리포트를 생성합니다.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pandas as pd

# 백테스트 임포트
sys.path.insert(0, "/app")
from tests.backtest.core import load_ohlcv, make_pool
from tests.backtest.core.metrics import sharpe, mdd, cagr, profit_factor, monthly_returns
from tests.backtest.analysis.cascade_detector import detect_cascade
from tests.backtest.stress.bt_liquidation_cascade import CascadeBacktester

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

KST = timezone(timedelta(hours=9))


async def generate_comprehensive_report():
    """종합 리포트 생성"""

    pool = await make_pool()
    try:
        # 데이터 로드 (3년: 2023-04-01 ~ 2026-04-11)
        start_time = datetime(2023, 4, 1, tzinfo=KST)
        end_time = datetime(2026, 4, 11, tzinfo=KST)

        logger.info(f"데이터 로드: BTCUSDT ({start_time} ~ {end_time})")
        ohlcv_1h = await load_ohlcv(pool, "BTCUSDT", timeframe="1h", start=start_time, end=end_time)

        if ohlcv_1h.empty:
            logger.error("OHLCV 데이터 없음")
            return

        # 인덱스 정리
        ohlcv_1h = ohlcv_1h.reset_index()
        ohlcv_1h = ohlcv_1h.rename(columns={"ts": "open_time"})

        logger.info(f"로드된 봉: {len(ohlcv_1h)}개 (1h)")

        # Stage 1: Baseline
        logger.info("=" * 60)
        logger.info("Stage 1: Baseline 실행")
        logger.info("=" * 60)

        tester = CascadeBacktester(
            ohlcv_1h,
            cascade_threshold_usd=500_000_000,
            oi_drop_threshold=-0.10,
            price_change_threshold=-0.03,
            take_profit_pct=0.015,
            stop_loss_pct=-0.015,
        )
        stage1_result = tester.run()

        # 캐스케이드 탐지 통계
        cascades = detect_cascade(ohlcv_1h)
        cascades = cascades.reset_index(drop=True)

        logger.info(f"총 캐스케이드 탐지: {len(cascades)}개")
        logger.info(f"  - long_squeeze: {len(cascades[cascades['side']=='long_squeeze'])}개")
        logger.info(f"  - short_squeeze: {len(cascades[cascades['side']=='short_squeeze'])}개")

        # Stage 1 결과
        logger.info(f"\nStage 1 결과:")
        logger.info(f"  CAGR: {stage1_result['cagr_pct']:.2f}%")
        logger.info(f"  Sharpe: {stage1_result['sharpe_ratio']:.2f}")
        logger.info(f"  MDD: {stage1_result['max_drawdown_pct']:.2f}%")
        logger.info(f"  Win Rate: {stage1_result['win_rate']:.2f}%")
        logger.info(f"  Trades: {stage1_result['num_trades']}")
        logger.info(f"  Final Capital: ${stage1_result['final_capital']:.0f}")

        # 최종 리포트 저장
        result_dir = Path("/app/results")
        result_dir.mkdir(exist_ok=True)

        report_path = result_dir / f"19_LIQUIDATION_CASCADE_{datetime.now(tz=KST).strftime('%Y%m%d_%H%M%S')}.json"

        report = {
            "title": "청산 캐스케이드 역발상 백테스트",
            "date": datetime.now(tz=KST).isoformat(),
            "period": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "total_hours": len(ohlcv_1h),
            },
            "cascade_statistics": {
                "total_detected": len(cascades),
                "long_squeeze": int(len(cascades[cascades['side']=='long_squeeze'])),
                "short_squeeze": int(len(cascades[cascades['side']=='short_squeeze'])),
            },
            "stage_1_baseline": {
                "cagr_pct": float(stage1_result['cagr_pct']),
                "sharpe_ratio": float(stage1_result['sharpe_ratio']),
                "max_drawdown_pct": float(stage1_result['max_drawdown_pct']),
                "win_rate": float(stage1_result['win_rate']),
                "num_trades": int(stage1_result['num_trades']),
                "num_wins": int(stage1_result['num_wins']),
                "num_losses": int(stage1_result['num_losses']),
                "final_capital": float(stage1_result['final_capital']),
                "profit_factor": float(stage1_result['profit_factor']),
            },
            "top_cascades": cascades.nlargest(20, "severity_score")[
                ["cascade_time", "side", "severity_score", "estimated_liquidation_usd"]
            ].to_dict("records"),
            "validation_notes": [
                "1m OHLCV 데이터 부재로 인한 1h 기반 간접 추정 사용",
                "공개 API 청산 데이터 신뢰성 낮음 → proxy 추정값 활용",
                "알려진 이벤트(2024-08-05 등) 부분 매칭 확인",
            ]
        }

        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"\n리포트 저장: {report_path}")

        # 콘솔 요약
        print("\n" + "=" * 70)
        print("청산 캐스케이드 역발상 백테스트 최종 결과")
        print("=" * 70)
        print(f"\n데이터 기간: {start_time.date()} ~ {end_time.date()}")
        print(f"총 캐스케이드 탐지: {len(cascades):,}개")
        print(f"  - long_squeeze: {len(cascades[cascades['side']=='long_squeeze']):,}개")
        print(f"  - short_squeeze: {len(cascades[cascades['side']=='short_squeeze']):,}개")

        print(f"\nStage 1 (Baseline) 성과:")
        print(f"  CAGR: {stage1_result['cagr_pct']:.2f}%")
        print(f"  Sharpe Ratio: {stage1_result['sharpe_ratio']:.2f}")
        print(f"  Max Drawdown: {stage1_result['max_drawdown_pct']:.2f}%")
        print(f"  Win Rate: {stage1_result['win_rate']:.2f}%")
        print(f"  Profit Factor: {stage1_result['profit_factor']:.2f}")
        print(f"  Total Trades: {stage1_result['num_trades']}")
        print(f"  최종 자본: ${stage1_result['final_capital']:,.0f}")

        # 합격 기준 평가
        print(f"\n합격 기준 평가:")
        checks = {
            "CAGR ≥ 25%": stage1_result['cagr_pct'] >= 25.0,
            "Sharpe ≥ 1.0": stage1_result['sharpe_ratio'] >= 1.0,
            "MDD ≤ 15%": abs(stage1_result['max_drawdown_pct']) <= 15.0,
            "Win Rate ≥ 55%": stage1_result['win_rate'] >= 55.0,
            "Profit Factor ≥ 1.5": stage1_result['profit_factor'] >= 1.5,
        }

        for check, result in checks.items():
            status = "✅" if result else "❌"
            print(f"  {status} {check}")

        print("\n" + "=" * 70)

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(generate_comprehensive_report())
