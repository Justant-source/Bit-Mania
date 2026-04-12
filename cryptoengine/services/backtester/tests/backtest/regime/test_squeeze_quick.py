"""Quick test of volatility squeeze strategy without DB writes"""
import sys
sys.path.insert(0, "/app")

import asyncio
import logging
from datetime import datetime, timezone
from tests.backtest.core import load_ohlcv, make_pool
from bt_volatility_squeeze import VolatilitySqueezeBacktester, PARAMS_STAGE1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_DT = datetime(2023, 4, 1, tzinfo=timezone.utc)
END_DT = datetime(2026, 4, 10, tzinfo=timezone.utc)

async def main():
    pool = await make_pool()
    df_1d = await load_ohlcv(pool, "BTCUSDT", "1d", START_DT, END_DT)
    df_6h = await load_ohlcv(pool, "BTCUSDT", "6h", START_DT, END_DT)
    await pool.close()

    if df_1d.empty or df_6h.empty:
        logger.error(f"데이터 없음: 1d={len(df_1d)}, 6h={len(df_6h)}")
        return

    logger.info(f"Stage 1 (기본): {PARAMS_STAGE1}")
    bt = VolatilitySqueezeBacktester(df_1d, df_6h, PARAMS_STAGE1, 5000.0)
    metrics = bt.run()

    logger.info(f"결과:")
    logger.info(f"  CAGR: {metrics['cagr_pct']:.2f}%")
    logger.info(f"  Sharpe: {metrics['sharpe_ratio']:.3f}")
    logger.info(f"  MDD: {metrics['max_drawdown_pct']:.2f}%")
    logger.info(f"  거래수: {metrics['trade_count']}")
    logger.info(f"  수익률: {metrics['total_return_pct']:.2f}%")
    logger.info(f"  최종자산: ${metrics['final_equity']:.2f}")

if __name__ == "__main__":
    asyncio.run(main())
