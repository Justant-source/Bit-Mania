import sys
sys.path.insert(0, "/app")
import asyncio
from datetime import datetime, timezone
from tests.backtest.core import load_ohlcv, make_pool
from tests.backtest.regime.squeeze_indicator import compute_squeeze

async def test():
    pool = await make_pool()
    df_1d = await load_ohlcv(pool, "BTCUSDT", "1d", 
                             datetime(2023, 4, 1, tzinfo=timezone.utc),
                             datetime(2026, 4, 10, tzinfo=timezone.utc))
    await pool.close()
    
    print(f"1d 데이터: {len(df_1d)}")
    print(f"처음 인덱스: {df_1d.index[0]}")
    print(f"마지막 인덱스: {df_1d.index[-1]}")
    
    df_1d = compute_squeeze(df_1d)
    print(f"\n스퀴즈 계산 후 컬럼: {df_1d.columns.tolist()}")
    
    # squeeze_off 조건 확인
    squeeze_off_count = df_1d['squeeze_off'].sum()
    print(f"squeeze_off=True인 바: {squeeze_off_count}")
    
    # 샘플 확인
    mask = df_1d['squeeze_off'].fillna(False)
    if mask.any():
        print("\nsqueeze_off=True인 샘플:")
        print(df_1d[mask].head(10)[['close', 'squeeze_off', 'in_squeeze', 'bb_upper', 'bb_lower', 'rsi']])
    
asyncio.run(test())
