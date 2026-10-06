"""Market-data WebSocket subscriptions are the 4h kline only."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
MARKET_DATA_DIR = ROOT / "services" / "market-data"
if str(MARKET_DATA_DIR) not in sys.path:
    sys.path.insert(0, str(MARKET_DATA_DIR))

from collector import MarketDataCollector  # noqa: E402


def test_core_topics_btcusdt_is_kline_240_only() -> None:
    collector = MarketDataCollector(
        exchange="bybit",
        symbol="BTCUSDT",
        api_key="",
        api_secret="",
        testnet=True,
        redis=None,
        db_pool=None,
    )
    assert collector._core_topics() == ["kline.240.BTCUSDT"]
