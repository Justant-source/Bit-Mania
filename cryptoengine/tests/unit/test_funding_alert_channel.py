"""funding_monitor.py 펀딩 요율 임계값 경고 채널 회귀 테스트.

2026-09 조사: 이 경고는 "alerts:funding"(ce: 접두사 누락)으로 발행돼 텔레그램 봇
구독 채널("ce:alerts:funding")과 아예 달라 한 번도 도달하지 않았다. 단순히
채널명만 고치면 여전히 도달하지 않는다 — 이 알림은 payment/expected_payment가
없는 "요율 임계값 초과" 이벤트인데, ce:alerts:funding은 FundingAccumulator가
payment 키를 기대해 0으로 읽고 일일 누적 버퍼로 조용히 삼켜버리기 때문이다.
그래서 이 알림은 ce:alerts:anomaly로, _format_anomaly_alert 스키마에 맞춰
재발행하도록 고쳤다 — 이 테스트는 그 라우팅이 유지되는지 잠근다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
MARKET_DATA_DIR = ROOT / "services" / "market-data"
if str(MARKET_DATA_DIR) not in sys.path:
    sys.path.insert(0, str(MARKET_DATA_DIR))

from funding_monitor import FundingMonitor  # noqa: E402


@pytest.fixture
def monitor():
    redis = AsyncMock()
    redis.publish = AsyncMock(return_value=1)
    m = FundingMonitor(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        api_key="",
        api_secret="",
        testnet=True,
        coinglass_api_key="",
        redis=redis,
        db_pool=AsyncMock(),
    )
    return m


def _published_channels(redis) -> list[str]:
    return [call.args[0] for call in redis.publish.call_args_list]


def _published_payload(redis, channel: str) -> dict:
    for call in redis.publish.call_args_list:
        if call.args[0] == channel:
            return json.loads(call.args[1])
    raise AssertionError(f"no publish to {channel}")


class TestFundingAlertChannel:
    @pytest.mark.asyncio
    async def test_critical_rate_publishes_to_anomaly_channel(self, monitor):
        await monitor._check_alerts(monitor.critical_threshold + 0.0005, {})
        channels = _published_channels(monitor.redis)
        assert "ce:alerts:anomaly" in channels
        assert "alerts:funding" not in channels
        assert "ce:alerts:funding" not in channels

    @pytest.mark.asyncio
    async def test_warning_rate_publishes_to_anomaly_channel(self, monitor):
        await monitor._check_alerts(monitor.warn_threshold + 0.00001, {})
        channels = _published_channels(monitor.redis)
        assert "ce:alerts:anomaly" in channels
        assert "alerts:funding" not in channels
        assert "ce:alerts:funding" not in channels

    @pytest.mark.asyncio
    async def test_payload_schema_matches_anomaly_formatter(self, monitor):
        await monitor._check_alerts(monitor.critical_threshold + 0.0005, {})
        payload = _published_payload(monitor.redis, "ce:alerts:anomaly")
        assert payload["anomaly_type"] == "funding_rate_threshold"
        assert payload["component"] == "funding_monitor"
        assert payload["severity"] == "critical"
        assert payload["level"] == "CRITICAL"
        assert "details" in payload
        # 회귀 잠금: payment 스키마로 되돌아가면 FundingAccumulator에 조용히 삼켜진다
        assert "payment" not in payload
        assert "expected_payment" not in payload

    @pytest.mark.asyncio
    async def test_warning_severity_lowercase(self, monitor):
        await monitor._check_alerts(monitor.warn_threshold + 0.00001, {})
        payload = _published_payload(monitor.redis, "ce:alerts:anomaly")
        assert payload["severity"] == "warning"
        assert payload["level"] == "WARNING"

    @pytest.mark.asyncio
    async def test_fingerprint_includes_exchange_symbol_level(self, monitor):
        await monitor._check_alerts(monitor.critical_threshold + 0.0005, {})
        payload = _published_payload(monitor.redis, "ce:alerts:anomaly")
        fp = payload["fingerprint"]
        assert "bybit" in fp
        assert "BTC/USDT:USDT" in fp
        assert "critical" in fp

    @pytest.mark.asyncio
    async def test_different_symbols_get_different_fingerprints(self, monitor):
        await monitor._check_alerts(monitor.critical_threshold + 0.0005, {})
        fp1 = _published_payload(monitor.redis, "ce:alerts:anomaly")["fingerprint"]

        monitor.redis.publish.reset_mock()
        monitor.symbol = "ETH/USDT:USDT"
        await monitor._check_alerts(monitor.critical_threshold + 0.0005, {})
        fp2 = _published_payload(monitor.redis, "ce:alerts:anomaly")["fingerprint"]

        assert fp1 != fp2

    @pytest.mark.asyncio
    async def test_below_threshold_publishes_nothing(self, monitor):
        await monitor._check_alerts(monitor.warn_threshold * 0.5, {})
        monitor.redis.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_negative_rate_direction_and_sign_agnostic_threshold(self, monitor):
        await monitor._check_alerts(-(monitor.critical_threshold + 0.0005), {})
        payload = _published_payload(monitor.redis, "ce:alerts:anomaly")
        assert payload["severity"] == "critical"
        assert "negative" in payload["direction"]
