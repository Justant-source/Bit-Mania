"""Unit tests for shared/logging_config.py's error→Telegram alert payload.

2026-09-19 개편: _make_error_alert_processor의 payload에 severity/component/
anomaly_type/fingerprint를 추가했다 — 이전에는 severity가 없어 telegram-bot이
항상 "WARNING"으로 표시했고(CRITICAL도 마찬가지), fingerprint가 없어 봇의
dedup이 alert_type 하나로 뭉개져 분당 서비스 전체에서 1건만 살아남았다.

Redis 발행(_publish_error_alert)까지는 통합하지 않는다 — 프로세서가 만드는
payload 구성 로직만 직접 검증한다(REDIS_URL 미설정이면 _publish_error_alert가
조용히 no-op하므로 실제 네트워크 호출은 발생하지 않는다).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.logging_config import _make_error_alert_processor  # noqa: E402


def _run_processor(service_name: str, level: str, event: str, message: str | None = None):
    """프로세서를 호출하고, 실제로 발행된 JSON payload를 dict로 반환한다.

    _publish_error_alert는 fire-and-forget(asyncio.ensure_future)이므로,
    호출 인자를 캡처하기 위해 patch한다.
    """
    processor = _make_error_alert_processor(service_name)
    event_dict = {"level": level, "event": event}
    if message is not None:
        event_dict["message"] = message

    with patch("shared.logging_config._publish_error_alert") as mock_publish:
        # dedup 캐시가 이전 테스트 실행과 충돌하지 않도록 매번 새 이벤트명 사용 권장
        result = processor(None, level.lower(), dict(event_dict))
        assert result == event_dict  # 프로세서는 event_dict를 그대로 반환해야 함(체이닝)
        if mock_publish.called:
            (payload_str,), _ = mock_publish.call_args
            return json.loads(payload_str)
    return None


class TestAlertPayloadFields:
    def test_error_level_gets_error_severity(self):
        payload = _run_processor("execution-engine", "ERROR", "order_rejected_test_1", "reason text")
        assert payload is not None
        assert payload["severity"] == "error"
        assert payload["level"] == "ERROR"

    def test_critical_level_gets_critical_severity(self):
        payload = _run_processor("orchestrator", "CRITICAL", "kill_switch_triggered_test_1", "Kill Switch 발동")
        assert payload is not None
        assert payload["severity"] == "critical"
        assert payload["level"] == "CRITICAL"

    def test_component_and_anomaly_type_mirror_service_and_event(self):
        payload = _run_processor("supertrend", "ERROR", "position_sync_error_test_1", "sync failed")
        assert payload["component"] == "supertrend"
        assert payload["anomaly_type"] == "position_sync_error_test_1"

    def test_fingerprint_is_service_and_event(self):
        payload = _run_processor("market-data", "ERROR", "funding_rate_alert_test_1", "msg")
        assert payload["fingerprint"] == "market-data:funding_rate_alert_test_1"

    def test_different_services_same_event_get_different_fingerprints(self):
        p1 = _run_processor("service-a", "ERROR", "shared_event_name_test_1", "m")
        p2 = _run_processor("service-b", "ERROR", "shared_event_name_test_1", "m")
        assert p1["fingerprint"] != p2["fingerprint"]

    def test_message_truncated_to_700_chars(self):
        long_message = "x" * 2000
        payload = _run_processor("execution-engine", "ERROR", "long_message_test_1", long_message)
        assert len(payload["message"]) == 700

    def test_backward_compat_fields_preserved(self):
        payload = _run_processor("execution-engine", "ERROR", "compat_test_1", "m")
        assert payload["type"] == "error_log"
        assert set(payload) >= {"type", "level", "service", "event", "message", "ts"}

    def test_warning_level_not_published(self):
        """WARNING(30)은 임계값(40) 미만이라 발행되지 않아야 한다."""
        payload = _run_processor("execution-engine", "WARNING", "warning_test_1", "m")
        assert payload is None

    def test_processor_never_raises_on_malformed_event_dict(self):
        processor = _make_error_alert_processor("svc")
        # event_dict에 흔한 키가 빠져 있어도 예외를 던지면 안 된다
        result = processor(None, "error", {})
        assert isinstance(result, dict)
