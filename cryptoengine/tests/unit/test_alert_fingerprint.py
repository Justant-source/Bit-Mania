"""Unit tests for telegram-bot main.py's _alert_fingerprint() (2026-09-19).

2026-09 조사에서 발견된 버그: 기존 dedup 지문은 alert_type 하나만 썼기 때문에
서로 다른 서비스의 서로 다른 에러가 전부 "anomaly"라는 동일 지문으로 뭉개져
분당 최대 1건만 살아남았다. 이 파일은 그 수정(_alert_fingerprint)을 검증한다.

main.py는 import 시점에 REDIS_URL/DB_PASSWORD를 require_env()로 읽으므로,
실제 인프라 연결 없이 더미 값을 환경변수로 미리 세팅한 뒤 import한다
(main() 함수 자체는 호출하지 않으므로 실제 연결은 발생하지 않는다).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TELEGRAM_BOT_DIR = ROOT / "services" / "telegram-bot"
if str(TELEGRAM_BOT_DIR) not in sys.path:
    sys.path.insert(0, str(TELEGRAM_BOT_DIR))

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("DB_PASSWORD", "test-password-not-real")

import pytest  # noqa: E402

from main import _alert_fingerprint  # noqa: E402


class TestAlertFingerprint:
    def test_explicit_fingerprint_wins(self):
        fp = _alert_fingerprint("anomaly", {"fingerprint": "custom-fp", "service": "x", "event": "y"})
        assert fp == "custom-fp"

    def test_different_services_produce_different_fingerprints(self):
        """이게 바로 2026-09 버그의 핵심 회귀 잠금 — 예전엔 둘 다 'anomaly'였다."""
        fp1 = _alert_fingerprint("anomaly", {"service": "execution", "event": "order_timeout"})
        fp2 = _alert_fingerprint("anomaly", {"service": "supertrend", "event": "position_sync_error"})
        assert fp1 != fp2
        assert fp1 != "anomaly"
        assert fp2 != "anomaly"

    def test_same_service_and_event_collide(self):
        fp1 = _alert_fingerprint("anomaly", {"service": "execution", "event": "order_timeout"})
        fp2 = _alert_fingerprint("anomaly", {"service": "execution", "event": "order_timeout"})
        assert fp1 == fp2

    def test_component_and_anomaly_type_used_when_no_service_event(self):
        fp = _alert_fingerprint("anomaly", {"component": "funding_monitor", "anomaly_type": "funding_rate_threshold"})
        assert "funding_monitor" in fp
        assert "funding_rate_threshold" in fp

    def test_kill_switch_uses_level_and_trigger_reason(self):
        fp1 = _alert_fingerprint("kill_switch", {"level": "PORTFOLIO", "trigger_reason": "daily_drawdown"})
        fp2 = _alert_fingerprint("kill_switch", {"level": "SYSTEM", "trigger_reason": "healthcheck"})
        assert fp1 != fp2

    def test_no_identifying_fields_falls_back_to_alert_type(self):
        assert _alert_fingerprint("anomaly", {}) == "anomaly"
        assert _alert_fingerprint("grafana_unknown", {"message": "hi"}) == "grafana_unknown"
