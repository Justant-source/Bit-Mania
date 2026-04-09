"""Unit tests for AlertManager."""

import json
import time
import pytest
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, ".")

from alert_manager import AlertManager


@pytest.fixture
def tmp_alert_env(tmp_path):
    """Provide temp directories for alerts and logs."""
    alert_dir = tmp_path / "alerts"
    log_file = tmp_path / "logs" / "health.log"
    return AlertManager(alert_dir=alert_dir, log_file=log_file)


class TestAlertEmit:
    def test_creates_alert_file(self, tmp_alert_env):
        mgr = tmp_alert_env
        path = mgr.emit(
            source_name="etf_flows",
            severity="critical",
            failure_stage="structural",
            error_details={"reason": "No table found"},
            recovery_hints=["Check HTML structure"],
        )
        assert path is not None
        assert path.exists()
        content = path.read_text()
        assert "etf_flows" in content
        assert "structural" in content
        assert "No table found" in content
        assert "Check HTML structure" in content

    def test_alert_file_name_format(self, tmp_alert_env):
        mgr = tmp_alert_env
        path = mgr.emit(
            source_name="onchain",
            severity="warning",
            failure_stage="sanity",
            error_details={"reason": "out of range"},
        )
        assert path is not None
        assert "onchain" in path.name
        assert path.suffix == ".md"

    def test_log_entry_written(self, tmp_alert_env):
        mgr = tmp_alert_env
        mgr.emit(
            source_name="derivatives",
            severity="critical",
            failure_stage="http",
            error_details={"status_code": 500},
        )
        log_content = mgr._log_file.read_text()
        entry = json.loads(log_content.strip())
        assert entry["source"] == "derivatives"
        assert entry["severity"] == "critical"

    def test_dedup_suppresses_same_error(self, tmp_alert_env):
        mgr = tmp_alert_env
        # First alert
        path1 = mgr.emit(
            source_name="etf_flows",
            severity="warning",
            failure_stage="structural",
            error_details={"reason": "selector missing"},
        )
        # Second alert (same source+stage) — should be deduped
        path2 = mgr.emit(
            source_name="etf_flows",
            severity="warning",
            failure_stage="structural",
            error_details={"reason": "selector missing"},
        )
        assert path1 is not None
        assert path2 is None  # Deduped

    def test_critical_bypasses_dedup(self, tmp_alert_env):
        mgr = tmp_alert_env
        path1 = mgr.emit(
            source_name="etf_flows",
            severity="critical",
            failure_stage="structural",
            error_details={"reason": "broken"},
        )
        path2 = mgr.emit(
            source_name="etf_flows",
            severity="critical",
            failure_stage="structural",
            error_details={"reason": "still broken"},
        )
        # Critical always emits
        assert path1 is not None
        assert path2 is not None

    def test_different_stage_not_deduped(self, tmp_alert_env):
        mgr = tmp_alert_env
        path1 = mgr.emit(
            source_name="etf_flows",
            severity="warning",
            failure_stage="structural",
            error_details={"reason": "a"},
        )
        path2 = mgr.emit(
            source_name="etf_flows",
            severity="warning",
            failure_stage="sanity",
            error_details={"reason": "b"},
        )
        assert path1 is not None
        assert path2 is not None

    def test_no_telegram_webhook_no_error(self, tmp_alert_env):
        """When no Telegram config, should silently skip."""
        mgr = tmp_alert_env
        # Should not raise even without ALERT_TELEGRAM_BOT_TOKEN
        path = mgr.emit(
            source_name="test",
            severity="warning",
            failure_stage="http",
            error_details={"reason": "test"},
        )
        assert path is not None

    def test_get_recent_alerts(self, tmp_alert_env):
        mgr = tmp_alert_env
        mgr.emit(source_name="a", severity="critical", failure_stage="http", error_details={"r": "1"})
        mgr.emit(source_name="b", severity="critical", failure_stage="http", error_details={"r": "2"})

        all_alerts = mgr.get_recent_alerts()
        assert len(all_alerts) == 2

        filtered = mgr.get_recent_alerts(source_name="a")
        assert len(filtered) == 1
        assert filtered[0]["source"] == "a"
