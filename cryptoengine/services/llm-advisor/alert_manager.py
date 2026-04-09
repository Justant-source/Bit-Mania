"""Alert manager for Fail-Loud data source monitoring.

Responsibilities:
- File-based alert queue (alerts/ directory)
- Health log file (logs/data_source_health.log)
- Optional Telegram webhook via existing AlertDispatcher
- Dedup: same source+error only once per hour
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Literal

import structlog

logger = structlog.get_logger(__name__)

# Dedup window
DEDUP_WINDOW_SECONDS = int(os.environ.get("ALERT_DEDUP_WINDOW_SECONDS", "3600"))


class AlertManager:
    """Emit structured alerts for data source failures."""

    def __init__(
        self,
        alert_dir: str | Path | None = None,
        log_file: str | Path | None = None,
    ):
        self._alert_dir = Path(alert_dir or os.environ.get("ALERT_DIR", "./alerts"))
        self._log_file = Path(log_file or os.environ.get("ALERT_LOG_FILE", "./logs/data_source_health.log"))
        self._alert_dir.mkdir(parents=True, exist_ok=True)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)

        # In-memory dedup tracker: {source_name:error_hash -> timestamp}
        self._recent_alerts: dict[str, float] = {}

    def _dedup_key(self, source_name: str, failure_stage: str) -> str:
        return f"{source_name}:{failure_stage}"

    def _is_duplicate(self, key: str) -> bool:
        last_time = self._recent_alerts.get(key)
        if last_time is None:
            return False
        return (time.time() - last_time) < DEDUP_WINDOW_SECONDS

    def emit(
        self,
        source_name: str,
        severity: Literal["info", "warning", "critical"],
        failure_stage: str,
        error_details: dict,
        recovery_hints: list[str] | None = None,
        url: str | None = None,
        response_snippet: str | None = None,
    ) -> Path | None:
        """Create alert file + log entry. Returns alert file path, or None if deduped."""
        dedup_key = self._dedup_key(source_name, failure_stage)

        # Dedup check
        if severity != "critical" and self._is_duplicate(dedup_key):
            logger.debug("alert_deduped", source=source_name, stage=failure_stage)
            return None

        self._recent_alerts[dedup_key] = time.time()

        now = datetime.now()
        timestamp_str = now.strftime("%Y-%m-%d_%H%M%S")
        kst_str = now.strftime("%Y-%m-%d %H:%M:%S KST")

        # Severity emoji
        severity_map = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}
        emoji = severity_map.get(severity, "❓")

        # Build markdown alert
        lines = [
            f"# {emoji} 데이터 소스 장애 알림\n",
            f"**시각**: {kst_str}",
            f"**소스**: {source_name}",
            f"**상태**: {'BROKEN' if severity == 'critical' else 'DEGRADED'}",
            f"**심각도**: {severity.upper()}",
            "",
            "## 장애 내역",
            f"- **실패 단계**: {failure_stage}",
        ]

        for key, val in error_details.items():
            lines.append(f"- **{key}**: {val}")

        if url:
            lines.append(f"- **대상 URL**: {url}")

        if response_snippet:
            lines.extend(["", "### 응답 스니펫 (첫 500자)", "```", response_snippet[:500], "```"])

        if recovery_hints:
            lines.extend(["", "## 복구 체크리스트"])
            for hint in recovery_hints:
                lines.append(f"- [ ] {hint}")

        content = "\n".join(lines) + "\n"

        # Write alert file
        filename = f"{timestamp_str}_{source_name}.md"
        alert_path = self._alert_dir / filename
        alert_path.write_text(content, encoding="utf-8")

        # Write to health log
        log_entry = {
            "timestamp": now.isoformat(),
            "source": source_name,
            "severity": severity,
            "stage": failure_stage,
            "details": error_details,
        }
        with self._log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

        logger.warning(
            "alert_emitted",
            source=source_name,
            severity=severity,
            stage=failure_stage,
            file=str(alert_path),
        )

        # Optional Telegram notification via Redis pub/sub
        self._try_telegram_notify(source_name, severity, failure_stage, error_details)

        return alert_path

    def _try_telegram_notify(
        self,
        source_name: str,
        severity: str,
        failure_stage: str,
        error_details: dict,
    ) -> None:
        """Best-effort Telegram notification. Never raises."""
        try:
            bot_token = os.environ.get("ALERT_TELEGRAM_BOT_TOKEN")
            chat_id = os.environ.get("ALERT_TELEGRAM_CHAT_ID")
            if not bot_token or not chat_id:
                return

            import httpx

            emoji = "🚨" if severity == "critical" else "⚠️"
            text = (
                f"{emoji} *LLM Advisor 데이터 소스 장애*\n\n"
                f"소스: `{source_name}`\n"
                f"단계: {failure_stage}\n"
                f"심각도: {severity.upper()}\n"
            )
            for k, v in list(error_details.items())[:3]:
                text += f"{k}: {v}\n"

            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            # Fire-and-forget sync call (we're in a best-effort block)
            httpx.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=5)
        except Exception as e:
            logger.debug("telegram_alert_failed", error=str(e))

    def get_recent_alerts(self, source_name: str | None = None, limit: int = 10) -> list[dict]:
        """Read recent alerts from log file."""
        if not self._log_file.exists():
            return []
        entries = []
        with self._log_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if source_name and entry.get("source") != source_name:
                        continue
                    entries.append(entry)
                except json.JSONDecodeError:
                    continue
        return entries[-limit:]
