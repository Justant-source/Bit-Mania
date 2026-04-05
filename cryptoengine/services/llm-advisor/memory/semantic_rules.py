"""Semantic Rule Extractor — LLM-driven pattern discovery.

Periodically analyzes recent trade outcomes via the LLM to extract
reusable trading rules, which are persisted to ``rules.yaml``.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import structlog
import yaml

from services.llm_advisor.model_manager import ModelManager
from shared.log_events import *

log = structlog.get_logger(__name__)

_DEFAULT_RULES_PATH = "/data/llm_advisor/rules.yaml"

_RULE_EXTRACTION_PROMPT = """\
You are a quantitative trading researcher. Analyze the following recent \
trade outcomes and extract reusable trading rules.

## Recent Trades
{trades}

## Instructions
Identify recurring patterns in winning and losing trades. Extract \
concrete, actionable rules. Respond with JSON:
{{
  "rules": [
    {{
      "id": "rule_001",
      "name": "Short descriptive name",
      "condition": "When X indicator shows Y and regime is Z",
      "action": "recommended action (buy/sell/hold/reduce_size)",
      "confidence": 0.0-1.0,
      "evidence_count": number_of_supporting_trades,
      "category": "entry|exit|sizing|regime",
      "notes": "brief explanation"
    }}
  ],
  "meta": {{
    "trades_analyzed": number,
    "patterns_found": number,
    "analysis_date": "ISO date"
  }}
}}
"""


class SemanticRuleExtractor:
    """Extract and manage trading rules from trade history."""

    def __init__(
        self,
        model_manager: ModelManager,
        rules_path: str | None = None,
    ) -> None:
        self._mm = model_manager
        self._rules_path = Path(
            rules_path or os.getenv("RULES_PATH", _DEFAULT_RULES_PATH)
        )
        self._last_run: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def extract_rules(
        self, recent_trades: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Ask the LLM to find patterns in recent trade outcomes.

        Returns a list of rule dicts.
        """
        if not recent_trades:
            log.info(LLM_ANALYSIS_START, message="룰 추출용 거래 없음")
            return []

        # Format trades for the prompt (limit to 50 most recent)
        trades_text = self._format_trades(recent_trades[:50])

        prompt = _RULE_EXTRACTION_PROMPT.format(trades=trades_text)
        result = await self._mm.invoke(prompt)

        if result is None:
            log.warning(LLM_API_ERROR, message="룰 추출 LLM 실패")
            return []

        rules = result.get("rules", [])
        log.info(LLM_ANALYSIS_COMPLETE, message="룰 추출 완료", count=len(rules))
        self._last_run = time.time()
        return rules

    def save_rules(self, rules: list[dict[str, Any]]) -> None:
        """Write extracted rules to ``rules.yaml``."""
        self._rules_path.parent.mkdir(parents=True, exist_ok=True)

        # Merge with existing rules (keep existing, update matching IDs)
        existing = self.load_rules()
        existing_by_id = {r.get("id", ""): r for r in existing}

        for rule in rules:
            rule_id = rule.get("id", "")
            if rule_id:
                existing_by_id[rule_id] = rule

        merged = list(existing_by_id.values())

        with open(self._rules_path, "w") as fh:
            yaml.safe_dump(
                {"rules": merged, "updated_at": time.time()},
                fh,
                default_flow_style=False,
                sort_keys=False,
            )

        log.info(LLM_ANALYSIS_COMPLETE, message="룰 저장 완료", path=str(self._rules_path), count=len(merged))

    def load_rules(self) -> list[dict[str, Any]]:
        """Load rules from ``rules.yaml``.

        Returns an empty list if the file does not exist.
        """
        if not self._rules_path.exists():
            return []

        try:
            with open(self._rules_path) as fh:
                data = yaml.safe_load(fh) or {}
            rules = data.get("rules", [])
            log.debug(LLM_ANALYSIS_COMPLETE, message="룰 로드 완료", count=len(rules))
            return rules
        except Exception:
            log.exception(LLM_API_ERROR, message="룰 로드 오류")
            return []

    async def run_weekly(
        self, recent_trades: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Weekly orchestration: extract and save rules.

        Intended to be called by a scheduler every 7 days.
        """
        log.info(LLM_ANALYSIS_START, message="주간 룰 추출 시작")
        rules = await self.extract_rules(recent_trades)
        if rules:
            self.save_rules(rules)
        return rules

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_trades(trades: list[dict[str, Any]]) -> str:
        """Format trade list into a readable string for the prompt."""
        lines: list[str] = []
        for i, t in enumerate(trades, 1):
            pnl = t.get("pnl", 0.0)
            label = "WIN" if pnl > 0 else "LOSS"
            lines.append(
                f"{i}. [{label}] Strategy: {t.get('strategy', 'N/A')} | "
                f"Action: {t.get('action', 'N/A')} | "
                f"PnL: {pnl:.4f} | "
                f"Regime: {t.get('regime', 'N/A')} | "
                f"Context: {t.get('context', '')}"
            )
        return "\n".join(lines)
