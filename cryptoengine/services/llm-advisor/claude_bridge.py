"""LLM bridge: Anthropic SDK primary, rule-based fallback.

Uses the Anthropic Python SDK when ANTHROPIC_API_KEY is available.
Falls back to a deterministic rule-based analysis so dashboards always
have data even without an API key.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_DEFAULT_TIMEOUT = 120
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2.0

_SYSTEM_PROMPT = (
    "You are an elite quantitative crypto-trading analyst. "
    "Respond ONLY with valid JSON. Never include markdown fences, "
    "commentary, or any text outside the JSON object. "
    "Be precise, data-driven, and concise."
)


class ClaudeCodeBridge:
    """Invoke LLM analysis via Anthropic SDK or fall back to rule-based analysis."""

    def __init__(
        self,
        cli_path: str = "claude",
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _MAX_RETRIES,
    ) -> None:
        self._cli_path = cli_path
        self._timeout = timeout
        self._max_retries = max_retries
        self._api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self._client: Any = None
        if self._api_key:
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(api_key=self._api_key)
                log.info("claude_bridge_using_anthropic_sdk")
            except ImportError:
                log.warning("anthropic_package_not_installed_using_fallback")
        else:
            log.warning("anthropic_api_key_missing_using_rule_based_fallback")

    async def invoke(
        self,
        task: str,
        context: dict[str, Any] | None = None,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """Invoke LLM analysis. Returns a dict or None on failure."""
        if self._client:
            result = await self._invoke_sdk(task, context, timeout or self._timeout)
            if result is not None:
                return result
            log.warning("sdk_failed_falling_back_to_rule_based")

        return self._rule_based_analysis(context or {})

    async def _invoke_sdk(
        self,
        task: str,
        context: dict[str, Any] | None,
        timeout: int,
    ) -> dict[str, Any] | None:
        """Call Anthropic API via SDK."""
        import asyncio
        import anthropic

        prompt = self._build_prompt(task, context)
        for attempt in range(1, self._max_retries + 1):
            try:
                msg = await asyncio.wait_for(
                    self._client.messages.create(
                        model="claude-opus-4-6",
                        max_tokens=2048,
                        messages=[{"role": "user", "content": prompt}],
                    ),
                    timeout=timeout,
                )
                raw = msg.content[0].text if msg.content else ""
                return self._parse_response(raw)
            except asyncio.TimeoutError:
                log.warning("sdk_timeout", attempt=attempt)
            except anthropic.RateLimitError:
                log.warning("sdk_rate_limit", attempt=attempt)
            except anthropic.APIError as exc:
                log.error("sdk_api_error", error=str(exc))
                return None
            except Exception as exc:
                log.error("sdk_unexpected_error", error=str(exc))
                return None

            if attempt < self._max_retries:
                import asyncio as _asyncio
                await _asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)
        return None

    def _rule_based_analysis(self, context: dict[str, Any]) -> dict[str, Any]:
        """Generate rule-based analysis from market context."""
        regime = context.get("regime", "ranging")
        funding_rate = float(context.get("funding_rate", 0.0001))
        btc_price = float(context.get("btc_price", 65000) if not isinstance(context.get("btc_price"), dict) else context["btc_price"].get("last", 65000))
        oi_change = float(context.get("oi_change_pct", 0.0))

        # Determine rating based on regime and funding rate
        if regime in ("trending_up",) and funding_rate < 0.0003:
            rating, confidence = "buy", 0.65
        elif regime in ("trending_down",) or funding_rate > 0.001:
            rating, confidence = "sell", 0.60
        elif funding_rate > 0.0005:
            rating, confidence = "hold", 0.70
        else:
            rating, confidence = "hold", 0.55

        risk_flags = []
        if funding_rate > 0.001:
            risk_flags.append("high_funding_rate")
        if abs(oi_change) > 5:
            risk_flags.append("significant_oi_change")

        return {
            "rating": rating,
            "confidence": confidence,
            "regime_assessment": regime,
            "reasoning": (
                f"Rule-based analysis: regime={regime}, "
                f"funding_rate={funding_rate:.4%}, btc_price=${btc_price:,.0f}. "
                f"Note: Enable ANTHROPIC_API_KEY for full AI analysis."
            ),
            "technical_summary": f"BTC at ${btc_price:,.0f}. Market regime: {regime}.",
            "sentiment_summary": f"Funding rate {funding_rate:.4%} suggests {'cautious' if funding_rate > 0.0005 else 'neutral'} sentiment.",
            "bull_summary": "Potential upside if funding normalises and regime shifts trending_up.",
            "bear_summary": f"Downside risk if regime remains {regime} with elevated funding.",
            "debate_conclusion": f"Neutral stance recommended given current {regime} regime.",
            "risk_assessment": "Risk managed via kill switch and 2x leverage cap.",
            "weight_adjustments": {
                "funding_arb": 0.5 if funding_rate > 0.0002 else 0.3,
                "grid_trading": 0.3 if regime == "ranging" else 0.1,
                "adaptive_dca": 0.2,
            },
            "risk_flags": risk_flags,
            "source": "rule_based_fallback",
        }

    @staticmethod
    def _build_prompt(task: str, context: dict[str, Any] | None) -> str:
        parts: list[str] = [_SYSTEM_PROMPT, "", f"### Task\n{task}"]
        if context:
            parts.append(
                f"\n### Market Context\n```json\n"
                f"{json.dumps(context, indent=2, default=str)}\n```"
            )
        parts.append("\nRespond with a single JSON object. No markdown, no commentary.")
        return "\n".join(parts)

    @staticmethod
    def _parse_response(raw: str) -> dict[str, Any]:
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(envelope, dict):
                for key in ("result", "content", "text"):
                    if key in envelope:
                        inner = envelope[key]
                        if isinstance(inner, dict):
                            return inner
                        if isinstance(inner, str):
                            try:
                                parsed = json.loads(inner)
                                if isinstance(parsed, dict):
                                    return parsed
                            except json.JSONDecodeError:
                                pass
                return envelope

        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                pass

        raise ValueError("Could not extract JSON from response")
