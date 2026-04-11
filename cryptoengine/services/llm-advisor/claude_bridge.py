"""LLM bridge: Claude Code CLI primary, Anthropic SDK secondary, rule-based fallback.

Priority order:
1. Claude Code CLI (``claude -p``) — uses Max subscription, no per-call cost
2. Anthropic SDK — when ANTHROPIC_API_KEY is set
3. Rule-based deterministic analysis — always available
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from typing import Any

import structlog

from shared.log_events import *

log = structlog.get_logger(__name__)

_DEFAULT_TIMEOUT = 30
_MAX_RETRIES = 1
_RETRY_BACKOFF_BASE = 2.0

_SYSTEM_PROMPT = (
    "You are an elite quantitative crypto-trading analyst. "
    "Respond ONLY with valid JSON. Never include markdown fences, "
    "commentary, or any text outside the JSON object. "
    "Be precise, data-driven, and concise."
)


class ClaudeCodeBridge:
    """Invoke LLM analysis via Claude Code CLI, SDK, or rule-based fallback."""

    def __init__(
        self,
        cli_path: str = "claude",
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _MAX_RETRIES,
    ) -> None:
        self._cli_path = cli_path
        self._timeout = timeout
        self._max_retries = max_retries

        # Check CLI availability
        self._cli_available = shutil.which(self._cli_path) is not None
        if self._cli_available:
            log.info(LLM_ANALYSIS_START, message="Claude Code CLI 사용 가능", path=self._cli_path)
        else:
            log.warning(LLM_API_ERROR, message="Claude Code CLI 없음", path=self._cli_path)

        # SDK fallback
        self._api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self._client: Any = None
        if self._api_key:
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(api_key=self._api_key)
                log.info(LLM_ANALYSIS_START, message="Anthropic SDK 초기화 완료 (CLI 실패 시 폴백)")
            except ImportError:
                pass

        if not self._cli_available and not self._client:
            log.warning(LLM_API_ERROR, message="CLI·SDK 모두 불가, 룰 기반 폴백만 사용")

    async def invoke(
        self,
        task: str,
        context: dict[str, Any] | None = None,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """Invoke LLM analysis. Returns a dict or None on failure."""
        effective_timeout = timeout or self._timeout

        # 1st: Claude Code CLI
        if self._cli_available:
            result = await self._invoke_cli(task, context, effective_timeout)
            if result is not None:
                return result
            log.warning(LLM_API_ERROR, message="CLI 실패, 다음 방법 시도")

        # 2nd: Anthropic SDK
        if self._client:
            result = await self._invoke_sdk(task, context, effective_timeout)
            if result is not None:
                return result
            log.warning(LLM_API_ERROR, message="SDK 실패, 룰 기반 폴백으로 전환")

        # 3rd: Rule-based fallback
        return self._rule_based_analysis(context or {})

    async def _invoke_cli(
        self,
        task: str,
        context: dict[str, Any] | None,
        timeout: int,
    ) -> dict[str, Any] | None:
        """Call Claude Code CLI via subprocess."""
        prompt = self._build_prompt(task, context)

        for attempt in range(1, self._max_retries + 1):
            try:
                proc = await asyncio.create_subprocess_exec(
                    self._cli_path, "-p", prompt,
                    "--model", "opus",
                    "--output-format", "json",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    preexec_fn=os.setsid,  # new process group for clean kill
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout
                )

                if proc.returncode != 0:
                    err_msg = stderr.decode("utf-8", errors="replace").strip()
                    log.warning(
                        LLM_API_ERROR,
                        message="CLI 비정상 종료",
                        returncode=proc.returncode,
                        stderr=err_msg[:200],
                        attempt=attempt,
                    )
                    if attempt < self._max_retries:
                        await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)
                    continue

                raw = stdout.decode("utf-8", errors="replace").strip()
                if not raw:
                    log.warning(LLM_API_ERROR, message="CLI 빈 출력", attempt=attempt)
                    continue

                # Parse the JSON envelope from --output-format json
                envelope = json.loads(raw)
                result_text = envelope.get("result", "")
                return self._parse_response(result_text)

            except asyncio.TimeoutError:
                log.warning(LLM_API_ERROR, message="CLI 타임아웃", attempt=attempt, timeout=timeout)
                # Force-kill the subprocess and all children
                try:
                    proc.kill()  # type: ignore[possibly-undefined]
                    await proc.wait()
                except Exception:
                    pass
                # Also kill by PID as a fallback (claude may spawn children)
                try:
                    import os as _os
                    import signal as _signal
                    _os.killpg(_os.getpgid(proc.pid), _signal.SIGKILL)  # type: ignore[possibly-undefined]
                except Exception:
                    pass
            except json.JSONDecodeError as exc:
                log.warning(LLM_API_ERROR, message="CLI JSON 파싱 실패", error=str(exc), attempt=attempt)
            except Exception as exc:
                log.warning(LLM_API_ERROR, message="CLI 예기치 않은 오류", error=str(exc), attempt=attempt)

            if attempt < self._max_retries:
                await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)

        return None

    async def _invoke_sdk(
        self,
        task: str,
        context: dict[str, Any] | None,
        timeout: int,
    ) -> dict[str, Any] | None:
        """Call Anthropic API via SDK."""
        import anthropic

        prompt = self._build_prompt(task, context)
        for attempt in range(1, self._max_retries + 1):
            try:
                msg = await asyncio.wait_for(
                    self._client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=2048,
                        messages=[{"role": "user", "content": prompt}],
                    ),
                    timeout=timeout,
                )
                raw = msg.content[0].text if msg.content else ""
                return self._parse_response(raw)
            except asyncio.TimeoutError:
                log.warning(LLM_API_ERROR, message="SDK 타임아웃", attempt=attempt)
            except anthropic.RateLimitError:
                log.warning(LLM_API_ERROR, message="SDK 레이트 리밋", attempt=attempt)
            except anthropic.APIError as exc:
                log.error(LLM_API_ERROR, message="SDK API 오류", error=str(exc))
                return None
            except Exception as exc:
                log.error(LLM_API_ERROR, message="SDK 예기치 않은 오류", error=str(exc))
                return None

            if attempt < self._max_retries:
                await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)
        return None

    def _rule_based_analysis(self, context: dict[str, Any]) -> dict[str, Any]:
        """Generate rule-based analysis from market context."""
        regime = context.get("regime", "ranging")
        _fr = context.get("funding_rate", 0.0001)
        funding_rate = float(_fr.get("rate", _fr.get("lastFundingRate", 0.0001)) if isinstance(_fr, dict) else _fr)
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
                "funding_arb": 0.7 if funding_rate > 0.0002 else 0.5,
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
