"""Claude Code CLI bridge for LLM invocations.

Calls the Claude Code CLI as a subprocess and parses JSON output.
"""

from __future__ import annotations

import asyncio
import json
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
    """Invoke Claude Code CLI and return parsed responses."""

    def __init__(
        self,
        cli_path: str = "claude",
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _MAX_RETRIES,
    ) -> None:
        self._cli_path = cli_path
        self._timeout = timeout
        self._max_retries = max_retries

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def invoke(
        self,
        task: str,
        context: dict[str, Any] | None = None,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """Build a prompt, call Claude Code, and return parsed JSON.

        Returns ``None`` on unrecoverable failure so that callers can
        continue without LLM output.
        """
        prompt = self._build_prompt(task, context)
        effective_timeout = timeout or self._timeout

        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                raw = await self._call_cli(prompt, effective_timeout)
                return self._parse_response(raw)
            except asyncio.TimeoutError:
                last_error = TimeoutError(
                    f"Claude Code timed out after {effective_timeout}s"
                )
                log.warning(
                    "claude_bridge_timeout",
                    attempt=attempt,
                    timeout=effective_timeout,
                )
            except _TransientError as exc:
                last_error = exc
                log.warning(
                    "claude_bridge_transient_error",
                    attempt=attempt,
                    error=str(exc),
                )
            except Exception as exc:
                log.error("claude_bridge_fatal_error", error=str(exc))
                return None

            if attempt < self._max_retries:
                backoff = _RETRY_BACKOFF_BASE ** attempt
                await asyncio.sleep(backoff)

        log.error(
            "claude_bridge_all_retries_exhausted",
            retries=self._max_retries,
            last_error=str(last_error),
        )
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_prompt(task: str, context: dict[str, Any] | None) -> str:
        """Construct the full prompt sent to Claude Code."""
        parts: list[str] = [_SYSTEM_PROMPT, "", f"### Task\n{task}"]
        if context:
            parts.append(
                f"\n### Market Context\n```json\n"
                f"{json.dumps(context, indent=2, default=str)}\n```"
            )
        parts.append(
            "\nRespond with a single JSON object. No markdown, no commentary."
        )
        return "\n".join(parts)

    async def _call_cli(self, prompt: str, timeout: int) -> str:
        """Execute the Claude Code CLI and return raw stdout."""
        cmd = [self._cli_path, "-p", prompt, "--output-format", "json"]
        t0 = time.monotonic()

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            raise

        elapsed = time.monotonic() - t0
        log.debug("claude_cli_completed", elapsed_s=round(elapsed, 2))

        if proc.returncode != 0:
            err_msg = (stderr or b"").decode(errors="replace").strip()
            if _is_transient(err_msg):
                raise _TransientError(err_msg)
            raise RuntimeError(f"Claude Code exited {proc.returncode}: {err_msg}")

        return (stdout or b"").decode(errors="replace")

    @staticmethod
    def _parse_response(raw: str) -> dict[str, Any]:
        """Extract a JSON dict from Claude Code's JSON-mode output.

        Claude Code ``--output-format json`` wraps the assistant message
        inside a JSON envelope.  We try the full payload first, then fall
        back to extracting the ``result`` or ``content`` field, and
        finally attempt to find an embedded JSON object.
        """
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            pass
        else:
            # Direct dict output (simple case)
            if isinstance(envelope, dict):
                # Claude Code JSON envelope typically has a "result" key
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
                # If envelope itself looks like the answer, return it
                return envelope

        # Last resort: find first { … } block
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                pass

        raise ValueError("Could not extract JSON from Claude Code output")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

class _TransientError(Exception):
    """Raised for errors that are likely transient and worth retrying."""


def _is_transient(error_text: str) -> bool:
    """Heuristic: decide if an error is transient."""
    transient_markers = (
        "rate limit",
        "overloaded",
        "timeout",
        "503",
        "502",
        "429",
        "temporarily unavailable",
        "connection reset",
    )
    lower = error_text.lower()
    return any(marker in lower for marker in transient_markers)
