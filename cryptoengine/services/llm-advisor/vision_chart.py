"""Vision Chart Analyzer — generate and analyze candlestick charts.

Uses matplotlib/mplfinance to render OHLCV data as candlestick PNGs,
then sends the image to Claude Code for visual pattern recognition.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

_CHART_DIR = Path(tempfile.gettempdir()) / "cryptoengine_charts"


class ChartAnalyzer:
    """Generate OHLCV candlestick charts and analyze them via Claude Code."""

    def __init__(self, cli_path: str = "claude") -> None:
        self._cli_path = cli_path
        _CHART_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Chart generation
    # ------------------------------------------------------------------

    async def generate_chart(
        self,
        ohlcv_data: pd.DataFrame,
        indicators: dict[str, pd.Series] | None = None,
        *,
        filename: str | None = None,
    ) -> Path:
        """Render OHLCV data as a candlestick chart and save as PNG.

        Parameters
        ----------
        ohlcv_data:
            DataFrame with columns ``open, high, low, close, volume`` and
            a ``DatetimeIndex``.
        indicators:
            Optional dict mapping indicator names to ``pd.Series`` to
            overlay on the chart (e.g. SMA_20, EMA_50).
        filename:
            Output filename.  Defaults to ``chart_<timestamp>.png``.

        Returns
        -------
        Path to the saved PNG.
        """
        import mplfinance as mpf

        if filename is None:
            ts = pd.Timestamp.now("UTC").strftime("%Y%m%d_%H%M%S")
            filename = f"chart_{ts}.png"

        output_path = _CHART_DIR / filename

        # Normalise column names to lowercase
        df = ohlcv_data.copy()
        df.columns = [c.lower() for c in df.columns]
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if "timestamp" in df.columns:
                df.index = pd.to_datetime(df["timestamp"])
            elif "date" in df.columns:
                df.index = pd.to_datetime(df["date"])
            else:
                df.index = pd.to_datetime(df.index)

        # Build additional plot overlays
        add_plots: list[Any] = []
        if indicators:
            for name, series in indicators.items():
                add_plots.append(
                    mpf.make_addplot(series, label=name)
                )

        # Render chart
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: mpf.plot(
                df,
                type="candle",
                style="charles",
                volume=True,
                title="BTC/USDT",
                addplot=add_plots if add_plots else None,
                savefig=str(output_path),
                figscale=1.4,
                figratio=(16, 9),
            ),
        )

        log.info("chart_generated", path=str(output_path))
        return output_path

    # ------------------------------------------------------------------
    # Chart analysis via Claude Code
    # ------------------------------------------------------------------

    async def analyze_chart(
        self,
        image_path: str | Path,
    ) -> dict[str, Any]:
        """Send a chart image to Claude Code for visual pattern analysis.

        Returns a dict with keys:
        - ``patterns``: list of detected chart patterns
        - ``support_levels``: list of price levels
        - ``resistance_levels``: list of price levels
        - ``trend``: overall trend description
        """
        image_path = Path(image_path)
        if not image_path.exists():
            log.error("chart_image_not_found", path=str(image_path))
            return self._empty_analysis()

        # Read and base64-encode the image for the prompt
        image_bytes = image_path.read_bytes()
        b64 = base64.b64encode(image_bytes).decode()

        prompt = (
            "You are an expert technical chart analyst. "
            "Analyze the attached candlestick chart image and respond "
            "with a JSON object containing:\n"
            '- "patterns": list of detected chart patterns '
            '  (e.g. "double bottom", "head and shoulders", "ascending triangle")\n'
            '- "support_levels": list of approximate price support levels (numbers)\n'
            '- "resistance_levels": list of approximate price resistance levels (numbers)\n'
            '- "trend": overall trend assessment '
            '  ("strong_uptrend"|"uptrend"|"sideways"|"downtrend"|"strong_downtrend")\n'
            '- "confidence": confidence in analysis (0.0-1.0)\n'
            '- "notes": brief additional observations\n\n'
            f"[Image data (base64 PNG, {len(image_bytes)} bytes) is attached]\n"
            f"data:image/png;base64,{b64}"
        )

        cmd = [self._cli_path, "-p", prompt, "--output-format", "json"]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=120
            )

            if proc.returncode != 0:
                err = (stderr or b"").decode(errors="replace").strip()
                log.warning("chart_analysis_cli_error", error=err)
                return self._empty_analysis()

            raw = (stdout or b"").decode(errors="replace")
            return self._parse_analysis(raw)

        except asyncio.TimeoutError:
            log.warning("chart_analysis_timeout")
            return self._empty_analysis()
        except Exception:
            log.exception("chart_analysis_error")
            return self._empty_analysis()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_analysis(raw: str) -> dict[str, Any]:
        """Extract analysis JSON from Claude Code output."""
        try:
            envelope = json.loads(raw)
            if isinstance(envelope, dict):
                for key in ("result", "content", "text"):
                    if key in envelope and isinstance(envelope[key], str):
                        try:
                            inner = json.loads(envelope[key])
                            if isinstance(inner, dict):
                                return inner
                        except json.JSONDecodeError:
                            pass
                return envelope
        except json.JSONDecodeError:
            pass

        # Fallback: find embedded JSON
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                pass

        log.warning("chart_analysis_parse_failed")
        return ChartAnalyzer._empty_analysis()

    @staticmethod
    def _empty_analysis() -> dict[str, Any]:
        return {
            "patterns": [],
            "support_levels": [],
            "resistance_levels": [],
            "trend": "unknown",
            "confidence": 0.0,
            "notes": "Analysis unavailable.",
        }
