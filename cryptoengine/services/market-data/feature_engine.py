"""Feature Engineering Pipeline — FreqAI-style multi-dimensional feature generation.

Generates a large feature matrix by combining:
  - Multiple timeframes (1m, 5m, 15m, 1h, 4h)
  - Base technical indicators (EMA, RSI, ADX, ATR, BB, MACD)
  - Correlated pairs (e.g., ETHUSDT)
  - Shifted candles (lag-1, lag-2, lag-3)
  - Multiple indicator periods

Example dimensionality:
  4 TFs x 6 indicators x 2 pairs x 3 shifts x 3 periods = 432+ features

Configuration is driven by YAML; output is a pandas DataFrame ready for ML
model consumption.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog
import yaml

from indicators import (
    compute_adx,
    compute_atr,
    compute_bb_width,
    compute_ema,
    compute_macd,
    compute_mfi,
    compute_obv,
    compute_rsi,
    compute_volume_ratio,
)

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Default configuration (overridden by YAML)
# ---------------------------------------------------------------------------

DEFAULT_CONFIG: dict[str, Any] = {
    "timeframes": ["1m", "5m", "15m", "1h"],
    "symbols": ["BTCUSDT", "ETHUSDT"],
    "indicators": {
        "ema": {"periods": [9, 20, 50]},
        "rsi": {"periods": [7, 14, 21]},
        "adx": {"periods": [7, 14, 21]},
        "atr": {"periods": [7, 14, 21]},
        "bb_width": {"periods": [14, 20, 30]},
        "macd": {"configs": [{"fast": 12, "slow": 26, "signal": 9}]},
    },
    "shifts": [1, 2, 3],
    "derived_features": {
        "price_vs_ema": True,
        "rsi_divergence": True,
        "volume_ratio": True,
        "returns": [1, 3, 5, 10],
    },
}


class FeatureEngine:
    """Generates a wide feature DataFrame from multi-timeframe OHLCV data."""

    def __init__(self, config_path: str | None = None) -> None:
        if config_path and Path(config_path).exists():
            with open(config_path) as f:
                self.config: dict[str, Any] = yaml.safe_load(f)
            log.info("feature_config_loaded", path=config_path)
        else:
            self.config = DEFAULT_CONFIG
            log.info("feature_config_default")

        self.timeframes: list[str] = self.config.get("timeframes", DEFAULT_CONFIG["timeframes"])
        self.symbols: list[str] = self.config.get("symbols", DEFAULT_CONFIG["symbols"])
        self.indicator_cfg: dict[str, Any] = self.config.get("indicators", DEFAULT_CONFIG["indicators"])
        self.shifts: list[int] = self.config.get("shifts", DEFAULT_CONFIG["shifts"])
        self.derived_cfg: dict[str, Any] = self.config.get("derived_features", DEFAULT_CONFIG["derived_features"])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_features(
        self,
        ohlcv_data: dict[str, dict[str, pd.DataFrame]],
    ) -> pd.DataFrame:
        """Generate full feature matrix.

        Parameters
        ----------
        ohlcv_data : dict[symbol][timeframe] -> DataFrame
            Each DataFrame has columns: open, high, low, close, volume.
            The primary symbol's primary timeframe determines the index.

        Returns
        -------
        pd.DataFrame with one row per bar (primary TF) and 400+ feature columns.
        """
        primary_symbol = self.symbols[0]
        primary_tf = self.timeframes[0]

        if primary_symbol not in ohlcv_data or primary_tf not in ohlcv_data[primary_symbol]:
            raise ValueError(f"Primary data missing: {primary_symbol}/{primary_tf}")

        primary_df = ohlcv_data[primary_symbol][primary_tf].copy()
        features = pd.DataFrame(index=primary_df.index)
        feature_count = 0

        for symbol in self.symbols:
            if symbol not in ohlcv_data:
                log.warning("feature_symbol_missing", symbol=symbol)
                continue

            sym_tag = symbol.replace("USDT", "").lower()

            for tf in self.timeframes:
                if tf not in ohlcv_data[symbol]:
                    log.warning("feature_tf_missing", symbol=symbol, tf=tf)
                    continue

                df = ohlcv_data[symbol][tf].copy()
                prefix = f"{sym_tag}_{tf}"

                # --- Base indicators ---
                indicator_series = self._compute_indicators(df, prefix)
                for name, series in indicator_series.items():
                    # Align to primary index via forward-fill for higher TFs
                    aligned = series.reindex(primary_df.index, method="ffill")
                    features[name] = aligned
                    feature_count += 1

                    # --- Shifted features ---
                    for shift in self.shifts:
                        shift_name = f"{name}_lag{shift}"
                        features[shift_name] = aligned.shift(shift)
                        feature_count += 1

                # --- Derived features ---
                derived = self._compute_derived(df, prefix)
                for name, series in derived.items():
                    aligned = series.reindex(primary_df.index, method="ffill")
                    features[name] = aligned
                    feature_count += 1

        # Drop rows with all NaNs (warm-up period)
        features = features.dropna(how="all")

        log.info("features_built", total_columns=len(features.columns), rows=len(features))
        return features

    async def build_features_async(
        self,
        ohlcv_data: dict[str, dict[str, pd.DataFrame]],
    ) -> pd.DataFrame:
        """Async wrapper for CPU-bound feature computation."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.build_features, ohlcv_data)

    def get_feature_names(self) -> list[str]:
        """Return the expected feature column names (for model alignment)."""
        names: list[str] = []
        for symbol in self.symbols:
            sym_tag = symbol.replace("USDT", "").lower()
            for tf in self.timeframes:
                prefix = f"{sym_tag}_{tf}"
                for ind_name, ind_cfg in self.indicator_cfg.items():
                    if ind_name == "macd":
                        for cfg in ind_cfg.get("configs", []):
                            for suffix in ["macd", "signal", "hist"]:
                                base = f"{prefix}_{suffix}"
                                names.append(base)
                                for s in self.shifts:
                                    names.append(f"{base}_lag{s}")
                    else:
                        for period in ind_cfg.get("periods", [14]):
                            base = f"{prefix}_{ind_name}_{period}"
                            names.append(base)
                            for s in self.shifts:
                                names.append(f"{base}_lag{s}")
        return names

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def _compute_indicators(self, df: pd.DataFrame, prefix: str) -> dict[str, pd.Series]:
        """Compute all configured indicators for a single symbol/TF."""
        result: dict[str, pd.Series] = {}

        # EMA
        for period in self.indicator_cfg.get("ema", {}).get("periods", [20]):
            key = f"{prefix}_ema_{period}"
            result[key] = compute_ema(df, period=period)

        # RSI
        for period in self.indicator_cfg.get("rsi", {}).get("periods", [14]):
            key = f"{prefix}_rsi_{period}"
            result[key] = compute_rsi(df, period=period)

        # ADX
        for period in self.indicator_cfg.get("adx", {}).get("periods", [14]):
            key = f"{prefix}_adx_{period}"
            result[key] = compute_adx(df, period=period)

        # ATR
        for period in self.indicator_cfg.get("atr", {}).get("periods", [14]):
            key = f"{prefix}_atr_{period}"
            result[key] = compute_atr(df, period=period)

        # BB width
        for period in self.indicator_cfg.get("bb_width", {}).get("periods", [20]):
            key = f"{prefix}_bb_width_{period}"
            result[key] = compute_bb_width(df, period=period)

        # MACD
        for cfg in self.indicator_cfg.get("macd", {}).get("configs", []):
            macd_line, signal_line, histogram = compute_macd(
                df,
                fast=cfg.get("fast", 12),
                slow=cfg.get("slow", 26),
                signal=cfg.get("signal", 9),
            )
            result[f"{prefix}_macd"] = macd_line
            result[f"{prefix}_signal"] = signal_line
            result[f"{prefix}_hist"] = histogram

        return result

    def _compute_derived(self, df: pd.DataFrame, prefix: str) -> dict[str, pd.Series]:
        """Compute derived / cross-indicator features."""
        result: dict[str, pd.Series] = {}

        # Price vs EMA ratios
        if self.derived_cfg.get("price_vs_ema", True):
            for period in self.indicator_cfg.get("ema", {}).get("periods", [20]):
                ema = compute_ema(df, period=period)
                ratio = (df["close"] - ema) / ema.replace(0, np.nan)
                ratio.name = f"{prefix}_price_vs_ema_{period}"
                result[ratio.name] = ratio

        # RSI divergence (RSI rate of change)
        if self.derived_cfg.get("rsi_divergence", True):
            for period in self.indicator_cfg.get("rsi", {}).get("periods", [14]):
                rsi = compute_rsi(df, period=period)
                rsi_roc = rsi.diff()
                rsi_roc.name = f"{prefix}_rsi_roc_{period}"
                result[rsi_roc.name] = rsi_roc

        # Volume ratio
        if self.derived_cfg.get("volume_ratio", True):
            vol_ratio = compute_volume_ratio(df, period=20)
            vol_ratio.name = f"{prefix}_vol_ratio"
            result[vol_ratio.name] = vol_ratio

        # Log returns over N bars
        for n in self.derived_cfg.get("returns", [1, 3, 5]):
            ret = np.log(df["close"] / df["close"].shift(n))
            ret.name = f"{prefix}_logret_{n}"
            result[ret.name] = ret

        # OBV normalised rate of change
        obv = compute_obv(df)
        obv_roc = obv.pct_change(periods=5)
        obv_roc.name = f"{prefix}_obv_roc"
        result[obv_roc.name] = obv_roc

        # MFI
        try:
            mfi = compute_mfi(df, period=14)
            mfi.name = f"{prefix}_mfi_14"
            result[mfi.name] = mfi
        except Exception:
            pass  # MFI may fail if volume is zero

        return result
