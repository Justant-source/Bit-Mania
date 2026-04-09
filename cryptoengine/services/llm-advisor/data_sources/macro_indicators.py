"""Macro indicators data source: DXY, US 10Y yield, CPI, FOMC, Global M2.

Primary: FRED API (CPI, Fed rate, real yield, M2) + yfinance (DXY, US10Y).
FOMC dates: hardcoded 2026 schedule + days-to-next calculation.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from .base import BaseDataSource

logger = structlog.get_logger(__name__)

# --------------------------------------------------------------------------
# FOMC schedule (updated annually)
# --------------------------------------------------------------------------

FOMC_DATES_2026 = [
    "2026-01-28",
    "2026-03-18",
    "2026-04-29",
    "2026-06-17",
    "2026-07-29",
    "2026-09-16",
    "2026-11-04",
    "2026-12-16",
]

# --------------------------------------------------------------------------
# FRED series IDs
# --------------------------------------------------------------------------

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "cpi": "CPIAUCSL",           # CPI All Urban Consumers (monthly)
    "fed_rate": "DFF",           # Effective Fed Funds Rate (daily)
    "real_yield_10y": "DFII10",  # 10Y TIPS breakeven real yield (daily)
    "global_m2": "MABMM301USM189S",  # US M2 money supply (monthly)
}

YFINANCE_SYMBOLS = {
    "dxy": "DX-Y.NYB",  # US Dollar Index
    "us10y": "^TNX",    # US 10Y Treasury yield
}

# --------------------------------------------------------------------------
# Trend classification helpers
# --------------------------------------------------------------------------

def _dxy_trend(change_pct: float) -> str:
    """Classify DXY 7d % change into named trend."""
    if change_pct > 1.0:
        return "strong_up"
    if change_pct > 0.3:
        return "up"
    if change_pct >= -0.3:
        return "flat"
    if change_pct >= -1.0:
        return "down"
    return "strong_down"


def _next_fomc(ref: datetime | None = None) -> tuple[str | None, int | None]:
    """Return (next_fomc_date_str, days_to_fomc) from today."""
    today = (ref or datetime.now(timezone.utc)).date()
    for ds in FOMC_DATES_2026:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        if d >= today:
            days = (d - today).days
            return ds, days
    return None, None


def _m2_trend(values: list[float]) -> str:
    """Simple 3-month direction: expanding / contracting / flat."""
    if len(values) < 2:
        return "unknown"
    recent = values[-1]
    prior = values[-4] if len(values) >= 4 else values[0]
    pct = (recent - prior) / abs(prior) * 100 if prior else 0
    if pct > 0.5:
        return "expanding"
    if pct < -0.5:
        return "contracting"
    return "flat"


# --------------------------------------------------------------------------
# FRED fetch helper (async)
# --------------------------------------------------------------------------

async def _fetch_fred_series(
    session,
    series_id: str,
    api_key: str,
    limit: int = 12,
) -> list[dict]:
    """Fetch the most recent `limit` observations from FRED."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": str(limit),
    }
    async with session.get(FRED_BASE_URL, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"FRED {series_id} HTTP {resp.status}")
        data = await resp.json(content_type=None)
    return data.get("observations", [])


def _fred_latest_value(obs: list[dict]) -> float | None:
    """Return the most recent non-'.' value from FRED observations."""
    for o in obs:  # already sorted desc
        v = o.get("value", ".")
        if v != ".":
            try:
                return float(v)
            except ValueError:
                pass
    return None


def _fred_values_list(obs: list[dict]) -> list[float]:
    """Extract all valid float values from FRED observations (newest first)."""
    result: list[float] = []
    for o in obs:
        v = o.get("value", ".")
        if v != ".":
            try:
                result.append(float(v))
            except ValueError:
                pass
    return result


# --------------------------------------------------------------------------
# yfinance fetch helper (sync → executor)
# --------------------------------------------------------------------------

def _yf_download_sync(symbols: list[str], period: str = "15d", interval: str = "1d") -> dict[str, list[float]]:
    """Synchronous yfinance download. Run in executor to avoid blocking."""
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance is not installed")

    result: dict[str, list[float]] = {}
    for sym in symbols:
        try:
            df = yf.download(sym, period=period, interval=interval, progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                close_col = df["Close"]
                # yfinance ≥0.2 may return a DataFrame (MultiIndex cols) for
                # single-ticker downloads; squeeze() collapses it to a Series.
                if hasattr(close_col, "squeeze"):
                    close_col = close_col.squeeze()
                closes = close_col.dropna().values.tolist()
                result[sym] = [float(v) for v in closes]
            else:
                result[sym] = []
        except Exception as exc:
            logger.warning("yfinance_symbol_failed", symbol=sym, error=str(exc))
            result[sym] = []
    return result


# --------------------------------------------------------------------------
# Main data source class
# --------------------------------------------------------------------------

class MacroIndicatorsSource(BaseDataSource):
    """Macro context: DXY, US 10Y yield, CPI, FOMC, Global M2."""

    SOURCE_NAME = "macro_indicators"
    CACHE_KEY_PREFIX = "datasource"
    CACHE_TTL_SECONDS = 1800  # 30 minutes

    # --------------------------------------------------------------------------
    # fetch_raw
    # --------------------------------------------------------------------------

    async def fetch_raw(self) -> dict:
        """Fetch FRED + yfinance data in parallel."""
        fred_api_key = os.environ.get("FRED_API_KEY", "")

        fred_task = asyncio.create_task(self._fetch_fred_all(fred_api_key))
        yf_task = asyncio.create_task(self._fetch_yfinance_all())

        fred_data, yf_data = await asyncio.gather(fred_task, yf_task, return_exceptions=True)

        if isinstance(fred_data, Exception):
            logger.warning("fred_fetch_error", error=str(fred_data))
            fred_data = {}
        if isinstance(yf_data, Exception):
            logger.warning("yfinance_fetch_error", error=str(yf_data))
            yf_data = {}

        return {
            "_source": "FRED+yfinance",
            "fred": fred_data,
            "yfinance": yf_data,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_fred_all(self, api_key: str) -> dict[str, Any]:
        """Fetch all configured FRED series."""
        if not api_key:
            logger.warning("fred_api_key_missing", hint="Set FRED_API_KEY env var")
            return {}

        session = await self._ensure_session()
        tasks = {
            name: asyncio.create_task(
                _fetch_fred_series(session, series_id, api_key, limit=13)
            )
            for name, series_id in FRED_SERIES.items()
        }

        results: dict[str, Any] = {}
        for name, task in tasks.items():
            try:
                results[name] = await task
            except Exception as exc:
                logger.warning("fred_series_failed", series=name, error=str(exc))
                results[name] = []

        return results

    async def _fetch_yfinance_all(self) -> dict[str, list[float]]:
        """Fetch DXY and US10Y via yfinance in a thread executor."""
        loop = asyncio.get_event_loop()
        symbols = list(YFINANCE_SYMBOLS.values())
        raw_by_symbol: dict[str, list[float]] = await loop.run_in_executor(
            None, _yf_download_sync, symbols
        )
        # Re-key by our friendly name
        result: dict[str, list[float]] = {}
        for name, sym in YFINANCE_SYMBOLS.items():
            result[name] = raw_by_symbol.get(sym, [])
        return result

    # --------------------------------------------------------------------------
    # summarize
    # --------------------------------------------------------------------------

    def summarize(self, raw: dict) -> dict:
        fred: dict[str, list[dict]] = raw.get("fred", {})
        yf: dict[str, list[float]] = raw.get("yfinance", {})
        fetched_at = raw.get("_fetched_at", datetime.now(timezone.utc).isoformat())

        # ── DXY ──────────────────────────────────────────────────────────────
        dxy_series = yf.get("dxy", [])
        dxy_current = round(dxy_series[-1], 4) if dxy_series else None
        dxy_7d_ago = dxy_series[-8] if len(dxy_series) >= 8 else (dxy_series[0] if dxy_series else None)
        dxy_7d_change_pct: float | None = None
        dxy_trend = "unknown"
        if dxy_current is not None and dxy_7d_ago:
            dxy_7d_change_pct = round((dxy_current - dxy_7d_ago) / dxy_7d_ago * 100, 3)
            dxy_trend = _dxy_trend(dxy_7d_change_pct)

        # ── US 10Y yield ─────────────────────────────────────────────────────
        us10y_series = yf.get("us10y", [])
        us10y_current = round(us10y_series[-1], 4) if us10y_series else None
        us10y_7d_ago = us10y_series[-8] if len(us10y_series) >= 8 else (us10y_series[0] if us10y_series else None)
        us10y_7d_change_bps: float | None = None
        if us10y_current is not None and us10y_7d_ago is not None:
            us10y_7d_change_bps = round((us10y_current - us10y_7d_ago) * 100, 1)

        # ── Real 10Y yield (TIPS) ─────────────────────────────────────────────
        real_yield_obs = fred.get("real_yield_10y", [])
        real_yield_10y = _fred_latest_value(real_yield_obs)
        if real_yield_10y is not None:
            real_yield_10y = round(real_yield_10y, 4)

        # ── CPI ───────────────────────────────────────────────────────────────
        cpi_obs = fred.get("cpi", [])
        cpi_values = _fred_values_list(cpi_obs)  # newest first
        cpi_yoy_last: float | None = None
        cpi_yoy_prev: float | None = None
        cpi_surprise = "unknown"

        # CPI YoY = (current / 12-months-ago - 1) * 100
        # FRED returns monthly; obs are newest-first, so [0]=most recent, [12]=year ago
        if len(cpi_values) >= 13:
            cpi_yoy_last = round((cpi_values[0] / cpi_values[12] - 1) * 100, 2)
        if len(cpi_values) >= 14:
            cpi_yoy_prev = round((cpi_values[1] / cpi_values[13] - 1) * 100, 2)

        if cpi_yoy_last is not None and cpi_yoy_prev is not None:
            if cpi_yoy_last < cpi_yoy_prev - 0.05:
                cpi_surprise = "below_expected"
            elif cpi_yoy_last > cpi_yoy_prev + 0.05:
                cpi_surprise = "above_expected"
            else:
                cpi_surprise = "in_line"

        # ── Fed Funds Rate ────────────────────────────────────────────────────
        fed_obs = fred.get("fed_rate", [])
        fed_rate_current = _fred_latest_value(fed_obs)
        if fed_rate_current is not None:
            fed_rate_current = round(fed_rate_current, 2)

        # ── FOMC ──────────────────────────────────────────────────────────────
        next_fomc_date, days_to_fomc = _next_fomc()

        # ── Rate cut probability (basic estimate) ────────────────────────────
        # Without CME FedWatch: use fed_rate vs 10Y as rough signal
        rate_cut_prob: float | None = None
        if fed_rate_current is not None and us10y_current is not None:
            spread = fed_rate_current - us10y_current  # inverted = market pricing cuts
            if spread > 0.5:
                rate_cut_prob = round(min(0.90, 0.40 + spread * 0.25), 2)
            elif spread > 0:
                rate_cut_prob = round(0.35 + spread * 0.10, 2)
            else:
                rate_cut_prob = round(max(0.05, 0.30 + spread * 0.10), 2)

        # ── Global M2 ─────────────────────────────────────────────────────────
        m2_obs = fred.get("global_m2", [])
        m2_values = _fred_values_list(m2_obs)
        global_m2_trend = _m2_trend(m2_values)

        # ── Confidence ───────────────────────────────────────────────────────
        data_points = sum(
            1 for v in [dxy_current, us10y_current, cpi_yoy_last, fed_rate_current]
            if v is not None
        )
        confidence = round(0.50 + data_points * 0.12, 2)  # 0.50 – 0.98

        # ── Narrative ─────────────────────────────────────────────────────────
        narrative = _build_narrative(
            dxy_change=dxy_7d_change_pct,
            dxy_trend=dxy_trend,
            real_yield=real_yield_10y,
            cpi_surprise=cpi_surprise,
            days_to_fomc=days_to_fomc,
        )

        return {
            "as_of": fetched_at,
            "source": "FRED + yfinance",
            "confidence": confidence,
            # DXY
            "dxy": dxy_current,
            "dxy_7d_change_pct": dxy_7d_change_pct,
            "dxy_trend": dxy_trend,
            # US 10Y
            "us10y_yield": us10y_current,
            "us10y_7d_change_bps": us10y_7d_change_bps,
            "real_yield_10y": real_yield_10y,
            # CPI
            "cpi_yoy_last": cpi_yoy_last,
            "cpi_yoy_prev": cpi_yoy_prev,
            "cpi_surprise": cpi_surprise,
            # FOMC
            "next_fomc_date": next_fomc_date,
            "days_to_fomc": days_to_fomc,
            # Fed rate
            "fed_rate_current": fed_rate_current,
            "rate_cut_probability_next_meeting": rate_cut_prob,
            # M2
            "global_m2_trend": global_m2_trend,
            # Narrative
            "narrative": narrative,
        }


# --------------------------------------------------------------------------
# Narrative builder
# --------------------------------------------------------------------------

def _build_narrative(
    dxy_change: float | None,
    dxy_trend: str,
    real_yield: float | None,
    cpi_surprise: str,
    days_to_fomc: int | None,
) -> str:
    """Build a concise Korean + English narrative for the LLM prompt."""
    parts: list[str] = []

    # DXY
    if dxy_change is not None:
        direction = "약세" if dxy_change < 0 else "강세"
        parts.append(f"DXY 주간 {dxy_change:+.1f}% {direction}")

    # Real yield
    if real_yield is not None:
        ry_desc = "하락" if real_yield < 1.5 else ("상승" if real_yield > 2.5 else "중립")
        parts.append(f"실질금리 {real_yield:.2f}% {ry_desc}")

    # CPI
    cpi_map = {
        "below_expected": "CPI 컨센서스 하회",
        "above_expected": "CPI 컨센서스 상회",
        "in_line": "CPI 예상 부합",
    }
    if cpi_surprise in cpi_map:
        parts.append(cpi_map[cpi_surprise])

    # Macro signal
    if dxy_change is not None and real_yield is not None:
        if dxy_change < 0 and cpi_surprise in ("below_expected", "in_line"):
            parts.append("→ 위험자산 우호적 매크로")
        elif dxy_change > 0 or cpi_surprise == "above_expected":
            parts.append("→ 위험자산 역풍")

    # FOMC countdown
    if days_to_fomc is not None:
        parts.append(f"FOMC까지 {days_to_fomc}일")

    return ". ".join(parts) + "." if parts else "매크로 데이터 없음."
