"""Derivatives data source — funding rates, open interest, long/short ratio.

Primary:   Binance FAPI public endpoints (no key required)
Secondary: Bybit v5 public endpoints (no key required, cross-check)

Liquidation heat-map data is NOT available from free public APIs.
Squeeze risk is estimated via heuristic from funding percentile + OI + long/short ratio.

Cache TTL: 900 seconds (15 min) — derivatives data changes frequently.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import structlog

from .base import BaseDataSource, SourceHealth
from .failure_detection import FailureDetector

logger = structlog.get_logger(__name__)

_BINANCE_FAPI = "https://fapi.binance.com"
_BYBIT_V5 = "https://api.bybit.com/v5"

SANITY_RANGES: dict[str, tuple[float, float]] = {
    "funding_rate_weighted_avg": (-0.01, 0.01),   # -1% to +1% per period
    "open_interest_usd_bn": (1.0, 200.0),
    "long_short_ratio": (0.1, 10.0),
}


# ---------------------------------------------------------------------------
# Threshold helpers
# ---------------------------------------------------------------------------

def _classify_funding_state(weighted_avg: float) -> str:
    """Map weighted-average funding rate to a state label."""
    if weighted_avg < -0.02:
        return "extreme_short"
    if weighted_avg < -0.005:
        return "elevated_short"
    if weighted_avg < 0.005:
        return "neutral"
    if weighted_avg < 0.02:
        return "elevated_long"
    return "extreme_long"


def _funding_percentile_from_history(current_rate: float, history: list[float]) -> int:
    """Compute 30-day percentile of current funding rate against historical rates."""
    if not history:
        return _funding_percentile_approx(current_rate)
    n_below = sum(1 for r in history if r < current_rate)
    return int(round(n_below / len(history) * 100))


def _funding_percentile_approx(rate: float) -> int:
    """Piecewise approximation when historical data is unavailable."""
    if rate <= -0.02:
        return 2
    if rate <= -0.005:
        return round(2 + (rate + 0.02) / 0.015 * 13)
    if rate <= 0.005:
        return round(15 + (rate + 0.005) / 0.01 * 35)
    if rate <= 0.02:
        return round(50 + (rate - 0.005) / 0.015 * 40)
    return min(99, round(90 + (rate - 0.02) * 300))


def _estimate_squeeze_risk(
    funding_pctl_30d: int,
    oi_24h_change_pct: float | None,
    long_ratio: float | None,
) -> str:
    """Heuristic squeeze risk estimate.

    No liquidation heat-map available via free APIs.
    Uses funding rate percentile, OI change, and long/short ratio.
    """
    oi_chg = oi_24h_change_pct or 0.0
    long_r = long_ratio or 1.0

    if funding_pctl_30d > 85 and oi_chg > 5 and long_r > 1.3:
        return "long_squeeze_elevated"
    if funding_pctl_30d > 70 and long_r > 1.15:
        return "long_squeeze_low"
    if funding_pctl_30d < 15 and oi_chg > 5 and long_r < 0.8:
        return "short_squeeze_elevated"
    if funding_pctl_30d < 30 and long_r < 0.85:
        return "short_squeeze_low"
    return "none"


# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------

class DerivativesSource(BaseDataSource):
    """Fetch and summarise BTC derivatives data from Binance FAPI + Bybit public APIs.

    Provides funding rates, open interest, and long/short ratio.
    Liquidation heat-map data is not available in free APIs; squeeze risk is
    estimated via heuristic and clearly labeled as such.
    """

    SOURCE_NAME = "derivatives_binance"
    CACHE_KEY_PREFIX = "datasource"
    CACHE_TTL_SECONDS = 900  # 15 minutes

    def __init__(self, redis_client=None, http_session=None) -> None:
        super().__init__(redis_client, http_session)
        self._detector = FailureDetector(source_name=self.SOURCE_NAME)

    # ------------------------------------------------------------------
    # Binance fetchers (primary)
    # ------------------------------------------------------------------

    async def _binance_get(self, session, path: str, params: dict | None = None) -> Any:
        """GET a Binance FAPI endpoint and return parsed JSON, or None on error."""
        url = f"{_BINANCE_FAPI}{path}"
        try:
            async with session.get(url, params=params or {}, timeout=15) as resp:
                if resp.status != 200:
                    logger.warning("binance_endpoint_error", path=path, status=resp.status)
                    return None
                return await resp.json(content_type=None)
        except asyncio.TimeoutError:
            logger.warning("binance_timeout", path=path)
            return None
        except Exception as exc:
            logger.warning("binance_request_error", path=path, error=str(exc))
            return None

    async def _fetch_binance(self, session) -> dict[str, Any]:
        """Fetch all required Binance FAPI endpoints in parallel."""
        (
            funding_hist,
            oi_spot,
            oi_hist,
            top_ls_ratio,
            global_ls_ratio,
        ) = await asyncio.gather(
            self._binance_get(
                session,
                "/fapi/v1/fundingRate",
                {"symbol": "BTCUSDT", "limit": 30},
            ),
            self._binance_get(
                session,
                "/fapi/v1/openInterest",
                {"symbol": "BTCUSDT"},
            ),
            self._binance_get(
                session,
                "/futures/data/openInterestHist",
                {"symbol": "BTCUSDT", "period": "1h", "limit": 48},
            ),
            self._binance_get(
                session,
                "/futures/data/topLongShortPositionRatio",
                {"symbol": "BTCUSDT", "period": "1h", "limit": 24},
            ),
            self._binance_get(
                session,
                "/futures/data/globalLongShortAccountRatio",
                {"symbol": "BTCUSDT", "period": "1h", "limit": 24},
            ),
            return_exceptions=True,
        )

        def _safe(v: Any) -> Any:
            return None if isinstance(v, Exception) else v

        return {
            "funding_hist": _safe(funding_hist),
            "oi_spot": _safe(oi_spot),
            "oi_hist": _safe(oi_hist),
            "top_ls_ratio": _safe(top_ls_ratio),
            "global_ls_ratio": _safe(global_ls_ratio),
        }

    # ------------------------------------------------------------------
    # Bybit fetchers (secondary / cross-check)
    # ------------------------------------------------------------------

    async def _bybit_get(self, session, path: str, params: dict | None = None) -> Any:
        """GET a Bybit v5 public endpoint and return parsed JSON body, or None."""
        url = f"{_BYBIT_V5}{path}"
        try:
            async with session.get(url, params=params or {}, timeout=15) as resp:
                if resp.status != 200:
                    logger.debug("bybit_endpoint_error", path=path, status=resp.status)
                    return None
                payload = await resp.json(content_type=None)
                if isinstance(payload, dict):
                    result = payload.get("result", {})
                    if isinstance(result, dict):
                        return result.get("list") or result
                return payload
        except Exception as exc:
            logger.debug("bybit_request_error", path=path, error=str(exc))
            return None

    async def _fetch_bybit(self, session) -> dict[str, Any]:
        """Fetch Bybit funding history and OI for cross-check."""
        funding, oi = await asyncio.gather(
            self._bybit_get(
                session,
                "/market/funding/history",
                {"category": "linear", "symbol": "BTCUSDT", "limit": 10},
            ),
            self._bybit_get(
                session,
                "/market/open-interest",
                {"category": "linear", "symbol": "BTCUSDT", "intervalTime": "1h", "limit": 48},
            ),
            return_exceptions=True,
        )

        def _safe(v: Any) -> Any:
            return None if isinstance(v, Exception) else v

        return {
            "funding": _safe(funding),
            "oi": _safe(oi),
        }

    # ------------------------------------------------------------------
    # Parser helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_binance_funding_history(data: Any) -> tuple[float | None, list[float]]:
        """Return (latest_rate, full_history_list) from Binance fundingRate response."""
        if not data or not isinstance(data, list):
            return None, []
        rates: list[float] = []
        for item in data:
            try:
                rates.append(float(item.get("fundingRate", 0)))
            except (TypeError, ValueError):
                pass
        if not rates:
            return None, []
        return rates[-1], rates  # latest is last entry

    @staticmethod
    def _parse_bybit_funding_latest(data: Any) -> float | None:
        """Extract latest funding rate from Bybit /market/funding/history."""
        if not data:
            return None
        try:
            if isinstance(data, list) and data:
                return float(data[0].get("fundingRate", 0))
            if isinstance(data, dict):
                return float(data.get("fundingRate", 0))
        except (TypeError, ValueError):
            pass
        return None

    @staticmethod
    def _weighted_avg_funding(
        binance_rate: float | None,
        binance_oi: float | None,
        bybit_rate: float | None,
        bybit_oi: float | None,
    ) -> float | None:
        """Compute OI-weighted average of Binance and Bybit funding rates."""
        pairs = [
            (binance_rate, binance_oi or 1.0),
            (bybit_rate, bybit_oi or 1.0),
        ]
        valid = [(r, w) for r, w in pairs if r is not None]
        if not valid:
            return None
        if len(valid) == 1:
            return valid[0][0]
        total_w = sum(w for _, w in valid)
        if total_w == 0:
            return valid[0][0]
        return round(sum(r * w for r, w in valid) / total_w, 6)

    @staticmethod
    def _parse_binance_oi_spot(data: Any) -> float | None:
        """Parse Binance spot OI; returns raw BTC contracts (no price available here)."""
        if not data or not isinstance(data, dict):
            return None
        try:
            return float(data.get("openInterest", 0))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_binance_oi_hist(data: Any) -> tuple[float | None, float | None]:
        """Parse Binance OI history. Returns (latest_usd_bn, 24h_change_pct)."""
        if not data or not isinstance(data, list) or len(data) < 2:
            return None, None
        try:
            latest = float(data[-1].get("sumOpenInterestValue", 0))
            prev24h = float(data[max(0, len(data) - 24)].get("sumOpenInterestValue", 0))
            if prev24h > 0:
                change_pct = round((latest - prev24h) / prev24h * 100, 2)
            else:
                change_pct = None
            oi_bn = round(latest / 1e9, 2)
            return oi_bn, change_pct
        except (TypeError, ValueError, IndexError):
            return None, None

    @staticmethod
    def _parse_bybit_oi_hist(data: Any) -> float | None:
        """Parse Bybit OI history. Returns latest USD OI in billions."""
        if not data or not isinstance(data, list):
            return None
        try:
            latest = data[0]
            oi_val = float(latest.get("openInterest", 0))
            # Bybit returns OI in contracts (BTC) not USD — note as BTC OI
            return round(oi_val, 0) if oi_val > 0 else None
        except (TypeError, ValueError, IndexError):
            return None

    @staticmethod
    def _parse_global_ls_ratio(data: Any) -> float | None:
        """Parse global long/short account ratio from Binance."""
        if not data or not isinstance(data, list) or not data:
            return None
        try:
            latest = data[-1]
            return round(float(latest.get("longShortRatio", 1.0)), 4)
        except (TypeError, ValueError, IndexError):
            return None

    @staticmethod
    def _parse_top_trader_long_ratio(data: Any) -> float | None:
        """Parse top-trader long position ratio from Binance."""
        if not data or not isinstance(data, list) or not data:
            return None
        try:
            latest = data[-1]
            return round(float(latest.get("longAccount", latest.get("longShortRatio", 0.5))), 4)
        except (TypeError, ValueError, IndexError):
            return None

    # ------------------------------------------------------------------
    # BaseDataSource interface
    # ------------------------------------------------------------------

    async def fetch_raw(self) -> dict:
        """Fetch derivatives data from Binance FAPI (primary) + Bybit (secondary).

        Returns normalised raw dict. Bybit failure is tolerated (cross-check only).
        """
        session = await self._ensure_session()
        fetched_at = datetime.now(timezone.utc).isoformat()

        binance_data, bybit_data = await asyncio.gather(
            self._fetch_binance(session),
            self._fetch_bybit(session),
            return_exceptions=True,
        )

        def _safe(v: Any) -> Any:
            return None if isinstance(v, Exception) else v

        binance_data = _safe(binance_data) or {}
        bybit_data = _safe(bybit_data) or {}

        # Mark completely failed if Binance returned nothing
        binance_ok = any(
            binance_data.get(k) is not None
            for k in ("funding_hist", "oi_spot", "oi_hist", "global_ls_ratio")
        )

        raw: dict[str, Any] = {
            "_fetched_at": fetched_at,
            "_binance_ok": binance_ok,
            # Binance
            "binance_funding_hist": binance_data.get("funding_hist"),
            "binance_oi_spot": binance_data.get("oi_spot"),
            "binance_oi_hist": binance_data.get("oi_hist"),
            "binance_top_ls_ratio": binance_data.get("top_ls_ratio"),
            "binance_global_ls_ratio": binance_data.get("global_ls_ratio"),
            # Bybit (cross-check)
            "bybit_funding": bybit_data.get("funding"),
            "bybit_oi_hist": bybit_data.get("oi"),
        }

        if not binance_ok:
            raw["_error"] = True
            raw["_error_reason"] = "Binance FAPI returned no usable data"
            logger.warning("derivatives_binance_all_failed")
        else:
            logger.info(
                "derivatives_fetch_ok",
                binance_funding_rows=len(binance_data.get("funding_hist") or []),
                bybit_ok=bybit_data.get("funding") is not None,
            )

        return raw

    def summarize(self, raw: dict) -> dict:  # noqa: C901
        """Transform raw derivatives data into an LLM-ready summary."""
        if not raw or raw.get("_error"):
            return self._fallback_context(
                raw.get("_error_reason", "Derivatives data fetch failed") if raw else "No data"
            )

        as_of = raw.get("_fetched_at", datetime.now(timezone.utc).isoformat())

        # --- Funding rates ---
        binance_latest, funding_history = self._parse_binance_funding_history(
            raw.get("binance_funding_hist")
        )
        bybit_latest = self._parse_bybit_funding_latest(raw.get("bybit_funding"))

        # OI for weighting (use Binance spot OI in BTC as proxy weight)
        binance_oi_btc = self._parse_binance_oi_spot(raw.get("binance_oi_spot"))
        # For Bybit weight we don't have OI in same units easily — treat equally
        funding_wavg = self._weighted_avg_funding(
            binance_latest,
            binance_oi_btc,
            bybit_latest,
            binance_oi_btc,  # equal weight fallback
        )

        # 30-day funding percentile from history
        if funding_history and funding_wavg is not None:
            funding_pct_30d = _funding_percentile_from_history(funding_wavg, funding_history)
        elif funding_wavg is not None:
            funding_pct_30d = _funding_percentile_approx(funding_wavg)
        else:
            funding_pct_30d = 50  # neutral fallback

        # --- Open Interest ---
        oi_bn, oi_24h_change_pct = self._parse_binance_oi_hist(raw.get("binance_oi_hist"))

        # --- Long/Short ratios ---
        global_ls_ratio = self._parse_global_ls_ratio(raw.get("binance_global_ls_ratio"))
        top_long_ratio = self._parse_top_trader_long_ratio(raw.get("binance_top_ls_ratio"))

        # --- Derived fields ---
        funding_state = _classify_funding_state(funding_wavg) if funding_wavg is not None else "unknown"
        squeeze_risk = _estimate_squeeze_risk(funding_pct_30d, oi_24h_change_pct, global_ls_ratio)

        # Confidence: higher if both Binance and Bybit returned data
        bybit_ok = raw.get("bybit_funding") is not None
        fields_present = sum(
            1 for v in [funding_wavg, oi_bn, global_ls_ratio, top_long_ratio] if v is not None
        )
        base_confidence = 0.82 if bybit_ok else 0.75
        confidence = round(min(0.92, base_confidence + 0.02 * fields_present), 2)

        # --- Narrative ---
        narrative_parts: list[str] = []
        if funding_wavg is not None:
            narrative_parts.append(
                f"펀딩 30일 {funding_pct_30d} percentile"
                + (", 롱 과열." if funding_state in ("elevated_long", "extreme_long") else
                   ", 숏 우세." if funding_state in ("elevated_short", "extreme_short") else
                   ", 중립.")
            )
        if oi_24h_change_pct is not None:
            narrative_parts.append(f"OI 24h {oi_24h_change_pct:+.1f}% 변화.")
        narrative_parts.append("청산 히트맵 미제공 → 스퀴즈 위험 휴리스틱 추정.")

        narrative = " ".join(narrative_parts) if narrative_parts else "파생상품 데이터 수집 중."

        return {
            "as_of": as_of,
            "source": "Binance + Bybit",
            "confidence": confidence,
            "funding_rate_weighted_avg": round(funding_wavg, 6) if funding_wavg is not None else None,
            "funding_rate_percentile_30d": funding_pct_30d,
            "funding_state": funding_state,
            "open_interest_usd_bn": oi_bn,
            "oi_24h_change_pct": round(oi_24h_change_pct, 2) if oi_24h_change_pct is not None else None,
            "long_short_ratio": round(global_ls_ratio, 2) if global_ls_ratio is not None else None,
            "top_trader_long_ratio": round(top_long_ratio, 2) if top_long_ratio is not None else None,
            # Not available in free public APIs
            "liquidation_24h_long_usd_mn": None,
            "liquidation_24h_short_usd_mn": None,
            "nearest_long_liq_cluster": None,
            "nearest_short_liq_cluster": None,
            "squeeze_risk": squeeze_risk,
            "squeeze_risk_basis": "heuristic_no_liquidation_map",
            "narrative": narrative,
        }

    async def get_context(self) -> tuple[dict, SourceHealth]:
        """Fetch, validate, and summarize derivatives data.

        Returns (summary_dict, SourceHealth). Liquidation fields are always null
        (free public APIs do not provide liquidation heat-maps).
        """
        detector = FailureDetector(source_name=self.SOURCE_NAME)
        try:
            raw = await self._cached_fetch()

            if raw.get("_error"):
                health = SourceHealth(
                    status="BROKEN",
                    failure_reason=raw.get("_error_reason", "Binance FAPI failed"),
                    failure_stage="http",
                )
                return self._fallback_context(raw.get("_error_reason", "")), health

            summary = self.summarize(raw)

            # Sanity check on key derived values
            sanity_data: dict[str, Any] = {}
            if summary.get("funding_rate_weighted_avg") is not None:
                sanity_data["funding_rate_weighted_avg"] = summary["funding_rate_weighted_avg"]
            if summary.get("open_interest_usd_bn") is not None:
                sanity_data["open_interest_usd_bn"] = summary["open_interest_usd_bn"]
            if summary.get("long_short_ratio") is not None:
                sanity_data["long_short_ratio"] = summary["long_short_ratio"]

            sanity_h = detector.check_value_sanity(sanity_data, SANITY_RANGES)

            # Determine overall health
            # DEGRADED is expected: liquidation data always absent
            fields_available = [k for k, v in summary.items() if v is not None and not k.startswith("_")]
            fields_missing = [
                "liquidation_24h_long_usd_mn",
                "liquidation_24h_short_usd_mn",
                "nearest_long_liq_cluster",
                "nearest_short_liq_cluster",
            ]

            final_status = sanity_h.status if sanity_h.status == "BROKEN" else "DEGRADED"

            health = SourceHealth(
                status=final_status,
                last_success_at=datetime.now(timezone.utc),
                failure_reason=sanity_h.failure_reason,
                failure_stage=sanity_h.failure_stage,
                fields_available=fields_available,
                fields_missing=fields_missing,
            )

            logger.info(
                "derivatives_context_ready",
                health=health.status,
                confidence=summary.get("confidence", 0),
                funding_wavg=summary.get("funding_rate_weighted_avg"),
            )
            return summary, health

        except Exception as exc:
            logger.error("derivatives_context_failed", error=str(exc))
            health = SourceHealth(
                status="BROKEN",
                failure_reason=str(exc)[:500],
                failure_stage="unhandled",
            )
            return self._fallback_context(str(exc)), health
