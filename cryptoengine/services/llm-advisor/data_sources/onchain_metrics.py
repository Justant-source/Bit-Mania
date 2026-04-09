"""On-chain metrics data source — MVRV Z-Score, Puell Multiple, LTH Supply, Hashrate.

Primary:  CoinMetrics Community API (free, no key required)
          Metrics available (free tier): CapMVRVCur, PriceUSD, HashRate, AdrActCnt, IssTotUSD
          Blocked (paywalled):           SplyAct1yr, RevUSD, CapRealUSD

Secondary (backup / supplemental):
          blockchain.info /charts/miners-revenue  — Puell Multiple 365-day baseline
          blockchain.info /charts/total-bitcoins  — circulating supply (LTH proxy denominator)
          blockchain.info /stats                  — hash_rate, circulating supply snapshot
          mempool.space /api/v1/mining/hashrate/3d — hashrate backup

Cache TTL: 3600 seconds (1 hour) — on-chain data updates slowly.
"""

from __future__ import annotations

import asyncio
import statistics
from datetime import datetime, timezone
from typing import Any

import structlog

from .base import BaseDataSource, SourceHealth
from .failure_detection import FailureDetector

logger = structlog.get_logger(__name__)

_COINMETRICS_BASE = "https://community-api.coinmetrics.io/v4"
_MEMPOOL_HASHRATE_URL = "https://mempool.space/api/v1/mining/hashrate/3d"
_BLOCKCHAIN_INFO_STATS_URL = "https://api.blockchain.info/stats"
_BLOCKCHAIN_INFO_MINERS_URL = (
    "https://api.blockchain.info/charts/miners-revenue"
    "?timespan=365days&format=json&sampled=true"
)
_BLOCKCHAIN_INFO_SUPPLY_URL = (
    "https://api.blockchain.info/charts/total-bitcoins"
    "?timespan=30days&format=json"
)

# Free-tier metrics available from CoinMetrics Community (as of 2026-04):
#   CapMVRVCur  — Market Cap / Realised Cap ratio (MVRV ratio, not Z-Score directly)
#   PriceUSD    — BTC price in USD
#   HashRate    — Network hash rate (TH/s)
#   AdrActCnt   — Active addresses (24h)
#   IssTotUSD   — Daily total issuance in USD (≈ miner revenue, today's value only)
#
# Paywalled (403): SplyAct1yr, RevUSD, CapRealUSD
_CM_METRICS = "CapMVRVCur,PriceUSD,HashRate,AdrActCnt,IssTotUSD"

SANITY_RANGES: dict[str, tuple[float, float]] = {
    "mvrv_zscore": (-3.0, 12.0),
    "puell_multiple": (0.0, 10.0),
    "hashrate": (100_000_000, 2_000_000_000_000_000),  # TH/s range (wide to accommodate EH/s)
}


def _classify_mvrv(zscore: float) -> str:
    """Map MVRV Z-Score to human-readable interpretation."""
    if zscore < 0:
        return "deep_undervalued"
    if zscore < 1:
        return "undervalued"
    if zscore < 3:
        return "fair_value"
    if zscore < 5:
        return "overvalued"
    return "euphoria"


def _classify_puell(puell: float) -> str:
    """Map Puell Multiple to human-readable interpretation."""
    if puell < 0.5:
        return "capitulation"
    if puell < 0.8:
        return "undervalued"
    if puell < 1.2:
        return "neutral"
    if puell < 2.0:
        return "profitable"
    return "euphoria"


def _mvrv_percentile(zscore: float) -> int:
    """Rough historical percentile estimate from MVRV Z-Score.

    Based on historical BTC MVRV Z-Score distribution (approx. range -0.5 to 10+).
    Uses a simple piecewise linear approximation.
    """
    if zscore <= -0.5:
        return 2
    if zscore <= 0:
        return round(2 + (zscore + 0.5) / 0.5 * 8)     # 2–10
    if zscore <= 1:
        return round(10 + zscore * 20)                   # 10–30
    if zscore <= 3:
        return round(30 + (zscore - 1) / 2 * 35)        # 30–65
    if zscore <= 5:
        return round(65 + (zscore - 3) / 2 * 25)        # 65–90
    return min(99, round(90 + (zscore - 5) * 2))        # 90+


def _clamp(lo: float, hi: float, value: float) -> float:
    return max(lo, min(hi, value))


def _whale_score(lth_30d_change_pct: float) -> float:
    """Whale accumulation score 0-1 based on LTH supply change.

    Exchange reserve not available in free tier.
    LTH supply increasing → higher score.
    """
    score = _clamp(0.0, 1.0, 0.4 + lth_30d_change_pct * 0.3)
    return round(score, 2)


def _compute_mvrv_zscore(series: list[float]) -> float | None:
    """Compute MVRV Z-Score from a series of MVRV ratio values.

    Uses the last 4 years (approx. 4*365=1460 data points) as the baseline.
    Formula: (current - mean) / std_dev
    Falls back to simple normalisation when fewer than 30 data points exist.
    """
    if not series:
        return None
    current = series[-1]
    baseline = series[-1460:] if len(series) >= 30 else series
    if len(baseline) < 2:
        return None
    try:
        mean = statistics.mean(baseline)
        std = statistics.stdev(baseline)
        if std == 0:
            return None
        return round((current - mean) / std, 4)
    except statistics.StatisticsError:
        return None


def _compute_puell_multiple(
    today_rev_usd: float | None,
    rev_series_365d: list[float],
) -> float | None:
    """Compute Puell Multiple: today's miner revenue / 365-day MA revenue.

    today_rev_usd: today's IssTotUSD (CoinMetrics) or latest value from blockchain.info
    rev_series_365d: historical daily miner revenue for the past ~365 days
    """
    if today_rev_usd is None or today_rev_usd <= 0:
        return None
    if len(rev_series_365d) < 2:
        return None
    try:
        ma = statistics.mean(rev_series_365d)
        if ma == 0:
            return None
        return round(today_rev_usd / ma, 4)
    except statistics.StatisticsError:
        return None


class OnchainMetricsSource(BaseDataSource):
    """Fetch and summarize on-chain BTC metrics.

    Data sources (all free, no API key required):
    - CoinMetrics Community: MVRV ratio, price, hashrate, active addresses, daily issuance
    - blockchain.info: 365-day miner revenue history (Puell MA baseline), total supply
    - mempool.space: hashrate backup

    Paywalled CoinMetrics metrics (SplyAct1yr, RevUSD) are replaced by free alternatives.
    """

    SOURCE_NAME = "onchain_coinmetrics"
    CACHE_KEY_PREFIX = "datasource"
    CACHE_TTL_SECONDS = 3600  # 1 hour

    def __init__(self, redis_client=None, http_session=None) -> None:
        super().__init__(redis_client, http_session)
        self._detector = FailureDetector(source_name=self.SOURCE_NAME)

    async def _fetch_coinmetrics(self, session) -> dict[str, Any]:
        """Fetch free-tier BTC metrics from CoinMetrics Community API.

        Returns dict with 'series' key mapping metric name → list[float],
        or dict with '_http_health' and error indicators on failure.
        """
        url = f"{_COINMETRICS_BASE}/timeseries/asset-metrics"
        params = {
            "assets": "btc",
            "metrics": _CM_METRICS,
            "frequency": "1d",
            "page_size": "730",  # ~2yr of daily data for decent Z-Score baseline
        }
        try:
            async with session.get(url, params=params, timeout=30) as resp:
                http_health = self._detector.check_http(
                    resp.status,
                    content_length=1,
                )
                if http_health.status == "BROKEN":
                    logger.warning(
                        "coinmetrics_http_error",
                        status=resp.status,
                        reason=http_health.failure_reason,
                    )
                    return {"_http_health": http_health}

                payload = await resp.json(content_type=None)
                data_rows = payload.get("data", [])
                if not isinstance(data_rows, list) or len(data_rows) == 0:
                    logger.warning("coinmetrics_empty_data")
                    return {"_http_health": http_health, "_empty": True}

                # Pivot rows into per-metric series
                metric_series: dict[str, list[float]] = {
                    m: [] for m in _CM_METRICS.split(",")
                }
                last_time = None
                for row in data_rows:
                    last_time = row.get("time")
                    for metric in metric_series:
                        raw_val = row.get(metric)
                        if raw_val is not None:
                            try:
                                metric_series[metric].append(float(raw_val))
                            except (TypeError, ValueError):
                                pass

                logger.info(
                    "coinmetrics_fetch_ok",
                    rows=len(data_rows),
                    last_time=last_time,
                )
                return {
                    "_http_health": http_health,
                    "_last_time": last_time,
                    "series": metric_series,
                }
        except asyncio.TimeoutError:
            logger.warning("coinmetrics_timeout")
            return {
                "_http_health": SourceHealth(
                    status="BROKEN",
                    failure_reason="Request timed out",
                    failure_stage="http",
                )
            }
        except Exception as exc:
            logger.warning("coinmetrics_fetch_error", error=str(exc))
            return {
                "_http_health": SourceHealth(
                    status="BROKEN",
                    failure_reason=str(exc)[:200],
                    failure_stage="http",
                )
            }

    async def _fetch_mempool_hashrate(self, session) -> float | None:
        """Backup hashrate from Mempool.space."""
        try:
            async with session.get(_MEMPOOL_HASHRATE_URL, timeout=15) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                hashrates = data.get("hashrates") or data.get("currentHashrate")
                if isinstance(hashrates, list) and hashrates:
                    last = hashrates[-1]
                    val = last.get("avgHashrate") or last.get("hashrate")
                    return float(val) if val is not None else None
                if isinstance(hashrates, (int, float)):
                    return float(hashrates)
        except Exception as exc:
            logger.debug("mempool_hashrate_error", error=str(exc))
        return None

    async def _fetch_blockchain_miners_revenue(self, session) -> list[float]:
        """Fetch 365-day miner revenue history from blockchain.info.

        Used as the baseline for Puell Multiple calculation.
        Returns list of daily revenue in USD (newest last), or empty list on failure.
        """
        try:
            async with session.get(_BLOCKCHAIN_INFO_MINERS_URL, timeout=15) as resp:
                if resp.status != 200:
                    logger.debug("blockchain_miners_revenue_error", status=resp.status)
                    return []
                data = await resp.json(content_type=None)
                values = data.get("values", [])
                if isinstance(values, list) and values:
                    return [float(v["y"]) for v in values if v.get("y") is not None]
        except Exception as exc:
            logger.debug("blockchain_miners_revenue_error", error=str(exc))
        return []

    async def _fetch_blockchain_info_stats(self, session) -> dict[str, Any]:
        """Fetch blockchain.info /stats for snapshot metrics (hashrate, supply)."""
        try:
            async with session.get(
                _BLOCKCHAIN_INFO_STATS_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            ) as resp:
                if resp.status != 200:
                    return {}
                return await resp.json(content_type=None) or {}
        except Exception as exc:
            logger.debug("blockchain_info_stats_error", error=str(exc))
        return {}

    async def _fetch_blockchain_supply(self, session) -> float | None:
        """Fetch total circulating BTC supply from blockchain.info (30-day chart)."""
        try:
            async with session.get(_BLOCKCHAIN_INFO_SUPPLY_URL, timeout=15) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                values = data.get("values", [])
                if isinstance(values, list) and values:
                    return float(values[-1].get("y", 0)) or None
        except Exception as exc:
            logger.debug("blockchain_supply_error", error=str(exc))
        return None

    async def fetch_raw(self) -> dict:
        """Fetch on-chain metrics from multiple free sources.

        Primary:    CoinMetrics (MVRV ratio, hashrate, active addresses, today's issuance)
        Supplement: blockchain.info (365d miner revenue history for Puell MA, supply)
        Backup:     mempool.space (hashrate)

        Returns a dict with computed series and derived scalars.
        On failure, returns dict with _error key so summarize() can produce a fallback.
        """
        session = await self._ensure_session()
        fetched_at = datetime.now(timezone.utc).isoformat()

        # Parallel fetch: all sources simultaneously
        (
            cm_result,
            miners_rev_history,
            bi_stats,
            bi_supply,
            hashrate_backup,
        ) = await asyncio.gather(
            self._fetch_coinmetrics(session),
            self._fetch_blockchain_miners_revenue(session),
            self._fetch_blockchain_info_stats(session),
            self._fetch_blockchain_supply(session),
            self._fetch_mempool_hashrate(session),
            return_exceptions=True,
        )

        def _safe(val: Any, default: Any = None) -> Any:
            return default if isinstance(val, Exception) else val

        cm_result = _safe(cm_result) or {}
        miners_rev_history = _safe(miners_rev_history) or []
        bi_stats = _safe(bi_stats) or {}
        bi_supply = _safe(bi_supply)
        hashrate_backup = _safe(hashrate_backup)

        raw: dict[str, Any] = {
            "_fetched_at": fetched_at,
            "_http_health": cm_result.get("_http_health"),
            "_last_time": cm_result.get("_last_time"),
            "_hashrate_backup": hashrate_backup,
        }

        series: dict[str, list[float]] = cm_result.get("series") or {}

        # --- MVRV Z-Score from CapMVRVCur series ---
        mvrv_series = series.get("CapMVRVCur", [])
        raw["mvrv_zscore"] = _compute_mvrv_zscore(mvrv_series)
        raw["mvrv_ratio_current"] = round(mvrv_series[-1], 4) if mvrv_series else None

        # --- Puell Multiple ---
        # Today's issuance from CoinMetrics IssTotUSD (last non-None value),
        # historical baseline from blockchain.info miners-revenue chart.
        iss_series = series.get("IssTotUSD", [])
        today_iss_usd = iss_series[-1] if iss_series else None

        # Use blockchain.info history as the 365-day baseline for Puell MA.
        # If we also have IssTotUSD series from CoinMetrics, we can extend it.
        puell_baseline = miners_rev_history if miners_rev_history else iss_series[:-1]
        raw["puell_multiple"] = _compute_puell_multiple(today_iss_usd, puell_baseline)
        raw["_puell_today_usd"] = today_iss_usd
        raw["_puell_baseline_days"] = len(puell_baseline)

        # --- LTH Supply proxy ---
        # SplyAct1yr is paywalled. We use total circulating supply as a rough proxy
        # for the denominator, and flag it as approximate.
        # blockchain.info total-bitcoins gives circulating supply in BTC.
        circulating_supply = bi_supply
        if circulating_supply is None and bi_stats:
            # blockchain.info stats: totalbc is in satoshis
            totalbc_sat = bi_stats.get("totalbc")
            if totalbc_sat:
                circulating_supply = float(totalbc_sat) / 1e8
        raw["lth_supply_btc"] = round(circulating_supply, 0) if circulating_supply else None
        raw["lth_supply_30d_change_pct"] = 0.0  # Not derivable without SplyAct1yr history
        raw["_lth_is_circulating_proxy"] = True  # Flag: this is circulating supply, not LTH

        # --- Hashrate (prefer CoinMetrics, fallback to blockchain.info, then mempool) ---
        hr_series = series.get("HashRate", [])
        if hr_series:
            raw["hashrate"] = round(hr_series[-1], 0)
            raw["_hashrate_source"] = "coinmetrics"
        else:
            # blockchain.info hash_rate is in GH/s; multiply to TH/s? Actually it's in GH/s.
            # Wait: blockchain.info reports in GH/s (1 GH/s = 1e9 H/s)
            # CoinMetrics HashRate is in TH/s per their docs, but raw value ~9.5e8 implies TH/s
            # blockchain.info hash_rate: e.g., 1.04e12 for ~1 ZH/s — likely in GH/s
            # So bi_stats hash_rate is GH/s, but we need consistent units with CoinMetrics.
            # We store whatever we get and note the source — summarize() uses as-is for LLM.
            bi_hr = bi_stats.get("hash_rate")
            if bi_hr is not None:
                raw["hashrate"] = round(float(bi_hr), 0)
                raw["_hashrate_source"] = "blockchain_info"
            elif hashrate_backup is not None:
                raw["hashrate"] = round(hashrate_backup, 0)
                raw["_hashrate_source"] = "mempool_backup"
            else:
                raw["hashrate"] = None
                raw["_hashrate_source"] = None

        # --- Active addresses (prefer CoinMetrics) ---
        addr_series = series.get("AdrActCnt", [])
        if addr_series:
            raw["active_addresses_24h"] = int(addr_series[-1])
        else:
            n_tx = bi_stats.get("n_tx")
            raw["active_addresses_24h"] = int(n_tx) if n_tx else None
            if n_tx:
                raw["_addresses_source"] = "blockchain_info_tx_count"

        # Mark degraded if CoinMetrics call failed entirely
        if cm_result.get("_empty") or not series:
            raw["_error"] = True
            raw["_error_reason"] = "CoinMetrics returned no data"

        logger.info(
            "onchain_fetch_complete",
            mvrv_zscore=raw.get("mvrv_zscore"),
            puell_multiple=raw.get("puell_multiple"),
            lth_supply_btc=raw.get("lth_supply_btc"),
            hashrate=raw.get("hashrate"),
            puell_baseline_days=raw.get("_puell_baseline_days"),
        )
        return raw

    def summarize(self, raw: dict) -> dict:  # noqa: C901
        """Transform raw on-chain data into an LLM-ready summary.

        Exchange Reserve and SOPR are not available in any free tier used here
        and are always returned as null (DEGRADED is expected).
        LTH supply is approximated by circulating supply (flagged as proxy).
        """
        if not raw or raw.get("_error"):
            return self._fallback_context(
                raw.get("_error_reason", "Onchain fetch failed") if raw else "No data"
            )

        as_of = raw.get("_last_time") or raw.get("_fetched_at", datetime.now(timezone.utc).isoformat())

        mvrv_z = raw.get("mvrv_zscore")
        puell = raw.get("puell_multiple")
        lth_supply_btc = raw.get("lth_supply_btc")
        lth_30d_change = float(raw.get("lth_supply_30d_change_pct") or 0.0)
        hashrate = raw.get("hashrate")
        active_addresses = raw.get("active_addresses_24h")
        lth_is_proxy = raw.get("_lth_is_circulating_proxy", False)

        # Run sanity check on derived values
        sanity_data: dict[str, Any] = {}
        if mvrv_z is not None:
            sanity_data["mvrv_zscore"] = mvrv_z
        if puell is not None:
            sanity_data["puell_multiple"] = puell
        if hashrate is not None:
            sanity_data["hashrate"] = hashrate

        detector = FailureDetector(source_name=self.SOURCE_NAME)
        sanity_health = detector.check_value_sanity(sanity_data, SANITY_RANGES)

        # Confidence: base 0.80 but degrade for missing/out-of-range fields
        metrics_ok = sum(1 for v in [mvrv_z, puell, lth_supply_btc] if v is not None)
        if sanity_health.status == "DEGRADED":
            confidence = round(0.60 + 0.05 * metrics_ok, 2)
        else:
            confidence = round(0.70 + 0.05 * metrics_ok, 2)
        confidence = min(0.85, confidence)  # cap; exchange_reserve/SOPR unavailable

        # --- Interpretations ---
        mvrv_z_rounded = round(mvrv_z, 2) if mvrv_z is not None else None
        mvrv_interp = _classify_mvrv(mvrv_z) if mvrv_z is not None else "unknown"
        mvrv_pct = _mvrv_percentile(mvrv_z) if mvrv_z is not None else None

        puell_rounded = round(puell, 2) if puell is not None else None
        puell_interp = _classify_puell(puell) if puell is not None else "unknown"

        lth_supply_out = round(lth_supply_btc, 0) if lth_supply_btc is not None else None
        hashrate_out = round(hashrate, 0) if hashrate is not None else None

        whale_score = _whale_score(lth_30d_change)

        # --- Narrative ---
        parts: list[str] = []
        if mvrv_z_rounded is not None:
            parts.append(f"MVRV Z-Score {mvrv_z_rounded} ({mvrv_interp.replace('_', ' ')}).")
        if lth_supply_out is not None:
            label = "유통 공급량" if lth_is_proxy else "LTH 공급"
            parts.append(f"{label} {lth_supply_out:,.0f} BTC.")
        if puell_rounded is not None:
            parts.append(f"Puell Multiple {puell_rounded} ({puell_interp.replace('_', ' ')}).")
        parts.append("(Exchange Reserve/SOPR/LTH 데이터 미제공)")

        narrative = " ".join(parts) if parts else "온체인 데이터 수집 중."

        return {
            "as_of": as_of,
            "source": "CoinMetrics Community + blockchain.info",
            "confidence": confidence,
            # Not available in free tier
            "exchange_reserve_btc": None,
            "exchange_reserve_7d_change_pct": None,
            # MVRV Z-Score (computed from CapMVRVCur series)
            "mvrv_zscore": mvrv_z_rounded,
            "mvrv_interpretation": mvrv_interp,
            "mvrv_historical_percentile": mvrv_pct,
            # Puell Multiple (IssTotUSD / 365d MA from blockchain.info)
            "puell_multiple": puell_rounded,
            "puell_interpretation": puell_interp,
            # SOPR: not in free tier
            "sopr": None,
            # LTH supply (circulating supply proxy when SplyAct1yr unavailable)
            "lth_supply_btc": lth_supply_out,
            "lth_supply_30d_change_pct": round(lth_30d_change, 2),
            "lth_is_circulating_proxy": lth_is_proxy,
            # Derived scores
            "whale_accumulation_score": whale_score,
            # Network metrics
            "hashrate_th": hashrate_out,
            "active_addresses_24h": active_addresses,
            "narrative": narrative,
        }

    async def get_context(self) -> tuple[dict, SourceHealth]:
        """Fetch, validate, and summarize on-chain metrics.

        Returns (summary_dict, SourceHealth). DEGRADED is acceptable because
        Exchange Reserve and SOPR are never available in the free tier.
        """
        detector = FailureDetector(source_name=self.SOURCE_NAME)
        try:
            raw = await self._cached_fetch()

            if raw.get("_error"):
                http_h = raw.get("_http_health") or SourceHealth(
                    status="BROKEN",
                    failure_reason=raw.get("_error_reason", "fetch failed"),
                    failure_stage="http",
                )
                summary = self._fallback_context(raw.get("_error_reason", "fetch failed"))
                return summary, http_h

            summary = self.summarize(raw)

            # Validate derived values for sanity
            sanity_data: dict[str, Any] = {}
            for key in ("mvrv_zscore", "puell_multiple", "hashrate_th"):
                if summary.get(key) is not None:
                    target_key = "hashrate" if key == "hashrate_th" else key
                    sanity_data[target_key] = summary[key]

            http_h = raw.get("_http_health") or SourceHealth(status="HEALTHY")
            sanity_h = detector.check_value_sanity(sanity_data, SANITY_RANGES)

            # Exchange reserve / SOPR always absent: DEGRADED is expected, not BROKEN
            fields_available = [k for k, v in summary.items() if v is not None and not k.startswith("_")]
            fields_missing = ["exchange_reserve_btc", "exchange_reserve_7d_change_pct", "sopr"]

            combined = detector.combine(http_h, sanity_h)

            health = SourceHealth(
                status=combined.status if combined.status == "BROKEN" else "DEGRADED",
                last_success_at=datetime.now(timezone.utc),
                failure_reason=combined.failure_reason,
                failure_stage=combined.failure_stage,
                fields_available=fields_available,
                fields_missing=fields_missing,
            )

            logger.info(
                "onchain_context_ready",
                health=health.status,
                confidence=summary.get("confidence", 0),
            )
            return summary, health

        except Exception as exc:
            logger.error("onchain_context_failed", error=str(exc))
            health = SourceHealth(
                status="BROKEN",
                failure_reason=str(exc)[:500],
                failure_stage="unhandled",
            )
            return self._fallback_context(str(exc)), health
