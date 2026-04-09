"""ETF flow data from Farside Investors (HTML scraping) + SoSoValue fallback.

HIGHEST FAILURE RISK — HTML scraping is inherently fragile.
Uses FailureDetector for 4-layer validation and AlertManager for alerts.

Primary:  Farside Investors (https://farside.co.uk/btc/) — HTML table scraping
Fallback: SoSoValue (https://sosovalue.com/assets/etf/us-btc-spot) — HTML scraping
Cache TTL: 43200 seconds (12 hours) — ETF data updates once daily after US market close.

Dependencies:
    beautifulsoup4>=4.12,<5.0

Farside HTML table structure (as of 2026-04):
  - Table with most rows is the data table
  - Row 0: header row with empty ticker cells and "Total" in last column
  - Row 1: ticker names (IBIT, FBTC, BITB, ARKB, BTCO, EZBC, BRRR, HODL, BTCW, MSBT, GBTC, BTC)
  - Row 2: fee row (skip)
  - Rows 3+: individual date rows in ascending chronological order; last data row = most recent
  - Summary rows at bottom: Total, Average, Maximum, Minimum (skip these)
  - NOTE: aiohttp must use Accept-Encoding: gzip, deflate (no brotli) to avoid 400 errors
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from .base import BaseDataSource, SourceHealth
from .failure_detection import FailureDetector

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Request config
# ---------------------------------------------------------------------------

USER_AGENT = os.environ.get(
    "SCRAPE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
REQUEST_TIMEOUT = int(os.environ.get("SCRAPE_REQUEST_TIMEOUT_SECONDS", "20"))

# Explicitly avoid brotli ('br') encoding — aiohttp cannot decode it without
# the optional Brotli package, and requesting it triggers a 400/ContentEncoding error.
SAFE_ACCEPT_ENCODING = "gzip, deflate"

# ---------------------------------------------------------------------------
# Structural validation constants
# ---------------------------------------------------------------------------

# Full set of known BTC spot ETF tickers on Farside table (updated 2026-04: added MSBT)
KNOWN_TICKERS: set[str] = {
    "IBIT", "FBTC", "BITB", "ARKB", "BTCO",
    "EZBC", "BRRR", "HODL", "BTCW", "MSBT", "GBTC", "BTC",
}
# At least one of these must appear or the page is considered BROKEN
CRITICAL_TICKERS: set[str] = {"IBIT", "FBTC"}

# Minimum columns expected in the main table (date + tickers + total)
EXPECTED_COLUMNS_MIN = 10

# Summary/footer rows that should be skipped when parsing data rows
_SKIP_ROW_PREFIXES: set[str] = {"TOTAL", "AVERAGE", "MAXIMUM", "MINIMUM", "FEE"}

# Sanity bounds — daily net flow in millions USD
SANITY_RANGES: dict[str, tuple[float, float]] = {
    "daily_net_flow_usd_mn": (-3000.0, 3000.0),   # ±$3 B extreme bound
}

RECOVERY_HINTS: list[str] = [
    "브라우저로 https://farside.co.uk/btc/ 접속 확인",
    "HTML 구조 변경 여부 확인 (DevTools → Elements)",
    "data_sources/etf_flows.py의 파싱 로직 업데이트",
    "tests/fixtures/ HTML 픽스처 갱신",
    "python scripts/health_check.py etf_flows 실행하여 HEALTHY 확인",
]

FARSIDE_URL = "https://farside.co.uk/btc/"
SOSOVALUE_URL = "https://sosovalue.com/assets/etf/us-btc-spot"


# ---------------------------------------------------------------------------
# Module-level parsing helpers (testable independently)
# ---------------------------------------------------------------------------

def _parse_flow_value(raw: str | None) -> float | None:
    """Convert Farside cell text to float (millions USD).

    Handles:
    - "123.4"   →  123.4
    - "(45.6)"  → -45.6   (parentheses = negative)
    - "-"       →  0.0    (no data, treat as zero)
    - ""        →  0.0
    - "45.6M"   →  45.6   (strip trailing M)
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s in ("-", "", "n/a", "N/A", "—"):
        return 0.0
    s = s.rstrip("Mm").replace(",", "").replace("$", "").strip()
    if s.startswith("(") and s.endswith(")"):
        inner = s[1:-1].strip()
        try:
            return -abs(float(inner))
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify_flow_trend(recent_totals: list[float]) -> str:
    """Classify 7-day flow momentum into named category."""
    if len(recent_totals) < 3:
        return "neutral"
    avg = sum(recent_totals) / len(recent_totals)
    if avg > 200:
        return "inflow_strong"
    if avg > 50:
        return "inflow_moderate"
    if avg < -200:
        return "outflow_accelerating"
    if avg < -50:
        return "outflow_moderate"
    return "neutral"


def _calculate_streak(totals: list[float]) -> int:
    """Return streak days: positive = consecutive inflow, negative = outflow.

    Iterates from newest (index 0) forwards.
    """
    if not totals:
        return 0
    direction = 1 if totals[0] > 0 else -1
    streak = 0
    for t in totals:
        if t == 0:
            break
        if (t > 0 and direction > 0) or (t < 0 and direction < 0):
            streak += direction
        else:
            break
    return streak


def _build_merged_headers(rows_html: list) -> tuple[int, list[str]]:
    """Build a merged header list by combining all header rows above the data rows.

    Farside page structure (2026-04):
      Row 0: mostly empty cells, last cell = "Total"
      Row 1: ticker names (IBIT, FBTC, ...) — CRITICAL_TICKERS are here
      Row 2: fee row (not a header)

    Strategy: find the row containing CRITICAL_TICKERS (ticker_row_idx), then
    merge it with any preceding rows to fill in blank cells (e.g. "Total" from Row 0
    fills the blank last cell of Row 1).

    Returns (ticker_row_idx, merged_headers_list).
    Returns (-1, []) if critical tickers not found.
    """
    ticker_row_idx = -1
    ticker_headers: list[str] = []

    for idx, tr in enumerate(rows_html[:5]):
        cells = tr.find_all(["th", "td"])
        h = [cell.get_text(strip=True).upper() for cell in cells]
        if {x for x in h if x in CRITICAL_TICKERS}:
            ticker_row_idx = idx
            ticker_headers = h
            break

    if ticker_row_idx == -1:
        return -1, []

    # Merge preceding rows into ticker_headers to fill empty slots.
    # Any preceding row that has a non-empty value in a position where
    # ticker_headers is empty takes priority (e.g. "TOTAL" from Row 0).
    merged = list(ticker_headers)
    for prev_idx in range(ticker_row_idx):
        cells = rows_html[prev_idx].find_all(["th", "td"])
        prev_h = [cell.get_text(strip=True).upper() for cell in cells]
        # Pad to same length
        while len(prev_h) < len(merged):
            prev_h.append("")
        for col_idx, (cur, prev) in enumerate(zip(merged, prev_h)):
            if not cur and prev:
                merged[col_idx] = prev

    # Column 0 is always the date column — ensure it's labelled DATE
    if merged and merged[0] in ("", "DATE"):
        merged[0] = "DATE"

    return ticker_row_idx, merged


def _parse_farside_html(html: str) -> tuple[list[dict], SourceHealth]:
    """Parse Farside BTC ETF HTML table with structural validation.

    Returns (rows, health) where rows is a list of dicts with keys:
      'date', per-ticker keys (float | None), 'total' (float | None)
    Rows are ordered most-recent-first.

    BROKEN if critical tickers absent or no table found.
    DEGRADED if column count below expected minimum.

    Farside table structure (2026-04):
      - Row 0: empty header row (only "Total" in last column)
      - Row 1: ticker name row (IBIT, FBTC, ...)
      - Row 2: fee row (skip)
      - Rows 3+: data rows in ascending chronological order
      - Last rows: summary rows Total/Average/Maximum/Minimum (skip)
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore[import]
    except ImportError:
        return [], SourceHealth(
            status="BROKEN",
            failure_reason="beautifulsoup4 not installed",
            failure_stage="structural",
        )

    # Detect Cloudflare challenge page
    if "just a moment" in html.lower() or "enable javascript and cookies" in html.lower():
        return [], SourceHealth(
            status="BROKEN",
            failure_reason="Cloudflare challenge page returned — bot protection active",
            failure_stage="structural",
        )

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    if not tables:
        return [], SourceHealth(
            status="BROKEN",
            failure_reason="No <table> elements found on Farside page",
            failure_stage="structural",
        )

    # Use the table with the most rows
    main_table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows_html = main_table.find_all("tr")

    if len(rows_html) < 3:
        return [], SourceHealth(
            status="BROKEN",
            failure_reason=f"Main table has only {len(rows_html)} rows (need ≥3)",
            failure_stage="structural",
        )

    # Locate the ticker header row (may NOT be row 0) and merge with preceding rows
    ticker_row_idx, headers = _build_merged_headers(rows_html)
    if ticker_row_idx == -1:
        # Fallback: try row 0 with old structure
        header_cells = rows_html[0].find_all(["th", "td"])
        headers = [cell.get_text(strip=True).upper() for cell in header_cells]
        if headers and headers[0] in ("", "DATE"):
            headers[0] = "DATE"
        ticker_row_idx = 0

    # Validate presence of critical tickers
    found_tickers = {h for h in headers if h in KNOWN_TICKERS}
    found_critical = found_tickers & CRITICAL_TICKERS

    if not found_critical:
        return [], SourceHealth(
            status="BROKEN",
            failure_reason=(
                f"Critical tickers {CRITICAL_TICKERS} not found in any header row. "
                f"Scanned first 5 rows. Last checked headers (first 15): {headers[:15]}"
            ),
            failure_stage="structural",
        )

    if len(headers) < EXPECTED_COLUMNS_MIN:
        struct_health: SourceHealth = SourceHealth(
            status="DEGRADED",
            failure_reason=(
                f"Only {len(headers)} columns found (expected ≥{EXPECTED_COLUMNS_MIN})"
            ),
            failure_stage="structural",
            fields_available=sorted(found_tickers),
            fields_missing=sorted(KNOWN_TICKERS - found_tickers),
        )
    else:
        struct_health = SourceHealth(
            status="HEALTHY",
            failure_stage="structural",
            fields_available=sorted(found_tickers),
        )

    # Parse data rows — skip everything up to and including the ticker row,
    # plus any immediately-following non-date rows (e.g. fee row).
    parsed_rows: list[dict] = []
    for tr in rows_html[ticker_row_idx + 1:]:
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        values = [cell.get_text(strip=True) for cell in cells]

        # Skip rows with far too many cells (browser-rendered mega-row artifact)
        if len(values) > len(headers) * 2:
            continue

        # Pad or trim to match header length
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        values = values[: len(headers)]

        # Identify first cell as the row label
        first_cell = values[0].strip().upper()

        # Skip fee row, summary rows, and empty rows
        if not first_cell:
            continue
        if any(first_cell.startswith(skip) for skip in _SKIP_ROW_PREFIXES):
            continue

        row: dict[str, Any] = {}
        for col_idx, (header, val) in enumerate(zip(headers, values)):
            header_upper = header.strip().upper()
            # Column 0 is always the date (even if header is still empty after merge)
            if col_idx == 0 or header_upper == "DATE":
                if val:
                    row["date"] = val
                continue
            if header_upper == "TOTAL":
                row["total"] = _parse_flow_value(val)
                continue
            # Skip remaining empty-header columns (should not occur after merge)
            if not header_upper:
                continue
            # Match known tickers (including partial like "BTC (Grayscale Mini)")
            matched = next(
                (t for t in KNOWN_TICKERS if header_upper == t or header_upper.startswith(t)),
                None,
            )
            if matched:
                new_val = _parse_flow_value(val)
                existing = row.get(matched)
                if existing is not None and new_val is not None:
                    row[matched] = existing + new_val
                elif new_val is not None:
                    row[matched] = new_val

        # Only keep rows that look like date-based data rows
        if row and row.get("date"):
            parsed_rows.append(row)

    if not parsed_rows:
        return [], SourceHealth(
            status="BROKEN",
            failure_reason="Table parsed but no data rows extracted",
            failure_stage="structural",
        )

    # Farside rows are in ascending chronological order — reverse so index 0 = most recent
    parsed_rows.reverse()

    return parsed_rows, struct_health


def _build_narrative(
    daily_net: float,
    streak: int,
    flow_trend: str,
    top_inflow: str | None,
    top_outflow: str | None,
    daily_nets: list[float],
) -> str:
    """Build a concise Korean narrative string."""
    trend_map = {
        "inflow_strong": "강한 매수 흐름",
        "inflow_moderate": "완만한 유입세",
        "neutral": "중립",
        "outflow_moderate": "완만한 유출세",
        "outflow_accelerating": "유출 가속",
    }
    streak_abs = abs(streak)
    streak_dir = "순유입" if streak > 0 else "순유출"
    cum_3d = sum(daily_nets[:3]) if len(daily_nets) >= 3 else daily_net

    parts: list[str] = []
    if streak_abs >= 2:
        parts.append(f"{streak_abs}일 연속 {streak_dir} 누적 ${abs(cum_3d):.0f}M")
    else:
        direction = "순유입" if daily_net >= 0 else "순유출"
        parts.append(f"전일 ETF {direction} ${abs(daily_net):.0f}M")

    issuer_notes: list[str] = []
    if top_inflow:
        issuer_notes.append(f"{top_inflow} 매수 우위")
    if top_outflow:
        issuer_notes.append(f"{top_outflow} 환매 압력")
    if issuer_notes:
        parts.append(", ".join(issuer_notes))

    parts.append(f"7일 추세: {trend_map.get(flow_trend, flow_trend)}")
    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Data source class
# ---------------------------------------------------------------------------

class ETFFlowsSource(BaseDataSource):
    """Daily US spot BTC ETF net flows with Fail-Loud validation.

    Primary: Farside Investors HTML scraping.
    Fallback: SoSoValue HTML scraping.
    Emits AlertManager alerts on BROKEN status.
    """

    SOURCE_NAME = "etf_flows"
    CACHE_KEY_PREFIX = "datasource"
    CACHE_TTL_SECONDS = 43200  # 12 hours — ETF data updates once daily

    def __init__(self, redis_client=None, http_session=None) -> None:
        super().__init__(redis_client=redis_client, http_session=http_session)
        self._detector = FailureDetector(source_name=self.SOURCE_NAME)
        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # Polite throttled HTTP
    # ------------------------------------------------------------------

    async def _throttled_get(self, url: str) -> tuple[int, str]:
        """Polite scraping: enforce minimum 10-second gap between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 10:
            await asyncio.sleep(10 - elapsed)

        session = await self._ensure_session()
        headers = {
            "User-Agent": USER_AGENT,
            # Explicitly avoid brotli ('br') — aiohttp cannot decode it without
            # the optional Brotli package, which causes a 400 ContentEncodingError.
            "Accept-Encoding": SAFE_ACCEPT_ENCODING,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            self._last_request_time = time.time()
            text = await resp.text()
            return resp.status, text

    # ------------------------------------------------------------------
    # fetch_raw
    # ------------------------------------------------------------------

    async def fetch_raw(self) -> dict:
        """Fetch Farside HTML, falling back to SoSoValue on failure."""
        # Try Farside first
        try:
            status, html = await self._throttled_get(FARSIDE_URL)
            http_health = self._detector.check_http(status, len(html))
            if http_health.status != "BROKEN":
                logger.info("farside_fetch_ok", status=status, length=len(html))
                return {"html": html, "source": "farside", "http_status": status}
            logger.warning(
                "farside_http_broken",
                status=status,
                reason=http_health.failure_reason,
            )
        except Exception as e:
            logger.warning("farside_fetch_failed", error=str(e))

        # Fallback: SoSoValue
        try:
            status, html = await self._throttled_get(SOSOVALUE_URL)
            logger.info("sosovalue_fetch_ok", status=status, length=len(html))
            return {"html": html, "source": "sosovalue", "http_status": status}
        except Exception as e:
            logger.error("sosovalue_fetch_failed", error=str(e))
            raise RuntimeError(f"Both Farside and SoSoValue failed: {e}") from e

    # ------------------------------------------------------------------
    # summarize
    # ------------------------------------------------------------------

    def summarize(self, raw: dict) -> dict:
        """Transform raw HTML data into LLM-ready ETF flow summary."""
        source_tag = raw.get("source", "farside")
        html = raw.get("html", "")
        http_status = raw.get("http_status", 200)

        # Layer 1: HTTP check
        http_health = self._detector.check_http(http_status, len(html))
        if http_health.status == "BROKEN":
            return self._broken_summary(source_tag, http_health.failure_reason or "HTTP error")

        # Layer 2–3: Structural parse + validation
        rows, struct_health = _parse_farside_html(html)

        if struct_health.status == "BROKEN" or not rows:
            return self._broken_summary(
                source_tag,
                struct_health.failure_reason or "No rows parsed",
            )

        # Build daily net series (rows[0] = most recent after reversal in parser)
        daily_nets: list[float] = []
        issuer_latest: dict[str, float] = {}
        cumulative = 0.0

        for row in rows:
            row_net = row.get("total")
            if row_net is None:
                row_net = sum(
                    v for k, v in row.items()
                    if k in KNOWN_TICKERS and isinstance(v, (int, float))
                )
            if isinstance(row_net, (int, float)):
                daily_nets.append(float(row_net))
                cumulative += float(row_net)

        # Per-issuer breakdown from most recent row (index 0)
        recent_row = rows[0] if rows else {}
        for ticker in KNOWN_TICKERS:
            v = recent_row.get(ticker)
            if v is not None:
                issuer_latest[ticker] = float(v)

        daily_net = daily_nets[0] if daily_nets else 0.0
        flows_7d = daily_nets[:7]

        inflow_issuers = sorted(
            [(t, v) for t, v in issuer_latest.items() if v > 0],
            key=lambda x: -x[1],
        )
        outflow_issuers = sorted(
            [(t, v) for t, v in issuer_latest.items() if v < 0],
            key=lambda x: x[1],
        )
        top_inflow = inflow_issuers[0][0] if inflow_issuers else None
        top_outflow = outflow_issuers[0][0] if outflow_issuers else None

        issuer_breakdown = [
            {"ticker": t, "flow_mn": round(v, 2), "aum_bn": 0}
            for t, v in sorted(issuer_latest.items(), key=lambda x: -x[1])
        ]

        streak = _calculate_streak(daily_nets)
        flow_trend = _classify_flow_trend(flows_7d)
        cumulative_bn = round(cumulative / 1000, 2)

        confidence = 0.90 if source_tag == "farside" else 0.70
        if struct_health.status == "DEGRADED":
            confidence = round(confidence * 0.7, 2)

        source_label = "Farside Investors" if source_tag == "farside" else "SoSoValue"
        narrative = _build_narrative(
            daily_net=daily_net,
            streak=streak,
            flow_trend=flow_trend,
            top_inflow=top_inflow,
            top_outflow=top_outflow,
            daily_nets=daily_nets,
        )

        return {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "source": source_label,
            "confidence": confidence,
            "daily_net_flow_usd_mn": round(daily_net, 2),
            "cumulative_flow_usd_bn": cumulative_bn,
            "flow_7d_trend": flow_trend,
            "flow_streak_days": streak,
            "top_inflow_issuer": top_inflow,
            "top_outflow_issuer": top_outflow,
            "issuer_breakdown": issuer_breakdown,
            "narrative": narrative,
        }

    def _broken_summary(self, source_tag: str, reason: str) -> dict:
        """Return a safe summary dict signalling a broken state."""
        source_label = "Farside Investors" if source_tag == "farside" else "SoSoValue"
        return {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "source": source_label,
            "confidence": 0.0,
            "daily_net_flow_usd_mn": None,
            "cumulative_flow_usd_bn": None,
            "flow_7d_trend": None,
            "flow_streak_days": 0,
            "top_inflow_issuer": None,
            "top_outflow_issuer": None,
            "issuer_breakdown": [],
            "narrative": f"⚠️ ETF 흐름 데이터 소스 장애: {reason}",
            "_error": True,
        }

    # ------------------------------------------------------------------
    # get_context — Fail-Loud entry point
    # ------------------------------------------------------------------

    async def get_context(self) -> tuple[dict, SourceHealth]:
        """Fetch + validate + summarize with Fail-Loud architecture.

        Returns (summary_dict, SourceHealth).  On BROKEN, emits an alert
        via AlertManager before returning.
        """
        try:
            raw = await self._cached_fetch()
            summary = self.summarize(raw)

            # If summarize() determined a broken state
            if summary.get("confidence", 1.0) == 0.0:
                health = SourceHealth(
                    status="BROKEN",
                    failure_reason=summary.get("narrative", "Parse failed"),
                    failure_stage="structural",
                )
                self._emit_alert(
                    severity="critical",
                    failure_stage="structural",
                    error_details={"reason": health.failure_reason or "unknown"},
                    url=FARSIDE_URL,
                )
                return summary, health

            # Layer 4: sanity check on output values
            sanity_health = self._detector.check_value_sanity(summary, SANITY_RANGES)

            http_health = SourceHealth(status="HEALTHY", failure_stage="http")
            final_health = self._detector.combine(http_health, sanity_health)
            final_health.last_success_at = datetime.now(timezone.utc)
            final_health.fields_available = list(summary.keys())

            if final_health.status == "BROKEN":
                self._emit_alert(
                    severity="critical",
                    failure_stage=final_health.failure_stage or "sanity",
                    error_details={"reason": final_health.failure_reason or "sanity check"},
                )
            elif final_health.status == "DEGRADED":
                self._emit_alert(
                    severity="warning",
                    failure_stage=final_health.failure_stage or "sanity",
                    error_details={"reason": final_health.failure_reason or "degraded"},
                )

            logger.info(
                "etf_flows_context_ready",
                status=final_health.status,
                confidence=summary.get("confidence"),
                daily_net=summary.get("daily_net_flow_usd_mn"),
            )
            return summary, final_health

        except Exception as e:
            logger.error("etf_flows_failed", error=str(e))
            health = SourceHealth(
                status="BROKEN",
                failure_reason=str(e)[:500],
                failure_stage="unhandled",
            )
            self._emit_alert(
                severity="critical",
                failure_stage="unhandled",
                error_details={"exception": str(e)[:300]},
                url=FARSIDE_URL,
            )
            return self._fallback_context(str(e)), health

    def _emit_alert(
        self,
        severity: str,
        failure_stage: str,
        error_details: dict,
        url: str | None = None,
    ) -> None:
        """Emit alert via AlertManager. Never raises."""
        try:
            from alert_manager import AlertManager  # type: ignore[import]

            AlertManager().emit(
                source_name=self.SOURCE_NAME,
                severity=severity,  # type: ignore[arg-type]
                failure_stage=failure_stage,
                error_details=error_details,
                recovery_hints=RECOVERY_HINTS,
                url=url,
            )
        except Exception as exc:
            logger.debug("alert_emit_failed", error=str(exc))
