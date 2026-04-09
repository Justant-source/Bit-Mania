"""Institutional research reports via free RSS feeds — Fail-Loud architecture.

Fetches the three most recent research summaries from free RSS feeds and
extracts title, description, and inferred sentiment for LLM context.
No API keys required. No article body scraping in free mode.

Feeds (no key required):
  - Glassnode Insights: https://insights.glassnode.com/rss/
  - The Block:          https://www.theblock.co/rss.xml
  - CoinTelegraph:      https://cointelegraph.com/rss
  - Decrypt:            https://decrypt.co/feed
  - Bitcoin Magazine:   https://bitcoinmagazine.com/feed
  - Kraken Blog:        https://blog.kraken.com/feed

Validation rules:
  - All feeds fail             → BROKEN  + AlertManager alert
  - Partial feed failure       → DEGRADED, proceed with remaining feeds
  - No articles within 14 days → DEGRADED

Cache TTL: 21600 seconds (6 hours).

Dependencies:
    feedparser>=6.0,<7.0
    beautifulsoup4>=4.12,<5.0   (optional — improves HTML stripping)
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import structlog

from .base import BaseDataSource, SourceHealth
from .failure_detection import FailureDetector

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_NAME_VALUE = "research_rss"

# RSS feeds: (url, display_name)
RSS_FEEDS: list[tuple[str, str]] = [
    ("https://insights.glassnode.com/rss/", "Glassnode"),
    ("https://www.theblock.co/rss.xml", "The Block"),
    ("https://cointelegraph.com/rss", "CoinTelegraph"),
    ("https://decrypt.co/feed", "Decrypt"),
    ("https://bitcoinmagazine.com/feed", "Bitcoin Magazine"),
    ("https://blog.kraken.com/feed", "Kraken Blog"),
]

MAX_REPORTS_PER_FEED = 3   # newest articles per feed
CONTENT_TRUNCATE = 800     # characters to keep from description
BULLET_TRUNCATE = 5        # max bullet points per report
STALE_DAYS = 14            # articles older than this → DEGRADED

SANITY_RANGES: dict[str, tuple[float, float]] = {
    "confidence": (0.0, 1.0),
}

RECOVERY_HINTS: list[str] = [
    "브라우저로 RSS 피드 URL 직접 접속 확인",
    "RSS URL이 변경되었는지 사이트 소스에서 확인",
    "feedparser 패키지 설치 확인: pip install feedparser",
    "python scripts/health_check.py research_rss 실행하여 HEALTHY 확인",
]


# ---------------------------------------------------------------------------
# Module-level helpers (testable independently)
# ---------------------------------------------------------------------------

def _safe_import_feedparser():
    """Import feedparser lazily; returns None if not installed."""
    try:
        import feedparser  # type: ignore[import]
        return feedparser
    except ImportError:
        logger.warning(
            "feedparser_not_installed",
            hint="pip install feedparser to enable RSS parsing",
        )
        return None


def _safe_import_bs4():
    """Import BeautifulSoup lazily; returns None if not installed."""
    try:
        from bs4 import BeautifulSoup  # type: ignore[import]
        return BeautifulSoup
    except ImportError:
        return None


def _strip_html(text: str) -> str:
    """Strip HTML tags from text, preferring BeautifulSoup when available."""
    BeautifulSoup = _safe_import_bs4()
    if BeautifulSoup and text:
        try:
            soup = BeautifulSoup(text, "html.parser")
            text = soup.get_text(separator=" ")
        except Exception:
            pass
    # Fallback regex strip
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_feedparser_date(entry) -> str:
    """Extract YYYY-MM-DD from a feedparser entry."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            import time as _time
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            pass
    if hasattr(entry, "published") and entry.published:
        raw = entry.published
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:len(fmt) + 5].strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return raw[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _infer_sentiment(text: str) -> str:
    """Classify sentiment from combined title + description text.

    Returns 'bullish', 'bearish', or 'neutral'.
    """
    lower = text.lower()
    bullish_kw = [
        "bull", "rally", "surge", "upside", "accumulate",
        "inflow", "adoption", "강세", "매집", "상승", "긍정",
    ]
    bearish_kw = [
        "bear", "sell", "dump", "risk", "outflow", "correction",
        "crash", "약세", "하락", "매도", "위험",
    ]
    bull_hits = sum(1 for kw in bullish_kw if kw in lower)
    bear_hits = sum(1 for kw in bearish_kw if kw in lower)
    if bull_hits > bear_hits:
        return "bullish"
    if bear_hits > bull_hits:
        return "bearish"
    return "neutral"


def _extract_bullets(text: str, max_bullets: int = BULLET_TRUNCATE) -> list[str]:
    """Extract short, distinct sentences from plain text."""
    clean = _strip_html(text)
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    bullets = [s.strip() for s in sentences if 20 < len(s.strip()) < 200]
    return bullets[:max_bullets]


def _build_key_thesis(title: str, bullets: list[str]) -> str:
    """Create a ≤120-char thesis string."""
    if bullets:
        candidate = bullets[0]
        return candidate if len(candidate) <= 120 else candidate[:117] + "..."
    return title[:120]


def _is_stale(date_str: str) -> bool:
    """Return True if the article date is older than STALE_DAYS."""
    try:
        pub = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - pub) > timedelta(days=STALE_DAYS)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Data source class
# ---------------------------------------------------------------------------

class ResearchReportsSource(BaseDataSource):
    """Institutional research reports via free RSS feeds with Fail-Loud validation.

    Polls multiple RSS feeds in parallel.  On partial failure degrades gracefully.
    On total failure emits an AlertManager alert.
    """

    SOURCE_NAME = SOURCE_NAME_VALUE
    CACHE_KEY_PREFIX = "datasource"
    CACHE_TTL_SECONDS = 21600  # 6 hours

    def __init__(self, redis_client=None, http_session=None) -> None:
        super().__init__(redis_client=redis_client, http_session=http_session)
        self._detector = FailureDetector(source_name=self.SOURCE_NAME)

    # ------------------------------------------------------------------
    # Internal RSS fetcher
    # ------------------------------------------------------------------

    async def _fetch_rss_feed(
        self,
        session: aiohttp.ClientSession,
        url: str,
        publisher_name: str,
    ) -> tuple[list[dict], str | None]:
        """Fetch and parse one RSS feed.

        Returns (articles, error_message).  error_message is None on success.
        """
        feedparser = _safe_import_feedparser()
        if feedparser is None:
            return [], "feedparser not installed"

        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    return [], f"HTTP {resp.status} for {url}"
                raw_xml = await resp.text()
        except Exception as e:
            return [], f"Network error for {url}: {str(e)[:100]}"

        try:
            feed = feedparser.parse(raw_xml)
        except Exception as e:
            return [], f"feedparser parse error: {str(e)[:100]}"

        if not feed.entries:
            return [], f"No entries in feed: {url}"

        articles: list[dict] = []
        for entry in feed.entries[:MAX_REPORTS_PER_FEED]:
            # Prefer full content over summary
            content_raw = ""
            if hasattr(entry, "content") and entry.content:
                content_raw = entry.content[0].get("value", "")
            elif hasattr(entry, "summary"):
                content_raw = entry.summary or ""

            content_clean = _strip_html(content_raw)[:CONTENT_TRUNCATE]
            pub_date = _parse_feedparser_date(entry)

            articles.append({
                "title": getattr(entry, "title", ""),
                "publisher": publisher_name,
                "published_at": pub_date,
                "url": getattr(entry, "link", ""),
                "content": content_clean,
            })

        return articles, None

    # ------------------------------------------------------------------
    # fetch_raw
    # ------------------------------------------------------------------

    async def fetch_raw(self) -> dict:
        """Fetch up to MAX_REPORTS_PER_FEED articles from each RSS feed.

        Partial feed failures are recorded but do not abort the fetch.
        Total failure raises RuntimeError.
        """
        session = await self._ensure_session()
        all_articles: list[dict] = []
        feed_errors: dict[str, str] = {}
        feeds_ok: list[str] = []

        # Deduplicate feeds — try each unique URL once
        seen_urls: set[str] = set()
        deduped_feeds: list[tuple[str, str]] = []
        for url, name in RSS_FEEDS:
            if url not in seen_urls:
                deduped_feeds.append((url, name))
                seen_urls.add(url)

        for url, name in deduped_feeds:
            articles, err = await self._fetch_rss_feed(session, url, name)
            if err:
                logger.warning("rss_feed_failed", url=url, publisher=name, error=err)
                feed_errors[name] = err
            else:
                logger.info("rss_feed_ok", publisher=name, count=len(articles))
                all_articles.extend(articles)
                feeds_ok.append(name)

        if not all_articles:
            # All feeds failed
            raise RuntimeError(
                f"All RSS feeds failed: {feed_errors}"
            )

        # Sort by date descending, deduplicate by URL
        seen_article_urls: set[str] = set()
        unique_articles: list[dict] = []
        for art in sorted(all_articles, key=lambda a: a.get("published_at", ""), reverse=True):
            art_url = art.get("url", "")
            if art_url and art_url in seen_article_urls:
                continue
            if art_url:
                seen_article_urls.add(art_url)
            unique_articles.append(art)

        return {
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "_feeds_ok": feeds_ok,
            "_feed_errors": feed_errors,
            "reports": unique_articles,
        }

    # ------------------------------------------------------------------
    # summarize
    # ------------------------------------------------------------------

    def summarize(self, raw: dict) -> dict:
        """Transform raw RSS data into an LLM-ready research summary.

        Each report gets: title, publisher, date, URL, key_thesis,
        bullet_points, sentiment.  Consensus is derived by majority vote.
        """
        if not raw or raw.get("_error"):
            return self._fallback_context("Research report fetch failed")

        as_of: str = raw.get("_fetched_at", datetime.now(timezone.utc).isoformat())
        feeds_ok: list[str] = raw.get("_feeds_ok", [])
        feed_errors: dict[str, str] = raw.get("_feed_errors", {})
        raw_reports: list[dict] = raw.get("reports", [])

        if not raw_reports:
            return {
                "as_of": as_of,
                "source": "RSS",
                "confidence": 0.20,
                "reports": [],
                "consensus_view": "neutral",
                "narrative": "리서치 리포트를 가져오지 못했습니다.",
            }

        summarized: list[dict[str, Any]] = []
        sentiments: list[str] = []
        stale_count = 0

        for rpt in raw_reports[:MAX_REPORTS_PER_FEED * len(feeds_ok or ["x"])]:
            title = rpt.get("title", "")
            content = rpt.get("content", "")
            pub_date = rpt.get("published_at", "")

            bullets = _extract_bullets(content)
            key_thesis = _build_key_thesis(title, bullets)
            sentiment = _infer_sentiment(f"{title} {content}")
            sentiments.append(sentiment)

            if _is_stale(pub_date):
                stale_count += 1

            summarized.append({
                "title": title,
                "publisher": rpt.get("publisher", "Unknown"),
                "published_at": pub_date,
                "url": rpt.get("url", ""),
                "key_thesis": key_thesis,
                "bullet_points": bullets,
                "sentiment": sentiment,
            })

        # Consensus by majority vote
        bull = sentiments.count("bullish")
        bear = sentiments.count("bearish")
        if bull > bear:
            consensus = "bullish"
        elif bear > bull:
            consensus = "bearish"
        else:
            consensus = "neutral"

        # Confidence: base on feed success ratio
        total_feeds = len(feeds_ok) + len(feed_errors)
        feed_ratio = len(feeds_ok) / max(total_feeds, 1)
        confidence = round(0.45 + 0.45 * feed_ratio + 0.10 * min(len(summarized), 3) / 3, 2)
        confidence = min(0.90, confidence)

        # Downgrade if all articles are stale
        if stale_count == len(summarized) and summarized:
            confidence = round(confidence * 0.6, 2)

        source_label = ", ".join(feeds_ok) if feeds_ok else "RSS"
        sentiment_label = {"bullish": "강세", "bearish": "약세", "neutral": "중립"}.get(
            consensus, consensus
        )
        publisher_list = ", ".join({r["publisher"] for r in summarized})
        thesis_preview = summarized[0]["key_thesis"] if summarized else ""

        narrative = (
            f"{publisher_list} 리서치 {len(summarized)}건 수집. "
            f"컨센서스: {sentiment_label}. "
            f"주요 논거: {thesis_preview}"
        )
        if feed_errors:
            narrative += f" ({len(feed_errors)}개 피드 실패: {', '.join(feed_errors.keys())})"

        return {
            "as_of": as_of,
            "source": source_label,
            "confidence": confidence,
            "reports": summarized,
            "consensus_view": consensus,
            "narrative": narrative,
        }

    # ------------------------------------------------------------------
    # get_context — Fail-Loud entry point
    # ------------------------------------------------------------------

    async def get_context(self) -> tuple[dict, SourceHealth]:
        """Fetch + validate + summarize with Fail-Loud architecture.

        Returns (summary_dict, SourceHealth).

        Health rules:
          - All feeds fail           → BROKEN  + alert
          - Partial feed failure     → DEGRADED
          - All articles stale       → DEGRADED
          - Normal success           → HEALTHY
        """
        try:
            raw = await self._cached_fetch()
            summary = self.summarize(raw)

            feeds_ok: list[str] = raw.get("_feeds_ok", [])
            feed_errors: dict[str, str] = raw.get("_feed_errors", {})
            reports: list[dict] = summary.get("reports", [])

            # Determine health from fetch metadata
            if not feeds_ok and feed_errors:
                # Total failure — should have raised in fetch_raw, but handle defensively
                health = SourceHealth(
                    status="BROKEN",
                    failure_reason=f"All RSS feeds failed: {list(feed_errors.keys())}",
                    failure_stage="structural",
                )
                self._emit_alert(
                    severity="critical",
                    failure_stage="structural",
                    error_details={"failed_feeds": str(feed_errors)},
                )
                return summary, health

            healths: list[SourceHealth] = []

            # Partial feed failure → DEGRADED
            if feed_errors:
                healths.append(SourceHealth(
                    status="DEGRADED",
                    failure_reason=f"Feeds failed: {list(feed_errors.keys())}",
                    failure_stage="structural",
                    fields_missing=list(feed_errors.keys()),
                    fields_available=feeds_ok,
                ))

            # Staleness check
            if reports:
                all_stale = all(_is_stale(r.get("published_at", "")) for r in reports)
                if all_stale:
                    healths.append(SourceHealth(
                        status="DEGRADED",
                        failure_reason=f"All {len(reports)} articles are older than {STALE_DAYS} days",
                        failure_stage="schema",
                    ))

            # Sanity check on output
            sanity_health = self._detector.check_value_sanity(summary, SANITY_RANGES)
            healths.append(sanity_health)

            # Base HTTP health
            healths.append(SourceHealth(status="HEALTHY", failure_stage="http"))

            final_health = self._detector.combine(*healths)
            final_health.last_success_at = datetime.now(timezone.utc)
            final_health.fields_available = [r["publisher"] for r in reports]

            if final_health.status == "DEGRADED":
                self._emit_alert(
                    severity="warning",
                    failure_stage=final_health.failure_stage or "structural",
                    error_details={"reason": final_health.failure_reason or "degraded"},
                )

            logger.info(
                "research_rss_context_ready",
                status=final_health.status,
                confidence=summary.get("confidence"),
                report_count=len(reports),
                feeds_ok=feeds_ok,
            )
            return summary, final_health

        except Exception as e:
            logger.error("research_rss_failed", error=str(e))
            health = SourceHealth(
                status="BROKEN",
                failure_reason=str(e)[:500],
                failure_stage="unhandled",
            )
            self._emit_alert(
                severity="critical",
                failure_stage="unhandled",
                error_details={"exception": str(e)[:300]},
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
            from services.llm_advisor.alert_manager import AlertManager  # type: ignore[import]

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
