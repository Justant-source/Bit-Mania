"""LLM Advisor — entry point.

Schedules periodic analysis every 4 hours and subscribes to the
llm:request Redis channel for on-demand analysis requests.
Full analysis reports are persisted to the ``llm_reports`` table
for dashboard browsing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import structlog
import yaml

from shared.log_events import *
from shared.logging_config import setup_logging
from shared.log_writer import init_log_writer, close_log_writer
from services.llm_advisor.agent_graph import TradingAnalysisGraph
from services.llm_advisor.claude_bridge import ClaudeCodeBridge
from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.prompt_templates.asset_report import ASSET_REPORT_PROMPT
from services.llm_advisor.reflection import DailyReflection
from services.llm_advisor.vision_chart import ChartAnalyzer
from shared.db.connection import create_pool, close_pool, get_pool

log = structlog.get_logger(__name__)

SERVICE_NAME = "llm-advisor"
ANALYSIS_INTERVAL_HOURS = 6
REQUEST_CHANNEL = "llm:request"
ADVISORY_CHANNEL = "llm:advisory"


def _load_config() -> dict[str, Any]:
    """Load LLM advisor configuration."""
    config_path = os.getenv("CONFIG_PATH", "/app/config/orchestrator.yaml")
    try:
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        cfg = {}

    cfg.setdefault("redis", {})["url"] = os.getenv("REDIS_URL", "redis://localhost:6379")
    cfg["claude_code_path"] = os.getenv("CLAUDE_CODE_PATH", "/usr/local/bin/claude")
    return cfg


class LLMAdvisorService:
    """Manages the LLM advisory lifecycle."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._redis_url = config.get("redis", {}).get("url", "redis://localhost:6379")
        self._redis: Any = None

        self._bridge = ClaudeCodeBridge(config.get("claude_code_path", "claude"))
        self._model_manager = ModelManager(self._bridge)
        self._chart_analyzer = ChartAnalyzer()
        self._analysis_graph: TradingAnalysisGraph | None = None
        self._reflection: DailyReflection | None = None

        self._running = False
        self._analysis_task: asyncio.Task[None] | None = None
        self._request_task: asyncio.Task[None] | None = None
        self._reflection_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize connections and start scheduled tasks."""
        import redis.asyncio as aioredis

        # Database pool must be created before setup_logging so db_pool can be passed
        db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://cryptoengine:cryptoengine@localhost:5432/cryptoengine",
        )
        await create_pool(dsn=db_url)
        pool = get_pool()
        await init_log_writer(SERVICE_NAME, pool)
        setup_logging(service_name=SERVICE_NAME, db_pool=pool)

        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        log.info(REDIS_CONNECTED, message="LLM Advisor Redis 연결 성공")
        log.info(DB_POOL_CREATED, message="LLM Advisor DB 풀 생성 완료")

        self._analysis_graph = TradingAnalysisGraph(
            self._model_manager, self._chart_analyzer
        )
        self._reflection = DailyReflection(self._redis)

        self._running = True
        self._analysis_task = asyncio.create_task(
            self._scheduled_analysis_loop(), name="scheduled-analysis"
        )
        self._request_task = asyncio.create_task(
            self._subscribe_requests(), name="request-subscriber"
        )
        self._reflection_task = asyncio.create_task(
            self._daily_reflection_loop(), name="daily-reflection"
        )
        log.info(SERVICE_STARTED, message="LLM Advisor 서비스 시작")

    async def stop(self) -> None:
        """Shut down all tasks."""
        self._running = False
        for task in (self._analysis_task, self._request_task, self._reflection_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._redis:
            await self._redis.aclose()
        log.info(SERVICE_STOPPED, message="LLM Advisor 서비스 종료")
        await close_pool()
        await close_log_writer()

    async def _scheduled_analysis_loop(self) -> None:
        """Run full analysis every 6 hours."""
        while self._running:
            try:
                await self._run_analysis("scheduled")
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(LLM_API_ERROR, message="정기 분석 오류")
            await asyncio.sleep(ANALYSIS_INTERVAL_HOURS * 3600)

    async def _subscribe_requests(self) -> None:
        """Listen for on-demand analysis requests on Redis pub/sub."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(REQUEST_CHANNEL)
        log.info(REDIS_CONNECTED, message="LLM 요청 채널 구독 시작", channel=REQUEST_CHANNEL)

        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                try:
                    request = json.loads(message["data"])
                    trigger = request.get("trigger", "on_demand")
                    await self._run_analysis(trigger, request)
                except json.JSONDecodeError:
                    log.warning(LLM_API_ERROR, message="잘못된 요청 데이터")
                except Exception:
                    log.exception(LLM_API_ERROR, message="온디맨드 분석 오류")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(REQUEST_CHANNEL)
            await pubsub.aclose()

    async def _run_analysis(
        self, trigger: str, request: dict[str, Any] | None = None
    ) -> None:
        """Execute the full analysis pipeline, persist report, and publish advisory."""
        log.info(LLM_ANALYSIS_START, message="분석 시작", trigger=trigger)

        # Gather market context from DB (primary) + Redis (fallback)
        context = await self._gather_market_context()
        if not context:
            log.warning(LLM_API_ERROR, message="시장 컨텍스트 없음")
            return

        log.info(
            LLM_ANALYSIS_START,
            message="컨텍스트 수집 완료",
            btc_price=context.get("btc_price", {}).get("last") if isinstance(context.get("btc_price"), dict) else None,
            regime=context.get("regime", {}).get("current") if isinstance(context.get("regime"), dict) else None,
            positions=len(context.get("open_positions", [])),
        )

        # Run the analysis graph
        assert self._analysis_graph is not None
        result = await self._analysis_graph.run(context)

        if result is None:
            log.warning(LLM_API_ERROR, message="분석 결과 없음")
            return

        rating = result.get("rating", "hold")
        confidence = result.get("confidence", 0.0)

        # Publish advisory to orchestrator
        advisory = {
            "rating": rating,
            "confidence": confidence,
            "weight_adjustments": result.get("weight_adjustments", {}),
            "reasoning": result.get("reasoning", ""),
            "regime_assessment": result.get("regime_assessment", ""),
            "trigger": trigger,
        }

        await self._redis.publish(ADVISORY_CHANNEL, json.dumps(advisory))
        await self._redis.set(
            "llm:latest_advisory", json.dumps(advisory), ex=21600  # 6 hours
        )

        # Generate 6-hour Korean narrative asset report
        asset_report = await self._generate_asset_report(result, context)

        # Persist full report to DB
        await self._save_report(trigger, result, context, asset_report)

        # Write HTML report file
        await self._write_html_report(result, context, asset_report)

        log.info(
            LLM_ANALYSIS_COMPLETE,
            message="분석 완료",
            rating=advisory["rating"],
            confidence=advisory["confidence"],
        )

    async def _generate_asset_report(
        self,
        result: dict[str, Any],
        context: dict[str, Any],
    ) -> str | None:
        """Generate a comprehensive 6-hour Korean narrative asset report."""
        try:
            # Extract BTC price
            ticker = context.get("btc_price", {})
            if isinstance(ticker, dict):
                btc_price = ticker.get("last") or ticker.get("close") or "N/A"
            else:
                btc_price = ticker or "N/A"

            # Extract 24h change from features
            features = context.get("features", {})
            price_change_24h = features.get("price_change_24h", "N/A")
            if isinstance(price_change_24h, float):
                price_change_24h = f"{price_change_24h:.2f}"

            # Extract funding rate
            funding = context.get("funding_rate", {})
            if isinstance(funding, dict):
                funding_rate = funding.get("rate") or funding.get("funding_rate") or "N/A"
            else:
                funding_rate = funding or "N/A"
            if isinstance(funding_rate, float):
                funding_rate = f"{funding_rate:.6f}"

            regime = result.get("regime_assessment") or context.get("regime", "unknown")
            rating = result.get("rating", "hold")
            confidence = result.get("confidence", 0.0)

            portfolio = context.get("portfolio", {})
            if isinstance(portfolio, dict):
                portfolio_state = json.dumps(portfolio, ensure_ascii=False, indent=2)
            else:
                portfolio_state = str(portfolio) if portfolio else "데이터 없음"

            def _fmt(d: Any) -> str:
                if not d:
                    return "데이터 없음"
                if isinstance(d, str):
                    return d
                return json.dumps(d, ensure_ascii=False)

            prompt = ASSET_REPORT_PROMPT.format(
                btc_price=btc_price,
                price_change_24h=price_change_24h,
                funding_rate=funding_rate,
                regime=regime,
                technical_report=_fmt(result.get("_technical_report")),
                sentiment_report=_fmt(result.get("_sentiment_report")),
                bull_argument=_fmt(result.get("_bull_argument")),
                bear_argument=_fmt(result.get("_bear_argument")),
                debate_conclusion=_fmt(result.get("_debate_conclusion")),
                risk_assessment=_fmt(result.get("_risk_assessment")),
                rating=rating,
                confidence=f"{confidence:.2f}",
                portfolio_state=portfolio_state,
            )

            report_data = await self._model_manager.invoke(prompt)
            if report_data is None:
                log.warning(LLM_API_ERROR, message="자산 리포트 생성 결과 없음")
                return None

            # Return full JSON as string for storage; full_report is the narrative
            return json.dumps(report_data, ensure_ascii=False)

        except Exception:
            log.exception(LLM_API_ERROR, message="자산 리포트 생성 오류")
            return None

    async def _save_report(
        self,
        trigger: str,
        result: dict[str, Any],
        context: dict[str, Any],
        asset_report: str | None = None,
    ) -> None:
        """Save the full analysis report to the llm_reports table."""
        try:
            pool = get_pool()
        except RuntimeError:
            log.warning(SERVICE_HEALTH_FAIL, message="DB 풀 없음, 리포트 저장 생략")
            return

        rating = result.get("rating", "hold")[:20]
        confidence = result.get("confidence", 0.0)
        regime_raw = result.get("regime_assessment", "") or ""
        # Normalize regime to a known short label or truncate
        _KNOWN_REGIMES = {"ranging", "trending_up", "trending_down", "volatile", "unknown"}
        regime = regime_raw if regime_raw in _KNOWN_REGIMES else regime_raw[:20]
        reasoning = result.get("reasoning", "")

        # Extract BTC price from context
        btc_price = None
        ticker = context.get("btc_price")
        if isinstance(ticker, dict):
            btc_price = ticker.get("last") or ticker.get("close")
        elif isinstance(ticker, (int, float)):
            btc_price = ticker

        # Build title (max 200 chars for varchar column)
        utc_now = datetime.now(timezone.utc)
        regime_short = (regime or "N/A")[:40]
        title = f"[{rating.upper()}] {regime_short} — {utc_now:%Y-%m-%d %H:%M} UTC"
        title = title[:200]

        def _summarise(d: dict | Any) -> str | None:
            if not d:
                return None
            if isinstance(d, str):
                return d
            return json.dumps(d, ensure_ascii=False, default=str)

        try:
            await pool.execute(
                """
                INSERT INTO llm_reports (
                    title, trigger, rating, confidence, regime, symbol,
                    btc_price, technical_summary, sentiment_summary,
                    bull_summary, bear_summary, debate_conclusion,
                    risk_assessment, reasoning, weight_adjustments, risk_flags,
                    asset_report
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14, $15, $16,
                    $17
                )
                """,
                title,
                trigger,
                rating,
                confidence,
                regime or None,
                "BTCUSDT",
                btc_price,
                _summarise(result.get("_technical_report")),
                _summarise(result.get("_sentiment_report")),
                _summarise(result.get("_bull_argument")),
                _summarise(result.get("_bear_argument")),
                _summarise(result.get("_debate_conclusion")),
                _summarise(result.get("_risk_assessment")),
                reasoning or None,
                json.dumps(result.get("weight_adjustments", {})),
                json.dumps(result.get("risk_flags", [])),
                asset_report,
            )
            log.info(LLM_ANALYSIS_COMPLETE, message="LLM 리포트 저장 완료", title=title)
        except Exception as exc:
            log.exception(
                LLM_API_ERROR,
                message="LLM 리포트 저장 오류",
                error=str(exc),
                title=title,
                confidence_type=type(confidence).__name__,
                btc_price_type=type(btc_price).__name__,
            )

    async def _write_html_report(
        self,
        result: dict[str, Any],
        context: dict[str, Any],
        asset_report: str | None,
    ) -> None:
        """Write analysis result as a standalone HTML report file."""
        import pathlib

        try:
            reports_dir = pathlib.Path("/app/reports")
            reports_dir.mkdir(parents=True, exist_ok=True)

            utc_now = datetime.now(timezone.utc)
            rating = result.get("rating", "hold")
            confidence = result.get("confidence", 0.0)
            reasoning = result.get("reasoning", "")

            # Parse asset_report JSON for full_report markdown
            full_report_md = ""
            sections: dict[str, str] = {}
            if asset_report:
                try:
                    ar = json.loads(asset_report) if isinstance(asset_report, str) else asset_report
                    full_report_md = ar.get("full_report", "")
                    for key in ("price_drivers", "regime_rationale", "strategy_view",
                                "portfolio_summary", "risk_alert"):
                        if ar.get(key):
                            sections[key] = ar[key]
                    watchpoints = ar.get("key_watchpoints", [])
                except (json.JSONDecodeError, AttributeError):
                    full_report_md = str(asset_report)
                    watchpoints = []
            else:
                watchpoints = []

            # Convert markdown to simple HTML (headings, bold, lists, paragraphs)
            import re

            def _md_to_html(md: str) -> str:
                lines = md.split("\n")
                html_lines: list[str] = []
                in_list = False
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith("### "):
                        if in_list:
                            html_lines.append("</ul>")
                            in_list = False
                        html_lines.append(f"<h3>{stripped[4:]}</h3>")
                    elif stripped.startswith("## "):
                        if in_list:
                            html_lines.append("</ul>")
                            in_list = False
                        html_lines.append(f"<h2>{stripped[3:]}</h2>")
                    elif stripped.startswith("- "):
                        if not in_list:
                            html_lines.append("<ul>")
                            in_list = True
                        html_lines.append(f"<li>{stripped[2:]}</li>")
                    elif stripped == "":
                        if in_list:
                            html_lines.append("</ul>")
                            in_list = False
                    else:
                        if in_list:
                            html_lines.append("</ul>")
                            in_list = False
                        # Bold
                        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", stripped)
                        html_lines.append(f"<p>{text}</p>")
                if in_list:
                    html_lines.append("</ul>")
                return "\n".join(html_lines)

            report_html = _md_to_html(full_report_md) if full_report_md else ""

            # Rating color
            rating_colors = {
                "strong_buy": "#22c55e", "buy": "#4ade80",
                "hold": "#fbbf24",
                "sell": "#f87171", "strong_sell": "#ef4444",
            }
            color = rating_colors.get(rating, "#6b7280")

            # Watchpoints HTML
            wp_html = ""
            if watchpoints:
                wp_items = "".join(f"<li>{w}</li>" for w in watchpoints)
                wp_html = f"<h3>주목할 변수</h3><ul>{wp_items}</ul>"

            # Risk alert HTML
            risk_html = ""
            if sections.get("risk_alert"):
                risk_html = f'<div class="risk-alert">{sections["risk_alert"]}</div>'

            html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CryptoEngine LLM Report — {utc_now:%Y-%m-%d %H:%M} UTC</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f172a; color: #e2e8f0; line-height: 1.7; padding: 2rem; max-width: 900px; margin: 0 auto; }}
  h1 {{ color: #f8fafc; margin-bottom: 0.5rem; font-size: 1.6rem; }}
  h2 {{ color: #94a3b8; font-size: 1.2rem; margin: 1.8rem 0 0.8rem; border-bottom: 1px solid #334155; padding-bottom: 0.3rem; }}
  h3 {{ color: #cbd5e1; font-size: 1.05rem; margin: 1.4rem 0 0.6rem; }}
  p {{ margin-bottom: 0.8rem; }}
  ul {{ margin-left: 1.5rem; margin-bottom: 1rem; }}
  li {{ margin-bottom: 0.4rem; }}
  .header {{ display: flex; align-items: center; gap: 1.5rem; margin-bottom: 2rem;
             padding: 1.2rem; background: #1e293b; border-radius: 12px; border: 1px solid #334155; }}
  .rating {{ font-size: 2rem; font-weight: 700; color: {color}; text-transform: uppercase; }}
  .meta {{ color: #94a3b8; font-size: 0.9rem; }}
  .meta span {{ display: block; }}
  .confidence {{ font-size: 1.3rem; font-weight: 600; color: {color}; }}
  .risk-alert {{ background: #451a1a; border: 1px solid #7f1d1d; border-radius: 8px;
                 padding: 1rem; margin: 1.5rem 0; color: #fca5a5; }}
  .section {{ background: #1e293b; border-radius: 8px; padding: 1.2rem; margin: 1rem 0; border: 1px solid #334155; }}
  .timestamp {{ color: #64748b; font-size: 0.85rem; text-align: center; margin-top: 2rem; }}
  strong {{ color: #f1f5f9; }}
</style>
</head>
<body>
<div class="header">
  <div>
    <div class="rating">{rating.upper()}</div>
    <div class="confidence">신뢰도 {confidence*100:.1f}%</div>
  </div>
  <div class="meta">
    <span>CryptoEngine LLM Analysis Report</span>
    <span>{utc_now:%Y-%m-%d %H:%M} UTC (KST {utc_now.hour+9:02d}:{utc_now.minute:02d})</span>
    <span>Trigger: scheduled</span>
  </div>
</div>

{risk_html}

<div class="section">
{report_html if report_html else f"<p>{reasoning}</p>"}
</div>

{wp_html}

<div class="timestamp">Generated by CryptoEngine LLM Advisor — Claude Code CLI Bridge</div>
</body>
</html>"""

            # Write latest + timestamped archive
            (reports_dir / "latest.html").write_text(html, encoding="utf-8")
            ts_name = f"report_{utc_now:%Y%m%d_%H%M}.html"
            (reports_dir / ts_name).write_text(html, encoding="utf-8")

            # Write index page listing all reports
            report_files = sorted(reports_dir.glob("report_*.html"), reverse=True)
            index_items = "".join(
                f'<li><a href="{f.name}">{f.stem.replace("report_", "")}</a></li>'
                for f in report_files[:30]
            )
            index_html = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>LLM Reports</title>
<style>body{{font-family:sans-serif;background:#0f172a;color:#e2e8f0;padding:2rem;max-width:600px;margin:0 auto}}
a{{color:#60a5fa;text-decoration:none}}a:hover{{text-decoration:underline}}
li{{margin:0.5rem 0}}h1{{margin-bottom:1rem}}</style></head>
<body><h1>LLM Analysis Reports</h1>
<p><a href="latest.html">Latest Report</a></p>
<h2>Archive</h2><ul>{index_items}</ul></body></html>"""
            (reports_dir / "index.html").write_text(index_html, encoding="utf-8")

            log.info(LLM_ANALYSIS_COMPLETE, message="HTML 리포트 생성 완료", file=ts_name)
        except Exception:
            log.exception(LLM_API_ERROR, message="HTML 리포트 생성 오류")

    async def _gather_market_context(self) -> dict[str, Any]:
        """Collect comprehensive market data from DB (primary) + Redis (fallback)."""
        context: dict[str, Any] = {}

        try:
            pool = get_pool()
        except RuntimeError:
            pool = None

        # ── 1. BTC Price (DB: ohlcv_history) ──
        if pool:
            try:
                row = await pool.fetchrow(
                    "SELECT close, high, low, volume, timestamp "
                    "FROM ohlcv_history WHERE symbol='BTCUSDT' AND timeframe='1m' "
                    "ORDER BY timestamp DESC LIMIT 1"
                )
                if row:
                    context["btc_price"] = {
                        "last": float(row["close"]),
                        "high_1m": float(row["high"]),
                        "low_1m": float(row["low"]),
                        "volume_1m": float(row["volume"]),
                        "timestamp": row["timestamp"].isoformat(),
                    }
                # 24h price change
                row24 = await pool.fetchrow(
                    "SELECT close FROM ohlcv_history "
                    "WHERE symbol='BTCUSDT' AND timeframe='1m' "
                    "AND timestamp <= NOW() - INTERVAL '24 hours' "
                    "ORDER BY timestamp DESC LIMIT 1"
                )
                if row24 and row:
                    old_price = float(row24["close"])
                    new_price = float(row["close"])
                    if old_price > 0:
                        context["price_change_24h_pct"] = round((new_price - old_price) / old_price * 100, 2)
            except Exception:
                pass

        # ── 2. Funding Rate (DB: funding_rate_history) ──
        if pool:
            try:
                frow = await pool.fetchrow(
                    "SELECT rate, timestamp FROM funding_rate_history "
                    "WHERE symbol='BTCUSDT' ORDER BY timestamp DESC LIMIT 1"
                )
                if frow:
                    rate = float(frow["rate"])
                    context["funding_rate"] = {
                        "rate": rate,
                        "rate_pct": f"{rate * 100:.4f}%",
                        "annualized_pct": round(rate * 3 * 365 * 100, 1),
                        "timestamp": frow["timestamp"].isoformat(),
                    }
                # Recent 4 rates for trend
                frows = await pool.fetch(
                    "SELECT rate, timestamp FROM funding_rate_history "
                    "WHERE symbol='BTCUSDT' ORDER BY timestamp DESC LIMIT 4"
                )
                if frows:
                    context["funding_rate_history_4"] = [
                        {"rate": float(r["rate"]), "ts": r["timestamp"].isoformat()} for r in frows
                    ]
            except Exception:
                pass

        # ── 3. Market Regime (DB: market_regime_history) ──
        if pool:
            try:
                rrow = await pool.fetchrow(
                    "SELECT regime, confidence, detected_at, indicators, change_reason, consecutive_count "
                    "FROM market_regime_history WHERE is_confirmed=TRUE "
                    "ORDER BY detected_at DESC LIMIT 1"
                )
                if rrow:
                    indicators = rrow["indicators"] or {}
                    context["regime"] = {
                        "current": rrow["regime"],
                        "label": rrow["regime"],  # backward compat for agents
                        "confidence": float(rrow["confidence"]),
                        "detected_at": rrow["detected_at"].isoformat(),
                        "consecutive_count": rrow["consecutive_count"],
                        "change_reason": rrow["change_reason"],
                        "adx": indicators.get("adx"),
                        "bb_width": indicators.get("bb_width"),
                        "rsi_14": indicators.get("rsi_14"),
                        "atr_14": indicators.get("atr_14"),
                    }
                # Recent regime transitions (last 24h)
                rtrans = await pool.fetch(
                    "SELECT regime, confidence, detected_at FROM market_regime_history "
                    "WHERE is_confirmed=TRUE AND detected_at > NOW() - INTERVAL '24 hours' "
                    "ORDER BY detected_at DESC LIMIT 10"
                )
                if rtrans:
                    context["regime_transitions_24h"] = [
                        {"regime": r["regime"], "confidence": float(r["confidence"]),
                         "at": r["detected_at"].isoformat()} for r in rtrans
                    ]
            except Exception:
                pass

        # ── 4. Portfolio State (DB: portfolio_snapshots) ──
        if pool:
            try:
                prow = await pool.fetchrow(
                    "SELECT total_equity, unrealized_pnl, daily_drawdown, weekly_drawdown, "
                    "monthly_drawdown, sharpe_ratio_30d, strategies, snapshot_at "
                    "FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1"
                )
                if prow:
                    context["portfolio"] = {
                        "total_equity_usd": float(prow["total_equity"]),
                        "unrealized_pnl": float(prow["unrealized_pnl"]),
                        "daily_drawdown_pct": round(float(prow["daily_drawdown"]) * 100, 2),
                        "weekly_drawdown_pct": round(float(prow["weekly_drawdown"]) * 100, 2),
                        "monthly_drawdown_pct": round(float(prow["monthly_drawdown"]) * 100, 2),
                        "sharpe_30d": float(prow["sharpe_ratio_30d"]) if prow["sharpe_ratio_30d"] else 0.0,
                        "snapshot_at": prow["snapshot_at"].isoformat(),
                    }
                    context["strategies"] = prow["strategies"]  # JSON array
            except Exception:
                pass

        # ── 5. Open Positions (DB: positions) ──
        if pool:
            try:
                pos_rows = await pool.fetch(
                    "SELECT strategy_id, symbol, side, size, entry_price, current_price, "
                    "unrealized_pnl, leverage, opened_at "
                    "FROM positions WHERE closed_at IS NULL ORDER BY opened_at DESC"
                )
                context["open_positions"] = [
                    {
                        "strategy": r["strategy_id"], "symbol": r["symbol"],
                        "side": r["side"], "size": float(r["size"]),
                        "entry_price": float(r["entry_price"]),
                        "current_price": float(r["current_price"]) if r["current_price"] else None,
                        "unrealized_pnl": float(r["unrealized_pnl"]) if r["unrealized_pnl"] else 0,
                        "leverage": float(r["leverage"]),
                        "opened_at": r["opened_at"].isoformat(),
                    } for r in pos_rows
                ]
            except Exception:
                context["open_positions"] = []

        # ── 6. Recent Trades (DB: trades, last 24h) ──
        if pool:
            try:
                trows = await pool.fetch(
                    "SELECT strategy_id, symbol, side, quantity, price, pnl, fee, status, filled_at "
                    "FROM trades WHERE filled_at > NOW() - INTERVAL '24 hours' "
                    "ORDER BY filled_at DESC LIMIT 20"
                )
                context["recent_trades_24h"] = [
                    {"strategy": r["strategy_id"], "symbol": r["symbol"], "side": r["side"],
                     "qty": float(r["quantity"]), "price": float(r["price"]),
                     "pnl": float(r["pnl"]) if r["pnl"] else 0,
                     "fee": float(r["fee"]) if r["fee"] else 0,
                     "status": r["status"], "at": r["filled_at"].isoformat()}
                    for r in trows
                ]
            except Exception:
                context["recent_trades_24h"] = []

        # ── 7. Kill Switch Status ──
        if pool:
            try:
                ks = await pool.fetchrow(
                    "SELECT level, reason, triggered_at, resolved_at "
                    "FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 1"
                )
                if ks:
                    context["kill_switch"] = {
                        "level": ks["level"],
                        "reason": ks["reason"],
                        "triggered_at": ks["triggered_at"].isoformat(),
                        "resolved_at": ks["resolved_at"].isoformat() if ks["resolved_at"] else None,
                        "active": ks["resolved_at"] is None,
                    }
                else:
                    context["kill_switch"] = {"active": False}
            except Exception:
                context["kill_switch"] = {"active": False}

        # ── 8. Strategy Configuration ──
        context["strategy_config"] = {
            "active_strategy": "funding-arb (fa80_lev5_r30)",
            "leverage": 5,
            "fa_capital_ratio": 0.75,
            "reinvest_ratio": 0.0,
            "phase5_mode": True,
            "sizing_mode": "fixed_notional",
            "fixed_notional_usd": 150,
            "max_concurrent_positions": 1,
            "entry_thresholds": {
                "min_funding_rate_8h": "0.012%",
                "min_annualized": "25%",
                "consecutive_intervals": 4,
                "bep_cycles": 2,
            },
            "fee_rates": {
                "spot_taker": "0.01%",
                "perp_taker": "0.055%",
                "round_trip": "0.13%",
            },
            "dca_status": "disabled (WF test failed: consistency 0.409, 2022 MDD -42%)",
        }

        # ── 9. Funding Payments (DB) ──
        if pool:
            try:
                fp_rows = await pool.fetch(
                    "SELECT SUM(payment) as total, COUNT(*) as count "
                    "FROM funding_payments WHERE collected_at > NOW() - INTERVAL '7 days'"
                )
                if fp_rows and fp_rows[0]["total"] is not None:
                    context["funding_income_7d"] = {
                        "total_usd": float(fp_rows[0]["total"]),
                        "count": fp_rows[0]["count"],
                    }
                else:
                    context["funding_income_7d"] = {"total_usd": 0, "count": 0}
            except Exception:
                context["funding_income_7d"] = {"total_usd": 0, "count": 0}

        # ── 10. Daily Report (today) ──
        if pool:
            try:
                dr = await pool.fetchrow(
                    "SELECT daily_pnl, daily_return, funding_income, trade_count, max_drawdown "
                    "FROM daily_reports WHERE date = CURRENT_DATE LIMIT 1"
                )
                if dr:
                    context["today_report"] = {
                        "daily_pnl": float(dr["daily_pnl"]) if dr["daily_pnl"] else 0,
                        "daily_return_pct": round(float(dr["daily_return"]) * 100, 3) if dr["daily_return"] else 0,
                        "funding_income": float(dr["funding_income"]) if dr["funding_income"] else 0,
                        "trade_count": dr["trade_count"] or 0,
                        "max_drawdown_pct": round(float(dr["max_drawdown"]) * 100, 3) if dr["max_drawdown"] else 0,
                    }
            except Exception:
                pass

        return context

    async def _daily_reflection_loop(self) -> None:
        """Run daily reflection at UTC 00:00."""
        while self._running:
            try:
                now = asyncio.get_event_loop().time()
                from datetime import datetime, timezone

                utc_now = datetime.now(timezone.utc)
                # Calculate seconds until next UTC midnight
                next_midnight = utc_now.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                if utc_now >= next_midnight:
                    from datetime import timedelta

                    next_midnight += timedelta(days=1)
                wait_seconds = (next_midnight - utc_now).total_seconds()
                await asyncio.sleep(wait_seconds)

                if self._reflection and self._running:
                    await self._reflection.run_daily_reflection()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(LLM_API_ERROR, message="일일 반성 오류")
                await asyncio.sleep(3600)


async def main() -> None:
    """Start the LLM Advisor service."""
    setup_logging(service_name=SERVICE_NAME)
    log = structlog.get_logger()
    config = _load_config()
    log.info(SERVICE_STARTED, message="LLM Advisor 서비스 시작 중")

    service = LLMAdvisorService(config)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info(SERVICE_STOPPING, message="종료 신호 수신")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        await service.start()
        await shutdown_event.wait()
    finally:
        await service.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
