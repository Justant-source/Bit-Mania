"""BacktestReportGenerator — HTML and Markdown report output.

Produces self-contained HTML reports with:
  * Equity curve chart (inline SVG)
  * Drawdown chart
  * Trade statistics table
  * Per-trade breakdown
  * Walk-forward window summary (when applicable)
  * Monte Carlo confidence intervals
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from freqtrade_bridge import BacktestResult
from walk_forward import MonteCarloResult, WalkForwardResult

log = structlog.get_logger(__name__)


class ReportGenerator:
    """Generate HTML and Markdown backtest reports."""

    def __init__(self, results_dir: Path | str = "/app/results") -> None:
        self._results_dir = Path(results_dir)
        self._results_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Simple backtest report
    # ------------------------------------------------------------------

    def generate_backtest_report(
        self,
        result: BacktestResult,
        strategy: str,
    ) -> Path:
        """Generate an HTML report for a single backtest run.

        Returns the path to the generated file.
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"backtest_{strategy}_{ts}.html"
        filepath = self._results_dir / filename

        html_content = self._render_backtest_html(result)
        filepath.write_text(html_content, encoding="utf-8")

        # Also write markdown summary
        md_path = filepath.with_suffix(".md")
        md_content = self._render_backtest_markdown(result)
        md_path.write_text(md_content, encoding="utf-8")

        # Write raw JSON
        json_path = filepath.with_suffix(".json")
        json_path.write_text(
            json.dumps(self._result_to_dict(result), indent=2, default=str),
            encoding="utf-8",
        )

        log.info(
            "backtest_report_generated",
            html=str(filepath),
            markdown=str(md_path),
            json=str(json_path),
        )
        return filepath

    # ------------------------------------------------------------------
    # Walk-forward report
    # ------------------------------------------------------------------

    def generate_walk_forward_report(
        self,
        result: WalkForwardResult,
        strategy: str,
    ) -> Path:
        """Generate an HTML report for walk-forward analysis."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"walkforward_{strategy}_{ts}.html"
        filepath = self._results_dir / filename

        html_content = self._render_walk_forward_html(result)
        filepath.write_text(html_content, encoding="utf-8")

        md_path = filepath.with_suffix(".md")
        md_content = self._render_walk_forward_markdown(result)
        md_path.write_text(md_content, encoding="utf-8")

        log.info("walk_forward_report_generated", path=str(filepath))
        return filepath

    # ==================================================================
    # HTML Rendering — Backtest
    # ==================================================================

    def _render_backtest_html(self, r: BacktestResult) -> str:
        equity_svg = self._svg_line_chart(
            r.equity_curve, title="Equity Curve", color="#2ecc71", height=250
        )
        dd_svg = self._svg_line_chart(
            r.drawdown_curve, title="Drawdown %", color="#e74c3c", height=180
        )

        trades_html = self._trades_table_html(r)

        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>CryptoEngine Backtest Report — {html.escape(r.strategy)}</title>
<style>
{_CSS}
</style>
</head>
<body>
<div class="container">
<h1>Backtest Report: {html.escape(r.strategy)}</h1>
<p class="meta">Period: {html.escape(r.start_date)} ~ {html.escape(r.end_date)}</p>

<div class="grid">
  <div class="card">
    <div class="card-label">Initial Capital</div>
    <div class="card-value">${r.initial_capital:,.2f}</div>
  </div>
  <div class="card">
    <div class="card-label">Final Capital</div>
    <div class="card-value">${r.final_capital:,.2f}</div>
  </div>
  <div class="card">
    <div class="card-label">Total Profit</div>
    <div class="card-value {'positive' if r.total_profit >= 0 else 'negative'}">{r.total_profit_pct:+.2f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Sharpe Ratio</div>
    <div class="card-value">{r.sharpe_ratio:.4f}</div>
  </div>
  <div class="card">
    <div class="card-label">Sortino Ratio</div>
    <div class="card-value">{r.sortino_ratio:.4f}</div>
  </div>
  <div class="card">
    <div class="card-label">Max Drawdown</div>
    <div class="card-value negative">{r.max_drawdown_pct:.2f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Win Rate</div>
    <div class="card-value">{r.win_rate:.1f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Total Trades</div>
    <div class="card-value">{r.total_trades}</div>
  </div>
  <div class="card">
    <div class="card-label">Profit Factor</div>
    <div class="card-value">{r.profit_factor:.2f}</div>
  </div>
</div>

<h2>Equity Curve</h2>
{equity_svg}

<h2>Drawdown</h2>
{dd_svg}

<h2>Trade Log</h2>
{trades_html}

<footer>Generated by CryptoEngine Backtester &mdash; {datetime.now(timezone.utc).isoformat()}</footer>
</div>
</body>
</html>"""

    # ==================================================================
    # HTML Rendering — Walk-Forward
    # ==================================================================

    def _render_walk_forward_html(self, r: WalkForwardResult) -> str:
        windows_html = self._windows_table_html(r)
        mc_html = self._monte_carlo_html(r.monte_carlo) if r.monte_carlo else ""

        alert_banner = ""
        if r.sharpe_alert:
            alert_banner = (
                '<div class="alert">'
                f'WARNING: Aggregate Sharpe ({r.aggregate_sharpe:.4f}) '
                f'is below the minimum threshold ({1.5}). '
                'Strategy may not be viable for live trading.'
                '</div>'
            )

        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Walk-Forward Report — {html.escape(r.strategy)}</title>
<style>
{_CSS}
</style>
</head>
<body>
<div class="container">
<h1>Walk-Forward Analysis: {html.escape(r.strategy)}</h1>
{alert_banner}

<div class="grid">
  <div class="card">
    <div class="card-label">Windows</div>
    <div class="card-value">{len(r.windows)}</div>
  </div>
  <div class="card">
    <div class="card-label">Avg OOS Sharpe</div>
    <div class="card-value">{r.aggregate_sharpe:.4f}</div>
  </div>
  <div class="card">
    <div class="card-label">Avg OOS Profit</div>
    <div class="card-value {'positive' if r.aggregate_profit_pct >= 0 else 'negative'}">{r.aggregate_profit_pct:+.2f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Max Drawdown</div>
    <div class="card-value negative">{r.aggregate_max_drawdown_pct:.2f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Consistency</div>
    <div class="card-value">{r.consistency_ratio:.0%}</div>
  </div>
  <div class="card">
    <div class="card-label">Total Trades</div>
    <div class="card-value">{r.aggregate_total_trades}</div>
  </div>
</div>

<h2>Per-Window Results</h2>
{windows_html}

{mc_html}

<footer>Generated by CryptoEngine Backtester &mdash; {datetime.now(timezone.utc).isoformat()}</footer>
</div>
</body>
</html>"""

    # ==================================================================
    # Markdown Rendering
    # ==================================================================

    def _render_backtest_markdown(self, r: BacktestResult) -> str:
        return f"""# Backtest Report: {r.strategy}

**Period:** {r.start_date} ~ {r.end_date}

## Summary

| Metric | Value |
|--------|-------|
| Initial Capital | ${r.initial_capital:,.2f} |
| Final Capital | ${r.final_capital:,.2f} |
| Total Profit | {r.total_profit_pct:+.2f}% |
| Sharpe Ratio | {r.sharpe_ratio:.4f} |
| Sortino Ratio | {r.sortino_ratio:.4f} |
| Max Drawdown | {r.max_drawdown_pct:.2f}% |
| Win Rate | {r.win_rate:.1f}% |
| Total Trades | {r.total_trades} |
| Profit Factor | {r.profit_factor:.2f} |
| Avg Trade Duration | {r.avg_trade_duration_hours:.1f}h |

## Trades

| # | Symbol | Side | Entry | Exit | PnL | Fee |
|---|--------|------|-------|------|-----|-----|
{self._trades_markdown_rows(r)}

---
*Generated by CryptoEngine Backtester*
"""

    def _render_walk_forward_markdown(self, r: WalkForwardResult) -> str:
        alert = ""
        if r.sharpe_alert:
            alert = (
                f"\n> **WARNING:** Aggregate Sharpe ({r.aggregate_sharpe:.4f}) "
                f"is below the 1.5 threshold.\n"
            )

        windows_md = "| # | Train Period | Test Period | OOS Profit % | OOS Sharpe | Trades |\n"
        windows_md += "|---|-------------|-------------|-------------|-----------|--------|\n"
        for i, w in enumerate(r.windows, 1):
            windows_md += (
                f"| {i} | {w.window.train_label} | {w.window.test_label} "
                f"| {w.test_result.total_profit_pct:+.2f}% "
                f"| {w.test_result.sharpe_ratio:.4f} "
                f"| {w.test_result.total_trades} |\n"
            )

        mc_md = ""
        if r.monte_carlo:
            mc = r.monte_carlo
            mc_md = f"""
## Monte Carlo ({mc.n_simulations} simulations)

| Metric | Mean | Std | 95% CI |
|--------|------|-----|--------|
| Sharpe | {mc.sharpe_mean:.4f} | {mc.sharpe_std:.4f} | [{mc.sharpe_ci_95[0]:.4f}, {mc.sharpe_ci_95[1]:.4f}] |
| Profit % | {mc.profit_mean:.2f} | {mc.profit_std:.2f} | [{mc.profit_ci_95[0]:.2f}, {mc.profit_ci_95[1]:.2f}] |
| Max DD % | {mc.max_dd_mean:.2f} | {mc.max_dd_std:.2f} | [{mc.max_dd_ci_95[0]:.2f}, {mc.max_dd_ci_95[1]:.2f}] |

Win Probability: **{mc.win_probability:.0%}**
"""

        return f"""# Walk-Forward Report: {r.strategy}
{alert}
## Aggregate Metrics

| Metric | Value |
|--------|-------|
| Windows | {len(r.windows)} |
| Avg OOS Sharpe | {r.aggregate_sharpe:.4f} |
| Avg OOS Profit | {r.aggregate_profit_pct:+.2f}% |
| Max Drawdown | {r.aggregate_max_drawdown_pct:.2f}% |
| Consistency Ratio | {r.consistency_ratio:.0%} |
| Total Trades | {r.aggregate_total_trades} |

## Per-Window Results

{windows_md}
{mc_md}
---
*Generated by CryptoEngine Backtester*
"""

    # ==================================================================
    # HTML building blocks
    # ==================================================================

    @staticmethod
    def _svg_line_chart(
        data: list[float],
        title: str = "",
        color: str = "#3498db",
        width: int = 800,
        height: int = 200,
    ) -> str:
        """Render a simple inline SVG line chart."""
        if not data or len(data) < 2:
            return f'<p class="no-data">No data for {html.escape(title)}</p>'

        min_val = min(data)
        max_val = max(data)
        val_range = max_val - min_val or 1.0

        padding = 10
        chart_w = width - 2 * padding
        chart_h = height - 2 * padding

        points: list[str] = []
        for i, val in enumerate(data):
            x = padding + (i / (len(data) - 1)) * chart_w
            y = padding + chart_h - ((val - min_val) / val_range) * chart_h
            points.append(f"{x:.1f},{y:.1f}")

        polyline = " ".join(points)

        return f"""<div class="chart">
<p class="chart-title">{html.escape(title)}</p>
<svg viewBox="0 0 {width} {height}" width="100%" preserveAspectRatio="xMidYMid meet">
  <rect width="{width}" height="{height}" fill="#1a1a2e" rx="4"/>
  <polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.5"/>
  <text x="{padding}" y="{padding + 12}" fill="#888" font-size="10">{max_val:,.2f}</text>
  <text x="{padding}" y="{height - padding}" fill="#888" font-size="10">{min_val:,.2f}</text>
</svg>
</div>"""

    @staticmethod
    def _trades_table_html(r: BacktestResult) -> str:
        if not r.trades:
            return '<p class="no-data">No trades executed.</p>'

        rows = ""
        for i, t in enumerate(r.trades, 1):
            pnl_class = "positive" if t.pnl >= 0 else "negative"
            rows += f"""<tr>
  <td>{i}</td>
  <td>{html.escape(t.symbol)}</td>
  <td>{t.side}</td>
  <td>{t.entry_price:,.2f}</td>
  <td>{t.exit_price:,.2f}</td>
  <td class="{pnl_class}">{t.pnl:+,.2f}</td>
  <td>{t.fee:,.4f}</td>
</tr>"""

        return f"""<table>
<thead><tr>
  <th>#</th><th>Symbol</th><th>Side</th>
  <th>Entry</th><th>Exit</th><th>PnL</th><th>Fee</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>"""

    @staticmethod
    def _windows_table_html(r: WalkForwardResult) -> str:
        if not r.windows:
            return '<p class="no-data">No walk-forward windows.</p>'

        rows = ""
        for i, w in enumerate(r.windows, 1):
            pnl_class = "positive" if w.test_result.total_profit_pct >= 0 else "negative"
            rows += f"""<tr>
  <td>{i}</td>
  <td>{html.escape(w.window.train_label)}</td>
  <td>{html.escape(w.window.test_label)}</td>
  <td>{w.train_result.total_profit_pct:+.2f}%</td>
  <td class="{pnl_class}">{w.test_result.total_profit_pct:+.2f}%</td>
  <td>{w.test_result.sharpe_ratio:.4f}</td>
  <td>{w.test_result.total_trades}</td>
</tr>"""

        return f"""<table>
<thead><tr>
  <th>#</th><th>Train Period</th><th>Test Period</th>
  <th>IS Profit</th><th>OOS Profit</th><th>OOS Sharpe</th><th>Trades</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>"""

    @staticmethod
    def _monte_carlo_html(mc: MonteCarloResult) -> str:
        return f"""<h2>Monte Carlo Analysis ({mc.n_simulations} simulations)</h2>
<table>
<thead><tr><th>Metric</th><th>Mean</th><th>Std</th><th>95% CI</th></tr></thead>
<tbody>
<tr><td>Sharpe</td><td>{mc.sharpe_mean:.4f}</td><td>{mc.sharpe_std:.4f}</td>
    <td>[{mc.sharpe_ci_95[0]:.4f}, {mc.sharpe_ci_95[1]:.4f}]</td></tr>
<tr><td>Profit %</td><td>{mc.profit_mean:.2f}</td><td>{mc.profit_std:.2f}</td>
    <td>[{mc.profit_ci_95[0]:.2f}, {mc.profit_ci_95[1]:.2f}]</td></tr>
<tr><td>Max DD %</td><td>{mc.max_dd_mean:.2f}</td><td>{mc.max_dd_std:.2f}</td>
    <td>[{mc.max_dd_ci_95[0]:.2f}, {mc.max_dd_ci_95[1]:.2f}]</td></tr>
</tbody>
</table>
<p>Win Probability: <strong>{mc.win_probability:.0%}</strong></p>"""

    @staticmethod
    def _trades_markdown_rows(r: BacktestResult) -> str:
        if not r.trades:
            return "| - | - | - | - | - | - | - |"
        lines = []
        for i, t in enumerate(r.trades, 1):
            lines.append(
                f"| {i} | {t.symbol} | {t.side} | {t.entry_price:,.2f} "
                f"| {t.exit_price:,.2f} | {t.pnl:+,.2f} | {t.fee:,.4f} |"
            )
        return "\n".join(lines)

    @staticmethod
    def _result_to_dict(r: BacktestResult) -> dict[str, Any]:
        return {
            "strategy": r.strategy,
            "start_date": r.start_date,
            "end_date": r.end_date,
            "initial_capital": r.initial_capital,
            "final_capital": r.final_capital,
            "total_profit": r.total_profit,
            "total_profit_pct": r.total_profit_pct,
            "max_drawdown": r.max_drawdown,
            "max_drawdown_pct": r.max_drawdown_pct,
            "sharpe_ratio": r.sharpe_ratio,
            "sortino_ratio": r.sortino_ratio,
            "win_rate": r.win_rate,
            "total_trades": r.total_trades,
            "profit_factor": r.profit_factor,
            "avg_trade_duration_hours": r.avg_trade_duration_hours,
            "trades": [
                {
                    "open_ts": str(t.open_ts),
                    "close_ts": str(t.close_ts),
                    "symbol": t.symbol,
                    "side": t.side,
                    "quantity": t.quantity,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "pnl": t.pnl,
                    "fee": t.fee,
                }
                for t in r.trades
            ],
        }


# ==================================================================
# CSS
# ==================================================================

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f0f23; color: #e0e0e0; line-height: 1.6; }
.container { max-width: 1000px; margin: 0 auto; padding: 24px; }
h1 { color: #fff; margin-bottom: 8px; font-size: 1.6rem; }
h2 { color: #aaa; margin: 24px 0 12px; font-size: 1.2rem; border-bottom: 1px solid #333; padding-bottom: 4px; }
.meta { color: #888; margin-bottom: 20px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 24px; }
.card { background: #1a1a2e; border-radius: 8px; padding: 14px; text-align: center; }
.card-label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }
.card-value { font-size: 1.4rem; font-weight: 700; margin-top: 4px; }
.positive { color: #2ecc71; }
.negative { color: #e74c3c; }
table { width: 100%; border-collapse: collapse; margin-bottom: 16px; font-size: 0.85rem; }
thead { background: #1a1a2e; }
th, td { padding: 8px 10px; text-align: right; border-bottom: 1px solid #2a2a3e; }
th { color: #aaa; font-weight: 600; }
td:first-child, th:first-child { text-align: left; }
tr:hover { background: rgba(255,255,255,0.03); }
.chart { margin-bottom: 16px; }
.chart-title { color: #aaa; font-size: 0.85rem; margin-bottom: 4px; }
.no-data { color: #666; font-style: italic; }
.alert { background: #5a1a1a; border: 1px solid #e74c3c; color: #e74c3c;
         padding: 12px 16px; border-radius: 6px; margin-bottom: 20px; font-weight: 600; }
footer { margin-top: 32px; color: #555; font-size: 0.75rem; text-align: center; }
"""
