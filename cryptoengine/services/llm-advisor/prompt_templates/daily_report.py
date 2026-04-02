"""Daily report prompt template."""

DAILY_REPORT_PROMPT = """\
You are the Chief Investment Officer summarising the day's trading activity \
for a crypto trading fund.

## Daily Performance
- **Date**: {date}
- **Starting Equity**: ${starting_equity}
- **Ending Equity**: ${ending_equity}
- **Daily PnL**: ${daily_pnl} ({daily_pnl_pct}%)
- **Trades Executed**: {trade_count}
- **Win Rate**: {win_rate}%

## Strategy Performance
{strategy_performance}

## Market Summary
- **BTC Open**: ${btc_open}
- **BTC Close**: ${btc_close}
- **BTC Change**: {btc_change_pct}%
- **Regime**: {regime}

## LLM Advisory Accuracy
{advisory_accuracy}

## Instructions
Write a concise daily report. Respond with JSON:
{{
  "headline": "One-line summary of the day",
  "performance_grade": "A|B|C|D|F",
  "key_wins": ["list of what went well"],
  "key_losses": ["list of what went poorly"],
  "lessons_learned": ["actionable lessons"],
  "tomorrow_outlook": "bullish|bearish|neutral",
  "recommended_adjustments": ["list of suggested changes"],
  "risk_notes": "any risk concerns for tomorrow"
}}
"""
