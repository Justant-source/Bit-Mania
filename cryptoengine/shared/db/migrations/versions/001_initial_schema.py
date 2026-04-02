"""Initial database schema for CryptoEngine

Revision ID: 001_initial_schema
Create Date: 2026-04-03
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ──────────────── trades ────────────────
    op.create_table(
        "trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("strategy_id", sa.String(50), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("order_type", sa.String(10), nullable=False),
        sa.Column("quantity", sa.Numeric(20, 8), nullable=False),
        sa.Column("price", sa.Numeric(20, 2), nullable=False),
        sa.Column("fee", sa.Numeric(20, 8)),
        sa.Column("fee_currency", sa.String(10)),
        sa.Column("pnl", sa.Numeric(20, 8)),
        sa.Column("order_id", sa.String(100)),
        sa.Column("request_id", sa.String(100), unique=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("filled_at", sa.DateTime(timezone=True)),
    )
    op.create_index("idx_trades_strategy", "trades", ["strategy_id", "created_at"])
    op.create_index("idx_trades_filled", "trades", ["filled_at"])
    op.create_index("idx_trades_request_id", "trades", ["request_id"])

    # ──────────────── positions ────────────────
    op.create_table(
        "positions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("strategy_id", sa.String(50), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("size", sa.Numeric(20, 8), nullable=False),
        sa.Column("entry_price", sa.Numeric(20, 2), nullable=False),
        sa.Column("current_price", sa.Numeric(20, 2)),
        sa.Column("unrealized_pnl", sa.Numeric(20, 8)),
        sa.Column("leverage", sa.Numeric(5, 2), server_default="1.0"),
        sa.Column(
            "opened_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("close_reason", sa.String(50)),
    )
    op.create_index("idx_positions_strategy", "positions", ["strategy_id", "opened_at"])
    op.create_index(
        "idx_positions_open",
        "positions",
        ["strategy_id"],
        postgresql_where=sa.text("closed_at IS NULL"),
    )

    # ──────────────── funding_payments ────────────────
    op.create_table(
        "funding_payments",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("funding_rate", sa.Numeric(10, 6), nullable=False),
        sa.Column("payment", sa.Numeric(20, 8), nullable=False),
        sa.Column("position_size", sa.Numeric(20, 8), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_funding_collected", "funding_payments", ["collected_at"]
    )
    op.create_index(
        "idx_funding_exchange_symbol",
        "funding_payments",
        ["exchange", "symbol", "collected_at"],
    )

    # ──────────────── portfolio_snapshots ────────────────
    op.create_table(
        "portfolio_snapshots",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("total_equity", sa.Numeric(20, 2), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(20, 8)),
        sa.Column("realized_pnl", sa.Numeric(20, 8)),
        sa.Column("drawdown", sa.Numeric(10, 6)),
        sa.Column("sharpe_30d", sa.Numeric(10, 4)),
        sa.Column("strategy_weights", JSONB),
        sa.Column("market_regime", sa.String(20)),
        sa.Column(
            "snapshot_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_snapshots_time", "portfolio_snapshots", ["snapshot_at"]
    )

    # ──────────────── daily_reports ────────────────
    op.create_table(
        "daily_reports",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("date", sa.Date, unique=True, nullable=False),
        sa.Column("starting_equity", sa.Numeric(20, 2)),
        sa.Column("ending_equity", sa.Numeric(20, 2)),
        sa.Column("daily_pnl", sa.Numeric(20, 8)),
        sa.Column("daily_return", sa.Numeric(10, 6)),
        sa.Column("trade_count", sa.Integer),
        sa.Column("funding_income", sa.Numeric(20, 8)),
        sa.Column("grid_income", sa.Numeric(20, 8)),
        sa.Column("dca_value", sa.Numeric(20, 8)),
        sa.Column("max_drawdown", sa.Numeric(10, 6)),
        sa.Column("llm_summary", sa.Text),
    )
    op.create_index("idx_daily_reports_date", "daily_reports", ["date"])

    # ──────────────── strategy_states ────────────────
    op.create_table(
        "strategy_states",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("strategy_id", sa.String(50), unique=True, nullable=False),
        sa.Column("is_running", sa.Boolean, server_default="false"),
        sa.Column("allocated_capital", sa.Numeric(20, 2)),
        sa.Column("current_pnl", sa.Numeric(20, 8)),
        sa.Column("position_count", sa.Integer, server_default="0"),
        sa.Column("config_override", JSONB),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )

    # ──────────────── kill_switch_events ────────────────
    op.create_table(
        "kill_switch_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("level", sa.Integer, nullable=False),
        sa.Column("reason", sa.String(200), nullable=False),
        sa.Column("positions_closed", sa.Integer),
        sa.Column("pnl_at_trigger", sa.Numeric(20, 8)),
        sa.Column("details", JSONB),
        sa.Column(
            "triggered_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "idx_kill_switch_triggered",
        "kill_switch_events",
        ["triggered_at"],
    )

    # ──────────────── llm_judgments ────────────────
    op.create_table(
        "llm_judgments",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("rating", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 3)),
        sa.Column("regime", sa.String(20)),
        sa.Column("reasoning", sa.Text),
        sa.Column("weight_adjustment", JSONB),
        sa.Column("bull_summary", sa.Text),
        sa.Column("bear_summary", sa.Text),
        sa.Column("risk_flags", JSONB),
        sa.Column("actual_outcome", sa.String(20)),
        sa.Column("accuracy_score", sa.Numeric(5, 3)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("evaluated_at", sa.DateTime(timezone=True)),
    )
    op.create_index("idx_llm_judgments_created", "llm_judgments", ["created_at"])

    # ──────────────── ohlcv_history ────────────────
    op.create_table(
        "ohlcv_history",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timeframe", sa.String(5), nullable=False),
        sa.Column("open", sa.Numeric(20, 2), nullable=False),
        sa.Column("high", sa.Numeric(20, 2), nullable=False),
        sa.Column("low", sa.Numeric(20, 2), nullable=False),
        sa.Column("close", sa.Numeric(20, 2), nullable=False),
        sa.Column("volume", sa.Numeric(20, 8), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_ohlcv_lookup",
        "ohlcv_history",
        ["exchange", "symbol", "timeframe", "timestamp"],
        unique=True,
    )

    # ──────────────── funding_rate_history ────────────────
    op.create_table(
        "funding_rate_history",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("rate", sa.Numeric(10, 6), nullable=False),
        sa.Column("predicted_rate", sa.Numeric(10, 6)),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_funding_rate_lookup",
        "funding_rate_history",
        ["exchange", "symbol", "timestamp"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("funding_rate_history")
    op.drop_table("ohlcv_history")
    op.drop_table("llm_judgments")
    op.drop_table("kill_switch_events")
    op.drop_table("strategy_states")
    op.drop_table("daily_reports")
    op.drop_table("portfolio_snapshots")
    op.drop_table("funding_payments")
    op.drop_table("positions")
    op.drop_table("trades")
