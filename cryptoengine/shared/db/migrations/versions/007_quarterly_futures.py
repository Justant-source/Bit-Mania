"""Quarterly futures OHLCV history and calendar spread results

Revision ID: 007_quarterly_futures
Create Date: 2026-04-11
"""

from alembic import op
import sqlalchemy as sa

revision = "007_quarterly_futures"
down_revision = "004_regime_dashboard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ──────────────── quarterly_futures_history ────────────────
    op.create_table(
        "quarterly_futures_history",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(20, 4), nullable=False),
        sa.Column("high", sa.Numeric(20, 4), nullable=False),
        sa.Column("low", sa.Numeric(20, 4), nullable=False),
        sa.Column("close", sa.Numeric(20, 4), nullable=False),
        sa.Column("volume", sa.Numeric(20, 8), nullable=False),
        sa.Column("turnover", sa.Numeric(30, 2)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("symbol", "timestamp", name="uq_quarterly_futures_symbol_ts"),
    )
    op.create_index(
        "idx_quarterly_futures_symbol_ts",
        "quarterly_futures_history",
        ["symbol", "timestamp"],
    )

    # ──────────────── calendar_spread_results ────────────────
    op.create_table(
        "calendar_spread_results",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("run_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("stage", sa.String(50), nullable=False),
        sa.Column("variant", sa.String(100), nullable=False),
        sa.Column("metrics", sa.JSON),
        sa.Column("params", sa.JSON),
    )
    op.create_index(
        "idx_calendar_spread_stage",
        "calendar_spread_results",
        ["stage"],
    )


def downgrade() -> None:
    op.drop_table("calendar_spread_results")
    op.drop_table("quarterly_futures_history")
