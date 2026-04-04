"""Add llm_reports table for full LLM analysis report storage.

Revision ID: 002
Revises: 001
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "002"
down_revision = "001"


def upgrade() -> None:
    op.create_table(
        "llm_reports",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("trigger", sa.String(30), nullable=False, server_default="scheduled"),
        sa.Column("rating", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 3)),
        sa.Column("regime", sa.String(20)),
        sa.Column("symbol", sa.String(20), server_default="BTCUSDT"),
        sa.Column("btc_price", sa.Numeric(20, 2)),
        sa.Column("technical_summary", sa.Text),
        sa.Column("sentiment_summary", sa.Text),
        sa.Column("bull_summary", sa.Text),
        sa.Column("bear_summary", sa.Text),
        sa.Column("debate_conclusion", sa.Text),
        sa.Column("risk_assessment", sa.Text),
        sa.Column("reasoning", sa.Text),
        sa.Column("weight_adjustments", JSONB),
        sa.Column("risk_flags", JSONB),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index(
        "idx_llm_reports_created", "llm_reports", [sa.text("created_at DESC")]
    )
    op.create_index(
        "idx_llm_reports_symbol",
        "llm_reports",
        ["symbol", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_table("llm_reports")
