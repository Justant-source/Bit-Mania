"""Add asset_report column to llm_reports for 6-hour Korean narrative reports.

Revision ID: 003
Revises: 002
"""

from alembic import op
import sqlalchemy as sa

revision = "003"
down_revision = "002"


def upgrade() -> None:
    op.add_column("llm_reports", sa.Column("asset_report", sa.Text))


def downgrade() -> None:
    op.drop_column("llm_reports", "asset_report")
