"""Add regime_raw_log and regime_transitions tables for regime dashboard.

Revision ID: 004
Revises: 003
"""

from alembic import op
import sqlalchemy as sa

revision = "004"
down_revision = "003"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_raw_log (
            id              BIGSERIAL PRIMARY KEY,
            symbol          VARCHAR(20) NOT NULL DEFAULT 'BTCUSDT',
            regime          VARCHAR(20) NOT NULL,
            confidence      DECIMAL(5,3),
            adx             DECIMAL(10,4),
            atr             DECIMAL(10,4),
            atr_avg         DECIMAL(10,4),
            bb_width        DECIMAL(10,6),
            ema20           DECIMAL(20,2),
            close_price     DECIMAL(20,2),
            is_confirmed    BOOLEAN DEFAULT FALSE,
            consecutive_count INTEGER DEFAULT 1,
            detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_regime_raw_detected ON regime_raw_log(detected_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_regime_raw_symbol ON regime_raw_log(symbol, detected_at DESC)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_transitions (
            id                          BIGSERIAL PRIMARY KEY,
            symbol                      VARCHAR(20) NOT NULL DEFAULT 'BTCUSDT',
            previous_regime             VARCHAR(20) NOT NULL,
            new_regime                  VARCHAR(20) NOT NULL,
            transition_type             VARCHAR(20) NOT NULL,
            confirmed                   BOOLEAN DEFAULT FALSE,
            weight_change_from          JSONB,
            weight_change_to            JSONB,
            transition_duration_seconds INTEGER,
            detected_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            confirmed_at                TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_regime_trans_detected ON regime_transitions(detected_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS regime_transitions")
    op.execute("DROP TABLE IF EXISTS regime_raw_log")
