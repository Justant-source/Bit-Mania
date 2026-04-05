#!/usr/bin/env python3
"""Initialize the CryptoEngine database.

Steps:
  1. Connect to PostgreSQL; create the database if it does not exist.
  2. Run Alembic migrations (``alembic upgrade head``).
  3. Verify all expected tables are present.

Usage:
  python scripts/init_db.py                        # use defaults
  python scripts/init_db.py --dsn postgresql://...  # explicit DSN
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path

import logging

import asyncpg
import structlog

# Project root — one level above /scripts
PROJECT_ROOT = Path(__file__).resolve().parent.parent

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("init_db")

# Expected tables after migration
EXPECTED_TABLES = {
    "trades",
    "positions",
    "funding_payments",
    "portfolio_snapshots",
    "daily_reports",
    "strategy_states",
    "kill_switch_events",
    "llm_judgments",
    "ohlcv_history",
    "funding_rate_history",
    "dca_purchases",
}

DEFAULT_DB_NAME = os.getenv("DB_NAME", "cryptoengine")
DEFAULT_DB_USER = os.getenv("DB_USER", "cryptoengine")
DEFAULT_DB_PASSWORD = os.getenv("DB_PASSWORD", "cryptoengine")
DEFAULT_DB_HOST = os.getenv("DB_HOST", "localhost")
DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "5432"))


def _build_dsn(
    *,
    user: str = DEFAULT_DB_USER,
    password: str = DEFAULT_DB_PASSWORD,
    host: str = DEFAULT_DB_HOST,
    port: int = DEFAULT_DB_PORT,
    dbname: str = DEFAULT_DB_NAME,
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


# ------------------------------------------------------------------
# Step 1 — Create database if it does not exist
# ------------------------------------------------------------------

async def _ensure_database(dsn: str) -> None:
    """Connect to the *postgres* maintenance database and CREATE DATABASE
    if the target does not exist.
    """
    # Parse the target db name from DSN
    parts = dsn.rsplit("/", 1)
    db_name = parts[-1].split("?")[0] if len(parts) == 2 else DEFAULT_DB_NAME
    maintenance_dsn = parts[0] + "/postgres" if len(parts) == 2 else dsn

    log.info("checking_database_exists", database=db_name)

    try:
        conn = await asyncpg.connect(maintenance_dsn)
    except Exception as exc:
        log.error("cannot_connect_maintenance_db", error=str(exc))
        raise

    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        if exists:
            log.info("database_exists", database=db_name)
        else:
            # CREATE DATABASE cannot run inside a transaction block
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            log.info("database_created", database=db_name)
    finally:
        await conn.close()


# ------------------------------------------------------------------
# Step 2 — Run Alembic migrations
# ------------------------------------------------------------------

def _run_alembic_migrations() -> None:
    """Run ``alembic upgrade head`` from the migrations directory."""
    migrations_dir = PROJECT_ROOT / "shared" / "db" / "migrations"
    alembic_ini = migrations_dir / "alembic.ini"

    if not alembic_ini.exists():
        log.warning(
            "alembic_ini_not_found",
            path=str(alembic_ini),
            fallback="running init_schema.sql directly",
        )
        return

    log.info("running_alembic_migrations", cwd=str(migrations_dir))
    result = subprocess.run(
        ["alembic", "upgrade", "head"],
        cwd=str(migrations_dir),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        log.error(
            "alembic_migration_failed",
            returncode=result.returncode,
            stderr=result.stderr[:2000],
        )
        # Fallback: apply SQL schema directly
        log.info("falling_back_to_sql_schema")
        _apply_sql_schema()
    else:
        log.info("alembic_migrations_complete", stdout=result.stdout.strip()[:500])


async def _apply_sql_schema_async(dsn: str) -> None:
    """Apply the raw SQL schema file directly via asyncpg."""
    schema_path = PROJECT_ROOT / "shared" / "db" / "init_schema.sql"
    if not schema_path.exists():
        log.error("schema_file_not_found", path=str(schema_path))
        return

    sql = schema_path.read_text(encoding="utf-8")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(sql)
        log.info("sql_schema_applied")
    finally:
        await conn.close()


def _apply_sql_schema() -> None:
    """Synchronous wrapper for SQL schema application."""
    dsn = _build_dsn()
    asyncio.get_event_loop().run_until_complete(_apply_sql_schema_async(dsn))


# ------------------------------------------------------------------
# Step 3 — Verify tables
# ------------------------------------------------------------------

async def _verify_tables(dsn: str) -> bool:
    """Check that all expected tables exist in the database."""
    log.info("verifying_tables")

    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            """
        )
        existing = {row["tablename"] for row in rows}
    finally:
        await conn.close()

    missing = EXPECTED_TABLES - existing
    extra = existing - EXPECTED_TABLES - {"alembic_version"}

    if missing:
        log.warning("missing_tables", tables=sorted(missing))
    else:
        log.info("all_expected_tables_present", count=len(EXPECTED_TABLES))

    if extra:
        log.info("extra_tables_found", tables=sorted(extra))

    for table in sorted(existing & EXPECTED_TABLES):
        log.debug("table_ok", table=table)

    return len(missing) == 0


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def init_db(dsn: str) -> bool:
    """Run all initialisation steps.  Returns True on success."""
    # Step 1: ensure database
    await _ensure_database(dsn)

    # Step 2: run migrations (or raw SQL fallback)
    try:
        _run_alembic_migrations()
    except Exception:
        log.exception("migration_error_falling_back_to_sql")
        await _apply_sql_schema_async(dsn)

    # Step 3: verify
    ok = await _verify_tables(dsn)

    if ok:
        log.info("database_initialization_complete")
    else:
        log.error("database_initialization_incomplete")

    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize CryptoEngine database")
    parser.add_argument(
        "--dsn",
        type=str,
        default=_build_dsn(),
        help="PostgreSQL connection string",
    )
    args = parser.parse_args()

    success = asyncio.run(init_db(args.dsn))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
