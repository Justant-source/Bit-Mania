#!/usr/bin/env python3
"""Initialize the CryptoEngine database.

Steps:
  1. Connect to PostgreSQL; create the database if it does not exist.
  2. Apply ``init_schema.sql`` then numbered ``*.sql`` in
     ``shared/db/migrations/`` in version order (ADR-0006).
  3. Verify expected tables are present.

Usage:
  python scripts/init_db.py                        # use defaults
  python scripts/init_db.py --dsn postgresql://...  # explicit DSN
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import asyncpg
import structlog
from shared.db.sql_migrations import (
    INIT_SCHEMA_PATH,
    MIGRATIONS_DIR,
    MigrationError,
    list_sql_migration_files,
    strip_psql_meta,
)
from shared.timezone_utils import kst_timestamper

PROJECT_ROOT = Path(__file__).resolve().parent.parent

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        kst_timestamper,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("init_db")

# Tables created by init_schema + numbered SQL after 018 (not drop targets).
EXPECTED_TABLES = {
    "trades",
    "positions",
    "funding_payments",
    "portfolio_snapshots",
    "daily_reports",
    "strategy_states",
    "kill_switch_events",
    "llm_judgments",
    "llm_reports",
    "ohlcv_history",
    "funding_rate_history",
    "service_logs",
    "supertrend_signals",
}

DEFAULT_DB_NAME = os.getenv("DB_NAME", "cryptoengine")
DEFAULT_DB_USER = os.getenv("DB_USER", "cryptoengine")
DEFAULT_DB_HOST = os.getenv("DB_HOST", "localhost")
DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "5432"))


def _db_password() -> str:
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise SystemExit("DB_PASSWORD is required (fail-closed)")
    return password


def _build_dsn(
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
    dbname: str | None = None,
) -> str:
    return (
        f"postgresql://{user or DEFAULT_DB_USER}:"
        f"{password if password is not None else _db_password()}"
        f"@{host or DEFAULT_DB_HOST}:{port or DEFAULT_DB_PORT}/"
        f"{dbname or DEFAULT_DB_NAME}"
    )


async def _ensure_database(dsn: str) -> None:
    """Connect to the *postgres* maintenance database and CREATE DATABASE
    if the target does not exist.
    """
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
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            log.info("database_created", database=db_name)
    finally:
        await conn.close()


def _load_sql_file(path: Path) -> str:
    if not path.is_file():
        raise MigrationError(f"SQL file missing: {path}")
    sql = strip_psql_meta(path.read_text(encoding="utf-8")).strip()
    if not sql:
        raise MigrationError(f"SQL file empty after stripping psql meta: {path}")
    return sql


async def _apply_sql_migrations(dsn: str) -> None:
    """Apply init_schema.sql then numbered migrations. Fail-closed on missing files."""
    if not INIT_SCHEMA_PATH.is_file():
        raise MigrationError(f"schema file missing: {INIT_SCHEMA_PATH}")

    files = list_sql_migration_files(MIGRATIONS_DIR)
    log.info(
        "applying_sql_migrations",
        init_schema=str(INIT_SCHEMA_PATH),
        count=len(files),
        first=files[0].name,
        last=files[-1].name,
    )

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(_load_sql_file(INIT_SCHEMA_PATH))
        log.info("sql_schema_applied", path=str(INIT_SCHEMA_PATH))

        for path in files:
            sql = _load_sql_file(path)
            log.info("applying_sql_file", file=path.name)
            await conn.execute(sql)
            log.info("sql_file_applied", file=path.name)
    finally:
        await conn.close()


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
    extra = existing - EXPECTED_TABLES

    if missing:
        log.warning("missing_tables", tables=sorted(missing))
    else:
        log.info("all_expected_tables_present", count=len(EXPECTED_TABLES))

    if extra:
        log.info("extra_tables_found", tables=sorted(extra))

    return len(missing) == 0


async def init_db(dsn: str) -> bool:
    """Run all initialisation steps.  Returns True on success."""
    await _ensure_database(dsn)

    try:
        await _apply_sql_migrations(dsn)
    except MigrationError:
        log.exception("sql_migration_failed")
        return False

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
        default=None,
        help="PostgreSQL connection string",
    )
    args = parser.parse_args()
    dsn = args.dsn if args.dsn else _build_dsn()

    success = asyncio.run(init_db(dsn))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
