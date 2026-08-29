"""Raw SQL migration helpers (ADR-0006).

Numbered ``NNN_*.sql`` files in ``shared/db/migrations/`` are the single
migration track. Alembic ``versions/`` is ignored if leftover.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
INIT_SCHEMA_PATH = Path(__file__).resolve().parent / "init_schema.sql"

SQL_NAME_RE = re.compile(r"^(\d+)_.+\.sql$")

# Tables 018 must never drop (dashboard + live Supertrend path).
KEEP_TABLES = frozenset(
    {
        "supertrend_signals",
        "orders",
        "service_logs",
        "portfolio_snapshots",
        "ohlcv_history",
        "positions",
        "strategy_states",
        "kill_switch_events",
        "daily_pnl",
        "llm_judgments",
        "llm_reports",
        "funding_rate_history",
    }
)

_DROP_TABLE_RE = re.compile(
    r"DROP\s+TABLE\s+IF\s+EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


class MigrationError(RuntimeError):
    """Fail-closed migration error (missing files or invalid sequence)."""


def list_sql_migration_files(migrations_dir: Path | None = None) -> list[Path]:
    """Return numbered ``*.sql`` files sorted by integer prefix.

    Skips any ``versions/`` directory (leftover Alembic). Fail-closed if the
    directory is missing, contains no numbered SQL, a listed file is gone, or
    two files share the same version number.
    """
    root = migrations_dir if migrations_dir is not None else MIGRATIONS_DIR
    if not root.is_dir():
        raise MigrationError(f"migrations directory missing: {root}")

    found: list[tuple[int, Path]] = []
    for path in sorted(root.iterdir()):
        if path.is_dir():
            continue
        match = SQL_NAME_RE.match(path.name)
        if match is None:
            continue
        found.append((int(match.group(1)), path))

    if not found:
        raise MigrationError(f"no numbered SQL migrations in {root}")

    by_version: dict[int, Path] = {}
    for version, path in found:
        if not path.is_file():
            raise MigrationError(f"migration file missing: {path}")
        if version in by_version:
            raise MigrationError(
                f"duplicate migration version {version}: "
                f"{by_version[version].name} and {path.name}"
            )
        by_version[version] = path

    return [by_version[v] for v in sorted(by_version)]


def strip_psql_meta(sql: str) -> str:
    """Remove psql meta-commands (e.g. ``\\echo``) so asyncpg can execute the file."""
    lines = []
    for line in sql.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("\\"):
            continue
        lines.append(line)
    return "\n".join(lines)


def drop_targets_in_sql(sql: str) -> set[str]:
    """Return table names targeted by ``DROP TABLE IF EXISTS`` in ``sql``."""
    return {m.group(1).lower() for m in _DROP_TABLE_RE.finditer(sql)}
