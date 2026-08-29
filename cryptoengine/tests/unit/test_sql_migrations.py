"""Raw SQL migration track (ADR-0006) — no live Postgres required."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.db.sql_migrations import (
    KEEP_TABLES,
    MIGRATIONS_DIR,
    MigrationError,
    drop_targets_in_sql,
    list_sql_migration_files,
    strip_psql_meta,
)

KEEP = KEEP_TABLES

EXPECTED_018_DROPS = {
    "quarterly_perp_spread",
    "quarterly_futures_history",
    "market_regime_history",
    "regime_raw_log",
    "regime_transitions",
    "macro_indicators",
    "onchain_metrics",
    "multi_exchange_ohlcv",
    "multi_exchange_funding",
    "dca_purchases",
    "grid_orders",
    "market_regimes",
    "etf_flow_history",
    "etf_flow_results",
    "xgboost_ensemble_results",
    "calendar_spread_results",
    "volatility_squeeze_results",
    "funding_extreme_reversal_results",
    "regime_accuracy_results",
    "liquidation_history",
    "macro_events",
    "fear_greed_history",
    "strategy_variant_results",
    "weight_optimization_results",
    "walk_forward_results",
    "test12_results",
    "backtest_results",
    "ohlcv",
    "funding_rates",
}


def test_list_sql_skips_versions_and_is_sorted() -> None:
    files = list_sql_migration_files(MIGRATIONS_DIR)
    names = [p.name for p in files]
    versions = [int(p.name.split("_", 1)[0]) for p in files]
    assert versions == sorted(versions)
    assert names[0] == "003_service_logs.sql"
    assert names[-1] == "018_drop_legacy_tables.sql"
    assert all("versions" not in p.parts[-2:] or p.parent.name != "versions" for p in files)
    assert all(p.parent == MIGRATIONS_DIR for p in files)


def test_list_sql_fail_closed_missing_dir(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="missing"):
        list_sql_migration_files(tmp_path / "nope")


def test_list_sql_fail_closed_empty(tmp_path: Path) -> None:
    leftover = tmp_path / "versions"
    leftover.mkdir()
    (leftover / "001_initial_schema.py").write_text("pass\n", encoding="utf-8")
    with pytest.raises(MigrationError, match="no numbered SQL"):
        list_sql_migration_files(tmp_path)


def test_list_sql_fail_closed_duplicate_version(tmp_path: Path) -> None:
    (tmp_path / "003_a.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "003_b.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(MigrationError, match="duplicate"):
        list_sql_migration_files(tmp_path)


def test_strip_psql_echo() -> None:
    sql = "DROP TABLE IF EXISTS ohlcv_1m_longterm CASCADE;\n\\echo hi\n"
    assert "\\echo" not in strip_psql_meta(sql)
    assert "DROP TABLE" in strip_psql_meta(sql)


def test_018_never_drops_keep_list() -> None:
    path = MIGRATIONS_DIR / "018_drop_legacy_tables.sql"
    sql = path.read_text(encoding="utf-8")
    dropped = drop_targets_in_sql(sql)
    overlap = dropped & KEEP
    assert not overlap, f"018 drops keep-list tables: {sorted(overlap)}"
    assert "trades" not in dropped
    assert "ohlcv_history" not in dropped
    assert "funding_rate_history" not in dropped
    missing = EXPECTED_018_DROPS - dropped
    assert not missing, f"018 missing DROP targets: {sorted(missing)}"
