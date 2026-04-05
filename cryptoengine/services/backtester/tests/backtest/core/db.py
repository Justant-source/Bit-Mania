"""core.db — DB 연결·저장 공통 유틸리티.

사용법:
    from core.db import make_pool, save_result, DB_DSN, CREATE_VARIANT_TABLE

    pool = await make_pool()
    await save_result(pool, stage="stage_a", variant="reinvest_50pct",
                      metrics={...}, params={...})
"""
from __future__ import annotations

import json
import math
import os
from typing import Any

import asyncpg


# ── DSN ───────────────────────────────────────────────────────────────────────

DB_DSN: str = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_VARIANT_TABLE: str = """
CREATE TABLE IF NOT EXISTS strategy_variant_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    test_name       TEXT        NOT NULL,
    variant_name    TEXT        NOT NULL,
    data_range      TEXT,
    total_return    DOUBLE PRECISION,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    trade_count     INTEGER,
    win_rate        DOUBLE PRECISION,
    profit_factor   DOUBLE PRECISION,
    monthly_returns JSONB,
    params          JSONB
);
"""

CREATE_BACKTEST_TABLE: str = """
CREATE TABLE IF NOT EXISTS backtest_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    strategy        TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    start_date      TIMESTAMPTZ NOT NULL,
    end_date        TIMESTAMPTZ NOT NULL,
    initial_capital DOUBLE PRECISION NOT NULL,
    final_equity    DOUBLE PRECISION NOT NULL,
    total_return    DOUBLE PRECISION NOT NULL,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    win_rate        DOUBLE PRECISION,
    total_trades    INTEGER,
    metadata        JSONB
);
"""

CREATE_TEST12_TABLE: str = """
CREATE TABLE IF NOT EXISTS test12_results (
    id      SERIAL PRIMARY KEY,
    run_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage   TEXT NOT NULL,
    variant TEXT NOT NULL,
    metrics JSONB,
    params  JSONB
);
"""


# ── 연결 ──────────────────────────────────────────────────────────────────────

async def make_pool(min_size: int = 2, max_size: int = 4) -> asyncpg.Pool:
    """DB 연결 풀을 생성한다.

    환경변수 DB_HOST, DB_PASSWORD, DB_USER, DB_PORT, DB_NAME 사용.
    실패 시 ConnectionError 발생.
    """
    try:
        return await asyncpg.create_pool(dsn=DB_DSN, min_size=min_size, max_size=max_size)
    except Exception as exc:
        raise ConnectionError(f"DB 연결 실패: {exc}\n  DB_HOST={os.getenv('DB_HOST', 'postgres')}") from exc


# ── 저장 ──────────────────────────────────────────────────────────────────────

def _sanitize(obj: Any) -> Any:
    """JSON 직렬화를 위해 NaN/Inf를 0으로 치환."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0.0
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


async def save_result(
    pool: asyncpg.Pool,
    stage: str,
    variant: str,
    metrics: dict[str, Any],
    params: dict[str, Any],
    table: str = "test12_results",
) -> None:
    """test12_results (또는 지정 테이블) 에 결과를 저장한다.

    Args:
        pool:    asyncpg 연결 풀
        stage:   "stage_a", "stage_b", ...
        variant: "reinvest_50pct", "fa_ratio_70pct", ...
        metrics: 측정 결과 dict (float NaN/Inf 자동 치환)
        params:  실행 파라미터 dict
        table:   저장 테이블명 (기본 "test12_results")
    """
    create_sql = CREATE_TEST12_TABLE.replace("test12_results", table)
    async with pool.acquire() as conn:
        await conn.execute(create_sql)
        await conn.execute(
            f"INSERT INTO {table}(stage, variant, metrics, params) "
            "VALUES($1, $2, $3::jsonb, $4::jsonb)",
            stage,
            variant,
            json.dumps(_sanitize(metrics)),
            json.dumps(_sanitize(params)),
        )


async def clear_stage(
    pool: asyncpg.Pool,
    stage: str,
    table: str = "test12_results",
) -> None:
    """특정 stage의 기존 결과를 삭제한다."""
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {table} WHERE stage=$1", stage)


async def load_stage_result(
    pool: asyncpg.Pool,
    stage: str,
    table: str = "test12_results",
) -> list[dict]:
    """특정 stage의 결과를 불러온다."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT variant, metrics, params FROM {table} "
            "WHERE stage=$1 ORDER BY run_at DESC",
            stage,
        )
    return [
        {
            "variant": r["variant"],
            "metrics": json.loads(r["metrics"]) if r["metrics"] else {},
            "params":  json.loads(r["params"])  if r["params"]  else {},
        }
        for r in rows
    ]
