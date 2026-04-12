#!/usr/bin/env python3
"""export_funding_rates.py — 펀딩비 데이터를 Parquet으로 내보내기

Bybit 펀딩비 히스토리를 PostgreSQL에서 쿼리하여 Parquet 형식으로 저장.
데이터 범위, 갭 분석, 통계 출력.

사용법:
    python scripts/data/export_funding_rates.py

환경변수:
    DB_HOST      = postgres (기본값)
    DB_USER      = cryptoengine (기본값)
    DB_PASSWORD  = CryptoEngine2026! (기본값)
    DB_NAME      = cryptoengine (기본값)
    DB_PORT      = 5432 (기본값)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import asyncpg
except ImportError:
    print("[ERROR] asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)

try:
    import polars as pl
except ImportError:
    print("[ERROR] polars not installed. Run: pip install polars")
    sys.exit(1)

import structlog

# ── 상수 ──────────────────────────────────────────────────────────────────────

SYMBOL = "BTCUSDT"
OUTPUT_DIR = Path("/data/funding_rates")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "cryptoengine")
DB_PASSWORD = os.getenv("DB_PASSWORD", "CryptoEngine2026!")
DB_NAME = os.getenv("DB_NAME", "cryptoengine")
DB_PORT = os.getenv("DB_PORT", "5432")

DB_DSN = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

log = structlog.get_logger(__name__)

# ── 유틸리티 ──────────────────────────────────────────────────────────────────


def setup_logging() -> None:
    """structlog 설정."""
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


def ensure_output_dir() -> None:
    """출력 디렉토리 생성."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("output_dir_ready", path=str(OUTPUT_DIR))


# ── DB 쿼리 ───────────────────────────────────────────────────────────────────


async def fetch_funding_rates(
    pool: asyncpg.Pool, symbol: str
) -> list[tuple[Any, float]]:
    """PostgreSQL에서 펀딩비 데이터 조회.

    Returns:
        [(timestamp, rate), ...] (UTC, 정렬됨)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate
            FROM funding_rate_history
            WHERE symbol = $1
            ORDER BY timestamp ASC
            """,
            symbol,
        )
    return [(row["ts"], row["rate"]) for row in rows]


async def check_schema_for_older_data(pool: asyncpg.Pool) -> dict[str, Any]:
    """2020-2023 초 이전 데이터가 있는 다른 테이블 존재 여부 확인."""
    async with pool.acquire() as conn:
        # 테이블 목록 조회
        tables = await conn.fetch(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )

    table_names = [t["table_name"] for t in tables]
    log.info("db_schema_check", tables=table_names)

    result = {
        "all_tables": table_names,
        "has_ohlcv": "ohlcv_history" in table_names,
        "has_funding": "funding_rate_history" in table_names,
    }

    # 각 관련 테이블 행 수 확인
    async with pool.acquire() as conn:
        if "funding_rate_history" in table_names:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM funding_rate_history WHERE symbol = 'BTCUSDT'"
            )
            result["funding_rate_history_count"] = count

        if "ohlcv_history" in table_names:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM ohlcv_history WHERE symbol = 'BTCUSDT' AND timeframe = '1h'"
            )
            result["ohlcv_history_1h_count"] = count

            # 가장 오래된 데이터
            earliest = await conn.fetchval(
                "SELECT MIN(timestamp) FROM ohlcv_history WHERE symbol = 'BTCUSDT' AND timeframe = '1h'"
            )
            if earliest:
                result["ohlcv_earliest"] = str(earliest)

    return result


# ── 데이터 분석 ────────────────────────────────────────────────────────────────


def analyze_gaps(rows: list[tuple[Any, float]], symbol: str) -> dict[str, Any]:
    """펀딩비 데이터 갭 분석.

    Returns:
        {
            "total_rows": int,
            "date_range": "YYYY-MM-DD ~ YYYY-MM-DD",
            "earliest": datetime,
            "latest": datetime,
            "gaps_gt_10d": [
                {"from": ts, "to": ts, "days": float},
                ...
            ],
            "avg_interval_hours": float,
        }
    """
    if not rows:
        return {
            "total_rows": 0,
            "date_range": "N/A",
            "gaps_gt_10d": [],
            "warning": f"No data for {symbol}",
        }

    timestamps = [ts for ts, _ in rows]
    rates = [rate for _, rate in rows]

    earliest = min(timestamps)
    latest = max(timestamps)
    date_range = f"{earliest.date()} ~ {latest.date()}"

    # 갭 분석 (10일 이상)
    gaps = []
    for i in range(len(timestamps) - 1):
        ts1 = timestamps[i]
        ts2 = timestamps[i + 1]
        delta = (ts2 - ts1).total_seconds() / 3600  # 시간
        if delta > 10 * 24:  # 10일 이상
            gaps.append(
                {
                    "from": str(ts1),
                    "to": str(ts2),
                    "hours": round(delta, 1),
                    "days": round(delta / 24, 1),
                }
            )

    # 평균 간격
    if len(timestamps) > 1:
        total_hours = (latest - earliest).total_seconds() / 3600
        avg_interval = total_hours / (len(timestamps) - 1)
    else:
        avg_interval = 0.0

    return {
        "total_rows": len(rows),
        "date_range": date_range,
        "earliest": str(earliest),
        "latest": str(latest),
        "gaps_gt_10d": gaps,
        "avg_interval_hours": round(avg_interval, 2),
        "expected_interval_hours": 8,  # Bybit 기준
    }


def analyze_statistics(rates: list[float]) -> dict[str, float]:
    """펀딩비 통계."""
    if not rates:
        return {}

    import statistics

    return {
        "min": round(min(rates), 6),
        "max": round(max(rates), 6),
        "mean": round(statistics.mean(rates), 6),
        "median": round(statistics.median(rates), 6),
        "stdev": round(statistics.stdev(rates) if len(rates) > 1 else 0.0, 6),
        "positive_count": sum(1 for r in rates if r > 0),
        "negative_count": sum(1 for r in rates if r < 0),
        "zero_count": sum(1 for r in rates if r == 0),
    }


# ── 저장 로직 ──────────────────────────────────────────────────────────────────


async def save_to_parquet(
    rows: list[tuple[Any, float]], symbol: str, output_dir: Path
) -> str:
    """Polars DataFrame으로 변환하여 Parquet 저장."""
    if not rows:
        log.warning("no_data", symbol=symbol)
        return ""

    timestamps = [ts for ts, _ in rows]
    rates = [rate for _, rate in rows]

    # Polars DataFrame
    df = pl.DataFrame(
        {
            "ts": timestamps,
            "rate": rates,
        }
    ).with_columns(
        pl.col("ts").dt.cast_time_unit("ms")  # UTC milliseconds
    )

    # 파일명
    filename = f"{symbol}_8h.parquet"
    filepath = output_dir / filename

    # 저장
    df.write_parquet(str(filepath))
    log.info("saved_parquet", file=str(filepath), rows=len(rows))
    return str(filepath)


# ── 메인 ──────────────────────────────────────────────────────────────────────


async def main() -> None:
    """메인 진입점."""
    setup_logging()
    ensure_output_dir()

    log.info("connecting_db", host=DB_HOST, port=DB_PORT, db=DB_NAME)
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    except Exception as exc:
        log.error("db_connection_failed", error=str(exc))
        print(f"\n[ERROR] DB 연결 실패: {exc}")
        print(f"[HINT]  DB_HOST={DB_HOST}, DB_USER={DB_USER}")
        sys.exit(1)

    try:
        # 스키마 확인
        log.info("checking_schema")
        schema_info = await check_schema_for_older_data(pool)
        print("\n" + "=" * 80)
        print("데이터베이스 스키마 확인")
        print("=" * 80)
        print(f"발견된 테이블: {', '.join(schema_info['all_tables'][:5])}...")
        if schema_info["has_funding"]:
            print(f"  ✓ funding_rate_history: {schema_info.get('funding_rate_history_count', 'N/A')} 행")
        if schema_info["has_ohlcv"]:
            print(f"  ✓ ohlcv_history (1h): {schema_info.get('ohlcv_history_1h_count', 'N/A')} 행")
            if "ohlcv_earliest" in schema_info:
                print(f"    - 가장 오래된 OHLCV: {schema_info['ohlcv_earliest']}")
        print()

        # 펀딩비 데이터 조회
        log.info("fetching_funding_rates", symbol=SYMBOL)
        rows = await fetch_funding_rates(pool, SYMBOL)

        if not rows:
            log.warning("no_funding_data", symbol=SYMBOL)
            print(f"\n[WARNING] {SYMBOL} 펀딩비 데이터 없음")
            print("[INFO]   데이터 수집 스크립트를 먼저 실행하세요")
            await pool.close()
            sys.exit(1)

        # 갭 분석
        gap_analysis = analyze_gaps(rows, SYMBOL)
        print("=" * 80)
        print("펀딩비 데이터 범위")
        print("=" * 80)
        print(f"총 레코드 수: {gap_analysis['total_rows']:,}")
        print(f"범위: {gap_analysis['date_range']}")
        print(f"평균 간격: {gap_analysis['avg_interval_hours']} 시간 (예상: {gap_analysis['expected_interval_hours']} 시간)")

        if gap_analysis["gaps_gt_10d"]:
            print(f"\n10일 이상 갭: {len(gap_analysis['gaps_gt_10d'])}건")
            for gap in gap_analysis["gaps_gt_10d"][:5]:  # 최대 5개만 표시
                print(f"  - {gap['from'][:10]} ~ {gap['to'][:10]} ({gap['days']:.1f}일)")
            if len(gap_analysis["gaps_gt_10d"]) > 5:
                print(f"  ... 외 {len(gap_analysis['gaps_gt_10d']) - 5}건")
        else:
            print("\n10일 이상 갭: 없음 ✓")

        # 통계
        rates = [rate for _, rate in rows]
        stats = analyze_statistics(rates)
        print("\n펀딩비 통계:")
        print(f"  범위: {stats.get('min', 'N/A'):.6f} ~ {stats.get('max', 'N/A'):.6f}")
        print(f"  평균: {stats.get('mean', 'N/A'):.6f}")
        print(f"  중위: {stats.get('median', 'N/A'):.6f}")
        print(f"  양수: {stats.get('positive_count', 0):,}건")
        print(f"  음수: {stats.get('negative_count', 0):,}건")
        print(f"  0: {stats.get('zero_count', 0):,}건")

        # Parquet 저장
        filepath = await save_to_parquet(rows, SYMBOL, OUTPUT_DIR)

        print("\n" + "=" * 80)
        print("저장 완료")
        print("=" * 80)
        print(f"파일: {filepath}")
        print(f"크기: {Path(filepath).stat().st_size / (1024*1024):.2f} MB")

        # ⚠️ 데이터 갭 경고
        print("\n" + "=" * 80)
        print("⚠️  데이터 갭 경고")
        print("=" * 80)
        earliest = gap_analysis["earliest"]
        if "2023-04" in earliest:
            print(f"펀딩비 데이터: {earliest[:7]} 부터 시작")
            print("→ 2020-2022 기간은 DB에 없음 (Bybit 미제공)")
            print("→ 기존 6년 백테스트는 합성 폴백(0.0001 고정)을 사용했을 가능성 HIGH")
            print("→ Jesse 재시뮬레이션 시 실데이터만으로 검증 권장")
        print()

    finally:
        await pool.close()
        log.info("main_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
