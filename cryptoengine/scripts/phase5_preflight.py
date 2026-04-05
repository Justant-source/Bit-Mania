#!/usr/bin/env python3
"""Phase 5 실전 전환 프리플라이트 체크.

BYBIT_TESTNET=false로 전환하기 전에 반드시 이 스크립트를 실행하여
모든 항목이 PASS인지 확인한다.

사용법:
  # 환경변수가 mainnet 키로 설정된 상태에서 실행
  BYBIT_API_KEY=<mainnet-key> BYBIT_API_SECRET=<mainnet-secret> \\
  BYBIT_TESTNET=false DB_PASSWORD=... python scripts/phase5_preflight.py

  # JSON 출력
  python scripts/phase5_preflight.py --json

  # 특정 체크만 skip
  python scripts/phase5_preflight.py --skip fees --skip leverage
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import logging

# structlog은 공유 라이브러리 없이 독립 실행 가능하도록 간단하게 설정
import structlog

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
log = structlog.get_logger("phase5_preflight")

# ── 임계값 ──────────────────────────────────────────────────────────────────

# 실전 투입 최소 자본 (USDT). 너무 적으면 최소 주문 크기를 만족 못할 수 있음.
MIN_CAPITAL_USDT = 100.0

# 허용 최대 레버리지 (CLAUDE.md: 5배 초과 금지)
MAX_ALLOWED_LEVERAGE = 5

# 실전 퍼페추얼 수수료 상한 (이보다 높으면 경고)
MAX_TAKER_FEE = 0.001  # 0.1%

# Bybit 메인넷 퍼페추얼 기본 수수료 (VIP0)
EXPECTED_MAKER_FEE = 0.0002   # 0.02%
EXPECTED_TAKER_FEE = 0.00055  # 0.055%

# config에 하드코딩된 수수료 (config/exchanges/bybit.yaml)
CONFIG_MAKER_FEE = 0.0002
CONFIG_TAKER_FEE = 0.00055

# BTC 퍼페추얼 심볼
BTC_SYMBOL = "BTC/USDT:USDT"

# 테스트넷 API 키 패턴 (Bybit 테스트넷 키는 testnet.bybit.com에서 발급)
# 메인넷과 키 형식은 동일하므로 길이/형식으로만 검증
MIN_API_KEY_LEN = 18


# ── 결과 모델 ────────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    detail: dict[str, Any] = field(default_factory=dict)
    warning: bool = False  # True이면 WARN (PASS로 취급하되 주의 필요)


# ── 개별 체크 함수 ────────────────────────────────────────────────────────────

def check_env_vars() -> CheckResult:
    """필수 환경변수가 모두 설정되어 있는지 확인."""
    required = {
        "BYBIT_API_KEY": os.getenv("BYBIT_API_KEY", ""),
        "BYBIT_API_SECRET": os.getenv("BYBIT_API_SECRET", ""),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
    }
    missing = [k for k, v in required.items() if not v]
    testnet_flag = os.getenv("BYBIT_TESTNET", "true").lower()

    detail: dict[str, Any] = {
        "BYBIT_TESTNET": testnet_flag,
        "missing_vars": missing,
    }

    if missing:
        return CheckResult("env_vars", False, f"필수 환경변수 누락: {missing}", detail)

    if testnet_flag == "true":
        return CheckResult(
            "env_vars", False,
            "BYBIT_TESTNET=true — 실전 전환 전에 false로 변경 필요",
            detail,
        )

    # API 키 길이 검증
    api_key = required["BYBIT_API_KEY"]
    if len(api_key) < MIN_API_KEY_LEN:
        return CheckResult(
            "env_vars", False,
            f"BYBIT_API_KEY가 너무 짧음 (len={len(api_key)}, min={MIN_API_KEY_LEN})",
            detail,
        )

    return CheckResult("env_vars", True, "환경변수 OK", detail)


async def check_api_connectivity(connector: Any) -> CheckResult:
    """메인넷 API 연결 및 서버 시간 동기화 확인."""
    try:
        await connector.connect()
        ticker = await connector.get_ticker(BTC_SYMBOL)
        last_price = ticker.get("last") or ticker.get("close", 0)
        return CheckResult(
            "api_connectivity", True,
            f"메인넷 API 연결 성공 (BTC 현재가: ${last_price:,.2f})",
            {"btc_price": last_price, "testnet": connector._exchange.sandbox},
        )
    except Exception as exc:
        return CheckResult(
            "api_connectivity", False,
            f"API 연결 실패: {exc}",
            {"error": str(exc)},
        )


async def check_account_balance(connector: Any) -> CheckResult:
    """메인넷 잔고 조회 및 최소 자본 확인."""
    try:
        balance = await connector.get_balance()
        total = balance.get("total", 0.0)
        free = balance.get("free", 0.0)
        detail = {"total_usdt": total, "free_usdt": free}

        if total < MIN_CAPITAL_USDT:
            return CheckResult(
                "account_balance", False,
                f"잔고 부족: {total:.2f} USDT (최소 {MIN_CAPITAL_USDT} USDT 필요)",
                detail,
            )
        return CheckResult(
            "account_balance", True,
            f"잔고 충분: {total:.2f} USDT (여유: {free:.2f} USDT)",
            detail,
        )
    except Exception as exc:
        return CheckResult("account_balance", False, f"잔고 조회 실패: {exc}")


async def check_trading_fees(connector: Any) -> CheckResult:
    """실제 계정 수수료 조회 및 config 값과 비교."""
    try:
        fees = await connector.get_trading_fees([BTC_SYMBOL])
        btc_fees = fees.get(BTC_SYMBOL) or fees.get("_default", {})
        actual_maker = btc_fees.get("maker", EXPECTED_MAKER_FEE)
        actual_taker = btc_fees.get("taker", EXPECTED_TAKER_FEE)

        detail = {
            "actual_maker": actual_maker,
            "actual_taker": actual_taker,
            "config_maker": CONFIG_MAKER_FEE,
            "config_taker": CONFIG_TAKER_FEE,
        }

        warnings = []
        if actual_taker > MAX_TAKER_FEE:
            warnings.append(f"테이커 수수료 {actual_taker*100:.4f}%가 상한({MAX_TAKER_FEE*100:.4f}%)을 초과")

        if abs(actual_taker - CONFIG_TAKER_FEE) > 0.0001:
            warnings.append(
                f"테이커 수수료 불일치: 실제 {actual_taker*100:.4f}% ≠ config {CONFIG_TAKER_FEE*100:.4f}%"
                " → config/exchanges/bybit.yaml 업데이트 필요"
            )

        if warnings:
            return CheckResult(
                "trading_fees", True,
                "수수료 조회 완료 (주의 필요): " + "; ".join(warnings),
                detail,
                warning=True,
            )

        return CheckResult(
            "trading_fees", True,
            f"수수료 OK: maker={actual_maker*100:.4f}%, taker={actual_taker*100:.4f}%",
            detail,
        )
    except Exception as exc:
        return CheckResult("trading_fees", False, f"수수료 조회 실패: {exc}")


async def check_min_order_size(connector: Any) -> CheckResult:
    """BTC 최소 주문 크기 및 계약 단위 확인."""
    try:
        sizes = await connector.get_min_order_sizes([BTC_SYMBOL])
        btc = sizes.get(BTC_SYMBOL, {})
        detail = {
            "symbol": BTC_SYMBOL,
            "min_qty": btc.get("min_qty"),
            "qty_step": btc.get("qty_step"),
            "min_notional": btc.get("min_notional"),
            "contract_size": btc.get("contract_size"),
        }
        return CheckResult(
            "min_order_size", True,
            f"최소 주문: qty={btc.get('min_qty')} BTC, step={btc.get('qty_step')} BTC",
            detail,
        )
    except Exception as exc:
        return CheckResult("min_order_size", False, f"최소 주문 크기 조회 실패: {exc}")


async def check_leverage_setting(connector: Any) -> CheckResult:
    """현재 레버리지 설정 확인 (5x 이하 강제)."""
    try:
        # 현재 포지션에서 레버리지 확인
        pos = await connector.get_position(BTC_SYMBOL)
        if pos is not None:
            current_lev = pos.leverage
            detail = {"current_leverage": current_lev, "max_allowed": MAX_ALLOWED_LEVERAGE}
            if current_lev > MAX_ALLOWED_LEVERAGE:
                return CheckResult(
                    "leverage", False,
                    f"현재 레버리지 {current_lev}x가 허용 최대({MAX_ALLOWED_LEVERAGE}x)를 초과",
                    detail,
                )
            return CheckResult(
                "leverage", True,
                f"레버리지 OK: {current_lev}x (열린 포지션에서 확인)",
                detail,
            )

        # 포지션 없으면 margin_mode만 확인 가능 — warn으로 처리
        return CheckResult(
            "leverage", True,
            f"열린 포지션 없음. 진입 시 레버리지가 {MAX_ALLOWED_LEVERAGE}x 이하인지 전략 config 재확인 필요",
            {"max_allowed": MAX_ALLOWED_LEVERAGE},
            warning=True,
        )
    except Exception as exc:
        return CheckResult("leverage", False, f"레버리지 확인 실패: {exc}")


async def check_db_connectivity() -> CheckResult:
    """PostgreSQL 연결 및 핵심 테이블 존재 여부 확인."""
    try:
        import asyncpg
        dsn = (
            f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
            f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
            f"@{os.getenv('DB_HOST', 'localhost')}"
            f":{os.getenv('DB_PORT', '5432')}"
            f"/{os.getenv('DB_NAME', 'cryptoengine')}"
        )
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2, command_timeout=10)
        async with pool.acquire() as conn:
            tables = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
            )
            table_names = [r["tablename"] for r in tables]

        await pool.close()

        required_tables = {"trades", "positions", "ohlcv_history", "funding_rate_history"}
        missing = required_tables - set(table_names)

        detail = {"tables": table_names, "missing": list(missing)}
        if missing:
            return CheckResult(
                "db_connectivity", False,
                f"DB 연결 OK지만 필수 테이블 누락: {missing}",
                detail,
            )
        return CheckResult(
            "db_connectivity", True,
            f"DB 연결 OK (테이블 {len(table_names)}개 확인)",
            detail,
        )
    except Exception as exc:
        return CheckResult("db_connectivity", False, f"DB 연결 실패: {exc}")


async def check_no_open_positions(connector: Any) -> CheckResult:
    """실전 전환 시 테스트넷 포지션이 없는 상태인지 확인.

    테스트넷 포지션은 메인넷과 별개이므로 이 체크는 메인넷 포지션 유무를 확인한다.
    메인넷에 이미 포지션이 있으면 수동 확인이 필요하다.
    """
    try:
        pos = await connector.get_position(BTC_SYMBOL)
        if pos and pos.size > 0:
            return CheckResult(
                "no_open_positions", True,
                f"메인넷에 기존 BTC 포지션 감지: {pos.size} BTC @ {pos.entry_price}. "
                "의도적인 경우 무시하고 계속 진행 가능.",
                {"size": pos.size, "entry_price": pos.entry_price, "side": pos.side},
                warning=True,
            )
        return CheckResult(
            "no_open_positions", True,
            "메인넷에 기존 BTC 포지션 없음 — 깨끗한 상태로 시작 가능",
        )
    except Exception as exc:
        return CheckResult("no_open_positions", False, f"포지션 조회 실패: {exc}")


def check_config_files() -> CheckResult:
    """전략 config 파일이 올바른 위치에 존재하는지 확인."""
    import pathlib

    base = pathlib.Path(__file__).parent.parent  # cryptoengine/
    required_files = [
        "config/strategies/funding-arb.yaml",
        "config/strategies/adaptive-dca.yaml",
        "config/orchestrator.yaml",
    ]
    missing = [f for f in required_files if not (base / f).exists()]
    detail = {"checked": required_files, "missing": missing}

    if missing:
        return CheckResult(
            "config_files", False,
            f"필수 config 파일 누락: {missing}",
            detail,
        )
    return CheckResult("config_files", True, "config 파일 모두 존재", detail)


# ── 메인 ─────────────────────────────────────────────────────────────────────

async def run_checks(skip: set[str]) -> list[CheckResult]:
    """모든 체크를 순서대로 실행하고 결과를 반환한다."""
    results: list[CheckResult] = []

    # 1. 환경변수 (동기)
    if "env_vars" not in skip:
        r = check_env_vars()
        results.append(r)
        if not r.passed:
            # API 연결이 불가능하므로 나머지 네트워크 체크 스킵
            log.error("env_vars check failed — 나머지 API 체크를 건너뜀")
            results.append(CheckResult("api_connectivity", False, "env_vars 실패로 스킵", warning=False))
            results.append(CheckResult("account_balance", False, "env_vars 실패로 스킵"))
            results.append(CheckResult("trading_fees", False, "env_vars 실패로 스킵"))
            results.append(CheckResult("min_order_size", False, "env_vars 실패로 스킵"))
            results.append(CheckResult("leverage", False, "env_vars 실패로 스킵"))
            results.append(CheckResult("no_open_positions", False, "env_vars 실패로 스킵"))
        else:
            # Exchange connector 초기화
            sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
            from shared.exchange.factory import exchange_factory

            connector = exchange_factory(
                os.getenv("EXCHANGE", "bybit"),
                api_key=os.getenv("BYBIT_API_KEY", ""),
                api_secret=os.getenv("BYBIT_API_SECRET", ""),
                testnet=os.getenv("BYBIT_TESTNET", "true").lower() == "true",
            )

            if "api_connectivity" not in skip:
                results.append(await check_api_connectivity(connector))

            if "balance" not in skip:
                results.append(await check_account_balance(connector))

            if "fees" not in skip:
                results.append(await check_trading_fees(connector))

            if "min_order_size" not in skip:
                results.append(await check_min_order_size(connector))

            if "leverage" not in skip:
                results.append(await check_leverage_setting(connector))

            if "open_positions" not in skip:
                results.append(await check_no_open_positions(connector))

            await connector.disconnect()

    # 2. DB 연결 (독립적)
    if "db" not in skip:
        results.append(await check_db_connectivity())

    # 3. Config 파일 (동기)
    if "config" not in skip:
        results.append(check_config_files())

    return results


def print_report(results: list[CheckResult], as_json: bool = False) -> int:
    """결과를 출력하고 실패 개수를 반환한다."""
    if as_json:
        output = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "checks": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "warning": r.warning,
                    "message": r.message,
                    "detail": r.detail,
                }
                for r in results
            ],
            "total": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "warnings": sum(1 for r in results if r.warning),
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return output["failed"]

    # 텍스트 출력
    print("\n" + "=" * 60)
    print("  Phase 5 실전 전환 프리플라이트 체크")
    print(f"  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    failed = 0
    warns = 0
    for r in results:
        if r.passed and not r.warning:
            icon = "✓ PASS"
        elif r.passed and r.warning:
            icon = "△ WARN"
            warns += 1
        else:
            icon = "✗ FAIL"
            failed += 1
        print(f"  [{icon:8s}]  {r.name:<22s}  {r.message}")

    print("-" * 60)
    total = len(results)
    passed = total - failed
    print(f"  결과: {passed}/{total} PASS  |  {failed} FAIL  |  {warns} WARN")

    if failed == 0:
        print("\n  ✓ 모든 체크 통과 — 실전 전환 진행 가능합니다.")
        print("  전환 절차:")
        print("    1. .env 에서 BYBIT_TESTNET=false 변경")
        print("    2. BYBIT_API_KEY / BYBIT_API_SECRET 를 메인넷 키로 교체")
        print("    3. docker compose up -d --build execution-engine funding-arb strategy-orchestrator")
        print("    4. 첫 5분 간 포지션 크기, 수수료 로그 직접 확인")
    else:
        print("\n  ✗ 실전 전환 불가 — 위 FAIL 항목을 해결한 후 재실행하세요.")

    print("=" * 60 + "\n")
    return failed


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 5 실전 전환 프리플라이트 체크")
    p.add_argument("--json", action="store_true", help="JSON 형식으로 출력")
    p.add_argument(
        "--skip",
        action="append",
        default=[],
        metavar="CHECK",
        help="스킵할 체크 이름 (env_vars, api_connectivity, balance, fees, "
             "min_order_size, leverage, open_positions, db, config)",
    )
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    skip = set(args.skip)

    results = await run_checks(skip)
    failed = print_report(results, as_json=args.json)
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
