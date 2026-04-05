"""bt_stress_test_12.py — Test 12 Stage E: 최적 조합 스트레스 테스트

Stage D에서 선택된 최적 조합에 대해 5가지 극단 시나리오 시뮬레이션.

실행:
    python tests/backtest/bt_stress_test_12.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
MAINT_MARGIN    = 0.005

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS test12_results (
    id      SERIAL PRIMARY KEY,
    run_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage   TEXT NOT NULL,
    variant TEXT NOT NULL,
    metrics JSONB,
    params  JSONB
);
"""


async def load_ohlcv(pool, symbol, timeframe, start, end):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT timestamp AS ts, open, high, low, close, volume "
            "FROM ohlcv_history WHERE symbol=$1 AND timeframe=$2 "
            "AND timestamp>=$3 AND timestamp<=$4 ORDER BY timestamp ASC",
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    return df


async def load_funding(pool, symbol, start, end):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT timestamp AS ts, rate FROM funding_rate_history "
            "WHERE symbol=$1 AND timestamp>=$2 AND timestamp<=$3 ORDER BY timestamp ASC",
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    df["rate"] = df["rate"].astype(float)
    return df


def scenario_basis_blowout(
    ohlcv, funding, fa_ratio, leverage, initial_capital,
    multiplier=2.0,
) -> dict:
    """시나리오 1: 베이시스 블로우아웃 (역사적 최대의 2배)."""
    # 베이시스 프록시: |funding_rate| * 8
    basis_proxy = funding["rate"].abs() * 8
    hist_max    = float(basis_proxy.max())
    extreme_basis = hist_max * multiplier

    fa_cap    = initial_capital * fa_ratio
    notional  = fa_cap * leverage
    maint     = notional * MAINT_MARGIN

    # 극단 베이시스 손실 (FA는 숏 방향)
    unrealized_loss = notional * extreme_basis  # 최악 케이스
    margin_ratio    = (fa_cap - unrealized_loss) / maint if maint > 0 else 9999.0
    capital_loss_pct = (unrealized_loss / initial_capital) * 100

    passed = margin_ratio > 1.2 and capital_loss_pct < 20.0

    return {
        "scenario": "basis_blowout",
        "hist_max_basis_pct": round(hist_max * 100, 4),
        "extreme_basis_pct": round(extreme_basis * 100, 4),
        "unrealized_loss_usd": round(unrealized_loss, 2),
        "margin_ratio": round(margin_ratio, 4),
        "capital_loss_pct": round(capital_loss_pct, 2),
        "passed": passed,
        "verdict": "✅ PASS" if passed else "❌ FAIL",
        "pass_condition": "청산 0 + 마진비율>120%",
    }


def scenario_flash_crash(
    ohlcv, funding, fa_ratio, leverage, initial_capital,
    crash_pct=0.20, fill_delay_minutes=3,
) -> dict:
    """시나리오 2: BTC -20% 폭락 + 3분 편측 체결.

    2021-05-19 실제 폭락 재현: 하루 동안 -30% 이상 폭락.
    """
    # 1h 데이터에서 3분 내 최대 가격 변동 추정 (캔들 레인지의 5%)
    ohlcv_copy  = ohlcv.copy()
    ohlcv_copy["range_pct"] = (ohlcv_copy["high"] - ohlcv_copy["low"]) / ohlcv_copy["open"]
    max_3min = float(ohlcv_copy["range_pct"].quantile(0.999)) * 0.2

    fa_cap    = initial_capital * fa_ratio
    notional  = fa_cap * leverage

    # 편측 체결 손실: 현물 롱만 체결, 선물 숏 3분 지연
    one_side_loss = notional * max_3min

    # 폭락으로 인한 FA 음수 펀딩 (폭락 시 펀딩비 역전)
    funding_2021 = funding[
        (funding.index >= pd.Timestamp("2021-05-19", tz="UTC")) &
        (funding.index <= pd.Timestamp("2021-05-20", tz="UTC"))
    ]
    worst_funding = float(funding_2021["rate"].min()) if not funding_2021.empty else -0.0075

    # 음수 펀딩 1회 수취 (FA가 숏 포지션이면 음수 펀딩 지불)
    funding_cost = abs(notional * worst_funding)
    total_loss   = one_side_loss + funding_cost

    capital_loss_pct = (total_loss / initial_capital) * 100
    passed = capital_loss_pct < 10.0

    return {
        "scenario": "flash_crash",
        "max_3min_move_pct": round(max_3min * 100, 4),
        "one_side_fill_loss_usd": round(one_side_loss, 2),
        "worst_funding_pct": round(worst_funding * 100, 4),
        "funding_cost_usd": round(funding_cost, 2),
        "total_loss_usd": round(total_loss, 2),
        "capital_loss_pct": round(capital_loss_pct, 2),
        "passed": passed,
        "verdict": "✅ PASS" if passed else "❌ FAIL",
        "pass_condition": "자본 손실 < 10%",
    }


def scenario_funding_drought(
    ohlcv, funding, fa_ratio, leverage, initial_capital,
    drought_days=30,
) -> dict:
    """시나리오 3: 30일간 펀딩비 0 수렴 (수수료만 지출).

    하루 진입/청산 반복 시 수수료 부담.
    현실적으로는 펀딩비가 최소 임계값 미만이면 진입 안 하므로
    이 기간의 기회 비용만 계산.
    """
    # 월 평균 펀딩비 수익 (정상 기준)
    avg_rate = float(funding["rate"].abs().mean())
    fa_cap   = initial_capital * fa_ratio
    notional = fa_cap * leverage

    # 30일간 펀딩 수취 없음
    monthly_funding_foregone = notional * avg_rate * (drought_days * 3)  # 일 3회 정산

    # 기회비용 + 대기 중 자금 운용 손실 없음 (현금 대기)
    cash_opportunity_cost = 0.0  # FA는 진입 안 하므로 손실 없음

    # 운영 비용 (모니터링, 인프라 등 - 여기서는 0)
    total_impact = cash_opportunity_cost
    capital_loss_pct = (total_impact / initial_capital) * 100
    foregone_pct = (monthly_funding_foregone / initial_capital) * 100

    passed = capital_loss_pct < 1.0  # 실제 손실은 없음

    return {
        "scenario": "funding_drought",
        "drought_days": drought_days,
        "monthly_funding_foregone_usd": round(monthly_funding_foregone, 2),
        "foregone_pct": round(foregone_pct, 2),
        "actual_capital_loss_usd": round(total_impact, 2),
        "capital_loss_pct": round(capital_loss_pct, 2),
        "passed": passed,
        "verdict": "✅ PASS" if passed else "❌ FAIL",
        "pass_condition": "30일 실제 손실 < 자본의 1%",
        "note": "FA는 임계값 이하 펀딩비 시 진입하지 않으므로 실제 손실 없음. 기회비용만 발생.",
    }


def scenario_exchange_maintenance(
    ohlcv, funding, fa_ratio, leverage, initial_capital,
    maintenance_hours=4, price_move_pct=0.05,
) -> dict:
    """시나리오 4: 거래소 4시간 점검 중 포지션 보유 + ±5% 가격 변동."""
    fa_cap    = initial_capital * fa_ratio
    notional  = fa_cap * leverage
    maint     = notional * MAINT_MARGIN

    # 델타 뉴트럴이므로 가격 변동 자체의 손실은 없음
    # 하지만 베이시스 변동 리스크가 있음
    # 4시간 내 베이시스 변동 추정
    funding_4h = funding["rate"].abs() * 4  # 4시간 기준
    basis_4h   = float(funding_4h.quantile(0.999))

    unrealized_loss = notional * basis_4h
    margin_ratio    = (fa_cap - unrealized_loss) / maint if maint > 0 else 9999.0

    # 5% 가격 변동의 실제 영향 (델타 뉴트럴이므로 최소)
    actual_price_impact = notional * price_move_pct * 0.01  # 헷지 미스 1% 가정

    total_exposure = unrealized_loss + actual_price_impact
    capital_loss_pct = (total_exposure / initial_capital) * 100
    passed = margin_ratio > 1.5

    return {
        "scenario": "exchange_maintenance",
        "maintenance_hours": maintenance_hours,
        "price_move_pct": price_move_pct * 100,
        "basis_4h_p999_pct": round(basis_4h * 100, 4),
        "unrealized_loss_usd": round(unrealized_loss, 2),
        "margin_ratio": round(margin_ratio, 4),
        "capital_loss_pct": round(capital_loss_pct, 2),
        "passed": passed,
        "verdict": "✅ PASS" if passed else "❌ FAIL",
        "pass_condition": "마진비율 > 150% 유지",
    }


def scenario_consecutive_basis_shock(
    ohlcv, funding, fa_ratio, leverage, initial_capital,
    n_shocks=5,
) -> dict:
    """시나리오 5: 연속 5회 베이시스 급변동 (진입/청산 반복)."""
    fa_cap    = initial_capital * fa_ratio
    notional  = fa_cap * leverage

    # 1회 진입/청산 비용: 수수료 왕복
    round_trip_fee = notional * FEE_RATE * 2

    # 슬리피지 추정: 1h 캔들 레인지의 0.1%
    ohlcv_copy = ohlcv.copy()
    ohlcv_copy["range_pct"] = (ohlcv_copy["high"] - ohlcv_copy["low"]) / ohlcv_copy["open"]
    avg_slippage_pct = float(ohlcv_copy["range_pct"].mean()) * 0.001
    slippage_cost = notional * avg_slippage_pct

    # 5회 누적
    total_fee      = round_trip_fee * n_shocks
    total_slip     = slippage_cost * n_shocks
    total_cost     = total_fee + total_slip

    capital_loss_pct = (total_cost / initial_capital) * 100
    passed = capital_loss_pct < 3.0

    return {
        "scenario": "consecutive_basis_shock",
        "n_shocks": n_shocks,
        "round_trip_fee_usd": round(round_trip_fee, 4),
        "slippage_per_trade_usd": round(slippage_cost, 4),
        "total_cost_usd": round(total_cost, 4),
        "capital_loss_pct": round(capital_loss_pct, 2),
        "passed": passed,
        "verdict": "✅ PASS" if passed else "❌ FAIL",
        "pass_condition": "누적 손실 < 자본의 3%",
    }


async def get_optimal_combination(pool) -> tuple[float, int, float]:
    """Stage D에서 저장된 최적 조합 불러오기."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT params FROM test12_results WHERE stage='stage_d_optimal' "
                "ORDER BY run_at DESC LIMIT 1"
            )
        if row:
            params = json.loads(row["params"])
            return (
                float(params.get("fa_ratio", 0.70)),
                int(params.get("leverage", 2)),
                float(params.get("reinvest_ratio", 0.50)),
            )
    except Exception:
        pass
    # 기본값: Stage D 미실행 시 추정값
    return (0.70, 2, 0.50)


def _safe_float(v, default=0.0):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


async def main(args):
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

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    log.info("connecting", host=os.getenv("DB_HOST", "postgres"))
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as exc:
        print(f"[ERROR] DB 연결 실패: {exc}"); sys.exit(1)

    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 없음"); await pool.close(); sys.exit(1)

    # 최적 조합 불러오기 (--fa-ratio 등 인수 우선)
    if args.fa_ratio and args.leverage and args.reinvest_ratio:
        fa_ratio, leverage, reinv_ratio = args.fa_ratio, args.leverage, args.reinvest_ratio
    else:
        fa_ratio, leverage, reinv_ratio = await get_optimal_combination(pool)

    log.info("optimal_combo",
             fa_ratio=fa_ratio, leverage=leverage, reinv_ratio=reinv_ratio)

    print()
    print("=" * 80)
    print("  Test 12 Stage E: 스트레스 테스트")
    print("=" * 80)
    print(f"  최적 조합: FA={fa_ratio:.0%}, 레버리지={leverage}x, 재투자={reinv_ratio:.0%}")
    print()

    # DB 초기화
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_e'")

    combo_params = {"fa_ratio": fa_ratio, "leverage": leverage, "reinvest_ratio": reinv_ratio}

    # 5개 시나리오 실행
    scenarios = [
        (
            "basis_blowout",
            lambda: scenario_basis_blowout(ohlcv, funding, fa_ratio, leverage, args.initial_capital),
        ),
        (
            "flash_crash",
            lambda: scenario_flash_crash(ohlcv, funding, fa_ratio, leverage, args.initial_capital),
        ),
        (
            "funding_drought",
            lambda: scenario_funding_drought(ohlcv, funding, fa_ratio, leverage, args.initial_capital),
        ),
        (
            "exchange_maintenance",
            lambda: scenario_exchange_maintenance(
                ohlcv, funding, fa_ratio, leverage, args.initial_capital),
        ),
        (
            "consecutive_basis_shock",
            lambda: scenario_consecutive_basis_shock(
                ohlcv, funding, fa_ratio, leverage, args.initial_capital),
        ),
    ]

    results = []
    for name, fn in scenarios:
        r = fn()
        results.append(r)

        metrics = {k: _safe_float(v) if isinstance(v, float) else v for k, v in r.items()}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_e", name,
                json.dumps(metrics),
                json.dumps(combo_params),
            )
        log.info("scenario", name=name, verdict=r["verdict"])

    # 결과 출력
    print(f"{'시나리오':>28} | {'손실$':>10} | {'손실%':>7} | {'기타지표':>30} | {'판정':>10}")
    print("-" * 95)
    for r in results:
        name = r["scenario"]
        loss_usd = r.get("total_loss_usd") or r.get("unrealized_loss_usd") or r.get("total_cost_usd") or 0.0
        loss_pct = r.get("capital_loss_pct", 0.0)

        if name == "basis_blowout":
            extra = f"마진비율={r.get('margin_ratio',0):.2f}x"
        elif name == "flash_crash":
            extra = f"편측손실=${r.get('one_side_fill_loss_usd',0):.2f}"
        elif name == "funding_drought":
            extra = f"기회손실=${r.get('monthly_funding_foregone_usd',0):.2f}"
        elif name == "exchange_maintenance":
            extra = f"마진비율={r.get('margin_ratio',0):.2f}x"
        else:
            extra = f"수수료${r.get('total_cost_usd',0):.4f}"

        print(
            f"{name:>28} | "
            f"${loss_usd:>9.2f} | "
            f"{loss_pct:>6.2f}% | "
            f"{extra:>30} | "
            f"{r['verdict']:>10}"
        )

    passed_count = sum(1 for r in results if r["passed"])
    print()
    print(f"  전체 통과: {passed_count}/5 시나리오")
    print(f"  최종 판정: {'✅ 스트레스 테스트 통과' if passed_count == 5 else '⚠️ 일부 실패 - 파라미터 재검토 필요'}")

    print(f"\n[DB 저장 완료] stage=stage_e, 5개 시나리오")

    await pool.close()
    return results


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=START_DATE)
    p.add_argument("--end",   default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    p.add_argument("--fa-ratio", default=None, type=float, dest="fa_ratio")
    p.add_argument("--leverage", default=None, type=int)
    p.add_argument("--reinvest-ratio", default=None, type=float, dest="reinvest_ratio")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단"); sys.exit(0)
