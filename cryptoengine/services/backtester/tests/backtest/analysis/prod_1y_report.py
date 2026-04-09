"""analysis/prod_1y_report.py
현재 production 파라미터로 최근 1년 시뮬레이션 → 상세 거래일지 + 수익 분석 Markdown 리포트.

초기 자본: $200 USD
파라미터:
    min_funding_rate    = 0.00005  (0.005%/8h, 연 ~5.48%)
    consecutive_intervals = 3
    fa_capital_ratio    = 0.80
    leverage            = 5x
    spot_fee            = 0.0001   (0.01%)
    perp_fee            = 0.00055  (0.055%)
    reinvest_ratio      = 0.30

실행:
    docker compose --profile backtest run --rm backtester \
      python tests/backtest/analysis/prod_1y_report.py
"""
from __future__ import annotations

import asyncio
import math
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp
import pandas as pd


# ── 파라미터 ─────────────────────────────────────────────────────────────────
INITIAL_CAPITAL      = 200.0
FA_RATIO             = 0.80
LEVERAGE             = 5.0
REINVEST_RATIO       = 0.30
SPOT_FEE             = 0.0001
PERP_FEE             = 0.00055
ONE_WAY_FEE          = SPOT_FEE + PERP_FEE   # 0.00065
MIN_RATE             = 0.00005   # 0.005%/8h
CONSECUTIVE          = 3
MAX_HOLD_BARS        = 720 // 8  # 90 bars × 8h = 720h = 30일
EXIT_MIN_RATE        = 0.0000457 # 연 5% / 1095
EXIT_FLIP_CNT        = 3
SLIPPAGE_RATE        = 0.0003    # 편도 슬리피지 0.03%

SYMBOL = "BTCUSDT"
KST    = timezone(timedelta(hours=9))


# ── Bybit 펀딩비 조회 ─────────────────────────────────────────────────────────
async def fetch_funding(start_ts: int, end_ts: int) -> pd.DataFrame:
    url     = "https://api.bybit.com/v5/market/funding/history"
    records: list[dict] = []
    cur_end = end_ts
    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            params = {
                "category": "linear",
                "symbol":   SYMBOL,
                "startTime": str(start_ts),
                "endTime":   str(cur_end),
                "limit":    "200",
            }
            async with session.get(url, params=params) as resp:
                data = await resp.json()
            rows = data.get("result", {}).get("list", [])
            if not rows:
                break
            for r in rows:
                records.append({"ts": int(r["fundingRateTimestamp"]), "rate": float(r["fundingRate"])})
            oldest = min(int(r["fundingRateTimestamp"]) for r in rows)
            if oldest <= start_ts:
                break
            cur_end = oldest - 1

    if not records:
        return pd.DataFrame(columns=["ts", "rate"])
    df = pd.DataFrame(records)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", inplace=True)
    df.set_index("ts", inplace=True)
    return df


# ── 시뮬레이션 ────────────────────────────────────────────────────────────────
def simulate(funding: pd.DataFrame) -> tuple[list[dict], list[float]]:
    """상세 거래 시뮬레이션. 재투자 포함."""
    equity          = INITIAL_CAPITAL
    equity_curve    = [equity]
    consec          = 0
    position: dict | None = None
    trades: list[dict] = []

    for ts, row in funding.iterrows():
        rate = float(row["rate"])

        if position is None:
            # ── 진입 판단 ──
            if rate >= MIN_RATE:
                consec += 1
            else:
                consec = 0

            if consec >= CONSECUTIVE:
                # 포지션 크기 계산
                fa_cap   = equity * FA_RATIO
                notional = fa_cap * LEVERAGE
                entry_fee    = notional * ONE_WAY_FEE
                entry_slip   = notional * SLIPPAGE_RATE
                total_entry_cost = entry_fee + entry_slip
                equity -= total_entry_cost

                position = {
                    "entry_ts":       ts,
                    "entry_rate":     rate,
                    "notional":       notional,
                    "entry_fee":      entry_fee,
                    "entry_slip":     entry_slip,
                    "bars_held":      0,
                    "funding_income": 0.0,
                    "flip_cnt":       0,
                }
                consec = 0
        else:
            # ── 펀딩비 수취 ──
            income = position["notional"] * rate
            position["funding_income"] += income
            equity += income
            position["bars_held"] += 1

            # ── 청산 판단 ──
            if rate < 0:
                position["flip_cnt"] += 1
            else:
                position["flip_cnt"] = 0

            reason = None
            if position["flip_cnt"] >= EXIT_FLIP_CNT:
                reason = "rate_flip"
            elif rate < EXIT_MIN_RATE and rate >= 0:
                reason = "rate_below_exit"
            elif position["bars_held"] >= MAX_HOLD_BARS:
                reason = "max_hold"

            if reason:
                notional  = position["notional"]
                exit_fee  = notional * ONE_WAY_FEE
                exit_slip = notional * SLIPPAGE_RATE
                equity   -= (exit_fee + exit_slip)

                net_pnl = (
                    position["funding_income"]
                    - position["entry_fee"]
                    - position["entry_slip"]
                    - exit_fee
                    - exit_slip
                )

                # 재투자: 수익의 30%를 equity에 복리 추가
                if net_pnl > 0:
                    reinvest = net_pnl * REINVEST_RATIO
                    # reinvest는 이미 equity에 반영됨 (이익 = 펀딩수취 - 수수료)
                    # 30%는 그냥 equity에 남김 (spot BTC로 가는 부분은 tracking용)
                    spot_btc_usd = reinvest
                else:
                    spot_btc_usd = 0.0

                trades.append({
                    "no":               len(trades) + 1,
                    "entry_ts":         position["entry_ts"],
                    "exit_ts":          ts,
                    "entry_rate":       position["entry_rate"],
                    "exit_rate":        rate,
                    "bars_held":        position["bars_held"],
                    "hold_hours":       position["bars_held"] * 8,
                    "notional":         round(notional, 2),
                    "funding_income":   round(position["funding_income"], 4),
                    "entry_fee":        round(position["entry_fee"], 4),
                    "entry_slip":       round(position["entry_slip"], 4),
                    "exit_fee":         round(exit_fee, 4),
                    "exit_slip":        round(exit_slip, 4),
                    "total_cost":       round(position["entry_fee"] + position["entry_slip"] + exit_fee + exit_slip, 4),
                    "net_pnl":          round(net_pnl, 4),
                    "equity_after":     round(equity, 4),
                    "exit_reason":      reason,
                    "reinvest_usd":     round(spot_btc_usd, 4),
                })
                position = None

        equity_curve.append(equity)

    # 미청산 포지션 강제 청산
    if position:
        notional  = position["notional"]
        exit_fee  = notional * ONE_WAY_FEE
        exit_slip = notional * SLIPPAGE_RATE
        equity   -= (exit_fee + exit_slip)
        net_pnl   = (
            position["funding_income"]
            - position["entry_fee"]
            - position["entry_slip"]
            - exit_fee
            - exit_slip
        )
        trades.append({
            "no":               len(trades) + 1,
            "entry_ts":         position["entry_ts"],
            "exit_ts":          funding.index[-1],
            "entry_rate":       position["entry_rate"],
            "exit_rate":        float(funding.iloc[-1]["rate"]),
            "bars_held":        position["bars_held"],
            "hold_hours":       position["bars_held"] * 8,
            "notional":         round(notional, 2),
            "funding_income":   round(position["funding_income"], 4),
            "entry_fee":        round(position["entry_fee"], 4),
            "entry_slip":       round(position["entry_slip"], 4),
            "exit_fee":         round(exit_fee, 4),
            "exit_slip":        round(exit_slip, 4),
            "total_cost":       round(position["entry_fee"] + position["entry_slip"] + exit_fee + exit_slip, 4),
            "net_pnl":          round(net_pnl, 4),
            "equity_after":     round(equity, 4),
            "exit_reason":      "forced_close (기간 만료)",
            "reinvest_usd":     0.0,
        })
        equity_curve.append(equity)

    return trades, equity_curve


# ── 지표 계산 ─────────────────────────────────────────────────────────────────
def calc_metrics(equity_curve: list[float], trades: list[dict], days: float) -> dict:
    eq   = equity_curve
    init = eq[0]
    final= eq[-1]
    total_ret = (final - init) / init * 100

    # CAGR
    years = days / 365
    cagr  = ((final / init) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    # MDD
    peak  = init
    mdd   = 0.0
    for v in eq:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd:
            mdd = dd

    # Sharpe (일별 수익률 기반)
    daily: dict[str, float] = {}
    for t in trades:
        day = t["exit_ts"].date().isoformat()
        daily[day] = daily.get(day, 0.0) + t["net_pnl"]
    if len(daily) > 1:
        rets = list(daily.values())
        mean = sum(rets) / len(rets)
        var  = sum((r - mean) ** 2 for r in rets) / len(rets)
        std  = math.sqrt(var) if var > 0 else 1e-9
        sharpe = (mean / std) * math.sqrt(252)
    else:
        sharpe = 0.0

    wins   = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    total_funding = sum(t["funding_income"] for t in trades)
    total_fees    = sum(t["total_cost"] for t in trades)
    total_net     = sum(t["net_pnl"] for t in trades)
    avg_hold_h    = sum(t["hold_hours"] for t in trades) / len(trades) if trades else 0

    return {
        "total_ret":      round(total_ret, 2),
        "cagr":           round(cagr, 2),
        "mdd":            round(mdd, 2),
        "sharpe":         round(sharpe, 3),
        "final_equity":   round(final, 4),
        "total_trades":   len(trades),
        "win_count":      len(wins),
        "loss_count":     len(losses),
        "win_rate":       round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "total_funding":  round(total_funding, 4),
        "total_fees":     round(total_fees, 4),
        "total_net":      round(total_net, 4),
        "avg_net_pnl":    round(total_net / len(trades), 4) if trades else 0,
        "avg_hold_h":     round(avg_hold_h, 1),
        "max_net_pnl":    round(max((t["net_pnl"] for t in trades), default=0), 4),
        "min_net_pnl":    round(min((t["net_pnl"] for t in trades), default=0), 4),
    }


# ── 월별 집계 ─────────────────────────────────────────────────────────────────
def monthly_summary(trades: list[dict]) -> dict[str, dict]:
    monthly: dict[str, dict] = {}
    for t in trades:
        key = t["exit_ts"].astimezone(KST).strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"count": 0, "funding": 0.0, "fees": 0.0, "net": 0.0}
        monthly[key]["count"]   += 1
        monthly[key]["funding"] += t["funding_income"]
        monthly[key]["fees"]    += t["total_cost"]
        monthly[key]["net"]     += t["net_pnl"]
    return dict(sorted(monthly.items()))


# ── Markdown 생성 ─────────────────────────────────────────────────────────────
def build_markdown(
    trades: list[dict],
    equity_curve: list[float],
    metrics: dict,
    monthly: dict[str, dict],
    start_dt: datetime,
    end_dt: datetime,
    funding: pd.DataFrame,
) -> str:
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    sd = start_dt.astimezone(KST).strftime("%Y-%m-%d")
    ed = end_dt.astimezone(KST).strftime("%Y-%m-%d")

    lines: list[str] = []
    A = lines.append

    A(f"# CryptoEngine FA 전략 1년 시뮬레이션 리포트")
    A(f"")
    A(f"> **생성:** {now_kst}  ")
    A(f"> **기간:** {sd} ~ {ed}  ")
    A(f"> **대상:** Bybit BTCUSDT 무기한 선물 (실제 Mainnet 펀딩비)")
    A(f"")
    A(f"---")
    A(f"")
    A(f"## 1. 운영 파라미터")
    A(f"")
    A(f"| 항목 | 값 |")
    A(f"|------|---|")
    A(f"| 초기 자본 | **${INITIAL_CAPITAL:.0f} USDT** |")
    A(f"| 전략 | FA (델타뉴트럴 펀딩비 차익거래) |")
    A(f"| FA 자본 비율 | {FA_RATIO*100:.0f}% |")
    A(f"| 레버리지 | {LEVERAGE:.0f}x |")
    A(f"| 진입 최소 펀딩비 | {MIN_RATE:.6f} ({MIN_RATE*3*365*100:.2f}% ann) |")
    A(f"| 진입 연속 구간 | {CONSECUTIVE}회 연속 |")
    A(f"| 청산 최소 펀딩비 | {EXIT_MIN_RATE:.7f} (연 ~5%) |")
    A(f"| 최대 보유 기간 | {MAX_HOLD_BARS*8}h ({MAX_HOLD_BARS*8//24}일) |")
    A(f"| 펀딩비 역전 청산 | {EXIT_FLIP_CNT}회 연속 음수 |")
    A(f"| 현물 수수료 | {SPOT_FEE*100:.3f}% (taker) |")
    A(f"| 선물 수수료 | {PERP_FEE*100:.4f}% (taker) |")
    A(f"| 왕복 수수료 합계 | {ONE_WAY_FEE*2*100:.4f}% |")
    A(f"| 슬리피지 (편도) | {SLIPPAGE_RATE*100:.2f}% |")
    A(f"| 수익 재투자 비율 | {REINVEST_RATIO*100:.0f}% |")
    A(f"")

    # 명목 포지션 크기
    notional_example = INITIAL_CAPITAL * FA_RATIO * LEVERAGE
    A(f"> **명목 포지션 크기 예시** (초기 자본 기준): ${notional_example:.0f}  ")
    A(f"> 왕복 수수료: ${notional_example * ONE_WAY_FEE * 2:.2f} + 슬리피지 ${notional_example * SLIPPAGE_RATE * 2:.2f} = **${notional_example*(ONE_WAY_FEE*2+SLIPPAGE_RATE*2):.2f}** per trade")
    A(f"")
    A(f"---")
    A(f"")
    A(f"## 2. 성과 요약")
    A(f"")
    A(f"| 지표 | 값 |")
    A(f"|------|----|")
    A(f"| 최종 자본 | **${metrics['final_equity']:.2f}** |")
    A(f"| 순수익 | **${metrics['final_equity'] - INITIAL_CAPITAL:.2f}** |")
    A(f"| 총 수익률 | {metrics['total_ret']:+.2f}% |")
    A(f"| 연환산 수익률 (CAGR) | {metrics['cagr']:+.2f}% |")
    A(f"| 최대 낙폭 (MDD) | -{metrics['mdd']:.2f}% |")
    A(f"| 샤프 비율 | {metrics['sharpe']:.3f} |")
    A(f"| 총 거래 횟수 | {metrics['total_trades']}회 |")
    A(f"| 승률 | {metrics['win_rate']:.1f}% ({metrics['win_count']}승 {metrics['loss_count']}패) |")
    A(f"| 평균 보유 시간 | {metrics['avg_hold_h']:.1f}h |")
    A(f"| 거래당 평균 순수익 | ${metrics['avg_net_pnl']:+.4f} |")
    A(f"| 최대 단일 수익 | ${metrics['max_net_pnl']:+.4f} |")
    A(f"| 최대 단일 손실 | ${metrics['min_net_pnl']:+.4f} |")
    A(f"")
    A(f"### 비용 분석")
    A(f"")
    A(f"| 항목 | 금액 | 비율 |")
    A(f"|------|------|------|")
    A(f"| 총 펀딩비 수취 | ${metrics['total_funding']:+.4f} | — |")
    A(f"| 총 수수료+슬리피지 | -${metrics['total_fees']:.4f} | 수취 대비 {metrics['total_fees']/max(metrics['total_funding'],0.0001)*100:.1f}% |")
    A(f"| **순수익 합계** | **${metrics['total_net']:+.4f}** | — |")
    A(f"")
    A(f"---")
    A(f"")
    A(f"## 3. 월별 성과")
    A(f"")
    A(f"| 월 | 거래수 | 펀딩 수취 | 수수료 | 순수익 | 누적 자본 |")
    A(f"|----|--------|----------|--------|--------|---------|")
    running = INITIAL_CAPITAL
    for ym, m in monthly.items():
        running += m["net"]
        win_sign = "+" if m["net"] >= 0 else ""
        A(f"| {ym} | {m['count']}회 | ${m['funding']:+.4f} | -${m['fees']:.4f} | {win_sign}${m['net']:.4f} | ${running:.2f} |")
    A(f"")
    A(f"---")
    A(f"")
    A(f"## 4. 전체 거래일지")
    A(f"")
    A(f"총 **{len(trades)}회** 거래 | 초기 ${INITIAL_CAPITAL:.0f} → 최종 ${metrics['final_equity']:.2f}")
    A(f"")
    A(f"| # | 진입 (KST) | 청산 (KST) | 진입율 | 청산율 | 보유 | 명목 | 펀딩수취 | 수수료+슬립 | 순수익 | 자본 | 청산사유 |")
    A(f"|---|-----------|-----------|--------|--------|------|------|---------|------------|--------|------|---------|")

    for t in trades:
        entry_kst  = t["entry_ts"].astimezone(KST).strftime("%m-%d %H:%M")
        exit_kst   = t["exit_ts"].astimezone(KST).strftime("%m-%d %H:%M")
        entry_rate_ann = t["entry_rate"] * 3 * 365 * 100
        exit_rate_ann  = t["exit_rate"] * 3 * 365 * 100
        net_sign   = "+" if t["net_pnl"] >= 0 else ""
        reason_map = {
            "rate_below_exit": "임계↓",
            "rate_flip":       "역전",
            "max_hold":        "만기",
            "forced_close (기간 만료)": "강제청산",
        }
        reason_short = reason_map.get(t["exit_reason"], t["exit_reason"])
        A(
            f"| {t['no']} "
            f"| {entry_kst} "
            f"| {exit_kst} "
            f"| {t['entry_rate']:.5f} ({entry_rate_ann:.1f}%) "
            f"| {t['exit_rate']:.5f} ({exit_rate_ann:.1f}%) "
            f"| {t['hold_hours']}h "
            f"| ${t['notional']:.0f} "
            f"| ${t['funding_income']:+.4f} "
            f"| -${t['total_cost']:.4f} "
            f"| {net_sign}${t['net_pnl']:.4f} "
            f"| ${t['equity_after']:.2f} "
            f"| {reason_short} |"
        )

    A(f"")
    A(f"---")
    A(f"")
    A(f"## 5. 최근 BTC 펀딩비 (마지막 15회)")
    A(f"")
    A(f"| 시간 (KST) | 펀딩비 | 연환산 | 임계값 초과? |")
    A(f"|-----------|--------|--------|------------|")
    for ts, row in funding.tail(15).iterrows():
        rate = float(row["rate"])
        rate_ann = rate * 3 * 365 * 100
        above = "✅ YES" if rate >= MIN_RATE else "❌ NO "
        A(f"| {ts.astimezone(KST).strftime('%Y-%m-%d %H:%M')} | {rate:.6f} | {rate_ann:+.2f}% | {above} |")
    A(f"")
    A(f"---")
    A(f"")
    A(f"## 6. 현황 및 시사점")
    A(f"")
    recent_neg = sum(1 for ts, r in funding.tail(9).iterrows() if float(r["rate"]) < 0)
    last_entry = trades[-1] if trades else None
    if last_entry:
        le_kst = last_entry["entry_ts"].astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
        lx_kst = last_entry["exit_ts"].astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
        days_since = (datetime.now(KST) - last_entry["exit_ts"].astimezone(KST)).days
    else:
        le_kst = lx_kst = "없음"
        days_since = 0

    A(f"### 현재 무포지션 이유")
    A(f"")
    A(f"- **마지막 거래:** {le_kst} → {lx_kst} (청산 후 {days_since}일 경과)")
    A(f"- **최근 음수 펀딩비:** 마지막 3일 중 {recent_neg}회 → consecutive 카운터 리셋")
    A(f"- **현재 펀딩비:** {float(funding.iloc[-1]['rate']):.6f} ({float(funding.iloc[-1]['rate'])*3*365*100:.2f}% ann) — 임계값 {MIN_RATE:.6f} 미달")
    A(f"- 진입하려면 임계값({MIN_RATE:.6f}) 초과 구간이 **{CONSECUTIVE}회(={CONSECUTIVE*8}h) 연속** 필요")
    A(f"")
    A(f"### 파라미터 비교")
    A(f"")
    A(f"| 파라미터 세트 | min_rate | consecutive | 1년 거래수 | 특징 |")
    A(f"|-------------|---------|------------|----------|------|")
    A(f"| **현재 PROD** | 0.000050 (연 5.5%) | 3회 | **46회** | 백테스터와 다름, 잦은 단타 |")
    A(f"| 백테스터 default | 0.000100 (연 10.9%) | 3회 | 11회 | 백테스트 결과 기준 |")
    A(f"| Phase 5 기준 | 0.000120 (연 13.1%) | 4회 | **0회** | 1년간 진입 불가 |")
    A(f"")
    A(f"### ⚠️ 중요: 백테스터↔Production 파라미터 불일치")
    A(f"")
    A(f"백테스트 결과 (CAGR +34.87%, Sharpe 3.583)는 `min_funding_rate=0.0001`(0.01%/8h) 기준으로 계산됐으나,")
    A(f"실제 production 코드의 기본값은 `MIN_FUNDING_RATE=0.00005`(0.005%/8h)입니다.")
    A(f"")
    A(f"- Production 기준 1년 수익: ${metrics['total_net']:+.4f} (수익률 {metrics['total_ret']:+.2f}%)")
    A(f"- 거래 횟수 4배 증가 → 수수료 부담 증가")
    A(f"- 단타 비율 높음 (8h~16h 보유가 다수)")
    A(f"")
    A(f"---")
    A(f"")
    A(f"*리포트 자동 생성: `services/backtester/tests/backtest/analysis/prod_1y_report.py`*")

    return "\n".join(lines)


# ── main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=365)
    end_ts   = int(now.timestamp() * 1000)
    start_ts = int(start.timestamp() * 1000)

    print("Bybit BTCUSDT 역사 펀딩비 조회 중...")
    funding = await fetch_funding(start_ts, end_ts)
    print(f"  → {len(funding)}개 레코드 ({funding.index[0].date()} ~ {funding.index[-1].date()})")

    print("시뮬레이션 실행 중...")
    trades, equity_curve = simulate(funding)
    days = (funding.index[-1] - funding.index[0]).total_seconds() / 86400
    metrics = calc_metrics(equity_curve, trades, days)
    monthly = monthly_summary(trades)

    print(f"  → {len(trades)}회 거래 완료")
    print(f"  → 최종 자본: ${metrics['final_equity']:.2f} (순수익 ${metrics['total_net']:+.4f})")

    md = build_markdown(trades, equity_curve, metrics, monthly, start, now, funding)

    # 결과 저장 — docker-compose: ../.result:/app/result_dir
    out_dir  = "/app/result_dir"
    os.makedirs(out_dir, exist_ok=True)
    filename = f"FA_1Y_SIM_{now.strftime('%Y%m%d')}.md"
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"\n리포트 저장: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
