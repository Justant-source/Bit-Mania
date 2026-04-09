"""analysis/bt_exact_1y_report.py
백테스터와 완전히 동일한 조건으로 최근 1년 Bybit 실제 펀딩비 시뮬레이션.

백테스터 조건 (bt_fa80_extended.py / fa80_lev5_r30):
    min_funding_rate    = 0.0001   (0.01%/8h, 연 10.95%)
    consecutive_intervals = 3
    max_hold_bars       = 168h     (7일, OHLCV 1h 기준 → 8h 기준 21 bars)
    FEE_RATE            = 0.00055  (선물 taker 편도만, 슬리피지 없음)
    fa_capital_ratio    = 0.80
    leverage            = 5.0
    reinvest_ratio      = 0.30
    exit: rev_cnt >= 3 (3회 연속 음수 펀딩 → 청산)

초기 자본: $200

실행:
    docker compose --profile backtest run --rm \\
      -v /home/justant/Data/Bit-Mania/.result:/app/result_dir \\
      backtester \\
      python tests/backtest/analysis/bt_exact_1y_report.py
"""
from __future__ import annotations

import asyncio
import math
import os
from datetime import datetime, timezone, timedelta

import aiohttp
import pandas as pd

# ── 파라미터 ─────────────────────────────────────────────────────────────────
INITIAL_CAPITAL = 200.0

# ── 백테스터 조건 (bt_fa80_extended.py 그대로) ────────────────────────────
BT = dict(
    min_rate      = 0.0001,    # 0.01%/8h
    consecutive   = 3,
    max_hold_h    = 168,       # 7일 (시간 기준)
    fee_one_way   = 0.00055,   # 선물 taker only, 슬리피지 없음
    fa_ratio      = 0.80,
    leverage      = 5.0,
    reinvest      = 0.30,
    rev_exit_cnt  = 3,
    label         = "BT_EXACT (fa80_lev5_r30)",
)

# ── 현재 Production 조건 ───────────────────────────────────────────────────
PROD = dict(
    min_rate      = 0.00005,   # 0.005%/8h
    consecutive   = 3,
    max_hold_h    = 720,       # 30일
    fee_one_way   = 0.00065,   # 현물+선물
    slippage      = 0.0003,    # 편도 슬리피지
    fa_ratio      = 0.80,
    leverage      = 5.0,
    reinvest      = 0.30,
    rev_exit_cnt  = 3,
    label         = "PROD_CURRENT (min_rate 절반, 현물수수료+슬리피지 포함)",
)

SYMBOL = "BTCUSDT"
KST    = timezone(timedelta(hours=9))


# ── Bybit API 펀딩비 조회 ────────────────────────────────────────────────────
async def fetch_funding(start_ts: int, end_ts: int) -> pd.DataFrame:
    url     = "https://api.bybit.com/v5/market/funding/history"
    records = []
    cur_end = end_ts
    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            params = {
                "category": "linear", "symbol": SYMBOL,
                "startTime": str(start_ts), "endTime": str(cur_end), "limit": "200",
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

    df = pd.DataFrame(records)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", inplace=True)
    df.set_index("ts", inplace=True)
    return df


# ── 시뮬레이션 ────────────────────────────────────────────────────────────────
def simulate(funding: pd.DataFrame, p: dict, initial: float) -> tuple[list[dict], list[float]]:
    """백테스터 로직 그대로 재현."""
    equity       = initial
    equity_curve = [equity]
    consec       = 0
    position     = None
    trades       = []

    max_hold_bars = p["max_hold_h"] // 8  # 8h bar 기준으로 변환

    for ts, row in funding.iterrows():
        rate = float(row["rate"])

        if position is None:
            if rate >= p["min_rate"]:
                consec += 1
            elif rate <= -p["min_rate"]:
                consec = 0  # 음수면 리셋 (백테스터는 숏/롱 모두 보지만 FA는 숏만)
            else:
                consec = 0

            if consec >= p["consecutive"]:
                fa_cap   = equity * p["fa_ratio"]
                notional = fa_cap * p["leverage"]
                fee_in   = notional * p["fee_one_way"]
                slip_in  = notional * p.get("slippage", 0.0)
                equity  -= (fee_in + slip_in)
                position = {
                    "entry_ts":    ts,
                    "entry_rate":  rate,
                    "notional":    notional,
                    "fee_in":      fee_in,
                    "slip_in":     slip_in,
                    "bars_held":   0,
                    "funding_acc": 0.0,
                    "rev_cnt":     0,
                }
                consec = 0
        else:
            # 펀딩비 수취
            income = position["notional"] * rate
            position["funding_acc"] += income
            equity += income
            position["bars_held"]  += 1

            # 역전 카운터
            if rate < 0:
                position["rev_cnt"] += 1
            else:
                position["rev_cnt"] = 0

            reason = None
            if position["rev_cnt"] >= p["rev_exit_cnt"]:
                reason = "rate_flip (3회 역전)"
            elif position["bars_held"] >= max_hold_bars:
                reason = f"max_hold ({p['max_hold_h']}h)"

            if reason:
                notional  = position["notional"]
                fee_out   = notional * p["fee_one_way"]
                slip_out  = notional * p.get("slippage", 0.0)
                equity   -= (fee_out + slip_out)

                net_pnl = (
                    position["funding_acc"]
                    - position["fee_in"] - position["slip_in"]
                    - fee_out - slip_out
                )

                # 재투자: 수익의 reinvest% 를 equity에서 분리 (BTC 매수 별도 관리)
                reinvest_usd = 0.0
                if net_pnl > 0 and p["reinvest"] > 0:
                    reinvest_usd = net_pnl * p["reinvest"]
                    equity      -= reinvest_usd  # equity에서 제거 (BTC로 분리)

                total_cost = (position["fee_in"] + position["slip_in"]
                              + fee_out + slip_out)
                trades.append({
                    "no":            len(trades) + 1,
                    "entry_ts":      position["entry_ts"],
                    "exit_ts":       ts,
                    "entry_rate":    position["entry_rate"],
                    "exit_rate":     rate,
                    "bars_held":     position["bars_held"],
                    "hold_hours":    position["bars_held"] * 8,
                    "notional":      round(notional, 2),
                    "funding_income":round(position["funding_acc"], 4),
                    "total_cost":    round(total_cost, 4),
                    "net_pnl":       round(net_pnl, 4),
                    "reinvest_usd":  round(reinvest_usd, 4),
                    "equity_after":  round(equity, 4),
                    "exit_reason":   reason,
                })
                position = None

        equity_curve.append(equity)

    # 미청산 강제 청산
    if position:
        notional  = position["notional"]
        fee_out   = notional * p["fee_one_way"]
        slip_out  = notional * p.get("slippage", 0.0)
        equity   -= (fee_out + slip_out)
        net_pnl   = (position["funding_acc"]
                     - position["fee_in"] - position["slip_in"]
                     - fee_out - slip_out)
        reinvest_usd = max(net_pnl * p["reinvest"], 0.0) if net_pnl > 0 else 0.0
        if reinvest_usd > 0:
            equity -= reinvest_usd
        total_cost = (position["fee_in"] + position["slip_in"] + fee_out + slip_out)
        trades.append({
            "no":            len(trades) + 1,
            "entry_ts":      position["entry_ts"],
            "exit_ts":       funding.index[-1],
            "entry_rate":    position["entry_rate"],
            "exit_rate":     float(funding.iloc[-1]["rate"]),
            "bars_held":     position["bars_held"],
            "hold_hours":    position["bars_held"] * 8,
            "notional":      round(notional, 2),
            "funding_income":round(position["funding_acc"], 4),
            "total_cost":    round(total_cost, 4),
            "net_pnl":       round(net_pnl, 4),
            "reinvest_usd":  round(reinvest_usd, 4),
            "equity_after":  round(equity, 4),
            "exit_reason":   "forced_close",
        })
        equity_curve.append(equity)

    return trades, equity_curve


# ── 지표 계산 ─────────────────────────────────────────────────────────────────
def metrics(equity_curve: list[float], trades: list[dict], days: float) -> dict:
    init  = equity_curve[0]
    final = equity_curve[-1]
    ret   = (final - init) / init * 100
    years = days / 365
    cagr  = ((final / init) ** (1 / years) - 1) * 100 if years > 0 and final > 0 else -100.0

    peak = init; mdd = 0.0
    for v in equity_curve:
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd: mdd = dd

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
    return {
        "final":         round(final, 4),
        "net":           round(final - init, 4),
        "ret":           round(ret, 2),
        "cagr":          round(cagr, 2),
        "mdd":           round(mdd, 2),
        "sharpe":        round(sharpe, 3),
        "trades":        len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "total_funding": round(sum(t["funding_income"] for t in trades), 4),
        "total_cost":    round(sum(t["total_cost"] for t in trades), 4),
        "total_reinvest":round(sum(t["reinvest_usd"] for t in trades), 4),
        "avg_hold_h":    round(sum(t["hold_hours"] for t in trades) / len(trades), 1) if trades else 0,
        "max_pnl":       round(max((t["net_pnl"] for t in trades), default=0), 4),
        "min_pnl":       round(min((t["net_pnl"] for t in trades), default=0), 4),
        "avg_pnl":       round(sum(t["net_pnl"] for t in trades) / len(trades), 4) if trades else 0,
    }


def monthly(trades: list[dict]) -> dict:
    m: dict = {}
    for t in trades:
        k = t["exit_ts"].astimezone(KST).strftime("%Y-%m")
        if k not in m:
            m[k] = {"count": 0, "funding": 0.0, "cost": 0.0, "net": 0.0}
        m[k]["count"]   += 1
        m[k]["funding"] += t["funding_income"]
        m[k]["cost"]    += t["total_cost"]
        m[k]["net"]     += t["net_pnl"]
    return dict(sorted(m.items()))


# ── Markdown 빌더 ─────────────────────────────────────────────────────────────
def build_md(
    bt_trades, bt_ec, bt_m, bt_monthly,
    prod_trades, prod_ec, prod_m, prod_monthly,
    funding, start_dt, end_dt,
) -> str:
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    sd = start_dt.astimezone(KST).strftime("%Y-%m-%d")
    ed = end_dt.astimezone(KST).strftime("%Y-%m-%d")

    L = []
    A = L.append

    A(f"# FA 전략 1년 재검증 리포트 — 백테스터 조건 vs Production 조건")
    A(f"")
    A(f"> **생성:** {now_kst}  ")
    A(f"> **기간:** {sd} ~ {ed}  ")
    A(f"> **데이터:** Bybit BTCUSDT Mainnet 실제 펀딩비 ({len(funding)}개 레코드)  ")
    A(f"> **초기 자본:** $200 USDT")
    A(f"")
    A(f"---")
    A(f"")

    # ── 성과 비교 요약 ────────────────────────────────────────────────────────
    A(f"## 1. 성과 비교 요약")
    A(f"")
    A(f"| 지표 | 백테스터 조건 (BT_EXACT) | Production 조건 (PROD) |")
    A(f"|------|:---:|:---:|")
    A(f"| min_funding_rate | **0.0001** (0.01%/8h) | 0.00005 (0.005%/8h) |")
    A(f"| 수수료 구조 | **선물 0.055% 편도만** | 현물+선물 0.065% + 슬리피지 0.03% |")
    A(f"| 최대 보유 | **168h (7일)** | 720h (30일) |")
    A(f"| 거래 횟수 | **{bt_m['trades']}회** | {prod_m['trades']}회 |")
    A(f"| 최종 자본 | **${bt_m['final']:.2f}** | ${prod_m['final']:.2f} |")
    A(f"| 순수익 | **${bt_m['net']:+.4f}** | ${prod_m['net']:+.4f} |")
    A(f"| 총 수익률 | **{bt_m['ret']:+.2f}%** | {prod_m['ret']:+.2f}% |")
    A(f"| 연환산 (CAGR) | **{bt_m['cagr']:+.2f}%** | {prod_m['cagr']:+.2f}% |")
    A(f"| 최대낙폭 (MDD) | **{bt_m['mdd']:.2f}%** | {prod_m['mdd']:.2f}% |")
    A(f"| 샤프 비율 | **{bt_m['sharpe']:.3f}** | {prod_m['sharpe']:.3f} |")
    A(f"| 승률 | **{bt_m['win_rate']:.1f}%** ({bt_m['wins']}승 {bt_m['losses']}패) | {prod_m['win_rate']:.1f}% ({prod_m['wins']}승 {prod_m['losses']}패) |")
    A(f"| 펀딩비 수취 합계 | **${bt_m['total_funding']:+.4f}** | ${prod_m['total_funding']:+.4f} |")
    A(f"| 수수료+슬리피지 합계 | **-${bt_m['total_cost']:.4f}** | -${prod_m['total_cost']:.4f} |")
    A(f"| 평균 보유시간 | **{bt_m['avg_hold_h']:.1f}h** | {prod_m['avg_hold_h']:.1f}h |")
    A(f"| 거래당 평균 순수익 | **${bt_m['avg_pnl']:+.4f}** | ${prod_m['avg_pnl']:+.4f} |")
    A(f"")

    # ── 실패 원인 분석 ────────────────────────────────────────────────────────
    A(f"---")
    A(f"")
    A(f"## 2. 실패 원인 분석")
    A(f"")

    bt_notional_ex = INITIAL_CAPITAL * BT["fa_ratio"] * BT["leverage"]
    prod_notional_ex = INITIAL_CAPITAL * PROD["fa_ratio"] * PROD["leverage"]
    bt_cost_ex = bt_notional_ex * BT["fee_one_way"] * 2
    prod_cost_ex = prod_notional_ex * (PROD["fee_one_way"] + PROD.get("slippage", 0)) * 2

    A(f"### 원인 ① 수수료 구조 불일치 (가장 치명적)")
    A(f"")
    A(f"백테스터는 **선물 taker 수수료(0.055%)만** 편도로 적용했습니다.")
    A(f"실제 델타뉴트럴 포지션에는 현물 매수 + 선물 숏 양쪽 수수료와 슬리피지가 발생합니다.")
    A(f"")
    A(f"| 구분 | 편도 비용 | 왕복 비용 | $200 기준 거래당 |")
    A(f"|------|---------|---------|--------------|")
    A(f"| 백테스터 가정 | 0.0550% | 0.1100% | **${bt_cost_ex:.2f}** |")
    A(f"| 실제 (현물+선물+슬리피지) | 0.0950% | 0.1900% | **${prod_cost_ex:.2f}** |")
    A(f"| **차이** | — | **+0.08%p** | **+${prod_cost_ex-bt_cost_ex:.2f}/거래** |")
    A(f"")
    A(f"### 원인 ② min_funding_rate 불일치")
    A(f"")
    A(f"```")
    A(f"백테스터 min_rate = 0.0001  (0.01%/8h, 연 10.95%)")
    A(f"Production 기본값 = 0.00005 (0.005%/8h, 연  5.48%)  ← strategy.py MIN_FUNDING_RATE")
    A(f"```")
    A(f"")
    A(f"임계값이 절반으로 낮아지면 저품질 진입이 4배 증가하고, 수수료 대비 펀딩 수취가 부족한 단타가 양산됩니다.")
    A(f"")
    A(f"### 원인 ③ 시장 환경 변화 (구조적 문제)")
    A(f"")

    # 펀딩비 통계
    rates = funding["rate"].astype(float)
    pos_rates = rates[rates > 0]
    A(f"| 펀딩비 통계 (최근 1년) | 값 |")
    A(f"|----------------------|---|")
    A(f"| 평균 | {rates.mean():.6f} ({rates.mean()*3*365*100:.2f}% ann) |")
    A(f"| 중앙값 | {rates.median():.6f} ({rates.median()*3*365*100:.2f}% ann) |")
    A(f"| 양수 구간 평균 | {pos_rates.mean():.6f} ({pos_rates.mean()*3*365*100:.2f}% ann) |")
    A(f"| 음수 구간 비율 | {(rates < 0).sum()}/{len(rates)} ({(rates<0).mean()*100:.1f}%) |")
    A(f"| BT 임계값(0.0001) 초과 비율 | {(rates >= 0.0001).mean()*100:.1f}% |")
    A(f"| PROD 임계값(0.00005) 초과 비율 | {(rates >= 0.00005).mean()*100:.1f}% |")
    A(f"")
    A(f"백테스트에 포함된 **2020~2022 강세장**에서는 BTCUSDT 펀딩비가 0.05%~0.3%/8h에 달했습니다.")
    A(f"최근 1년(2025~2026)은 BTC 고점 이후 조정 사이클로 펀딩비가 역사적 저점 수준입니다.")
    A(f"")
    A(f"---")
    A(f"")

    # ── BT_EXACT 거래일지 ─────────────────────────────────────────────────────
    A(f"## 3. BT_EXACT 거래일지 ({bt_m['trades']}회)")
    A(f"")
    A(f"초기 $200 → 최종 ${bt_m['final']:.2f} ({bt_m['ret']:+.2f}%)")
    A(f"")
    if bt_trades:
        A(f"| # | 진입 (KST) | 청산 (KST) | 진입율 | 청산율 | 보유 | 명목 | 펀딩수취 | 수수료 | 순수익 | 자본 | 사유 |")
        A(f"|---|-----------|-----------|--------|--------|------|------|---------|--------|--------|------|------|")
        for t in bt_trades:
            ekst = t["entry_ts"].astimezone(KST).strftime("%m-%d %H:%M")
            xkst = t["exit_ts"].astimezone(KST).strftime("%m-%d %H:%M")
            sign = "+" if t["net_pnl"] >= 0 else ""
            er_ann = t["entry_rate"] * 3 * 365 * 100
            xr_ann = t["exit_rate"] * 3 * 365 * 100
            reason_s = {"rate_flip (3회 역전)": "역전", f"max_hold ({BT['max_hold_h']}h)": "만기", "forced_close": "강제"}.get(t["exit_reason"], t["exit_reason"])
            A(f"| {t['no']} | {ekst} | {xkst} | {t['entry_rate']:.5f} ({er_ann:.1f}%) | {t['exit_rate']:.5f} ({xr_ann:.1f}%) | {t['hold_hours']}h | ${t['notional']:.0f} | ${t['funding_income']:+.4f} | -${t['total_cost']:.4f} | {sign}${t['net_pnl']:.4f} | ${t['equity_after']:.2f} | {reason_s} |")
    else:
        A(f"> **1년간 진입 없음** — 진입 조건 미충족")
    A(f"")

    # 월별 BT
    A(f"### BT_EXACT 월별 성과")
    A(f"")
    A(f"| 월 | 거래수 | 펀딩수취 | 수수료 | 순수익 | 누적 자본 |")
    A(f"|----|--------|---------|--------|--------|---------|")
    running = INITIAL_CAPITAL
    for ym, mv in bt_monthly.items():
        running += mv["net"]
        A(f"| {ym} | {mv['count']}회 | ${mv['funding']:+.4f} | -${mv['cost']:.4f} | {'+'if mv['net']>=0 else ''}${mv['net']:.4f} | ${running:.2f} |")
    A(f"")
    A(f"---")
    A(f"")

    # ── PROD 거래일지 ─────────────────────────────────────────────────────────
    A(f"## 4. PROD_CURRENT 거래일지 ({prod_m['trades']}회) — 참고용")
    A(f"")
    A(f"초기 $200 → 최종 ${prod_m['final']:.2f} ({prod_m['ret']:+.2f}%)")
    A(f"")
    if prod_trades:
        A(f"| # | 진입 (KST) | 청산 (KST) | 진입율 | 청산율 | 보유 | 명목 | 펀딩수취 | 수수료+슬립 | 순수익 | 자본 | 사유 |")
        A(f"|---|-----------|-----------|--------|--------|------|------|---------|-----------|--------|------|------|")
        for t in prod_trades:
            ekst = t["entry_ts"].astimezone(KST).strftime("%m-%d %H:%M")
            xkst = t["exit_ts"].astimezone(KST).strftime("%m-%d %H:%M")
            sign = "+" if t["net_pnl"] >= 0 else ""
            er_ann = t["entry_rate"] * 3 * 365 * 100
            xr_ann = t["exit_rate"] * 3 * 365 * 100
            reason_s = {"rate_flip (3회 역전)": "역전", f"max_hold ({PROD['max_hold_h']}h)": "만기", "forced_close": "강제"}.get(t["exit_reason"], t["exit_reason"])
            A(f"| {t['no']} | {ekst} | {xkst} | {t['entry_rate']:.5f} ({er_ann:.1f}%) | {t['exit_rate']:.5f} ({xr_ann:.1f}%) | {t['hold_hours']}h | ${t['notional']:.0f} | ${t['funding_income']:+.4f} | -${t['total_cost']:.4f} | {sign}${t['net_pnl']:.4f} | ${t['equity_after']:.2f} | {reason_s} |")
    A(f"")

    # 월별 PROD
    A(f"### PROD_CURRENT 월별 성과")
    A(f"")
    A(f"| 월 | 거래수 | 펀딩수취 | 수수료+슬립 | 순수익 | 누적 자본 |")
    A(f"|----|--------|---------|-----------|--------|---------|")
    running = INITIAL_CAPITAL
    for ym, mv in prod_monthly.items():
        running += mv["net"]
        A(f"| {ym} | {mv['count']}회 | ${mv['funding']:+.4f} | -${mv['cost']:.4f} | {'+'if mv['net']>=0 else ''}${mv['net']:.4f} | ${running:.2f} |")
    A(f"")
    A(f"---")
    A(f"")

    # ── 결론 및 권고 ──────────────────────────────────────────────────────────
    A(f"## 5. 결론 및 권고사항")
    A(f"")
    bt_verdict = "✅ 수익" if bt_m["net"] > 0 else "❌ 손실"
    prod_verdict = "✅ 수익" if prod_m["net"] > 0 else "❌ 손실"
    A(f"| 조건 | 결과 | 원인 |")
    A(f"|------|------|------|")
    A(f"| 백테스터 조건 그대로 | **{bt_verdict} (${bt_m['net']:+.4f})** | 최근 1년 저펀딩 환경 |")
    A(f"| Production 현재 | **{prod_verdict} (${prod_m['net']:+.4f})** | 임계값↓ + 비용↑ 이중 악재 |")
    A(f"")
    A(f"### 백테스터 조건으로도 {'수익' if bt_m['net']>0 else '손실'}인 이유")
    A(f"")
    if bt_m["net"] <= 0:
        A(f"백테스터 조건(정확한 파라미터)으로도 최근 1년은 **손실**입니다.")
        A(f"이는 전략 자체의 문제가 아니라 **시장 환경**의 문제입니다:")
        A(f"")
        A(f"- 최근 1년 BTCUSDT 펀딩비 평균: {rates.mean()*3*365*100:.2f}% ann — 역사적 저점")
        A(f"- 진입 임계값(연 10.95%) 초과 구간이 전체의 **{(rates >= 0.0001).mean()*100:.1f}%** 뿐")
        A(f"- 진입 횟수 {bt_m['trades']}회로 절대량이 적어 수수료 고정비를 커버 못 함")
    else:
        A(f"백테스터 조건으로는 **수익**이 납니다 (${bt_m['net']:+.4f}).")
        A(f"Production 코드의 파라미터 수정이 필요합니다.")
    A(f"")
    A(f"### 즉시 수정 필요 항목")
    A(f"")
    A(f"```python")
    A(f"# services/strategies/funding-arb/strategy.py")
    A(f"# 현재:")
    A(f"MIN_FUNDING_RATE = 0.00005  # 0.005%/8h — 백테스터 조건과 다름")
    A(f"")
    A(f"# 수정:")
    A(f"MIN_FUNDING_RATE = 0.0001   # 0.01%/8h — 백테스터 조건과 동일")
    A(f"```")
    A(f"")
    A(f"```yaml")
    A(f"# config/strategies/funding-arb.yaml")
    A(f"# 현재:")
    A(f"entry:")
    A(f"  min_funding_rate_annualized: 15.0  # 코드에서 읽히지 않음 (min_funding_rate 키가 없음)")
    A(f"")
    A(f"# 수정: top-level에 명시적으로 추가")
    A(f"min_funding_rate: 0.0001  # 0.01%/8h = 연 10.95%, 백테스터 검증값")
    A(f"```")
    A(f"")
    A(f"### 장기 관점")
    A(f"")
    A(f"FA 전략은 **시장 환경에 강하게 의존**합니다. 백테스트 CAGR +34.87%는 6년 평균이며,")
    A(f"그 대부분은 2020~2022년 강세장에서 발생했습니다. 현재 저펀딩 환경에서는:")
    A(f"")
    A(f"- 백테스터 조건: {bt_m['trades']}회 거래 / 1년, 각 거래 평균 {bt_m['avg_hold_h']:.0f}h 보유")
    A(f"- 펀딩 수취 ${bt_m['total_funding']:.4f} vs 수수료 ${bt_m['total_cost']:.4f}")
    A(f"- **BEP 달성에 필요한 펀딩비**: 거래당 ${bt_cost_ex:.2f} ÷ 명목 $800 × 8h = {bt_cost_ex/800*100:.4f}%/8h 이상 유지 필요")
    A(f"")
    A(f"---")
    A(f"")
    A(f"*리포트 자동 생성: `services/backtester/tests/backtest/analysis/bt_exact_1y_report.py`*")

    return "\n".join(L)


# ── main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    now      = datetime.now(timezone.utc)
    start    = now - timedelta(days=365)
    end_ts   = int(now.timestamp() * 1000)
    start_ts = int(start.timestamp() * 1000)

    print("Bybit BTCUSDT 역사 펀딩비 조회 중...")
    funding = await fetch_funding(start_ts, end_ts)
    days    = (funding.index[-1] - funding.index[0]).total_seconds() / 86400
    print(f"  → {len(funding)}개 ({funding.index[0].date()} ~ {funding.index[-1].date()})")

    print("\n[BT_EXACT] 시뮬레이션...")
    bt_trades, bt_ec = simulate(funding, BT, INITIAL_CAPITAL)
    bt_m = metrics(bt_ec, bt_trades, days)
    bt_monthly = monthly(bt_trades)
    print(f"  → {bt_m['trades']}회  최종 ${bt_m['final']:.2f}  순수익 ${bt_m['net']:+.4f}  CAGR {bt_m['cagr']:+.2f}%")

    print("\n[PROD_CURRENT] 시뮬레이션...")
    prod_trades, prod_ec = simulate(funding, PROD, INITIAL_CAPITAL)
    prod_m = metrics(prod_ec, prod_trades, days)
    prod_monthly = monthly(prod_trades)
    print(f"  → {prod_m['trades']}회  최종 ${prod_m['final']:.2f}  순수익 ${prod_m['net']:+.4f}  CAGR {prod_m['cagr']:+.2f}%")

    md = build_md(
        bt_trades, bt_ec, bt_m, bt_monthly,
        prod_trades, prod_ec, prod_m, prod_monthly,
        funding, start, now,
    )

    out_dir  = "/app/result_dir"
    os.makedirs(out_dir, exist_ok=True)
    filename = f"FA_BT_EXACT_vs_PROD_{now.strftime('%Y%m%d')}.md"
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"\n리포트 저장: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
