"""core.metrics — 백테스트 공통 지표 계산 함수.

사용법:
    from core.metrics import sharpe, mdd, cagr, safe_float, monthly_returns

    sr  = sharpe(equity_series)          # Sharpe (연율화, 1h 기준)
    dd  = mdd(equity_series)             # 최대 낙폭 % (음수)
    cr  = cagr(total_return_pct, years)  # 연환산 수익률 %
    v   = safe_float(x)                  # NaN/Inf 방어
    mr  = monthly_returns(equity, dates) # 월별 수익률 dict
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Sequence

import numpy as np
import pandas as pd


# ── Sharpe ────────────────────────────────────────────────────────────────────

def sharpe(equity: pd.Series, periods_per_year: int = 8760) -> float:
    """연율화 Sharpe 비율 (무위험이자율 0 가정).

    Args:
        equity: 자산 시계열 (타임스텝당 1개 값)
        periods_per_year: 연간 타임스텝 수 (기본 8760 = 1h 기준)

    Returns:
        float (0.0 if std == 0)
    """
    rets = equity.pct_change().dropna()
    std  = float(rets.std())
    if std == 0 or np.isnan(std):
        return 0.0
    return float(rets.mean() / std * math.sqrt(periods_per_year))


# ── MDD ───────────────────────────────────────────────────────────────────────

def mdd(equity: pd.Series) -> float:
    """최대 낙폭(Maximum Drawdown) %.

    Returns:
        음수 float (예: -12.34). 낙폭 없으면 0.0.
    """
    roll_max = equity.cummax()
    dd = (equity - roll_max) / roll_max
    val = float(dd.min()) * 100
    return val if not math.isnan(val) else 0.0


# ── CAGR ──────────────────────────────────────────────────────────────────────

def cagr(total_return_pct: float, n_years: float) -> float:
    """연환산 수익률(CAGR) %.

    Args:
        total_return_pct: 총 수익률 (%, 예: 59.24)
        n_years: 보유 기간 (년, 예: 6.0)

    Returns:
        float % (연환산)
    """
    if n_years <= 0:
        return 0.0
    factor = 1.0 + total_return_pct / 100.0
    if factor <= 0:
        return -100.0
    return ((factor ** (1.0 / n_years)) - 1.0) * 100.0


# ── Calmar ────────────────────────────────────────────────────────────────────

def calmar(annual_return_pct: float, mdd_pct: float) -> float:
    """Calmar 비율 = 연수익률 / |MDD|.

    Returns:
        float (0.0 if mdd == 0)
    """
    abs_mdd = abs(mdd_pct)
    if abs_mdd == 0:
        return 0.0
    return annual_return_pct / abs_mdd


# ── Monthly Returns ───────────────────────────────────────────────────────────

def monthly_returns(
    equity: pd.Series,
    index: pd.DatetimeIndex | None = None,
) -> dict[str, float]:
    """월별 수익률 dict 반환.

    Args:
        equity: 자산 시계열
        index:  타임스탬프 인덱스 (equity.index가 DatetimeIndex이면 None 가능)

    Returns:
        {"2020-04": 1.23, "2020-05": -0.45, ...}
    """
    if index is not None:
        eq = pd.Series(equity.values, index=index)
    else:
        eq = equity

    if not isinstance(eq.index, pd.DatetimeIndex):
        return {}

    monthly = eq.resample("ME").last()
    monthly_ret = monthly.pct_change().dropna()
    return {
        str(ts)[:7]: round(float(v) * 100, 4)
        for ts, v in monthly_ret.items()
        if not math.isnan(v)
    }


# ── safe_float ────────────────────────────────────────────────────────────────

def safe_float(v: float | None, default: float = 0.0) -> float:
    """NaN / Inf / None을 default 값으로 치환."""
    if v is None:
        return default
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return default
    return float(v)


# ── Profit Factor ─────────────────────────────────────────────────────────────

def profit_factor(pnls: Sequence[float]) -> float:
    """Profit Factor = 총이익 / 총손실.

    Returns:
        float (inf if no losing trades)
    """
    wins  = sum(p for p in pnls if p > 0)
    loses = abs(sum(p for p in pnls if p < 0))
    return (wins / loses) if loses > 0 else float("inf")
