"""
Backtest Result Validator
==========================
Phase 6: V4 백테스트 결과 자동 검증 (Task 6.1).
이상한 결과(높은 Sharpe + 낮은 MDD, 100% 승률 등)를 탐지합니다.

사용법:
    from tests.backtest.core.validation import validate_backtest_result

    warnings = validate_backtest_result(result)
    for w in warnings:
        print(w)
"""

from __future__ import annotations


def validate_backtest_result(result: dict) -> list[str]:
    """
    백테스트 결과에 대한 sanity check를 실행합니다.
    의심스러운 결과를 경고 목록으로 반환합니다.

    Parameters
    ----------
    result : dict
        Required keys:
            sharpe (float), cagr (float, 0~1 range e.g. 0.35 = 35%),
            mdd (float, 0~1 range e.g. 0.05 = 5% drawdown),
            num_trades (int), win_rate (float, 0~1),
            total_fees (float), gross_pnl (float)
        Optional:
            avg_trade_duration_hours (float), max_consecutive_wins (int)

    Returns
    -------
    list[str]
        List of warning messages. Empty list means no issues detected.
    """
    warnings = []

    sharpe = result.get("sharpe", 0.0)
    cagr = result.get("cagr", 0.0)
    mdd = result.get("mdd", 0.0)
    num_trades = result.get("num_trades", 0)
    win_rate = result.get("win_rate", 0.0)
    total_fees = result.get("total_fees", 0.0)
    gross_pnl = result.get("gross_pnl", 0.0)
    avg_duration = result.get("avg_trade_duration_hours", None)

    # ── Check 1: Sharpe ~0 with many trades ──
    if abs(sharpe) < 0.01 and num_trades > 100:
        warnings.append(
            "SUSPECT [Sharpe≈0]: Sharpe가 거의 0인데 거래수가 많음. "
            "P&L 부호 또는 수수료 적용 확인 필요."
        )

    # ── Check 2: Fee ratio sanity ──
    if gross_pnl > 0 and total_fees > gross_pnl * 0.5:
        ratio = total_fees / gross_pnl
        warnings.append(
            f"SUSPECT [Fee>50%PnL]: 수수료({total_fees:.2f})가 총수익({gross_pnl:.2f})의 "
            f"{ratio:.0%}. 거래 빈도 너무 높거나 수수료 계산 오류."
        )

    # ── Check 3: 100% win rate with tiny sample ──
    if win_rate > 0.99 and num_trades < 20:
        warnings.append(
            f"SUSPECT [100%WR-SmallSample]: 승률 {win_rate:.0%}이지만 거래 {num_trades}건. "
            "통계적으로 유의하지 않음. 데이터 기간 연장 필요."
        )

    # ── Check 4: Impossibly high CAGR with tiny MDD ──
    if cagr > 0.5 and mdd < 0.01:
        warnings.append(
            f"SUSPECT [CAGR>{cagr:.0%}/MDD<{mdd:.1%}]: CAGR이 50%+ 인데 MDD가 1% 미만. "
            "룩어헤드 바이어스(미래 데이터 사용) 가능성 높음."
        )

    # ── Check 5: Very high CAGR overall ──
    if cagr > 2.0:  # 200% annual
        warnings.append(
            f"SUSPECT [CAGR={cagr:.0%}]: 연 수익률 200% 초과. "
            "레버리지 계산 오류 또는 수수료 누락 가능성."
        )

    # ── Check 6: Negative Sharpe with positive CAGR ──
    if sharpe < 0 and cagr > 0:
        warnings.append(
            "SUSPECT [NegSharpe+PosCagr]: Sharpe가 음수인데 CAGR이 양수. "
            "수익률 시계열 계산 오류 가능성."
        )

    # ── Check 7: Zero trades ──
    if num_trades == 0:
        warnings.append(
            "CRITICAL [0 Trades]: 거래가 하나도 없음. "
            "진입 조건이 너무 엄격하거나 데이터 로딩 실패."
        )

    # ── Check 8: Average trade duration too short ──
    if avg_duration is not None and avg_duration < 1.0 and num_trades > 50:
        warnings.append(
            f"SUSPECT [AvgDuration={avg_duration:.1f}h]: 평균 보유 1시간 미만 + 거래 50건 이상. "
            "과도한 빈도매매 또는 MIN_HOLD_BARS 미적용."
        )

    # ── Check 9: MDD == 0 with trades ──
    if mdd == 0.0 and num_trades > 0:
        warnings.append(
            "SUSPECT [MDD=0]: 거래가 있는데 MDD가 정확히 0. "
            "낙폭 계산 로직 버그 가능성."
        )

    # ── Check 10: Very high win rate (not 100%) ──
    if win_rate > 0.90 and num_trades > 50:
        warnings.append(
            f"SUSPECT [WR={win_rate:.0%}]: 승률 90% 초과 + 충분한 샘플. "
            "손절 로직 부재 또는 손실 거래 누락 의심."
        )

    return warnings


def format_validation_report(result: dict, warnings: list[str]) -> str:
    """Sanity check 결과를 마크다운 형식으로 포매팅."""
    lines = [
        "## Sanity Check 결과",
        "",
        f"- Sharpe: {result.get('sharpe', 'N/A')}",
        f"- CAGR: {result.get('cagr', 0)*100:.1f}%" if isinstance(result.get('cagr'), (int, float)) else f"- CAGR: N/A",
        f"- MDD: {result.get('mdd', 0)*100:.1f}%" if isinstance(result.get('mdd'), (int, float)) else f"- MDD: N/A",
        f"- 거래수: {result.get('num_trades', 'N/A')}",
        f"- 승률: {result.get('win_rate', 0)*100:.1f}%" if isinstance(result.get('win_rate'), (int, float)) else f"- 승률: N/A",
        "",
    ]

    if not warnings:
        lines.append("OK — 모든 sanity check 통과")
    else:
        lines.append(f"WARNING — {len(warnings)}개 경고 발견")
        lines.append("")
        for i, w in enumerate(warnings, 1):
            lines.append(f"{i}. {w}")

    return "\n".join(lines)
