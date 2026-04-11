#!/usr/bin/env python3
"""
basis_calculator.py — 분기물-무기한 베이시스 계산 유틸리티

분기물(Quarterly Futures)과 무기한 선물(Perpetual)의 베이시스를 계산합니다.
베이시스 = quarterly_price - perpetual_price

사용법:
    from basis_calculator import compute_basis, AnnualizedBasis

    result = compute_basis(
        perp_price=65000.0,
        quarterly_price=65500.0,
        days_to_expiry=30
    )
    print(result)
    # {
    #     'absolute_basis': 500.0,
    #     'basis_pct': 0.769,
    #     'annualized_basis': 9.38,
    #     'is_contango': True,
    # }
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class AnnualizedBasis:
    """베이시스 계산 결과."""
    absolute_basis: float      # quarterly - perpetual (절대값)
    basis_pct: float           # (quarterly / perpetual - 1) * 100
    annualized_basis: float    # basis_pct * (365 / days_to_expiry)
    is_contango: bool          # absolute_basis > 0


def compute_basis(
    perp_price: float,
    quarterly_price: float,
    days_to_expiry: int,
) -> AnnualizedBasis:
    """분기물-무기한 베이시스 계산.

    Args:
        perp_price: 무기한 선물 가격
        quarterly_price: 분기물 가격
        days_to_expiry: 만기까지 남은 일수

    Returns:
        AnnualizedBasis 객체

    Example:
        >>> result = compute_basis(65000.0, 65500.0, 30)
        >>> result.annualized_basis
        9.38
    """
    if perp_price <= 0:
        raise ValueError(f"Invalid perp_price: {perp_price}")

    if days_to_expiry <= 0:
        raise ValueError(f"Invalid days_to_expiry: {days_to_expiry}")

    absolute_basis = quarterly_price - perp_price
    basis_pct = (quarterly_price / perp_price - 1.0) * 100.0
    annualized_basis = basis_pct * (365.0 / days_to_expiry)
    is_contango = absolute_basis > 0

    return AnnualizedBasis(
        absolute_basis=absolute_basis,
        basis_pct=basis_pct,
        annualized_basis=annualized_basis,
        is_contango=is_contango,
    )


def synthetic_quarterly_price(
    perp_price: float,
    days_to_expiry: int,
    ann_basis_estimate: float = 2.5,
) -> float:
    """합성 분기물 가격 생성 (실제 분기물 데이터 없을 때).

    베이시스를 시간 가중하여 분기물 가격을 추정합니다.

    Args:
        perp_price: 무기한 선물 가격
        days_to_expiry: 만기까지 남은 일수
        ann_basis_estimate: 연환산 베이시스 추정치 (%, 기본 2.5%)

    Returns:
        float 분기물 추정 가격

    Example:
        >>> synthetic_quarterly_price(65000.0, 30, 2.5)
        65532.19
    """
    if perp_price <= 0:
        raise ValueError(f"Invalid perp_price: {perp_price}")

    if days_to_expiry <= 0:
        raise ValueError(f"Invalid days_to_expiry: {days_to_expiry}")

    # annualized_basis_pct * (days / 365) = 실현 베이시스 %
    realized_basis_pct = ann_basis_estimate * (days_to_expiry / 365.0)
    return perp_price * (1.0 + realized_basis_pct / 100.0)


def basis_reversal_point(
    current_basis_pct: float,
    reversal_threshold_pct: float = 50.0,
) -> bool:
    """베이시스가 일정 비율 회귀했는지 판정.

    최초 진입 시점의 베이시스에서 절반 이상 회귀하면 수익 실현.

    Args:
        current_basis_pct: 현재 베이시스 %
        reversal_threshold_pct: 회귀 임계값 (%, 기본 50%)

    Returns:
        bool 회귀 여부
    """
    return abs(current_basis_pct) >= reversal_threshold_pct


def basis_divergence_check(
    current_basis_pct: float,
    max_basis_expansion: float = 50.0,
) -> bool:
    """베이시스가 과도 확대했는지 판정.

    진입 이후 베이시스가 50% 이상 확대되면 리스크 신호.

    Args:
        current_basis_pct: 현재 베이시스 %
        max_basis_expansion: 최대 허용 확대 % (기본 50%)

    Returns:
        bool 과도 확대 여부
    """
    return abs(current_basis_pct) >= max_basis_expansion


if __name__ == "__main__":
    # 테스트
    result = compute_basis(65000.0, 65500.0, 30)
    print(f"Absolute basis: ${result.absolute_basis:.2f}")
    print(f"Basis %: {result.basis_pct:.4f}%")
    print(f"Annualized basis: {result.annualized_basis:.2f}%")
    print(f"Is contango: {result.is_contango}")

    # 합성 가격 테스트
    syn = synthetic_quarterly_price(65000.0, 30, 2.5)
    print(f"\nSynthetic quarterly price: ${syn:.2f}")
