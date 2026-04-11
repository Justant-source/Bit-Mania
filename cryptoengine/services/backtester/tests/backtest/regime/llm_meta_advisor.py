"""llm_meta_advisor.py — 결정론적 LLM 메타전략 선택자 (결정론적 룰 기반 모사)

실제 LLM 호출 없이, 시장 지표 기반 규칙으로 전략 방향과 신뢰도를 결정합니다.
- 입력: price_change_24h, price_change_7d, hmm_state, hmm_proba,
         funding_rate, funding_zscore, fear_greed_index, mvrv_zscore(옵션)
- 출력: direction ("long"|"short"|"flat"), confidence (0~100),
         preferred_strategy ("trend"|"reversion"|"flat"|"carry"),
         size_multiplier (0.0~1.5), risk_level ("low"|"medium"|"high"|"extreme")

실행:
    python tests/backtest/regime/llm_meta_advisor.py --validate
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/app")

from dataclasses import dataclass
from typing import Any


@dataclass
class LLMAdvisory:
    """LLM 자문 결과."""
    direction: str           # "long" | "short" | "flat"
    confidence: int          # 0~100
    preferred_strategy: str  # "trend" | "reversion" | "flat" | "carry"
    size_multiplier: float   # 0.0~1.5
    risk_level: str         # "low" | "medium" | "high" | "extreme"
    reasoning: str          # 한 줄 설명


def simulate_llm_advisory(context: dict[str, Any]) -> LLMAdvisory:
    """
    결정론적 룰 기반 LLM 메타전략 선택.

    입력 컨텍스트:
        price_change_24h: float (-100~100)
        price_change_7d: float (-100~100)
        hmm_state: int (0=저변동, 1=중간, 2=고변동)
        hmm_proba: list[float] (3개 확률)
        funding_rate: float (-0.02~0.02)
        funding_zscore: float
        fear_greed: int (0~100) - 선택, 기본값 50
        mvrv_zscore: float (선택)

    반환:
        LLMAdvisory 객체
    """
    p_24h = context.get("price_change_24h", 0.0)
    p_7d = context.get("price_change_7d", 0.0)
    hmm_st = context.get("hmm_state", 1)
    hmm_pr = context.get("hmm_proba", [0.33, 0.34, 0.33])
    fund_rate = context.get("funding_rate", 0.0)
    fund_z = context.get("funding_zscore", 0.0)
    fear_greed = context.get("fear_greed", 50)
    mvrv_z = context.get("mvrv_zscore", 0.0)

    # 초기값
    direction = "flat"
    confidence = 50
    preferred = "flat"
    size_mult = 0.50
    risk = "medium"
    reasoning = "neutral"

    # ─────────────────────────────────────────────────────────────────────────
    # Rule Set A: 극단 호가 (Fear&Greed 기반)
    # ─────────────────────────────────────────────────────────────────────────

    # 공포 극단 (Fear) → Long 진입 신호
    if fear_greed < 20:
        direction = "long"
        confidence = min(75, 50 + abs(fear_greed - 20))
        preferred = "trend"
        size_mult = 1.0
        risk = "high"
        reasoning = f"Extreme fear (FG={fear_greed}), trend opportunity"

    # 탐욕 극단 (Greed) → Short 신호
    elif fear_greed > 80:
        direction = "short"
        confidence = min(75, 50 + abs(fear_greed - 80))
        preferred = "reversion"
        size_mult = 0.80
        risk = "high"
        reasoning = f"Extreme greed (FG={fear_greed}), reversion setup"

    # ─────────────────────────────────────────────────────────────────────────
    # Rule Set B: 가격 급변동 + 펀딩비 극단치 (역발상)
    # ─────────────────────────────────────────────────────────────────────────

    # 24h +5% 이상 상승 + 펀딩비 z-score > 2.0 → Short (과열)
    elif p_24h > 5.0 and fund_z > 2.0:
        direction = "short"
        confidence = 75
        preferred = "reversion"
        size_mult = 0.70
        risk = "medium"
        reasoning = f"Price spike ({p_24h:.1f}%) + high funding (z={fund_z:.2f}), reversion expected"

    # 24h -5% 이상 하락 + 펀딩비 z-score < -2.0 → Long (약세 기회)
    elif p_24h < -5.0 and fund_z < -2.0:
        direction = "long"
        confidence = 75
        preferred = "trend"
        size_mult = 0.80
        risk = "medium"
        reasoning = f"Price drop ({p_24h:.1f}%) + low funding (z={fund_z:.2f}), trend opportunity"

    # ─────────────────────────────────────────────────────────────────────────
    # Rule Set C: HMM 레짐 기반 선호 전략
    # ─────────────────────────────────────────────────────────────────────────

    # 저변동 (state=0) + 펀딩비 극단 → 회귀 전략
    elif hmm_st == 0 and abs(fund_z) > 1.5:
        if fund_z > 1.5:
            direction = "short"
            reasoning = "Low vol regime + high funding, mean reversion"
        else:
            direction = "long"
            reasoning = "Low vol regime + low funding, mean reversion"
        confidence = 65
        preferred = "reversion"
        size_mult = 0.60
        risk = "low"

    # 고변동 (state=2) + 강한 추세 → 추세추종
    elif hmm_st == 2 and abs(p_7d) > 5.0:
        if p_7d > 5.0:
            direction = "long"
            preferred = "trend"
            reasoning = "High vol regime + strong uptrend, follow momentum"
        else:
            direction = "short"
            preferred = "trend"
            reasoning = "High vol regime + strong downtrend, follow momentum"
        confidence = 70
        size_mult = 0.75
        risk = "high"

    # ─────────────────────────────────────────────────────────────────────────
    # Rule Set D: 캐리 전략 (펀딩비 30일 평균 기반)
    # ─────────────────────────────────────────────────────────────────────────

    # 펀딩비 > 0.01% 지속 가능 → 롱 캐리
    elif fund_rate > 0.0001 and fund_z > 0.5:
        direction = "long"
        preferred = "carry"
        confidence = 60
        size_mult = 0.50
        risk = "low"
        reasoning = f"Positive funding ({fund_rate:.5f}), carry opportunity"

    # 펀딩비 < -0.005% 지속 가능 → 숏 캐리 (드물지만)
    elif fund_rate < -0.00005 and fund_z < -1.0:
        direction = "short"
        preferred = "carry"
        confidence = 50
        size_mult = 0.30
        risk = "medium"
        reasoning = f"Negative funding ({fund_rate:.5f}), carry reverse setup"

    # ─────────────────────────────────────────────────────────────────────────
    # Rule Set E: 방향성 신호 없을 때 기본값
    # ─────────────────────────────────────────────────────────────────────────

    if direction == "flat" and reasoning == "neutral":
        # 마지막 기회: MVRV z-score로 추가 신호
        if mvrv_z is not None:
            if mvrv_z > 2.0:
                direction = "short"
                confidence = 55
                preferred = "reversion"
                size_mult = 0.40
                risk = "medium"
                reasoning = f"MVRV extreme (z={mvrv_z:.2f}), toppy"
            elif mvrv_z < -2.0:
                direction = "long"
                confidence = 55
                preferred = "trend"
                size_mult = 0.50
                risk = "medium"
                reasoning = f"MVRV depressed (z={mvrv_z:.2f}), bottomy"

        # 그도 없으면 정말 Flat
        if direction == "flat":
            confidence = 50
            preferred = "flat"
            size_mult = 0.0
            risk = "low"
            reasoning = "No clear signal, neutral bias"

    # ─────────────────────────────────────────────────────────────────────────
    # Sanity Checks
    # ─────────────────────────────────────────────────────────────────────────

    confidence = max(0, min(100, int(confidence)))
    size_mult = max(0.0, min(1.5, float(size_mult)))

    if direction not in ("long", "short", "flat"):
        direction = "flat"
        confidence = 50

    if preferred not in ("trend", "reversion", "flat", "carry"):
        preferred = "flat"

    if risk not in ("low", "medium", "high", "extreme"):
        risk = "medium"

    return LLMAdvisory(
        direction=direction,
        confidence=confidence,
        preferred_strategy=preferred,
        size_multiplier=size_mult,
        risk_level=risk,
        reasoning=reasoning,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Validation & Testing
# ═══════════════════════════════════════════════════════════════════════════════

def validate_llm_advisor():
    """결정론적 LLM 메타 어드바이저 검증."""
    test_cases = [
        # Case 1: 극단 공포 (Fear&Greed)
        {
            "name": "Extreme Fear",
            "context": {
                "price_change_24h": 0.0,
                "price_change_7d": -2.0,
                "hmm_state": 2,
                "hmm_proba": [0.1, 0.2, 0.7],
                "funding_rate": 0.0001,
                "funding_zscore": 0.0,
                "fear_greed": 15,
            },
            "expected_direction": "long",
            "expected_confidence_min": 50,
        },
        # Case 2: 극단 탐욕 (Fear&Greed)
        {
            "name": "Extreme Greed",
            "context": {
                "price_change_24h": 0.0,
                "price_change_7d": 5.0,
                "hmm_state": 2,
                "hmm_proba": [0.1, 0.2, 0.7],
                "funding_rate": 0.0008,
                "funding_zscore": 1.5,
                "fear_greed": 85,
            },
            "expected_direction": "short",
            "expected_confidence_min": 50,
        },
        # Case 3: 24h +5% 상승 + 높은 펀딩비
        {
            "name": "Price Spike + High Funding",
            "context": {
                "price_change_24h": 7.0,
                "price_change_7d": 10.0,
                "hmm_state": 2,
                "hmm_proba": [0.1, 0.2, 0.7],
                "funding_rate": 0.0012,
                "funding_zscore": 2.5,
                "fear_greed": 60,
            },
            "expected_direction": "short",
            "expected_confidence_min": 70,
        },
        # Case 4: 저변동 + 펀딩비 극단
        {
            "name": "Low Vol + Funding Extreme",
            "context": {
                "price_change_24h": 0.5,
                "price_change_7d": -1.0,
                "hmm_state": 0,
                "hmm_proba": [0.6, 0.3, 0.1],
                "funding_rate": -0.0008,
                "funding_zscore": -2.0,
                "fear_greed": 50,
            },
            "expected_direction": "long",
            "expected_confidence_min": 60,
        },
        # Case 5: 고변동 + 강한 추세
        {
            "name": "High Vol + Strong Downtrend",
            "context": {
                "price_change_24h": -3.0,
                "price_change_7d": -8.0,
                "hmm_state": 2,
                "hmm_proba": [0.1, 0.2, 0.7],
                "funding_rate": 0.0,
                "funding_zscore": 0.0,
                "fear_greed": 30,
            },
            "expected_direction": "short",
            "expected_confidence_min": 65,
        },
        # Case 6: 양수 펀딩비 (캐리)
        {
            "name": "Positive Funding Rate (Carry)",
            "context": {
                "price_change_24h": 0.2,
                "price_change_7d": 0.5,
                "hmm_state": 1,
                "hmm_proba": [0.3, 0.4, 0.3],
                "funding_rate": 0.0002,
                "funding_zscore": 1.0,
                "fear_greed": 50,
            },
            "expected_direction": "long",
            "expected_preferred": "carry",
        },
        # Case 7: HMM 저신뢰도 → Flat
        {
            "name": "HMM Uncertain",
            "context": {
                "price_change_24h": 1.0,
                "price_change_7d": 2.0,
                "hmm_state": 1,
                "hmm_proba": [0.3, 0.4, 0.3],  # max=0.4 < 0.5
                "funding_rate": 0.0001,
                "funding_zscore": 0.0,
                "fear_greed": 50,
            },
            "expected_direction": "flat",
        },
        # Case 8: 완전 중립 상태
        {
            "name": "Neutral (No Signal)",
            "context": {
                "price_change_24h": 0.0,
                "price_change_7d": 0.0,
                "hmm_state": 1,
                "hmm_proba": [0.3, 0.4, 0.3],
                "funding_rate": 0.00005,
                "funding_zscore": 0.0,
                "fear_greed": 50,
            },
            "expected_direction": "flat",
            "expected_confidence": 50,
        },
    ]

    passed = 0
    failed = 0

    print("\n" + "=" * 80)
    print("LLM Meta Advisor Validation Suite")
    print("=" * 80)

    for i, test in enumerate(test_cases, 1):
        advisory = simulate_llm_advisory(test["context"])

        # 검증
        success = True
        failures = []

        if "expected_direction" in test:
            if advisory.direction != test["expected_direction"]:
                success = False
                failures.append(
                    f"Direction mismatch: got {advisory.direction}, "
                    f"expected {test['expected_direction']}"
                )

        if "expected_confidence_min" in test:
            if advisory.confidence < test["expected_confidence_min"]:
                success = False
                failures.append(
                    f"Confidence too low: got {advisory.confidence}, "
                    f"expected >= {test['expected_confidence_min']}"
                )

        if "expected_confidence" in test:
            if advisory.confidence != test["expected_confidence"]:
                success = False
                failures.append(
                    f"Confidence mismatch: got {advisory.confidence}, "
                    f"expected {test['expected_confidence']}"
                )

        if "expected_preferred" in test:
            if advisory.preferred_strategy != test["expected_preferred"]:
                success = False
                failures.append(
                    f"Strategy mismatch: got {advisory.preferred_strategy}, "
                    f"expected {test['expected_preferred']}"
                )

        status = "✓ PASS" if success else "✗ FAIL"
        passed += success
        failed += not success

        print(f"\n[{i}] {test['name']}: {status}")
        print(f"    Direction: {advisory.direction} (confidence={advisory.confidence})")
        print(f"    Strategy: {advisory.preferred_strategy}")
        print(f"    Size: {advisory.size_multiplier:.2f}x, Risk: {advisory.risk_level}")
        print(f"    Reasoning: {advisory.reasoning}")

        if failures:
            for failure in failures:
                print(f"    ERROR: {failure}")

    print("\n" + "=" * 80)
    print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print("=" * 80)

    if failed == 0:
        print("[PASS] LLM Meta Advisor validation complete")
        return True
    else:
        print(f"[FAIL] {failed} validation test(s) failed")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LLM Meta Advisor validation")
    parser.add_argument("--validate", action="store_true", help="Run validation suite")
    args = parser.parse_args()

    if args.validate or len(sys.argv) == 1:
        success = validate_llm_advisor()
        sys.exit(0 if success else 1)
