---
title: TradeIQ220323Strategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/TradeIQ220323Strategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# TradeIQ220323Strategy

CCI 크로스 + Chandelier Exit 기반 전략 (TradeIQ 2022-03-23).

## 전략 개요

- **진입 조건**: CCI가 cci_lower/cci_upper 크로스 + Chandelier Exit 방향 확인
- **청산**: ATR 기반 stop-loss + Chandelier Exit 반전 시 청산

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `cci_period` | 20 | **20** | 14~30 | CCI 계산 기간 |
| `ce_mult` | 3.0 | **3.0** | 2.0~4.0 | Chandelier Exit ATR 배수 |
| `cci_lower` | -100.0 | -100.0 | — | CCI 하단 크로스 기준 (고정) |
| `cci_upper` | 100.0 | 100.0 | — | CCI 상단 크로스 기준 (고정) |
| `ce_period` | 22 | 22 | 14~30 | Chandelier Exit ATR 기간 (고정) |
| `atr_mult` | 3.0 | 3.0 | 1.5~5.0 | ATR stop-loss 배수 (고정) |

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | +14.65% | 0.705 | -28.26% | 108 | **PASS** |
| 4h | long_only | +9.49% | 0.753 | -31.45% | 58 | FAIL |
| 1D | bidirectional | -13.26% | -0.638 | -63.95% | 19 | FAIL |
| 1D | long_only | -12.06% | -1.300 | -52.84% | 9 | FAIL |

> 4h bidirectional PASS. 1D 전체 심각한 음수 CAGR (-13%/-12%) — trades 부족(9/19).

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `cci_period` × `ce_mult`

| combo | cci_period | ce_mult | 비고 |
|-------|-----------|---------|-----|
| 1 | 14 | 3.0 | cci_period 낮춤 |
| 2 | 20 | 3.0 | **baseline** |
| 3 | 26 | 3.0 | cci_period 높임 |
| 4 | 20 | 2.5 | ce_mult 낮춤 |
| 5 | 20 | 3.5 | ce_mult 높임 |
| 6 | 14 | 2.5 | 둘 다 낮춤 |

> 1D 결과는 trades 수가 극히 낮아 통계적 유의성 주의 필요.

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #2 (cci_period=20, ce_mult=3.0) | 4h | bidirectional | +14.65% | 0.665 | -32.19% | 채택 (기본값 확인) |

> 스윕 2026-05-11 완료. 6가지 조합 중 기본값(#2)이 최선. fee 적용 후 MDD -32.19% (기존 문서 -28.26%). 1D 전체 음수 CAGR — 구조적 한계. **최적값 = 기본값 유지**.
