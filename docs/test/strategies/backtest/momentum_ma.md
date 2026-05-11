---
title: MomentumMAStrategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/MomentumMAStrategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# MomentumMAStrategy

선형 회귀 편차(VAL) 기반 모멘텀 전략. EMA 방향성 필터 결합.

## 전략 개요

- **지표**: lin_len 기간 선형 회귀로 편차(VAL) 계산, val_ma_len EMA로 평활화
- **진입 조건**: VAL EMA 크로스 업(롱) / 크로스 다운(숏)
- **청산**: ATR 기반 stop-loss

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `lin_len` | 20 | — | 10~50 | 선형 회귀 기간 |
| `atr_mult` | 3.0 | — | 1.5~5.0 | ATR stop-loss 배수 |
| `val_ma_len` | 100 | 100 | 50~200 | VAL EMA 기간 (고정) |

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | -1.72% | 0.216 | -76.76% | 845 | FAIL |
| 4h | long_only | +3.84% | 0.330 | -68.53% | 441 | FAIL |
| 1D | bidirectional | -10.42% | 0.041 | -70.80% | 119 | FAIL |
| 1D | long_only | +2.25% | 0.305 | -69.75% | 81 | FAIL |

> 주요 문제: CAGR 음수 또는 매우 낮음, MDD -70%+

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `lin_len` × `atr_mult`

| combo | lin_len | atr_mult | 비고 |
|-------|---------|---------|-----|
| 1 | 14 | 3.0 | lin_len 낮춤 |
| 2 | 20 | 3.0 | **baseline** |
| 3 | 30 | 3.0 | lin_len 높임 |
| 4 | 20 | 2.0 | atr_mult 낮춤 |
| 5 | 20 | 4.0 | atr_mult 높임 |
| 6 | 14 | 2.0 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #3 (lin_len=30, atr_mult=3.0) | 4h | long_only | +16.98% | 0.565 | -69.32% | 미채택 |

> 스윕 2026-05-11 완료. **6가지 조합 전체 FAIL** — MDD -60~-89%, CAGR 음수 조합 다수. 선형 회귀 편차 전략은 현재 파라미터 범위로 개선 불가. 추가 구조적 변경 필요.
