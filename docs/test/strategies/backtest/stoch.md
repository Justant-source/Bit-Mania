---
title: StochStrategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/StochStrategy.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - 전략 로직 변경 시
---

# StochStrategy

스토캐스틱 오실레이터 기반 전략. 과매수/과매도 신호와 EMA 트렌드 필터를 결합.

## 전략 개요

- **진입 조건**: 스토캐스틱 K가 OS 레벨에서 반등(롱) / OB 레벨에서 반전(숏)
- **추세 필터**: fast/slow EMA 크로스
- **청산**: ATR 기반 stop-loss (atr_mult × ATR)
- **방향성 EMA**: `use_direction_ema=True` 시 direction_ema_len EMA 위/아래에서만 진입

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `stoch_k_period` | 14 | — | 10~20 | 스토캐스틱 K 기간 |
| `atr_mult` | 3.0 | — | 1.5~5.0 | ATR stop-loss 배수 |
| `fast_n` | 7 | 7 | 5~15 | 빠른 EMA 기간 (고정) |
| `slow_n` | 20 | 20 | 15~30 | 느린 EMA 기간 (고정) |
| `direction_ema_len` | 200 | 200 | 100~300 | 방향성 EMA 기간 (고정) |
| `stoch_smooth` | 3 | 3 | 1~5 | 스토캐스틱 K 스무딩 (고정) |
| `ob_level` | 80.0 | 80.0 | — | 과매수 레벨 (고정) |
| `os_level` | 20.0 | 20.0 | — | 과매도 레벨 (고정) |
| `use_direction_ema` | False | False | — | 방향성 EMA 활성화 (고정) |

> `—` 는 스윕에서 고정된 값. 최적화값은 파라미터 스윕 실행 후 채워진다.

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | +5.59% | 0.339 | -59.65% | 385 | FAIL |
| 4h | long_only | +17.09% | 0.550 | -61.40% | 345 | FAIL |
| 1D | bidirectional | +9.51% | 0.499 | -66.17% | 68 | FAIL |
| 1D | long_only | +12.75% | 0.568 | -77.97% | 60 | FAIL |

> 주요 문제: MDD -60~-78% (PASS 기준 -30% 미달)

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `stoch_k_period` × `atr_mult`

| combo | stoch_k_period | atr_mult | 비고 |
|-------|---------------|---------|-----|
| 1 | 10 | 3.0 | k_period 낮춤 |
| 2 | 14 | 3.0 | **baseline** |
| 3 | 18 | 3.0 | k_period 높임 |
| 4 | 14 | 2.0 | atr_mult 낮춤 |
| 5 | 14 | 4.0 | atr_mult 높임 |
| 6 | 10 | 2.0 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #1 (stoch_k=10, atr=3.0) | 4h | long_only | +20.54% | 0.624 | -66.11% | 미채택 |

> 스윕 2026-05-11 완료. **6가지 조합 전체 FAIL** — MDD -54~-89%, PASS 기준(-35%) 대폭 미달. 스토캐스틱 전략은 현재 파라미터 범위로 개선 불가. 추가 구조적 변경 필요.
