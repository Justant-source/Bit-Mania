---
title: TrendTypeStrategy 파라미터 참조
category: backtest-strategy
related_code:
  - cryptoengine/services/jesse_engine/strategies/external/TrendTypeStrategy.py
  - cryptoengine/services/jesse_engine/scripts/wf_optimize_skopt.py
last_updated: 2026-05-11 (스윕 완료)
when_to_update: |
  - 파라미터 최적화 스윕 실행 후
  - wf_optimize_skopt.py Bayesian 최적화 실행 후
  - 전략 로직 변경 시
---

# TrendTypeStrategy

ADX/DI 기반 TrendType 지표 전략. 강한 추세(±2.0) 발생 시 진입.

## 전략 개요

- **지표**: ATR + ADX + ±DI로 TrendType 값 계산 (±2.0 = 강한 추세)
- **진입 조건**: TrendType == +2.0 (롱) / == -2.0 (숏)
- **청산**: ATR 기반 stop-loss

## 파라미터 정의

| 파라미터 | 현재 기본값 | 최적화값 | 범위 | 설명 |
|---------|-----------|---------|-----|-----|
| `atr_len` | 14 | **14** | 7~21 | ATR 계산 기간 |
| `di_len` | 14 | **18** | 7~21 | DI/ADX 계산 기간 |
| `atr_ma_len` | 20 | 20 | 10~30 | ATR 이동평균 기간 (고정) |
| `adx_len` | 14 | — | 7~21 | ADX 기간 (= di_len 동기화) |
| `smooth` | 1 | 1 | 1~5 | 스무딩 기간 (고정) |
| `atr_mult` | 3.0 | 3.0 | 1.5~5.0 | ATR stop-loss 배수 (고정) |

> `adx_len`은 항상 `di_len`과 동일하게 설정 (`wf_optimize_skopt.py` 패턴).

## 현재 기준 성과 (기본값, 2021-01~2026-04)

| TF | variant | CAGR | Sharpe | MDD | Trades | verdict |
|----|---------|------|--------|-----|--------|---------|
| 4h | bidirectional | +0.86% | 0.213 | -65.18% | 871 | FAIL |
| 4h | long_only | +15.83% | 0.670 | -36.42% | 432 | **PASS** |
| 1D | bidirectional | +33.33% | 1.057 | -23.26% | 131 | **PASS** |
| 1D | long_only | +24.23% | 1.333 | -20.94% | 61 | **PASS** |

## 파라미터 스윕 설정 (2026-05-11)

최적화 대상: `atr_len` × `di_len` (`adx_len` = `di_len` 동기화)

| combo | atr_len | di_len | 비고 |
|-------|---------|--------|-----|
| 1 | 10 | 14 | atr_len 낮춤 |
| 2 | 14 | 14 | **baseline** |
| 3 | 18 | 14 | atr_len 높임 |
| 4 | 14 | 10 | di_len 낮춤 |
| 5 | 14 | 18 | di_len 높임 |
| 6 | 10 | 10 | 둘 다 낮춤 |

## 최적화 히스토리

| 날짜 | combo | TF | variant | CAGR | Sharpe | MDD | 채택 |
|------|-------|----|---------|----|--------|-----|-----|
| 2026-05-11 | #5 (atr_len=14, di_len=18) | 1D | long_only | +25.83% | 1.040 | -24.83% | **채택** |

> 스윕 2026-05-11 완료. di_len=18이 baseline(14) 대비 CAGR +1.6%p 향상, MDD -24.83%로 개선. **최적값 atr_len=14, di_len=18 (adx_len=18)**.

## 관련 스크립트

- `wf_optimize_skopt.py` — TrendType 전용 Bayesian (skopt) 최적화 스크립트
  IS: 2021-04-01~2022-12-31, OOS: 2023-01-01~2025-12-31, n_calls=40
