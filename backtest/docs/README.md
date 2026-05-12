---
title: 백테스트 문서 홈
category: backtest-docs
last_updated: 2026-05-12
---

# 백테스트 문서

이 디렉토리(`backtest/docs/`)가 백테스트 R&D의 **유일한 문서 source-of-truth**입니다.

## 문서 3계층 구조

| 디렉토리 | 설명 | 예시 |
|---|---|---|
| `strategies/` | 전략 **사양** (WHAT + WHY) | 001_funding_arb.md |
| `runs/` | 전략별 **실험 ledger** (HOW-RAN) | supertrend.md |
| `optimization/` | **최적해 탐색 과정** (WHY-CHOSEN) | param_sweep_v2.md |
| `methodology/` | 공통 방법론 + 도구 | backtest-skillset.md |

## 실행 빠른 참조

- 스크립트: `backtest/scripts/`
- 데이터: `backtest/data/` (Parquet, gitignore)
- 결과: `backtest/results/` (Parquet)
- 인프라: `backtest/docker/`

## CODE_MAP

코드 ↔ 문서 역인덱스: `backtest/docs/CODE_MAP.md`

## 주요 문서

### 방법론 (methodology/)
- **backtest-skillset.md** — 스크립트 관리 규칙 (필수 읽음)
- **jesse-engine.md** — Jesse 프레임워크 설정
- **jesse-strategies.md** — Jesse 전략 목록
- **jesse-vs-self-engine.md** — 성과 비교 분석
- **live-postmortem-template.md** — 거래 사후분석 템플릿

### 전략 사양 (strategies/)
001~015: 설계 중인 전략들의 개념 사양서

### 실험 레저 (runs/)
각 전략별 실행 결과 기록:
- supertrend.md, momentum_ma.md, stoch.md 등 (외부 전략)

### 최적화 탐색 (optimization/)
- param_sweep_v2.md, param_sweep_v3.md — 매개변수 최적화 과정
- rolling_window_test.md — 롤링윈도우 검증
- 결과 리포트들

### 기타
- phase4-checklist.md — Phase 4 완료 기준
