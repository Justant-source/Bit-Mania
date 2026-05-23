---
title: 백테스트 문서 홈
category: backtest-docs
last_updated: 2026-05-18
---

# 백테스트 문서

이 디렉토리(`backtest/docs/`)가 백테스트 R&D의 **유일한 문서 source-of-truth**입니다.

## 문서 구조

| 디렉토리 | 설명 |
|---|---|
| `strategies/` | 전략 **R&D 레저** (001-021, 과거 설계 히스토리) |
| `methodology/` | 백테스트 **방법론 + 도구** (공통 가이드) |

## 핵심 진입점

→ **`backtest/README.md`**: 백테스트 실행 및 구조 (여기서 시작)

## 코드 ↔ 문서 매핑

코드 경로 역인덱스: **`backtest/docs/CODE_MAP.md`**

## 주요 문서

### 방법론 (methodology/)
**현활 백테스트 인프라 가이드**:
- **backtest-skillset.md** — 스크립트 관리 규칙 + 카테고리별 도구 정보
- **jesse-engine.md** — Jesse 프레임워크 설정 및 데이터 파이프라인 (아카이브)
- **jesse-strategies.md** — Jesse 전략 v1-v3 사양서 (아카이브)
- **jesse-vs-self-engine.md** — 성과 비교 분석 (아카이브)
- **live-postmortem-template.md** — 거래 사후분석 템플릿

### 전략 사양 (strategies/)
**001-021**: 전략 R&D 히스토리 (설계부터 폐기까지)
- 001-015: 개념 설계 전략
- 016-019: 외부 구현 전략 (Stoch, TradeIQ, TrendType 등) — 현재 폐기됨
- 020: Supertrend + TrendType 조합
- 021: TradeIQ CCI+CE (최후 폐기 후보)

**현활 전략**: Supertrend 4h 3x long-only (combo #7908) — 메인넷 배포 중

## 빌드 및 실행

```bash
# 백테스트 환경 기동
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres

# 단일 외부 전략 백테스트
docker compose --profile backtest run --rm backtester \
  python /app/scripts/runners/run_external_backtest.py --strat SupertrendStrategy --tf 1h

# 대시보드 재빌드
docker compose --profile backtest run --rm backtester \
  python /app/dashboards/script/build_supertrend_dashboard.py
docker compose --profile backtest run --rm backtester \
  python /app/dashboards/script/build_strategy_dashboard.py
```

## 관련 링크

- **가이드**: `backtest/README.md`
- **PostgreSQL 스키마**: `backtest/scripts/db/schema_st.sql`
- **결과**: `backtest/results/supertrend_x3_long_only/` + `backtest/results/7-strategies/`
