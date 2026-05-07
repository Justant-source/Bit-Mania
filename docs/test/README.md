---
title: 테스트 및 백테스트 가이드
category: test
last_updated: 2026-05-01
---

# 테스트 및 백테스트 가이드

CryptoEngine의 백테스트 시스템, Jesse 프레임워크, 라이브 거래 검증 문서를 통합 관리합니다.

**핵심 원칙**:
- 백테스트 시뮬레이터는 라이브 거래와 완전히 독립 (데이터 갭 존재)
- 모든 백테스트 스크립트는 **스킬셋** 방식으로 관리
- Phase 4 진행 중: Walk-Forward 월간 자동 실행

## 백테스트 시스템 개요

### 두 가지 엔진

| 엔진 | 위치 | 용도 | 데이터 | 성과 |
|------|------|------|--------|------|
| **Jesse Framework** | `services/jesse_engine/` | 공식 검증, WF 최적화 | 6년 (2019-2025) | CAGR +34.87% |
| **자체 시뮬레이터** | (폐기됨) | 초기 프로토타입 | 제한적 | 데이터 갭 |

**현재 정책**: Jesse 데이터만 신뢰. 라이브 거래와 비교하지 말 것 (개발 용도로만 사용).

### 성과 갭 설명

백테스트 vs 라이브 거래 성과가 다른 이유:

1. **데이터 품질**: Jesse 6년 데이터 vs 라이브 몇 개월 데이터
2. **시장 환경**: 과거 vs 현재 펀딩레이트, 변동성 차이
3. **거래 비용**: 모의 수수료 vs 실제 수수료 (0.055%)
4. **미체결**: 슬리피지, 부분 체결 미반영
5. **스톱로스**: 온 익스체인지 기능 백테스트에 미포함

**해석**: Jesse 결과는 **상한선**(이상적 조건), 라이브는 **현실** (비용 포함)

---

## 문서 맵

| 문서 | 목적 | 대상 사용자 |
|------|------|-----------|
| **[backtest-skillset.md](./backtest-skillset.md)** | 스크립트 관리 규칙 (필수) | 모든 개발자 |
| **[jesse-engine.md](./jesse-engine.md)** | Jesse 프레임워크 설정 | Jesse 실행자 |
| **[jesse-strategies.md](./jesse-strategies.md)** | Jesse 전략 목록, V5 기준 | 전략 개발자 |
| **[jesse-vs-self-engine.md](./jesse-vs-self-engine.md)** | 성과 비교, 갭 해석 | 성과 분석 담당 |
| **[live-postmortem-template.md](./live-postmortem-template.md)** | 거래 분석 템플릿 (A/B/C/D RCA) | 사후 분석 |
| **[phase4-checklist.md](./phase4-checklist.md)** | Phase 4 완료 기준 (7개 시나리오) | Phase 관리자 |

### 추가 참고

- **데이터 파이프라인**: 5개 스크립트로 관리 (docs/structure/ 참조)
- **Walk-Forward**: 매월 1일 02:00 KST 자동 실행 (wf-scheduler 서비스)
- **라이브 데이터**: PostgreSQL `trades`, `positions`, `funding_payments` 테이블

## 빠른 시작

### Jesse 백테스트 실행

```bash
# 전체 검증 실행 (모든 전략)
docker compose --profile backtest build --no-cache jesse_engine
docker compose --profile backtest run --rm jesse_engine \
  python scripts/run_full_validation.sh

# 특정 전략만 테스트
docker compose --profile backtest run --rm jesse_engine \
  python scripts/run_full_validation.sh FundingArb

# 단일 기간 백테스트
docker compose --profile backtest run --rm jesse_engine \
  jesse backtest 2024-01-01 2024-12-31 \
  --route 'Bybit Perpetual:BTCUSDT:1h:FundingArbStrategy'

# JSON 결과 출력
docker compose --profile backtest run --rm jesse_engine \
  python scripts/run_full_validation.sh --json > backtest_result.json
```

### 라이브 거래 분석

```bash
# PostgreSQL에서 거래 데이터 추출
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT * FROM trades WHERE created_at > NOW() - INTERVAL '7 days' \
       ORDER BY created_at DESC;"

# 펀딩비 수익 집계
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT DATE(settlement_time), SUM(funding_payment) as daily_funding \
       FROM funding_payments GROUP BY DATE(settlement_time) \
       ORDER BY DATE DESC LIMIT 30;"

# Postmortem 템플릿으로 A/B/C/D 분석
# → [live-postmortem-template.md](./live-postmortem-template.md) 참조
```

---

## Phase 4 진행상황

| 항목 | 상태 | 진행률 | 마감 |
|------|------|-------|------|
| 7일 무중단 운영 | ✅ 진행 중 | ? | 2026-05-15 |
| Walk-Forward 월간 파이프라인 | ✅ 활성 | 1회차 완료 | 매월 1일 |
| Phase 5 Preflight 체크 | ✅ 준비 중 | 7/8 항목 | Phase 5 진입 전 |
| Jesse 6년 백테스트 | ✅ 검증 | CAGR +34.87% | 기준선 유지 |

---

## 데이터 파이프라인 (5개 스크립트)

위치: `services/jesse_engine/scripts/`

| 스크립트 | 목적 | 실행 주기 |
|---------|------|---------|
| `import_ohlcv.py` | Bybit에서 OHLCV 임포트 | 온디맨드 |
| `import_funding_rates.py` | 펀딩레이트 히스토리 임포트 | 온디맨드 |
| `run_full_validation.sh` | 모든 전략 검증 + 결과 생성 | 온디맨드 |
| `backtest_funding_arb.py` | FA 전략 단일 백테스트 | 온디맨드 |
| `walk_forward.py` | Walk-Forward 최적화 | 매월 1일 |

**중요**: 모든 스크립트 추가 시 README.md 업데이트 필수 (스킬셋 규칙)

---

**최종 수정**: 2026-05-01  
**백테스트 시스템 상태**: Jesse 6년 데이터 검증 완료, Phase 4 라이브 진행 중
