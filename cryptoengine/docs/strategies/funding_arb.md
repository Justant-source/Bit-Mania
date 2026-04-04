---
title: 펀딩 레이트 차익거래 전략
tags:
  - strategy
  - core
  - funding-rate
  - delta-neutral
  - arbitrage
aliases:
  - Funding Arb
  - 펀딩비 차익거래
related:
  - "[[architecture]]"
  - "[[grid_trading]]"
  - "[[adaptive_dca]]"
  - "[[api]]"
  - "[[runbook]]"
---

# 펀딩 레이트 차익거래 전략

> [!info] 핵심 전략
> CryptoEngine의 **핵심(Core) 전략**으로, 델타 중립 포지션을 통해 펀딩비를 수취합니다.
> 관련 서비스: `services/strategies/funding-arb/`

## 개요

무기한 선물(Perpetual Futures)의 펀딩레이트를 수취하기 위해 델타 중립(Delta-Neutral) 포지션을 유지하는 전략입니다. 현물 매수 + 무기한 선물 매도를 동시에 진행하여 방향성 리스크를 제거하고 펀딩비만 수취합니다.

> [!tip] 다른 전략과의 관계
> - [[grid_trading|그리드 트레이딩]]: 횡보장 보조 전략 (레짐이 `ranging`일 때 활성화)
> - [[adaptive_dca|적응형 DCA]]: 장기 축적 보조 전략
> - 전략 가중치는 [[architecture#2. Strategy Orchestrator|오케스트레이터]]가 레짐에 따라 동적 배분

## 수익 구조

```
수익 = 펀딩비 수입 - 거래 수수료 - 슬리피지 - 기회비용
```

- **펀딩레이트**: Bybit 기준 8시간마다 정산 (하루 3회)
- **연환산 수익률**: 펀딩레이트 0.01% 기준 → 연 10.95%
- **목표**: 연 15-30% (펀딩레이트가 높은 시기 선별 진입)

## 진입 조건

| 조건 | 임계값 | 설명 |
|------|--------|------|
| 펀딩레이트 | > 0.005% | 최소 수익성 확보 |
| 연속 양수 구간 | 3회 이상 | 일시적 급등 필터링 |
| Basis Spread | < 0.3% | 현물-선물 가격 괴리 제한 |
| 미결제약정 | > $5M | 유동성 확보 |
| 예측 펀딩레이트 | 같은 방향 | 다음 기 펀딩레이트 확인 |

> [!note] 펀딩레이트 데이터 흐름
> [[api#`market:funding:{symbol}`|market:funding 채널]]을 통해 market-data → funding-arb로 실시간 전달

## 진입 프로세스

1. **조건 검증**: 위 조건 모두 충족 확인
2. **포지션 크기 계산**: 자본의 95% 활용 (5% 수수료/슬리피지 버퍼)
3. **동시 주문**: 현물 매수 + 선물 매도 (Post-Only Limit)
   - 주문은 [[api#`order:request`|order:request 채널]]을 통해 [[architecture#3. Execution Engine|실행 엔진]]으로 전달
4. **한쪽 체결 복구**:
   - 한쪽만 체결 시 3분 대기
   - 타임아웃 후 미체결 주문 취소
   - 체결된 레그 시장가 청산
5. **Basis Spread SM 상태 전환**: `IDLE → OPEN`

## 청산 조건

| 트리거 | 조건 | 우선순위 |
|--------|------|----------|
| 펀딩 반전 | 펀딩레이트 < 0 | 높음 |
| Basis 발산 | Spread > 1.0% | 높음 |
| Basis 수렴 | Spread < 0.1% | 중간 (이익 실현) |
| 최대 보유 기간 | > 30일 | 낮음 |
| Kill Switch | 시스템 신호 | 최우선 |

> [!warning] Kill Switch 연동
> [[architecture#Kill Switch 4단계|Kill Switch]] 발동 시 즉시 청산됩니다. 대응 절차는 [[runbook#Kill Switch 대응]] 참조.

## 청산 프로세스

1. **선물 포지션 우선 청산** (리스크가 더 높은 레그)
2. **현물 매도**
3. **Basis Spread SM 상태 전환**: `OPEN → IDLE`
4. **P&L 기록**: 펀딩비 수입 + Basis P&L

## 델타 중립 관리 (`DeltaNeutralManager`)

### 수량 균형
- 현물과 선물 수량 차이 모니터링
- 허용 오차: 0.1% (MAX_QTY_DIVERGENCE)
- 초과 시 자동 리밸런싱 주문 생성

### 마진 건전성
- 마진 버퍼 = |마진 비율| / 유지마진 비율
- 최소 3배 유지 (MARGIN_BUFFER_MULTIPLIER)
- 미달 시 포지션 25% 축소 (디레버리징)

## 펀딩비 수취 프로세스

1. **5분 전 확인**: 펀딩 정산 5분 전 포지션 크기 검증
2. **정산 후 기록**: 수취 금액 = 포지션 크기 x 펀딩레이트
3. **Redis + DB 저장**: 실시간 추적 및 히스토리 보관
   - DB 테이블: `funding_payments`, `funding_rate_history` ([[architecture#PostgreSQL 16|DB 스키마 참조]])

## 설정 파일

`config/strategies/funding_arb.yaml` 참조

### 주요 파라미터

```yaml
entry:
  min_funding_rate_annualized: 15.0
  consecutive_intervals: 3
  max_entry_spread_pct: 0.05

position:
  sizing_mode: pct_equity
  pct_equity: 5.0
  max_leverage: 3
  max_concurrent_positions: 5

exit:
  exit_on_rate_flip: true
  stop_loss_pct: 2.0
  take_profit_pct: 3.0
```

## 리스크 요소

| 리스크 | 영향 | 완화 방법 |
|--------|------|-----------|
| 펀딩레이트 반전 | 지급 전환 | 즉시 청산 트리거 |
| Basis 발산 | 현물/선물 가격 괴리 | 최대 Spread 임계값 |
| 유동성 부족 | 슬리피지 증가 | 최소 OI 요건 |
| 거래소 장애 | 포지션 관리 불가 | [[architecture#Kill Switch 4단계|Kill Switch L3]] |
| 마진 부족 | 강제 청산 위험 | 3배 마진 버퍼 |
| 한쪽 체결 실패 | 방향성 노출 | 3분 대기 + 청산 복구 |

## 백테스트 결과 해석

- **Sharpe > 2.0**: 양호
- **최대 낙폭 < 3%**: 양호
- **승률 > 60%**: 양호
- **Profit Factor > 2.0**: 양호

> [!seealso] 관련 문서
> - [[architecture|시스템 아키텍처]] — 전체 서비스 구조
> - [[api|내부 API]] — Redis 채널 및 메시지 포맷
> - [[runbook|운영 매뉴얼]] — 인시던트 대응
> - [[grid_trading|그리드 트레이딩]] — 횡보장 보조 전략
> - [[adaptive_dca|적응형 DCA]] — 장기 축적 보조 전략
> - [[changelog|변경 이력]] — 버전별 변경사항
