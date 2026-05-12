---
title: CryptoEngine 문서 홈
category: root
last_updated: 2026-05-12
---

# CryptoEngine 문서

> **BTC 선물 자동매매 시스템**. 펀딩비 차익거래(FA) 핵심 전략, BTC 단일 운영, Bybit 테스트넷 → 메인넷 소액 투자.
>
> **현재 상태 (Phase 4)**: 테스트넷 포워드 테스트 진행 중
> - 채택 설정: `fa80_lev5_r30` (FA 80% 자본, 5x 레버리지, 30% 재투자)
> - 백테스트 성과: CAGR +34.87% | Sharpe 3.583 | MDD -4.52% | 6년 청산 0회
> - 운영 모드: 테스트넷 (`BYBIT_TESTNET=true`)

이 디렉토리(`docs/`)가 프로젝트의 **유일한 문서 source-of-truth**이다.
어떤 코드를 수정하든 [CODE_MAP.md](CODE_MAP.md) 에서 관련 문서를 찾아 함께 업데이트해야 한다.

---

## 빠른 참조

### 긴급 상황
- 포지션 수동 청산: [policies/emergency-manual-close.md](policies/emergency-manual-close.md)
- Kill Switch 상태 확인: `redis-cli GET ce:kill_switch:active`
- 서비스 비상 정지: `make emergency` (모든 포지션 청산)

### Phase 전환
- Phase 4→5 진입 (메인넷): [scripts/switch_to_mainnet.py](../cryptoengine/scripts/switch_to_mainnet.py)
- 메인넷→테스트넷 롤백: [scripts/switch_to_testnet.py](../cryptoengine/scripts/switch_to_testnet.py)
- 실전 전환 체크: [scripts/phase5_preflight.py](../cryptoengine/scripts/phase5_preflight.py) 8개 항목 PASS 필수
- 상세 절차: [policies/operations/mainnet-switch.md](policies/operations/mainnet-switch.md)

### 백테스트 실행
- 현재 활성 엔진: Jesse (6년 검증 데이터)
- 스킬셋 규칙: [backtest/docs/methodology/backtest-skillset.md](../backtest/docs/methodology/backtest-skillset.md) 필수 확인
- 실행: `docker compose --profile backtest run --rm jesse_engine python scripts/<script>.py`

### 모니터링
- Grafana 대시보드: http://localhost:3002 (admin / ***REMOVED***)
- Telegram 알림: /status, /positions, /kill, /resume 등
- 상세 가이드: [policies/operations/monitoring.md](policies/operations/monitoring.md)

---

## 작업별 가이드

**처음 합류 (신규 컨텍스트)**:
1. [architecture/system-overview.md](architecture/system-overview.md) — 전체 아키텍처
2. [glossary.md](glossary.md) — 용어 정리 (FA, Delta Neutral, Kill Switch 등)
3. [policies/btc-only.md](policies/btc-only.md) — BTC 단일 운영 원칙
4. [policies/operations/runbook.md](policies/operations/runbook.md) — 일상 운영

**전략 수정**:
→ [policies/strategies/](policies/strategies/)
→ [policies/btc-only.md](policies/btc-only.md) (BTC 제약 재확인)

**운영 (시작/중지/장애)**:
1. [policies/operations/runbook.md](policies/operations/runbook.md) — 기본 명령
2. [policies/operations/monitoring.md](policies/operations/monitoring.md) — 모니터링 설정
3. [policies/kill-switch.md](policies/kill-switch.md) — Kill Switch 임계값
4. [policies/emergency-manual-close.md](policies/emergency-manual-close.md) — 비상 청산

**메인넷 전환 (Phase 5)**:
1. [policies/operations/mainnet-switch.md](policies/operations/mainnet-switch.md) — 9단계 절차
2. `scripts/phase5_preflight.py` — 8개 항목 체크
3. 루트 `CLAUDE.md`의 "다음 작업" 섹션

**배포 & 포지션 보호**:
→ [policies/deployment-position.md](policies/deployment-position.md)
→ [policies/operations/deployment-procedure.md](policies/operations/deployment-procedure.md)

**코드 수정 후 문서 찾기**:
→ [CODE_MAP.md](CODE_MAP.md)

---

## 카테고리

| 카테고리 | 설명 | 핵심 문서 |
|---------|------|---------|
| [architecture/](architecture/) | 시스템 설계 — 시스템 개요, 데이터 흐름 | system-overview.md |
| [policies/](policies/) | 정책 — BTC-only, Kill Switch, 운영 규칙 | btc-only.md, kill-switch.md |
| [policies/operations/](policies/operations/) | 운영 매뉴얼 — Runbook, 모니터링, 배포 | runbook.md, monitoring.md, mainnet-switch.md |
| [policies/strategies/](policies/strategies/) | 전략 사양 — Funding Arb, Adaptive DCA | funding-arb.md, adaptive-dca.md |
| [structure/](structure/) | 코드 레이아웃 — 19개 서비스, 디렉토리 트리 | — |
| [../backtest/docs/](../backtest/docs/) | 백테스트 & 검증 — Jesse, 스킬셋, Phase 4 체크 | methodology/backtest-skillset.md, phase4-checklist.md |

---

## 핵심 지표 & 임계값

| 항목 | 현재값 | 정상 범위 | 경고 |
|------|--------|-----------|------|
| **CAGR** (연환산) | +34.87% | > +15% | < 0% |
| **Sharpe Ratio** | 3.583 | > 2.0 | < 1.0 |
| **Max Drawdown** | -4.52% | -5% ~ 0% | > -10% |
| **Daily P&L** | -2% ~ +5% | -2% ~ +5% | < -3% |
| **Margin Ratio** | > 36x (기준) | > 10x | 5x ~ 10x (경고), < 5x (긴급) |
| **Kill Switch Events** | 0회 (7일) | 0회 | 1회 이상 |

---

## 단일 출처 / 도움말

| 리소스 | 목적 | 언제 확인 |
|--------|------|---------|
| [glossary.md](glossary.md) | 프로젝트 용어집 (FA, Kill Switch, 레짐 등) | 용어 불명확 시 |
| [CODE_MAP.md](CODE_MAP.md) | 코드 경로 → 문서 역인덱스 | 코드 수정 후 |
| 루트 `CLAUDE.md` | 진입 규칙, 핵심 원칙, 배포 규칙 | Phase 전환, 서비스 배포 전 |
| [policies/operations/pubsub-channels.md](policies/operations/pubsub-channels.md) | Redis Pub/Sub 채널 & API | 서비스 간 통신 이해 |

---

## 문서 구조

```
docs/
├── README.md (여기) — 진입점, 빠른 참조
├── glossary.md — 용어집 (95+ 항목)
├── CODE_MAP.md — 코드 경로 역인덱스
│
├── architecture/
│   ├── system-overview.md — 전체 아키텍처, 19개 서비스
│   ├── data-flow.md — 시장 데이터부터 거래 실행까지
│   └── ...
│
├── policies/
│   ├── btc-only.md — BTC 단일 운영 원칙 (변동성, 신뢰성)
│   ├── kill-switch.md — 4단계 Kill Switch (STRATEGY/PORTFOLIO/SYSTEM/MANUAL)
│   ├── leverage-limits.md — 5x 초과 금지
│   ├── deployment-position.md — 배포 시 포지션 보호 (service_shutdown)
│   │
│   ├── operations/
│   │   ├── runbook.md — 기본 명령, 시작/중지, 장애 대응
│   │   ├── monitoring.md — Grafana, Telegram, 모니터링 지표
│   │   ├── pubsub-channels.md — Redis Pub/Sub 채널 카탈로그
│   │   ├── mainnet-switch.md — 메인넷 전환 9단계 + Phase 5 특수 설정
│   │   ├── deployment-procedure.md — Docker 배포, 빌드 절차
│   │   └── README.md — 운영 매뉴얼 인덱스
│   │
│   ├── strategies/
│   │   ├── funding-arb.md — 펀딩비 차익거래 (핵심 전략)
│   │   ├── adaptive-dca.md — Fear&Greed 기반 DCA (보조)
│   │   └── README.md — 전략 인덱스
│   │
│   └── README.md — 정책 인덱스
│
├── structure/
│   ├── services.md — 19개 서비스 상세
│   └── directory-tree.md — 전체 프로젝트 구조
│
└── backtest/docs/
    ├── README.md — 백테스트 문서 홈
    ├── CODE_MAP.md — 백테스트 코드 역인덱스
    │
    ├── methodology/
    │   ├── backtest-skillset.md — 스킬셋 관리 규칙 (필수)
    │   ├── jesse-engine.md — Jesse 프레임워크 설정
    │   ├── jesse-strategies.md — Jesse 전략 목록
    │   ├── jesse-vs-self-engine.md — 성과 비교
    │   └── live-postmortem-template.md — 거래 분석 템플릿
    │
    ├── strategies/
    │   ├── 001_funding_arb.md ~ 015_contrarian_sentiment.md — 15개 전략 사양
    │   └── README.md
    │
    ├── runs/
    │   └── *.md — 7개 전략별 실험 ledger (supertrend, momentum_ma, stoch 등)
    │
    ├── optimization/
    │   ├── param_sweep_v2.md — v2 파라미터 스윕 (2,688 backtests, 완료)
    │   ├── param_sweep_v2_results.md — v2 실행 리포트
    │   ├── param_sweep_v3.md — v3 세밀 격자 스윕 (supertrend 96 combo, 완료)
    │   ├── rolling_window_test.md — 롤링 윈도우 방법론
    │   └── rolling_window_results_v2.md — v2 champion 기준 롤링 결과
    │
    └── phase4-checklist.md — Phase 4 완료 기준
```

---

## 마지막 업데이트 & 상태

| 섹션 | 상태 | 마지막 업데이트 |
|------|------|-----------------|
| 운영 매뉴얼 (runbook) | ✅ 최신 | 2026-05-01 |
| Kill Switch | ✅ 최신 | 2026-04-12 |
| 모니터링 | ✅ 최신 | 2026-05-01 |
| 메인넷 전환 | ✅ 최신 | 2026-05-01 |
| 용어집 | 📝 확장 필요 | 2026-05-01 |
| Phase 4 체크리스트 | ✅ 활성 | 2026-04-12 |
| param_sweep v2 | ✅ 완료 | 2026-05-12 |
| param_sweep v3 | ⚠️ 부분 완료 | 2026-05-12 |
| 7개 전략 백테스트 MD | ✅ v2+v3 반영 | 2026-05-12 |
| V4 대시보드 | ✅ v2+v3 champion | 2026-05-12 |
