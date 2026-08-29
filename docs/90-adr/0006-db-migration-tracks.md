---
title: Dual DB Migration Track Consolidation
adr_number: "0006"
status: Accepted
date: 2026-08-29
related_code:
  - cryptoengine/shared/db/migrations/versions/
  - cryptoengine/shared/db/migrations/003_asset_report.py
  - cryptoengine/shared/db/init_schema.sql
  - cryptoengine/scripts/init_db.py
---

# ADR-0006. Dual DB Migration Track — 정리 결정

## Status

Accepted (2026-08-29) — **결정만 확정, 구현은 미착수(다음 세션으로 지연)**.
원안(2026-06-15, Proposed)에서는 Alembic을 SSOT로 채택하는 방향이었으나, 2026-08-29
레거시 정리 작업(Q18)에서 실측 결과를 근거로 **반대 방향(raw SQL 단일 트랙, Alembic
제거)으로 결론을 뒤집어 Accepted로 확정**했다. 아래 Decision 절 참조.

## Context

`cryptoengine/shared/db/migrations/` 아래에 두 가지 마이그레이션 트랙이 공존한다.

**Alembic 트랙** (`versions/*.py`): Python 코드로 작성된 순방향/역방향 마이그레이션.  
**수동 SQL 트랙** (루트 `.sql` 파일): 번호 프리픽스(`001_`, `002_`, …)를 붙인 순수 SQL 파일.

이 구조에서 **003 번호 충돌**이 발생했다.

- `versions/003_asset_report.py` — Alembic 마이그레이션
- `003_service_logs.sql` — 수동 SQL 스크립트

두 트랙을 동시에 운영하면 번호 순서 보장이 불가능하고, 운영자가 어느 트랙이 현재 DB 상태를 반영하는지 판단하기 어렵다. `init_schema.sql`도 전체 스키마를 별도로 선언하고 있어 Alembic 버전 히스토리와 일치 여부를 수동 검증해야 한다.

### 2026-08-29 재검토 — 원안(Alembic SSOT)을 폐기하는 근거

2026-06-15 원안은 "Alembic을 SSOT로 채택"이었다. 그러나 2026-08-29 레거시 정리 작업에서
실제 두 트랙의 진행 상태를 재확인한 결과, 원안의 전제가 현실과 맞지 않음이 드러났다.

**실측 (`cryptoengine/shared/db/migrations/` 확인, 2026-08-29)**:

- **Alembic 트랙** (`versions/*.py`): `001_initial_schema` → `002_llm_reports` →
  `003_asset_report` → `004_regime_dashboard` → `007_quarterly_futures`
  (`down_revision = "004_regime_dashboard"`)에서 **정지**. 005/006 리비전은 존재하지
  않고, 007 이후로도 더 진행되지 않았다.
- **raw SQL 트랙** (루트 `*.sql`): `003_service_logs.sql`부터 `017_drop_ohlcv_1m_longterm.sql`까지
  **계속 이어져 실제 운영 스키마를 반영**하고 있다 (005 ETF flow, 007 quarterly futures,
  008 liquidations, 009 onchain metrics, 010 macro indicators, 011 ohlcv_1m_longterm,
  012 quarterly_perp_spread, 013 multi_exchange, 014 dashboard indexes, 015 unique
  제약, 016 supertrend_signals, 017 drop 등).

즉 **Alembic이 SSOT가 아니라 raw SQL이 이미 사실상의 SSOT였다.** `cryptoengine/scripts/init_db.py`는
`alembic upgrade head`를 호출해 신규 환경을 부트스트랩하는데, Alembic 체인이 007에서
멈춰 있으므로 **이 명령은 008~017에서 도입된 테이블(quarterly futures 이후 전체,
supertrend_signals 포함)을 전혀 만들지 않는다.** 신규 환경(재해복구, 신규 개발자 셋업,
CI 등)에서 `init_db.py`를 실행하면 실제 운영 스키마와 다른, 불완전한 DB가 생성되는
**잠재적 파손(latent break)**이 방치되어 있었다.

## Decision

**원안을 뒤집는다. raw SQL을 단일 마이그레이션 트랙의 SSOT로 채택하고, Alembic을
제거한다.**

근거: Alembic 체인이 `007_quarterly_futures`에서 정지된 반면 raw SQL 마이그레이션은
`017`까지 계속되어 실제 스키마를 반영해왔다. 이미 죽어 있는 트랙(Alembic)을 살리기
위해 10개 리비전(008~017)을 소급 작성하는 것보다, 이미 살아 움직이는 트랙(raw SQL)을
유일한 SSOT로 공식화하는 편이 낮은 리스크로 같은 목표(단일 트랙, 번호 충돌 제거)를
달성한다.

구체적으로 (구현은 아래 "구현 상태" 절 참조):

1. `shared/db/migrations/versions/` (Alembic 리비전 5개) + `alembic.ini`, `env.py`,
   `script.py.mako` 제거.
2. `cryptoengine/scripts/init_db.py`를 `alembic upgrade head` 호출 대신 raw SQL
   파일을 번호 순으로 순차 적용하는 방식으로 변경.
3. 신규 마이그레이션 `018_drop_legacy_tables.sql` 작성 — 이번 레거시 정리에서 DROP
   대상이 된 레거시 테이블 정리 문 수록 (별도 작업 D2/D3 범위).
4. `shared/db/init_schema.sql`에서 레거시 테이블 정의 제거, raw SQL 트랙과 정합화.
5. `Makefile`의 `migrate` 타깃을 raw SQL 순차 적용 기준으로 갱신.

## 구현 상태 — DEFERRED (미착수)

**본 ADR은 결정만 Accepted이며, 구현은 오늘(2026-08-29) 착수하지 않는다.**

사유: 구현(Alembic 트랙 제거, `init_db.py` 로직 변경, `018_drop_legacy_tables.sql`
작성 및 적용)은 **컨테이너 재시작·DB 마이그레이션 실행을 필요로 하는 작업**이며,
오늘의 정리 범위는 "무중단 작업만"으로 한정되어 있다(`.request/legacy-cleanup-plan-20260829.md`
Q14: "오늘 = 무중단 작업만. 재빌드·DB 수술은 분리"). 실행 계획은
`.request/legacy-cleanup-deferred-20260829.md`의 **작업 D4 — 마이그레이션 트랙
단일화 (Q18 / ADR-0006)**로 인수인계되어 **다음 세션에서 실행**된다(Q19). 실행
시점은 Q20에 따라 봉 마감 직후(포지션 청산 여부와 무관하게, 다음 평가까지 3시간
50분의 여유가 있는 시점)로 계획되어 있다.

**정직하게 남기는 갭**: 본 ADR이 Accepted로 전환된 지금부터 D4가 실제로 실행되기
전까지, `cryptoengine/scripts/init_db.py`는 여전히 (이제는 공식적으로 폐기 결정된)
`alembic upgrade head`를 호출하는 코드 그대로 남아 있다. 즉 **"결정은 내려졌지만
코드는 아직 옛 상태"**인 과도기이며, 이 기간 동안 신규 환경 부트스트랩을 시도하면
위에서 서술한 잠재적 파손이 여전히 발생한다. D4 완료 전까지는 신규 환경 구축 시
raw SQL 마이그레이션(`001`~`017`)을 수동으로 순서대로 적용해야 한다.

## Consequences

**긍정**:
- 이미 실제 스키마를 반영해온 트랙(raw SQL)을 공식 SSOT로 인정 — 문서와 현실의 괴리 해소
- `init_db.py` 수정 후에는 `alembic upgrade head`의 불완전 재현 문제(007에서 멈춤)가 원천 해소
- 번호 충돌 구조적 제거 (raw SQL 트랙 단일 넘버링)
- Alembic 의존성(별도 CLI, 버전 관리 개념) 제거로 운영 단순화

**부정/위험**:
- Alembic이 제공하던 프로그래매틱 downgrade(역방향 마이그레이션) 기능 상실 — raw SQL
  트랙은 순방향 적용만 지원. 롤백이 필요하면 수동 역-SQL 작성 필요
- D4 실행 전까지 위 "구현 상태" 절의 갭이 존재 — 신규 환경 구축 시 수동 개입 필요
- `018_drop_legacy_tables.sql` 작성 시 DROP 대상 테이블 실사용 여부 재검증 필요 (D2/D3와 조율)

**후속 작업**: `.request/legacy-cleanup-deferred-20260829.md` 작업 D4에서 코드 변경
수행 (다음 세션, 컨테이너 재시작 가능한 시점).
