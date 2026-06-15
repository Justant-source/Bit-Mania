---
title: Dual DB Migration Track Consolidation
adr_number: "0006"
status: Proposed
date: 2026-06-15
related_code:
  - cryptoengine/shared/db/migrations/versions/
  - cryptoengine/shared/db/migrations/003_asset_report.py
  - cryptoengine/shared/db/init_schema.sql
---

# ADR-0006. Dual DB Migration Track — 정리 결정

## Status

Proposed (2026-06-15)

## Context

`cryptoengine/shared/db/migrations/` 아래에 두 가지 마이그레이션 트랙이 공존한다.

**Alembic 트랙** (`versions/*.py`): Python 코드로 작성된 순방향/역방향 마이그레이션.  
**수동 SQL 트랙** (루트 `.sql` 파일): 번호 프리픽스(`001_`, `002_`, …)를 붙인 순수 SQL 파일.

이 구조에서 **003 번호 충돌**이 발생했다.

- `versions/003_asset_report.py` — Alembic 마이그레이션
- `003_service_logs.sql` — 수동 SQL 스크립트

두 트랙을 동시에 운영하면 번호 순서 보장이 불가능하고, 운영자가 어느 트랙이 현재 DB 상태를 반영하는지 판단하기 어렵다. `init_schema.sql`도 전체 스키마를 별도로 선언하고 있어 Alembic 버전 히스토리와 일치 여부를 수동 검증해야 한다.

## Decision

Alembic 단일 트랙을 SSOT로 채택한다. 구체적으로:

1. 수동 SQL 파일 (`001_*.sql`, `002_*.sql`, `003_service_logs.sql` 등)을 Alembic `versions/` 아래 동등한 Python 마이그레이션으로 흡수한다.
2. 흡수 완료 후 루트 `.sql` 파일을 아카이브(`migrations/archive/`)로 이동한다.
3. `init_schema.sql`은 `alembic upgrade head` 실행으로 대체하고, 신규 환경 부트스트랩 절차를 `docs/70-policy/operations.md`에 명시한다.
4. 번호 계획을 Alembic 단독으로 관리한다 (자동 revision ID, 사람이 읽기 쉬운 slug suffix 사용).

본 ADR은 **결정만 기록**한다. 실제 코드 마이그레이션은 후속 PR에서 수행한다.

## Consequences

**긍정**:
- 단일 도구(Alembic)로 마이그레이션 히스토리 추적
- `alembic history` / `alembic current`로 DB 상태 즉시 확인
- 번호 충돌 구조적 제거

**부정/위험**:
- 수동 SQL → Python 변환 작업 필요 (예상 1-2일)
- 기존 SQL 파일을 참조하는 운영 스크립트 업데이트 필요
- 변환 과정에서 마이그레이션 순서 재정렬 필요 (002/003 내용 검증)

**후속 작업**: `chore/consolidate-db-migrations` PR에서 코드 변경 수행.
