---
title: docs — 문서 지도 & Doc-Sync 트리거 맵
last_updated: 2026-08-31
---

# docs/_index.md — 문서 지도 & Doc-Sync 트리거 맵

> **SSOT 해결 규칙**: 충돌 시 **코드(runtime) > 이 문서 > 다른 문서** 순으로 우선한다.
> 새 컨텍스트를 시작할 때 이 파일을 첫 번째로 읽는다.

## §1. 계층 인덱스 (대분류 × 계층)

| 계층 | `cryptoengine/` | `backtest/` | `dashboard/` | `shared/` |
|---|---|---|---|---|
| **10** context | — | — | — | `10-context.md` 🏛 |
| **20** containers | `20-containers.md` | `20-containers.md` | `20-containers.md` | `20-containers.md` 🏛 |
| **30** components | `30-components.md` | — | — | — |
| **40** data | `40-data.md` 🏛 | — | — | — |
| **50** api | `50-api.md` | — | `50-api.md` | — |
| **60** runtime | `60-runtime.md` | — | — | — |
| **70** policy | `70-policy/` (2) 🏛 | — | — | `70-policy.md` 🏛 |
| **90** adr | — | — | — | `90-adr/` (11) 🏛 |
| 계층 밖 | — | — | — | `glossary.md` |

경로 접두는 `docs/<대분류>/`. 🏛 = 그 주제의 전역 권위본. `—` = 해당 계층 문서 없음 (빈 껍데기를 만들지 않음).

이 저장소의 compose 파일은 `docs/` 를 볼륨 마운트하지 않는다. 런타임 자산 섹션은 두지 않는다.

## §2. 작업별 진입 문서

| 작업 | 1차 진입(이것만 읽기) | 2차(필요 시) | 실제 코드 확인 |
|---|---|---|---|
| 시스템 전체 그림 | `docs/shared/10-context.md` | `docs/shared/20-containers.md` | `cryptoengine/docker-compose.yml` |
| 배포 · 포트 · 네트워크 · env | `docs/cryptoengine/20-containers.md` | `docs/shared/20-containers.md` · `docs/shared/70-policy.md` | `cryptoengine/docker-compose.yml` · `cryptoengine/shared/required_env.py` |
| 서비스 내부 모듈 | `docs/cryptoengine/30-components.md` | — | `cryptoengine/services/**` · `cryptoengine/shared/**` |
| DB 스키마 · 마이그레이션 | `docs/cryptoengine/40-data.md` | `docs/shared/90-adr/0006-db-migration-tracks.md` | `cryptoengine/shared/db/**` |
| Redis 채널 | `docs/cryptoengine/50-api.md` | `docs/cryptoengine/60-runtime.md` | `cryptoengine/shared/kill_switch.py` · `cryptoengine/services/execution/engine.py` |
| Dashboard REST | `docs/dashboard/50-api.md` | `docs/dashboard/20-containers.md` | `dashboard/src/**` |
| 상태 전이 · 복구 | `docs/cryptoengine/60-runtime.md` | `docs/cryptoengine/70-policy/safety.md` | `cryptoengine/services/execution/order_manager.py` |
| Kill Switch · 레버리지 | `docs/cryptoengine/70-policy/safety.md` | `docs/cryptoengine/60-runtime.md` | `cryptoengine/shared/kill_switch.py` · `cryptoengine/config/orchestrator.yaml` |
| 운영 Runbook · 배포 | `docs/shared/70-policy.md` | `docs/cryptoengine/20-containers.md` | `cryptoengine/docker-compose.yml` |
| Supertrend 사양 · 파라미터 | `docs/cryptoengine/70-policy/strategy.md` | `docs/shared/glossary.md` | `cryptoengine/config/strategies/supertrend.yaml` |
| 아키텍처 결정 | `docs/shared/90-adr/README.md` | 해당 ADR | ADR `related_code` |
| 백테스트 Compose | `docs/backtest/20-containers.md` | `docs/cryptoengine/70-policy/strategy.md` | `backtest/docker/docker-compose.yml` |

## §4. 문서 권위 그래프

| 충돌 | 이긴 쪽 |
|---|---|
| 코드 vs 문서 | 코드 |
| 이 파일 vs 다른 문서 | 이 파일 (`docs/_index.md`) |
| `docs/cryptoengine/70-policy/strategy.md` vs `AGENTS.md` 파라미터 표 | `docs/cryptoengine/70-policy/strategy.md` |
| `docs/cryptoengine/70-policy/safety.md` vs `AGENTS.md` Kill Switch 임계값 | `docs/cryptoengine/70-policy/safety.md` |
| ADR vs 계층 문서 (현재 운영) | 계층 문서. ADR은 결정 기록 |

## §5. Doc-Sync 트리거 맵

코드 glob은 저장소 루트 기준 전체 경로. 등급 M=필수 · C=조건부.

| # | 코드 영역 (glob) | 갱신 대상 문서 | 등급 |
|---|---|---|---|
| 1 | `cryptoengine/shared/required_env.py` | `docs/cryptoengine/20-containers.md` · `docs/shared/70-policy.md` | M |
| 2 | `cryptoengine/docker-compose*.yml` | `docs/cryptoengine/20-containers.md` · `docs/shared/20-containers.md` · `README.md` | M |
| 3 | `dashboard/docker-compose.yml` | `docs/dashboard/20-containers.md` · `docs/shared/20-containers.md` | M |
| 4 | `backtest/docker/docker-compose.yml` | `docs/backtest/20-containers.md` · `docs/shared/20-containers.md` | M |
| 5 | `cryptoengine/shared/db/migrations/**` | `docs/cryptoengine/40-data.md` · `docs/shared/90-adr/0006-db-migration-tracks.md` | M |
| 6 | `cryptoengine/shared/db/init_schema.sql` | `docs/cryptoengine/40-data.md` | M |
| 7 | `cryptoengine/shared/db/sql_migrations.py` | `docs/cryptoengine/40-data.md` · `docs/shared/90-adr/0006-db-migration-tracks.md` | M |
| 8 | `cryptoengine/scripts/init_db.py` | `docs/cryptoengine/40-data.md` · `docs/shared/90-adr/0006-db-migration-tracks.md` | M |
| 9 | `cryptoengine/Makefile` | `docs/cryptoengine/40-data.md` · `docs/shared/90-adr/0006-db-migration-tracks.md` | C |
| 10 | `cryptoengine/shared/kill_switch.py` | `docs/cryptoengine/60-runtime.md` · `docs/cryptoengine/70-policy/safety.md` | M |
| 11 | `cryptoengine/services/execution/order_manager.py` | `docs/cryptoengine/60-runtime.md` | M |
| 12 | `cryptoengine/services/execution/**` | `docs/cryptoengine/50-api.md` · `docs/cryptoengine/60-runtime.md` | M |
| 13 | `cryptoengine/shared/redis_client.py` | `docs/cryptoengine/50-api.md` | M |
| 14 | `cryptoengine/services/strategies/supertrend/**` | `docs/cryptoengine/70-policy/strategy.md` · `docs/cryptoengine/60-runtime.md` | M |
| 15 | `cryptoengine/config/strategies/supertrend.yaml` | `docs/cryptoengine/70-policy/strategy.md` | M |
| 16 | `cryptoengine/config/orchestrator.yaml` | `docs/cryptoengine/70-policy/safety.md` | M |
| 17 | `.env` / `.env.example` | `docs/cryptoengine/20-containers.md` | C |
| 18 | `cryptoengine/services/*/Dockerfile` | `docs/cryptoengine/20-containers.md` | C |
| 19 | `cryptoengine/shared/models/**` | `docs/cryptoengine/30-components.md` · `docs/cryptoengine/50-api.md` | M |
| 20 | `cryptoengine/shared/exchange/**` | `docs/cryptoengine/30-components.md` | M |
| 21 | 포트 · env 변수 변경 | `docs/cryptoengine/20-containers.md` · `README.md` | M |
| 22 | `backtest/docs/**` | `docs/cryptoengine/70-policy/strategy.md` | C |
| 23 | `dashboard/src/**` | `docs/dashboard/20-containers.md` · `docs/dashboard/50-api.md` | M |

## §6. Code → Docs 역인덱스

| 코드 경로 접두 | 소유 모듈 | 먼저 읽을 문서 | 권위본 |
|---|---|---|---|
| `cryptoengine/services/strategies/supertrend/**` | cryptoengine | `docs/cryptoengine/70-policy/strategy.md` | 🏛 |
| `cryptoengine/services/strategies/base_strategy.py` | cryptoengine | `docs/cryptoengine/30-components.md` | |
| `cryptoengine/services/execution/**` | cryptoengine | `docs/cryptoengine/50-api.md` | `docs/cryptoengine/60-runtime.md` |
| `cryptoengine/services/orchestrator/**` | cryptoengine | `docs/cryptoengine/30-components.md` | |
| `cryptoengine/services/market-data/**` | cryptoengine | `docs/cryptoengine/20-containers.md` | `docs/cryptoengine/50-api.md` |
| `cryptoengine/services/telegram-bot/**` | cryptoengine | `docs/cryptoengine/70-policy/safety.md` | |
| `cryptoengine/shared/kill_switch.py` | cryptoengine | `docs/cryptoengine/70-policy/safety.md` | `docs/cryptoengine/60-runtime.md` |
| `cryptoengine/shared/exchange/**` | cryptoengine | `docs/cryptoengine/30-components.md` | |
| `cryptoengine/shared/models/**` | cryptoengine | `docs/cryptoengine/30-components.md` | `docs/cryptoengine/50-api.md` |
| `cryptoengine/shared/required_env.py` | cryptoengine | `docs/cryptoengine/20-containers.md` | `docs/shared/90-adr/0010-ops-cleanup-20260829.md` |
| `cryptoengine/shared/db/**` | cryptoengine | `docs/cryptoengine/40-data.md` | `docs/shared/90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/scripts/init_db.py` | cryptoengine | `docs/cryptoengine/40-data.md` | `docs/shared/90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/config/orchestrator.yaml` | cryptoengine | `docs/cryptoengine/70-policy/safety.md` | |
| `cryptoengine/config/strategies/supertrend.yaml` | cryptoengine | `docs/cryptoengine/70-policy/strategy.md` | |
| `cryptoengine/docker-compose.yml` | cryptoengine | `docs/cryptoengine/20-containers.md` | `docs/shared/20-containers.md` |
| `dashboard/**` | dashboard | `docs/dashboard/20-containers.md` | `docs/dashboard/50-api.md` |
| `backtest/**` | backtest | `docs/backtest/20-containers.md` | `docs/cryptoengine/70-policy/strategy.md` |
| `scripts/lint_docs.py` | shared | 이 파일. 검사 4·5·10이 링크·docs 백틱 경로·트리거 맵 glob·ADR `related_code` 실재를 본다. glob은 와일드카드 앞 접두사만 확인하고, mermaid 의미 일치는 `last-verified`에 맡긴다. 삭제 기록 ADR은 `allow_missing_refs: true`. | |
