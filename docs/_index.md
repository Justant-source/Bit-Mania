---
title: docs — 문서 지도 & Doc-Sync 트리거 맵
last_updated: 2026-08-29
---

# docs/_index.md — 문서 지도 & Doc-Sync 트리거 맵

> **SSOT 해결 규칙**: 충돌 시 **코드(runtime) > 이 문서 > 다른 문서** 순으로 우선한다.  
> Claude/사람이 새 컨텍스트를 시작할 때 이 파일을 첫 번째로 읽는다.

---

## 계층 인덱스

| 계층 | 경로 | 내용 | last-verified |
|---|---|---|---|
| 10 Context | [10-context/system-context.md](10-context/system-context.md) | L1 외부 액터 · 3 서브시스템 경계 | 2026-08-29 |
| 20 Container | [20-containers/containers.md](20-containers/containers.md) | 배포 단위 · 포트 · 네트워크 · 볼륨 · env-vars | 2026-08-29 |
| 30 Component | [30-components/components.md](30-components/components.md) | 서비스 내부 모듈 · 클래스 책임 · shared/ | 2026-08-29 |
| 40 Data | [40-data/data-model.md](40-data/data-model.md) | PG 스키마 · ER · 마이그레이션 트랙 | 2026-08-31 |
| 50 API | [50-api/pubsub-catalog.md](50-api/pubsub-catalog.md) | Redis pub/sub 채널 카탈로그 · Dashboard REST | 2026-08-29 |
| 60 Runtime | [60-runtime/state-machines.md](60-runtime/state-machines.md) | OrderState · KillLevel · service_shutdown 복구 · 외부 KS 수신 | 2026-08-29 |
| 70 Policy (안전) | [70-policy/safety.md](70-policy/safety.md) | Kill Switch · 레버리지 · BTC-only · 긴급 청산 | 2026-08-29 |
| 70 Policy (운영) | [70-policy/operations.md](70-policy/operations.md) | Runbook · 모니터링 · 메인넷 전환 · 배포 절차 | 2026-08-31 |
| 70 Policy (전략) | [70-policy/strategy.md](70-policy/strategy.md) | Supertrend SSOT · 백테스트 방법론 | 2026-08-31 |
| 90 ADR | [90-adr/README.md](90-adr/README.md) | 아키텍처 의사결정 인덱스 | 2026-08-29 |
| 용어집 | [glossary.md](glossary.md) | 도메인 용어 정의 | 2026-08-31 |

---

## 작업별 진입 문서

| 작업 | 진입 문서 |
|---|---|
| 시스템 전체 그림 파악 | `docs/10-context/system-context.md` |
| 배포 · 포트 · 네트워크 · 환경변수 | `docs/20-containers/containers.md` |
| 서비스 내부 모듈 · 클래스 구조 | `docs/30-components/components.md` |
| DB 스키마 · 마이그레이션 | `docs/40-data/data-model.md` |
| Redis 채널 · Dashboard REST API | `docs/50-api/pubsub-catalog.md` |
| 상태 전이 · 복구 플로우 | `docs/60-runtime/state-machines.md` |
| Kill Switch · 레버리지 · 포지션 보호 정책 | `docs/70-policy/safety.md` |
| 운영 Runbook · 배포 · 모니터링 | `docs/70-policy/operations.md` |
| Supertrend 전략 사양 · 파라미터 | `docs/70-policy/strategy.md` |
| 아키텍처 의사결정 배경 | `docs/90-adr/README.md` |

---

## Doc-Sync 트리거 맵 (코드 변경 → 갱신 대상 문서)

> §SSOT Doc-Sync 게이트(CLAUDE.md §절대 규칙)가 이 표를 조회한다. 코드 경로가 glob에 매치되면 대응 문서를 **같은 커밋**에서 갱신한다.

| 코드 영역 (glob) | 갱신 대상 문서 |
|---|---|
| `cryptoengine/shared/required_env.py` | `20-containers/containers.md` §환경변수, `70-policy/operations.md` |
| `cryptoengine/docker-compose*.yml` | `20-containers/containers.md` (포트/서비스/볼륨 표), `README.md` |
| `dashboard/docker-compose.yml` | `20-containers/containers.md` §Dashboard |
| `backtest/docker/docker-compose.yml` | `20-containers/containers.md` §Backtest |
| `cryptoengine/shared/db/migrations/**` | `40-data/data-model.md` (ER + 마이그레이션 표), `90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/shared/db/init_schema.sql` | `40-data/data-model.md` |
| `cryptoengine/shared/db/sql_migrations.py` | `40-data/data-model.md`, `90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/scripts/init_db.py` | `40-data/data-model.md`, `90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/Makefile` | `40-data/data-model.md` (`migrate` 타깃), `90-adr/0006-db-migration-tracks.md` |
| `cryptoengine/shared/kill_switch.py` | `60-runtime/state-machines.md`, `70-policy/safety.md` |
| `cryptoengine/services/execution/order_manager.py` | `60-runtime/state-machines.md` (OrderState) |
| `cryptoengine/services/execution/**` | `50-api/pubsub-catalog.md`, `60-runtime/state-machines.md` |
| `cryptoengine/shared/redis_client.py` | `50-api/pubsub-catalog.md` |
| `cryptoengine/services/strategies/supertrend/**` | `70-policy/strategy.md`, `60-runtime/state-machines.md` |
| `cryptoengine/config/strategies/supertrend.yaml` | `70-policy/strategy.md` §파라미터 |
| `cryptoengine/config/orchestrator.yaml` | `70-policy/safety.md` §Kill Switch 임계값 |
| `.env` / `.env.example` | `20-containers/containers.md` §환경변수 |
| `cryptoengine/services/*/Dockerfile` | `20-containers/containers.md` §빌드 패턴 |
| `cryptoengine/shared/models/**` | `30-components/components.md`, `50-api/pubsub-catalog.md` |
| `cryptoengine/shared/exchange/**` | `30-components/components.md` §shared |
| 포트 · env 변수 변경 | `20-containers/containers.md`, `README.md` |
| `backtest/docs/**` | `70-policy/strategy.md` §백테스트 방법론 |
| `dashboard/src/**` | `20-containers/containers.md` §Dashboard, `50-api/pubsub-catalog.md` §Dashboard REST |

---

## Code → Docs 역인덱스

| 코드 경로 | 권위 문서 |
|---|---|
| `cryptoengine/services/strategies/supertrend/**` | [70-policy/strategy.md](70-policy/strategy.md) |
| `cryptoengine/services/strategies/base_strategy.py` | [30-components/components.md](30-components/components.md) |
| `cryptoengine/services/execution/**` | [50-api/pubsub-catalog.md](50-api/pubsub-catalog.md) · [60-runtime/state-machines.md](60-runtime/state-machines.md) |
| `cryptoengine/services/orchestrator/**` | [30-components/components.md](30-components/components.md) |
| `cryptoengine/services/market-data/**` | [20-containers/containers.md](20-containers/containers.md) · [50-api/pubsub-catalog.md](50-api/pubsub-catalog.md) |
| `cryptoengine/services/telegram-bot/**` | [70-policy/safety.md](70-policy/safety.md) §비상 청산 |
| `cryptoengine/shared/kill_switch.py` | [70-policy/safety.md](70-policy/safety.md) · [60-runtime/state-machines.md](60-runtime/state-machines.md) |
| `cryptoengine/shared/exchange/**` | [30-components/components.md](30-components/components.md) |
| `cryptoengine/shared/models/**` | [30-components/components.md](30-components/components.md) · [50-api/pubsub-catalog.md](50-api/pubsub-catalog.md) |
| `cryptoengine/shared/required_env.py` | [20-containers/containers.md](20-containers/containers.md) · [90-adr/0010-ops-cleanup-20260829.md](90-adr/0010-ops-cleanup-20260829.md) |
| `cryptoengine/shared/db/**` | [40-data/data-model.md](40-data/data-model.md) · [90-adr/0006-db-migration-tracks.md](90-adr/0006-db-migration-tracks.md) |
| `cryptoengine/scripts/init_db.py` | [40-data/data-model.md](40-data/data-model.md) · [90-adr/0006-db-migration-tracks.md](90-adr/0006-db-migration-tracks.md) |
| `cryptoengine/config/orchestrator.yaml` | [70-policy/safety.md](70-policy/safety.md) §Kill Switch 임계값 |
| `cryptoengine/config/strategies/supertrend.yaml` | [70-policy/strategy.md](70-policy/strategy.md) §파라미터 |
| `docker-compose.yml` | [20-containers/containers.md](20-containers/containers.md) |
| `dashboard/**` | [20-containers/containers.md](20-containers/containers.md) §Dashboard |
| `backtest/**` | [70-policy/strategy.md](70-policy/strategy.md) §백테스트 |
| `scripts/lint_docs.py` | 이 파일(`_index.md`)의 마크다운 링크, 백틱으로 적은 docs 트리의 마크다운 경로, 트리거 맵/역인덱스의 코드 glob, ADR `related_code`가 실재하는지까지 검증한다(검사 4·5·10). 남은 한계: glob은 와일드카드 앞 접두사 존재만 확인하고, mermaid가 코드와 의미적으로 일치하는지는 사람이 `last-verified`로 보증한다. 삭제 기록 ADR은 `allow_missing_refs: true`로 부재 경로를 남긴다. |
