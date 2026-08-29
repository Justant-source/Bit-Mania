---
title: C4 SSOT Docs Restructure Adoption
adr_number: "0008"
status: Accepted
date: 2026-06-15
related_code:
  - docs/_index.md
  - scripts/lint_docs.py
  - cryptoengine/Makefile
---

# ADR-0008. C4 SSOT 문서 구조 채택

## Status

Accepted (2026-06-15). **2026-08-29**: C4 트리·`lint-docs` 6항목 게이트 유지. 당일 대규모 운영 변경의 문서 정본은 [ADR-0010](0010-ops-cleanup-20260829.md) 및 각 계층 `last_updated: 2026-08-29`.

## Context

마이그레이션 이전 `docs/` 트리는 다음 문제를 가지고 있었다.

1. **계층 혼재**: `architecture/`, `policies/`, `structure/`, `env/` — 의미 기반 분류가 C4 줌 순서를 따르지 않아 "큰 그림 → 세부" 순서로 읽기 어려웠다.
2. **코드↔문서 드리프트**: `order:update` 14건(4파일) — 코드는 `order:result`/`order:result:{strategy_id}`를 사용.  
   `docs/policies/safety/leverage-limits.md`의 레버리지 한도 "5x" — 코드 하드캡 3x와 불일치.  
   Adaptive DCA "재활성화 검토 중" — 이미 폐기.
3. **자동 검증 부재**: 코드↔문서 동기화 강제 도구 없음.
4. **모노레포 3 트리 분산**: `cryptoengine/`, `backtest/`, `dashboard/` 문서가 각 트리에 흩어짐.

## Decision

WO-docs-c4-ssot 표준을 적용한다.

**사용자 결정 4항목 (불변)**:
1. **범위**: 3 트리(cryptoengine·backtest·dashboard) 통합 — 단일 `docs/`
2. **이동 방식**: 계층별 소수 파일로 압축 재구성 (단순 git mv 금지)
3. **트리거 맵**: `docs/_index.md` (`docs/CODE_MAP.md` 개명 및 흡수)
4. **자동 검증**: Python `scripts/lint_docs.py` 6항목 + `make lint-docs` 타깃

**새 구조** (C4 줌 순서):

| 계층 | 경로 | 내용 |
|---|---|---|
| 10 Context | `docs/10-context/system-context.md` | L1 외부 액터·3 서브시스템 |
| 20 Container | `docs/20-containers/containers.md` | 배포 단위·포트·볼륨·env |
| 30 Component | `docs/30-components/components.md` | 서비스 내부 모듈·클래스 |
| 40 Data | `docs/40-data/data-model.md` | PG 스키마·ER·마이그레이션 |
| 50 API | `docs/50-api/pubsub-catalog.md` | Redis pub/sub·Dashboard REST |
| 60 Runtime | `docs/60-runtime/state-machines.md` | OrderState·KillLevel·복구 |
| 70 Policy | `docs/70-policy/{safety,operations,strategy}.md` | 안전·운영·전략 |
| 90 ADR | `docs/90-adr/` | 아키텍처 결정 |

**압축**: 44+ mermaid 블록·36파일 → 11 다이어그램·12파일. 폐기된 33 다이어그램 목록:

<details>
<summary>폐기 다이어그램 목록 (33건)</summary>

기존 파일별로 보존 가치 없는 중복·드리프트·FA/DCA 폐기 관련 다이어그램 전량. 권위 선정 기준: ① 최신 last_updated ② code-ref 정확도 ③ 정보 밀도. 11개 권위 다이어그램이 전체 시스템 표현에 충분하다고 판단 (사용자 명시적 승인).

</details>

**드리프트 정정** (동시 수행):
- `order:update` 14건 → `order:result`/`order:result:{strategy_id}` (새 50-api 문서에서 정정)
- 레버리지 한도 5x → 3x (새 70-policy/safety 문서에서 정정)
- Adaptive DCA "재활성화 검토 중" 표현 제거 (glossary + strategy 문서)

**자동 검증** (`scripts/lint_docs.py` 6항목):
1. root-markdown-whitelist (루트 .md ⊆ {CLAUDE.md, README.md, AGENTS.md})
2. forbidden-diagram-dialects (비표준 다이어그램 방언 = 0)
3. mermaid-blocks-parseable (첫 줄 = 유효 다이어그램 타입)
4. index-trigger-targets (_index.md 링크 실재)
5. relative-links-resolvable (전 .md 상대 링크 깨짐 0)
6. mermaid-provenance-headers (last-verified + code-ref 헤더 필수)

**SSOT Doc-Sync 게이트** (`CLAUDE.md` §절대 규칙):
코드 변경 시 `docs/_index.md` 트리거 맵 조회 → 대응 문서 동시 갱신 → `make -C cryptoengine lint-docs` PASS → 위반 시 커밋 중단.

## Consequences

**긍정**:
- 결정론적 로드 순서: 10 → 20 → 30 → 40 → 50 → 60 → 70 → 90
- lint gate로 드리프트 구조적 차단
- 단일 `docs/` 트리 — 3 서브시스템 어디서든 동일 진입점

**부정/위험**:
- 33 다이어그램 영구 삭제 (근거는 이 ADR에 보존)
- `backtest/docs/methodology/` 5,224줄 → ~800줄 흡수 (정보 압축 손실)
- ADR-0006·0007은 결정만 기록, 코드 변경은 후속 PR

**후속 작업**:
- `.github/workflows/lint-docs.yml` CI gate (별도 PR)
- ADR-0006 코드 마이그레이션 (`chore/consolidate-db-migrations`)
- ADR-0007 코드 중복 제거 (`fix/portfolio-state-dedup`)
