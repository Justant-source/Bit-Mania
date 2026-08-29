---
title: Architecture Decision Records
category: ADR
last_updated: 2026-08-29
---

# Architecture Decision Records (ADR)

CryptoEngine의 번복하기 어려운 아키텍처·정책·전략 결정을 시간순으로 기록합니다.

---

## ADR 인덱스

| # | 제목 | 상태 | 날짜 |
|---|------|------|------|
| [0001](0001-btc-only-policy.md) | BTC 단일 운영 정책 채택 | Accepted | 2026-05-01 |
| [0002](0002-multi-symbol-fa-review.md) | Multi-symbol FA 도입 검토 | Superseded by 0004 | 2026-05-02 |
| [0003](0003-supertrend-single-strategy.md) | Supertrend 단일 전략 채택 | Accepted | 2026-05-18 |
| [0004](0004-funding-arb-deprecated.md) | Funding Arbitrage 전략 폐기 | Accepted | 2026-05-18 |
| [0005](0005-adaptive-dca-deprecated.md) | Adaptive DCA 전략 폐기 | Accepted | 2026-05-18 |
| [0006](0006-db-migration-tracks.md) | Dual DB Migration Track 정리 (raw SQL 단일화, D4 구현 · D3 018 라이브 적용) | Accepted | 2026-08-29 |
| [0007](0007-portfolio-state-canonical.md) | PortfolioState 정본 통일 | Proposed | 2026-06-15 |
| [0008](0008-c4-ssot-adoption.md) | C4 SSOT 문서 구조 채택 | Accepted | 2026-06-15 |
| [0009](0009-legacy-strategy-retirement.md) | 레거시 전략 계보 일괄 폐기 (001~016·018~021 + Track-C) | Accepted | 2026-08-29 |

---

## 파일 명명 규칙

```
<4자리번호>-<kebab-en-slug>.md
```

**예시**: `0001-btc-only-policy.md`, `0009-next-decision.md`

번호는 단조 증가. 재사용 금지.

---

## Frontmatter 스키마

```yaml
---
title: [의사결정 제목]
adr_number: "0001"
status: [Accepted|Superseded by 0XXX|Deprecated|Proposed]
date: YYYY-MM-DD
related_code:
  - cryptoengine/path/to/file.py
---
```

---

## 상태 전환

| 상태 | 의미 |
|------|------|
| `Proposed` | 검토 중 |
| `Accepted` | 현재 적용 중 |
| `Superseded by 0XXX` | 새 ADR로 대체됨 |
| `Deprecated` | 더 이상 유효 안 함 |

번복 시: 기존 ADR status 업데이트 + 새 ADR 상호 참조.

---

## 필수 섹션

```markdown
## Status
## Context
## Decision
## Consequences
```
