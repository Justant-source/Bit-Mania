---
title: Architecture Decision Records
category: ADR
last_updated: 2026-05-01
---

# Architecture Decision Records (ADR)

CryptoEngine의 번복하기 어려운 아키텍처, 정책, 전략 결정을 시간순으로 기록합니다.

---

## ADR 개요

### 목적

- 중대 결정의 **근거**와 **맥락** 보존
- 미래 팀원이 **왜 이렇게 했는지** 이해
- 번복(Supersede) 시 **이전 결정** 추적

### 파일 명명 규칙

```
<3자리번호>. <주요 결정내용>_<YYYY-MM-DD>.md
```

**예시**:
- `001. BTC 단일 운영 정책_2026-05-01.md`
- `002. Kill Switch 절대값 기준 전환_2026-06-15.md`
- `003. Jesse 프레임워크 도입_2026-07-01.md`

### 번호 규칙

- **단조 증가**: 새 ADR마다 번호 증가
- **재사용 금지**: 삭제된 ADR의 번호는 재사용 안 함
- **시간순 정렬**: 번호 순서 = 의사결정 순서

### 상태 관리

| 상태 | 의미 | 전환 |
|-----|------|------|
| `Accepted` | 현재 적용 중 | → Superseded 또는 Deprecated |
| `Superseded by ADR-XXX` | 새 ADR이 대체 | (과거 결정) |
| `Deprecated` | 더 이상 유효 안 함 | (결정 번복) |
| `Proposed` | 검토 중 | → Accepted |

**번복 시**:
1. 이전 ADR의 `status: "Superseded by ADR-XXX"` 변경
2. 새 ADR 작성
3. 링크 상호 참조

### Frontmatter 스키마

모든 ADR은 다음 frontmatter 포함:

```yaml
---
title: [의사결정 제목]
adr_number: [003]
status: [Accepted|Superseded by ADR-XXX|Deprecated]
date: [YYYY-MM-DD]
related_code:
  - [코드 경로 1]
  - [코드 경로 2]
---
```

---

## ADR 인덱스

### 현재 ADR 목록

| # | 제목 | 상태 | 날짜 | 요약 |
|---|-----|------|------|------|
| 001 | BTC 단일 운영 정책 | Accepted | 2026-05-01 | Multi-symbol 거래 금지, BTCUSDT만 사용 |

### 계획 중 ADR

(향후 의사결정에 따라 추가)

---

## ADR 작성 가이드

### 필수 섹션

```markdown
# ADR-[000]. [제목]

## Status
[Accepted|Proposed|Superseded]

## Context
- 문제점
- 현재 상황
- 압박 요인

## Decision
명확한 결정문 (한 문장 또는 짧은 단락)

## Consequences

### Positive (긍정적)
- 이점 1
- 이점 2

### Negative (부정적)
- 위험 1
- 위험 2

## Rejected Alternatives
- 대안 A: 이유
- 대안 B: 이유

## Enforcement
실제 코드에서의 강제 방법:
- 설정 파일: xxx.yaml
- 코드 검증: yyy.py
- CI/CD 규칙: zzz

## References
- 관련 문서 링크
- 코드 경로
```

---

## ADR 검토 프로세스

### 1. 제안 (Proposed)

```bash
# 1. 파일 생성 (draft)
git checkout -b adr/002-new-decision

# 2. 작성 및 commit
git add docs/ADR/002._...md
git commit -m "ADR-002: [제목]"
```

### 2. 토론 (Review)

- Team review 진행
- 근거 타당성 검증
- 대안 충분히 검토됨 확인

### 3. 채택 (Accepted)

```bash
# status: Proposed → Accepted
git commit -m "ADR-002: Accept"
git push origin adr/002-new-decision
```

### 4. 병합 (Merge)

```bash
git pull origin main
git merge adr/002-new-decision
```

---

## 번복 (Supersede) 프로세스

### 상황

기존 결정 ADR-001을 새 결정 ADR-002가 대체할 때:

### Step 1: 이전 ADR 업데이트

```yaml
# docs/ADR/001. BTC 단일 운영 정책_2026-05-01.md

status: Superseded by ADR-002
```

### Step 2: 새 ADR 작성

```yaml
# docs/ADR/002. Multi-symbol 재도입_2026-06-01.md

status: Accepted
supersedes: ADR-001
```

### Step 3: 교차 참조

새 ADR 본문에:
```markdown
## Supersedes

Replaces [ADR-001. BTC 단일 운영 정책](./001.%20BTC%20단일%20운영%20정책_2026-05-01.md)

**이유**: 2026년 6월 시장 상황 변화로 ETH 펀딩비 회복
```

---

## 자주하는 실수

### ❌ ADR 미작성

중대 결정을 내렸지만 ADR 미기록:
```bash
# 1개월 후...
"왜 이렇게 한 거지?" → 맥락 손실
```

### ❌ Status 미업데이트

기존 ADR의 상태를 Accepted로 두고 새 결정 추진:
```bash
# 결과: 혼란스러운 문서 + 구현 불일치
```

### ❌ 번호 재사용

삭제된 ADR-002 번호를 나중에 재사용:
```bash
# 결과: git history에서 중복 번호 + 혼동
```

---

## 팁

### 1. 주기적 리뷰

**월간**: 최근 ADR 검토
```bash
# 최근 3개월 ADR 확인
git log --follow --oneline docs/ADR/ | head -20
```

### 2. 링크 검증

ADR 간 참조가 정확한지 확인:
```bash
# 모든 링크 유효성 검사
grep -r "ADR-" docs/ADR/ | grep -o "ADR-[0-9]\+" | sort -u
```

### 3. 코드 일관성

ADR에서 명시한 Enforcement를 코드 검토 시 확인:
```bash
# 예: BTC 단일 운영 정책
grep -r "BTCUSDT" cryptoengine/config/ | wc -l
# 기대: funding-arb.yaml, adaptive-dca.yaml 2개 파일
```

---

**최종 수정**: 2026-05-01
