# 프로젝트 정리 최종 리포트

> 작성일: 2026-04-12
> 기준 커밋: `aac664a`

---

## 1. 삭제 결과

### 추적 파일 (git 반영)

| 파일 | 분류 | 삭제 라인 | 사유 |
|------|------|-----------|------|
| `cryptoengine/shared/risk.py` | Dead code | 151줄 | 프로덕션 미사용, 테스트만 참조 |
| `cryptoengine/tests/unit/test_risk.py` | 연동 테스트 | 238줄 | risk.py 삭제로 함께 제거 |
| `cryptoengine/tests/backtest/bt_funding_arb.py` | 구 백테스트 | 228줄 | FreqtradeBridge 기반, Jesse로 대체 |
| `cryptoengine/services/jesse_engine/storage/json/*.json` (16개) | 실행 아티팩트 | — | 과거 백테스트 원시 결과, storage/results/ 에 정리본 보존 |

**총 삭제 라인 (추적 파일):** 617줄

### 비추적 파일 (git 미포함, 직접 삭제)

| 경로 | 크기 | 사유 |
|------|------|------|
| `.result/backtest_v4/` | ~200KB | v4 결과 문서, V5/Jesse 채택으로 불필요 |
| `.request/system/` | ~248KB | Grafana/LLM/로깅 설계 문서, 구현 완료 |
| `.result/phase3/12_aggressive_fa_report.md` | — | CLAUDE.md 참조 제거 후 삭제 |
| `cryptoengine/tests/backtest/` | — | 빈 디렉토리 |
| `cryptoengine/services/backtester/` | — | 소스 0개, __pycache__ 5개 잔재 (Docker root 소유) |

**비추적 파일 총 삭제:** ~450KB

---

## 2. CLAUDE.md 압축 결과

| 항목 | 이전 | 이후 |
|------|------|------|
| 줄 수 | 359줄 | 275줄 |
| 축소율 | — | **23%** |
| 현재 진행 상태 섹션 | ~50줄 (완료 이력 나열) | 3줄 요약 + 아카이브 링크 |
| 백테스트 경로 | `services/backtester/tests/backtest/` (오류) | `services/jesse_engine/scripts/` (정정) |
| risk.py 구조도 항목 | 포함 | 제거 |
| Jesse TODO 섹션 | 15줄 | 아카이브로 이동 |

**아카이브 생성:** `cryptoengine/docs/archive/CLAUDE_history.md` (138줄)
- Phase 0~14 완료 이력
- 백테스트 v2 재건 Track A/B/C/D 상세
- Phase 14 이후 남은 TODO

---

## 3. 커밋 이력

| 커밋 | 내용 |
|------|------|
| `ff60c7c` | Phase 3-1: risk.py, test_risk.py, bt_funding_arb.py 삭제 + CLAUDE.md 참조 수정 |
| `4294c74` | Phase 4: CLAUDE.md 압축 + archive 생성 |
| `aac664a` | Jesse UUID JSON 16개 삭제 + CLAUDE.md README 참조 수정 |

---

## 4. 발견된 추가 정보

### config_loader.py 보존 사유
`shared/config_loader.py`는 직접 import가 0건이지만 `shared/__init__.py`를 통해
`funding-arb/main.py`, `adaptive-dca/main.py` 2개 서비스에서 간접 사용 중.
**삭제 불가.**

### services/backtester/ 완전 빈 상태 확인
Phase 13 마이그레이션 이후 소스 코드 전량 삭제됐으나 디렉토리 잔재.
Docker __pycache__(root 소유)만 존재 확인 후 삭제 완료.

---

## 5. 남은 기술 부채

| 항목 | 우선순위 | 설명 |
|------|---------|------|
| `jesse_engine/scripts/README.md` 부재 | 낮음 | 스킬 인덱스 파일 없음. 스크립트 추가 시 함께 생성 권장 |
| `config_loader.py` 직접 접근화 | 낮음 | `__init__.py` 경유 간접 사용 → 필요 시 직접 import 구조로 개선 가능 |
| `tests/unit/` 커버리지 | 중간 | risk.py·bt_funding_arb 삭제 후 단위 테스트 대상 재점검 필요 |

---

## 6. 재발 방지 권장사항

1. **백테스트 결과물 gitignore**: `services/jesse_engine/storage/json/` 를 `.gitignore`에 추가 (UUID 파일 재축적 방지)
2. **__pycache__ gitignore**: `**/__pycache__/` 이미 gitignore 등록 여부 확인 — Docker 볼륨이 root 소유로 생성하는 경우 `.dockerignore`에도 추가
3. **주기적 dead code 점검**: 분기 1회 `grep -r "from shared" --include="*.py"` 로 미사용 모듈 탐지
