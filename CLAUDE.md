# CryptoEngine — Claude Code 작업 가이드

## 프로젝트 개요

비트코인 선물 자동매매 시스템. 펀딩비 차익거래 핵심 전략, BTC 단일 운영. Docker Compose 기반, 테스트넷 → 메인넷 소액 투자 준비.

## 현재 진행 상태

- **Phase 4 진행 중**: 테스트넷 포워드 테스트 (fa80_lev5_r30, CAGR +34.87%)
- **Phase 5 준비**: 메인넷 소액 전환 대기 ($200 USDT, `switch_to_mainnet.py`)

## 핵심 원칙

1. **생존 우선**: 수익보다 포지션 보호. Kill Switch 4단계 계층 유지
2. **테스트넷 우선**: `BYBIT_TESTNET=true` 확인 후 작업
3. **단계별 검증**: Phase 3 → 4 → 5 순서, 절대 건너뛰지 않음
4. **BTC 단일 운영**: 다중 심볼 금지 (변동성/신뢰)
5. **포지션 보호**: 배포 시에도 청산하지 않음 (service_shutdown → Redis 복구)

## 문서 관리 규칙 ★ 필수

**모든 프로젝트 문서는 `cryptoengine/docs/` 단일 트리에서 관리한다.**

새 컨텍스트 진입 시: `cryptoengine/docs/README.md` (Map of Content)를 먼저 읽는다.

### 코드 ↔ 문서 동기화
코드 수정 시 같은 커밋에서 관련 문서를 업데이트한다:
1. `cryptoengine/docs/CODE_MAP.md` 역인덱스에서 변경 코드 경로 검색
2. 해당 doc의 `last_updated` frontmatter 갱신
3. 신규 영역은 같은 커밋에서 doc 신규 작성 + CODE_MAP.md 행 추가

문서 미업데이트는 작업 미완으로 간주한다.

### 참고 문서
- `cryptoengine/docs/README.md` — MOC (시작점)
- `cryptoengine/docs/CODE_MAP.md` — 코드 경로 → 문서 역인덱스
- `cryptoengine/docs/glossary.md` — 용어집
- `cryptoengine/docs/architecture/` — 시스템 설계
- `cryptoengine/docs/policies/` — 정책 + Kill Switch
- `cryptoengine/docs/strategies/` — 전략 사양
- `cryptoengine/docs/runbook.md` — Docker 운영 가이드

## 배포 및 운영

→ `cryptoengine/docs/runbook.md` 참조

포지션 보호 원칙: 배포(재시작)는 포지션을 청산하지 않는다. `service_shutdown` 사유로 종료될 때 Redis에 상태를 저장한 뒤, 1시간 내 재시작 시 자동 복구된다.

```bash
# 핵심 명령
docker compose up -d --build --no-deps funding-arb          # 단일 서비스 재빌드
docker compose logs --tail=20 funding-arb | grep 복구        # 복구 확인

# shared/ 변경 시 전체 서비스 재빌드
docker compose build market-data execution-engine funding-arb strategy-orchestrator telegram-bot
```

## Docker 및 공유 라이브러리

→ `cryptoengine/docs/runbook.md` + `cryptoengine/docs/structure/README.md` 참조

**빌드 컨텍스트**: 프로젝트 루트(`.`)에서 COPY 경로는 `cryptoengine/` 프리픽스 사용.
```dockerfile
COPY cryptoengine/shared /app/shared  # ✓ 올바름
COPY ../../shared /app/shared        # ✗ 실패
```

**자주 쓰는 명령**:
```bash
docker compose up -d postgres redis        # 인프라만 기동
docker compose up -d --build --no-deps <s> # 특정 서비스 재빌드
docker compose logs -f funding-arb          # 실시간 로그
make emergency                              # 비상 청산
```

## 참고 문서

**프로젝트 구조**: `cryptoengine/docs/structure/services.md`
**Redis Pub/Sub 채널**: `cryptoengine/docs/architecture/data-flow.md`
**PostgreSQL 테이블**: `cryptoengine/docs/structure/README.md`
**환경 변수**: `cryptoengine/docs/runbook.md`
**백테스트 스킬셋**: `services/jesse_engine/scripts/README.md`

## 코드 작업 시 필수 규칙

1. **BYBIT_TESTNET=false 금지**: Phase 5 명시적 승인 전까지 절대 변경 금지
2. **Kill Switch 약화 금지**: `shared/kill_switch.py` 로직 절대 수정 금지
3. **레버리지 5x 제한**: `fa80_lev5_r30` 초과 금지 (CAGR +34.87%, Sharpe 3.583, MDD -4.52%)
4. **BTC 단일 운영**: 다중 심볼 거래 금지
5. **shared/ 변경 시**: 모든 서비스 이미지 재빌드 필수

## 다음 작업

→ `cryptoengine/docs/archive/CLAUDE_history.md` 및 본 CLAUDE.md의 "현재 진행 상태" 참조

**Phase 4 → 5 전환**:
```bash
scripts/phase5_preflight.py              # Phase 5 진입 전 8개 항목 검증
scripts/switch_to_mainnet.py             # 메인넷 전환 (9단계, 이중 확인)
# 설정: EXPECTED_INITIAL_BALANCE_USD=200, STRICT_MONITORING_HOURS=24, PHASE5_MODE=true
```
