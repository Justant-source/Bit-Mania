# CryptoEngine — Claude Code 작업 가이드

## 프로젝트 개요

비트코인 선물 자동매매 시스템. Supertrend 4h 추세추종 단일 전략, BTC 단일 운영. Docker Compose 기반, 메인넷 소액 실전 운영 중.

## 현재 진행 상태

- **Phase 5 실전 운영 중** (2026-05-18~): 메인넷 $185.31 USDT 운영 (2026-06-14 기준)
  - 전략: `supertrend_4h_x3_7908` (Supertrend 4h Long-only, 3x, combo #7908)
  - 백테스트 (Bybit 네이티브 4h 정본): CAGR +137.64% | Sharpe 1.349 | MDD -73.29% | 360 trades | 2017-08-17~2026-04-30
  - 환경: `BYBIT_TESTNET=false`, `PHASE5_MODE=true`, `EXPECTED_INITIAL_BALANCE_USD=185.31`
  - ⚠️ execution-engine 재시작 전 잔고 게이트 현행화 필수 → `docs/policies/operations/runbook.md` §문제 해결

## 전략 파라미터 — combo #7908 (SSOT)

| 파라미터 | 값 | 설명 |
|----------|-----|------|
| st_factor | 2.6 | Supertrend ATR 배수 |
| st_period | 9 | Supertrend ATR 기간 |
| fast_ema | 7 | 빠른 EMA |
| slow_ema | 29 | 느린 EMA |
| dir_ema | 240 | 방향 필터 EMA |
| atr_mult | 3.3 | ATR 손절·익절 배수 |
| 레버리지 | 3x | 하드캡 (SAFETY_LEVERAGE_LIMIT=3.0) |
| 타임프레임 | 4h | |
| 방향 | Long-only | Short 없음 |
| 자본 배분 | 95%×3x | 전액 단일 배분 |
| min_notional | $65 | 최소 주문 금액 |
| CANDLE_LOOKBACK | 1000 | 지표 계산 캔들 수 |

**진입 조건 (AND)**:
1. `Close > ST선` — Supertrend 상승 국면
2. `EMA(7) > EMA(29)` — 골든크로스
3. `Close > EMA(240)` — 장기 방향 확인

**청산 조건**:
- `EMA(7) < EMA(29)` → 즉시 청산 (데드크로스)
- `가격 ≤ 진입가 − ATR(14)×3.3` → 손절
- `가격 ≥ 진입가 + ATR(14)×3.3` → 익절
- 청산 후 1봉(4h) 재진입 금지
- 안전 stop: 진입가 − 70%/lev (시장가)

## 핵심 원칙

1. **생존 우선**: 수익보다 포지션 보호. Kill Switch 4단계 계층 유지
2. **메인넷 신중 운영**: Phase 5 실전 중. 코드 변경 전 포지션 확인 (Telegram `/positions`)
3. **단계별 검증**: Phase 3 → 4 → 5 순서, 절대 건너뛰지 않음 (이미 완료)
4. **BTC 단일 운영**: 다중 심볼 금지 (변동성/신뢰)
5. **포지션 보호**: 배포 시에도 청산하지 않음 (service_shutdown → Redis 복구)

## 불변 규칙 ★ 위반 절대 금지

1. **BYBIT_TESTNET=false**: Phase 5 메인넷 실전 중 — 테스트넷 전환 시 포지션 수동 청산 후 진행
2. **Kill Switch 약화 금지**: `shared/kill_switch.py` 로직 절대 수정 금지
3. **레버리지 3x 하드캡**: 초과 절대 금지 (`SAFETY_LEVERAGE_LIMIT=3.0`)
4. **BTC 단일 운영**: 다중 심볼 거래 금지
5. **shared/ 변경 시**: 모든 서비스 이미지 재빌드 필수
6. **지정가 우선**: 시장가 직접 진입 금지 (긴급 청산 제외)
7. **전액 95%×3x 배분**: 레짐 배분 없음, 항상 고정

## Kill Switch Phase 5 임계값

| 주기 | 퍼센트 | 절대값 | 발동 조건 |
|------|--------|--------|-----------|
| 일일 | 5% | $10 | 둘 다 초과 (AND) |
| 주간 | 10% | $20 | 둘 다 초과 (AND) |
| 월간 | 15% | $30 | 둘 다 초과 (AND) |
| cooldown | — | — | 60분 |

정본: `cryptoengine/config/orchestrator.yaml` §phase5

## 문서 관리 규칙 ★ 필수

**모든 프로젝트 문서는 `docs/` 단일 트리에서 관리한다.**

새 컨텍스트 진입 시: `docs/README.md` (Map of Content)를 먼저 읽는다.

### 코드 ↔ 문서 동기화
코드 수정 시 같은 커밋에서 관련 문서를 업데이트한다:
1. `docs/CODE_MAP.md` 역인덱스에서 변경 코드 경로 검색
2. 해당 doc의 `last_updated` frontmatter 갱신
3. 신규 영역은 같은 커밋에서 doc 신규 작성 + CODE_MAP.md 행 추가

문서 미업데이트는 작업 미완으로 간주한다.

### 참고 문서
- `docs/README.md` — MOC (시작점)
- `docs/CODE_MAP.md` — 코드 경로 → 문서 역인덱스
- `docs/glossary.md` — 용어집
- `docs/architecture/` — 시스템 설계
- `docs/policies/` — 정책 + Kill Switch
- `docs/policies/strategies/supertrend.md` — 전략 사양 (SSOT)
- `docs/policies/operations/runbook.md` — Docker 운영 가이드

## 배포 및 운영

→ `docs/policies/operations/runbook.md` 참조

포지션 보호 원칙: 배포(재시작)는 포지션을 청산하지 않는다. `service_shutdown` 사유로 종료될 때 Redis에 상태를 저장한 뒤, 1시간 내 재시작 시 자동 복구된다.

```bash
# 핵심 명령
docker compose up -d --build --no-deps supertrend           # 단일 서비스 재빌드
docker compose logs --tail=20 supertrend | grep 복구         # 복구 확인

# shared/ 변경 시 전체 서비스 재빌드
docker compose build market-data execution-engine supertrend strategy-orchestrator telegram-bot
```

## Docker 및 공유 라이브러리

→ `docs/policies/operations/runbook.md` + `docs/structure/README.md` 참조

**빌드 컨텍스트**: 프로젝트 루트(`.`)에서 COPY 경로는 `cryptoengine/` 프리픽스 사용.
```dockerfile
COPY cryptoengine/shared /app/shared  # ✓ 올바름
COPY ../../shared /app/shared        # ✗ 실패
```

**자주 쓰는 명령**:
```bash
docker compose up -d postgres redis        # 인프라만 기동
docker compose up -d --build --no-deps <s> # 특정 서비스 재빌드
docker compose logs -f supertrend          # 실시간 로그
make emergency                              # 비상 청산
```

## 참고 문서

**프로젝트 구조**: `docs/structure/services.md`
**Redis Pub/Sub 채널**: `docs/architecture/data-flow.md`
**PostgreSQL 테이블**: `docs/structure/README.md`
**환경 변수**: `docs/env/env-vars.md`
**백테스트 진입점**: `backtest/README.md` (전략 R&D 통합 트리)
**백테스트 스킬셋**: `backtest/docs/methodology/backtest-skillset.md`
**백테스트 스크립트**: `backtest/scripts/` (카테고리별 분류: runners/sweep/analysis/reports/audit/data)
**백테스트 인프라**: `backtest/docker/docker-compose.yml` (별도 backtest-postgres, port 5433)
