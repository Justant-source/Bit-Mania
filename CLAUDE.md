# CryptoEngine — Claude Code 작업 가이드

## 프로젝트 개요

비트코인 선물 자동매매 시스템. Supertrend 4h 추세추종 단일 전략, BTC 단일 운영. Docker Compose 기반, 메인넷 소액 실전 운영 중.

## 현재 진행 상태

- **Phase 5 실전 운영 중** (2026-05-18~): 메인넷 $185.31 USDT 운영 (2026-06-14 기준)
  - 전략: `supertrend_4h_x3_7908` (Supertrend 4h Long-only, 3x, combo #7908)
  - 백테스트 (4h 00계열 정본, **시세 출처는 Binance 현물** — 2026-08-31 정정): CAGR +219.06% | Sharpe 1.667 | MDD -66.70% | 198 trades | 2017-08-17~2026-04-30 (ATR 익절 없음, 2026-08-20)
    - ⚠️ 라이브는 **Bybit USDT 무기한**을 거래한다. 백테스트 픽스처는 **Binance 현물**이라 종가 기준 평균 +0.05% 차이가 나고, **무기한 펀딩비가 미반영**돼 있다 → 라이브-리플레이 대조 시 반드시 감안. 상세: `backtest/results/2026-08-31/csv_ohlcv_drift.md`
  - 환경: `BYBIT_TESTNET=false`, `PHASE5_MODE=true`, `EXPECTED_INITIAL_BALANCE_USD=185.31`
  - ⚠️ execution-engine 재시작 전 잔고 게이트 현행화 필수 → `docs/70-policy/operations.md` §실행엔진 재시작

## 전략 파라미터 — combo #7908 (SSOT)

| 파라미터 | 값 | 설명 |
|----------|-----|------|
| st_factor | 2.6 | Supertrend ATR 배수 |
| st_period | 9 | Supertrend ATR 기간 |
| fast_ema | 7 | 빠른 EMA |
| slow_ema | 29 | 느린 EMA |
| dir_ema | 240 | 방향 필터 EMA |
| atr_mult | 3.3 | ATR 손절 배수 (익절 없음) |
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
- ATR 익절 없음 (상승은 EMA 데드크로스까지 보유)
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
8. **브랜치는 `main` 단일 운영**: 피처 브랜치 생성 금지 — 모든 커밋은 `main`에 직접

## Kill Switch Phase 5 임계값

| 주기 | 퍼센트 | 절대값 | 발동 조건 |
|------|--------|--------|-----------|
| 일일 | 5% | $10 | 둘 다 초과 (AND) |
| 주간 | 10% | $20 | 둘 다 초과 (AND) |
| 월간 | 15% | $30 | 둘 다 초과 (AND) |
| cooldown | — | — | 60분 |

정본: `cryptoengine/config/orchestrator.yaml` §phase5

## 🚨 절대 규칙: SSOT Doc-Sync 게이트 (commit/push 전 필수)

**모든 프로젝트 문서는 `docs/` 단일 트리에서 관리한다.**

새 컨텍스트 진입 시: `docs/_index.md`를 **첫 번째**로 읽는다. (Doc-Sync 트리거 맵 포함)

### 코드 변경 시 Doc-Sync 절차

코드 수정 시 **같은 커밋**에서 아래 절차를 수행한다:

```bash
# 1. 스테이징된 파일 확인
git diff --staged --name-only

# 2. docs/_index.md 트리거 맵에서 변경 코드 경로 검색
#    → 대응 문서를 동시 갱신, last_updated frontmatter 업데이트

# 3. 신규 영역이면 해당 계층 문서 + _index.md 행 추가

# 4. lint gate PASS 확인
make -C cryptoengine lint-docs   # 6항목 모두 PASS여야 커밋 가능
```

문서 미업데이트 + lint FAIL = 작업 미완으로 간주한다.

## 문서 계층 (C4 줌 순서)

| 계층 | 경로 | 내용 |
|---|---|---|
| 10 Context | `docs/10-context/system-context.md` | L1 외부 액터·3 서브시스템 경계 |
| 20 Container | `docs/20-containers/containers.md` | 배포 단위·포트·볼륨·env-vars |
| 30 Component | `docs/30-components/components.md` | 서비스 내부 모듈·클래스 책임 |
| 40 Data | `docs/40-data/data-model.md` | PG 스키마·ER·마이그레이션 트랙 |
| 50 API | `docs/50-api/pubsub-catalog.md` | Redis pub/sub 채널·Dashboard REST |
| 60 Runtime | `docs/60-runtime/state-machines.md` | OrderState·KillLevel·복구 플로우 |
| 70 Policy | `docs/70-policy/safety.md` | Kill Switch·레버리지·포지션 보호 |
| 70 Policy | `docs/70-policy/operations.md` | 운영 Runbook·배포·모니터링 |
| 70 Policy | `docs/70-policy/strategy.md` | Supertrend SSOT·백테스트 방법론 |
| 90 ADR | `docs/90-adr/README.md` | 아키텍처 결정 인덱스 |
| 용어집 | `docs/glossary.md` | 도메인 용어 정의 |

## 배포 및 운영

→ `docs/70-policy/operations.md` 참조

포지션 보호 원칙: 배포(재시작)는 포지션을 청산하지 않는다. `service_shutdown` 사유로 종료될 때 Redis에 상태를 저장한 뒤, 1시간 내 재시작 시 자동 복구된다.

```bash
# 핵심 명령
docker compose up -d --build --no-deps supertrend           # 단일 서비스 재빌드
docker compose logs --tail=20 supertrend | grep 복구         # 복구 확인

# shared/ 변경 시 전체 서비스 재빌드
docker compose build market-data execution-engine supertrend strategy-orchestrator telegram-bot
```

## Docker 및 공유 라이브러리

→ `docs/70-policy/operations.md` + `docs/30-components/components.md` 참조

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
make -C cryptoengine lint-docs             # 문서 lint (6항목)
```

## 빠른 참조

| 목적 | 경로 |
|---|---|
| 시스템 전체 그림 | `docs/10-context/system-context.md` |
| 포트·볼륨·env-vars | `docs/20-containers/containers.md` |
| 서비스 구조·클래스 | `docs/30-components/components.md` |
| DB 스키마 | `docs/40-data/data-model.md` |
| Redis 채널·REST API | `docs/50-api/pubsub-catalog.md` |
| 상태머신·복구 | `docs/60-runtime/state-machines.md` |
| Kill Switch·안전 정책 | `docs/70-policy/safety.md` |
| 운영 Runbook·배포 | `docs/70-policy/operations.md` |
| 전략 사양·백테스트 | `docs/70-policy/strategy.md` |
| 백테스트 진입점 | `backtest/README.md` |
