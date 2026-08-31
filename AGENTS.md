# CryptoEngine — AI 에이전트 작업 가이드

> 이 파일이 AI 에이전트 지침의 **정본**이다. `CLAUDE.md` 등은 이 파일을 참조만 한다.
> 문서 충돌 시 우선순위: 코드(runtime) > `docs/_index.md` > 이 파일

비트코인 선물 자동매매 시스템. Supertrend 4h 추세추종 단일 전략, BTC 단일 운영. Docker Compose 기반, 메인넷 소액 실전 운영 중.

운영 잔고·env 플래그·재시작 주의: `docs/shared/70-policy.md`.
전략 파라미터 combo #7908·진입/청산: `docs/cryptoengine/70-policy/strategy.md`.
Kill Switch 임계값: `docs/cryptoengine/70-policy/safety.md`.

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

문서 린트가 긴급 커밋을 막으면 `SKIP_DOC_LINT=1 git commit ...` 로만 우회한다. 운영 코드·`.env`·Kill Switch·전략 파라미터는 이 우회로 바꾸지 않는다.

## 🚨 절대 규칙: SSOT Doc-Sync 게이트 (commit 전 필수)

새 컨텍스트 진입 시 `docs/_index.md`를 **첫 번째**로 읽는다.
코드 수정 시 **같은 커밋**에서 아래를 수행한다.

```bash
git diff --staged --name-only          # 1. 변경 파일 확인
# 2. docs/_index.md 트리거 맵에서 해당 코드 경로 검색 → 대응 문서 갱신
#    + 문서 frontmatter last_updated 갱신
# 3. 신규 영역이면 계층 문서 + _index.md 행 추가
python3 scripts/lint_docs.py           # 4. 10개 검사 전부 PASS
#    또는 make -C cryptoengine lint-docs
```

갱신 대상이 없으면 커밋 메시지에 `Doc-Sync: 없음`을 명시한다.
**HALT** — API·포트·스키마·상태전이·정책·환경변수를 바꿨는데 대응 문서를 못 찾으면 중단하고 보고한다.

## 문서 계층

| 대분류 | 계층 | 경로 | 내용 |
|---|---|---|---|
| shared | 10 context | `docs/shared/10-context.md` | L1 외부 액터·3 서브시스템 경계 |
| shared | 20 containers | `docs/shared/20-containers.md` | 세 Compose 네트워크 경계 |
| cryptoengine | 20 containers | `docs/cryptoengine/20-containers.md` | 운영 서비스표·포트·볼륨·env |
| cryptoengine | 30 components | `docs/cryptoengine/30-components.md` | 서비스 내부 모듈 |
| cryptoengine | 40 data | `docs/cryptoengine/40-data.md` | PG 스키마·마이그레이션 |
| cryptoengine | 50 api | `docs/cryptoengine/50-api.md` | Redis pub/sub |
| dashboard | 50 api | `docs/dashboard/50-api.md` | Dashboard REST |
| cryptoengine | 60 runtime | `docs/cryptoengine/60-runtime.md` | OrderState·KillLevel·복구 |
| cryptoengine | 70 policy | `docs/cryptoengine/70-policy/` | 안전·Supertrend SSOT |
| shared | 70 policy | `docs/shared/70-policy.md` | Runbook·배포·모니터링 |
| backtest | 20 containers | `docs/backtest/20-containers.md` | 백테스트 Compose |
| shared | 90 adr | `docs/shared/90-adr/` | 아키텍처 결정 |
| shared | — | `docs/shared/glossary.md` | 도메인 용어 |

## 배포 및 운영

상세는 `docs/shared/70-policy.md`. 배포(재시작)는 포지션을 청산하지 않는다. `service_shutdown` 시 Redis 저장 후 1시간 내 재시작이면 자동 복구.

```bash
docker compose up -d --build --no-deps supertrend
docker compose logs --tail=20 supertrend | grep 복구
docker compose build market-data execution-engine supertrend strategy-orchestrator telegram-bot
```

## Docker 및 공유 라이브러리

상세는 `docs/shared/70-policy.md` · `docs/cryptoengine/30-components.md`.
빌드 컨텍스트는 프로젝트 루트. `COPY cryptoengine/shared /app/shared`.

```bash
docker compose up -d postgres redis
make emergency
make -C cryptoengine lint-docs
```

## 빠른 참조

| 목적 | 경로 |
|---|---|
| 문서 지도 · Doc-Sync | `docs/_index.md` |
| 시스템 전체 그림 | `docs/shared/10-context.md` |
| 포트·볼륨·env | `docs/cryptoengine/20-containers.md` |
| 서비스 구조 | `docs/cryptoengine/30-components.md` |
| DB 스키마 | `docs/cryptoengine/40-data.md` |
| Redis 채널 | `docs/cryptoengine/50-api.md` |
| Dashboard REST | `docs/dashboard/50-api.md` |
| 상태머신·복구 | `docs/cryptoengine/60-runtime.md` |
| Kill Switch·안전 | `docs/cryptoengine/70-policy/safety.md` |
| 운영 Runbook | `docs/shared/70-policy.md` |
| 전략 사양·백테스트 | `docs/cryptoengine/70-policy/strategy.md` |
| 백테스트 Compose | `docs/backtest/20-containers.md` |
| 백테스트 진입점 | `backtest/README.md` |
