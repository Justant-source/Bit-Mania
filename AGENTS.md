# CryptoEngine — AI 에이전트 작업 가이드

> 이 파일이 AI 에이전트 지침의 **정본**이다. `CLAUDE.md` 등은 이 파일을 참조만 한다.
> 문서 충돌 시 우선순위: 코드(runtime) > 이 파일 > `docs/`

비트코인 선물 자동매매 시스템. Supertrend 4h 추세추종 단일 전략, BTC 단일 운영. **하나의 Python 프로세스(`cryptoengine/bot/`) + Postgres**, Docker Compose, 메인넷 소액 실전 운영 중.

운영·배포·긴급 청산: `docs/operations.md`. 전략 파라미터 combo #7908·진입/청산: `docs/strategy.md`. 킬스위치 임계값·한계: `docs/safety.md` (값은 `cryptoengine/config/bot.yaml`).

## 핵심 원칙

1. **생존 우선**: 수익보다 포지션 보호. 킬스위치 유지
2. **메인넷 신중 운영**: 실전 중. 코드 변경·재시작 전 포지션 확인 (Telegram `/status` 또는 `make status`)
3. **BTC 단일 운영**: 다중 심볼 금지 (변동성/신뢰)
4. **포지션 보호**: 배포·재시작·종료(SIGTERM)는 청산하지 않는다. 복구는 봇 시작 절차(아래)가 한다
5. **거래소가 진실**: 내부 상태는 체결이 확정된 뒤에만 바꾸고, 판단 전에 거래소 포지션과 동기화한다

## 불변 규칙 ★ 위반 절대 금지

1. **BYBIT_TESTNET=false**: 메인넷 실전 중 — 테스트넷 전환 시 포지션 수동 청산 후 진행 (`bot/main.py`는 정확히 `false`가 아니면 기동 거부)
2. **Kill Switch 약화 금지**: `cryptoengine/shared/kill_switch.py` 로직 절대 수정 금지
3. **레버리지 3x 하드캡**: 초과 절대 금지 (`SAFETY_LEVERAGE_LIMIT=3.0`)
4. **BTC 단일 운영**: 다중 심볼 거래 금지
5. **이미지는 하나**: `shared/` 또는 `bot/` 변경 시 `bot` 이미지를 재빌드한다 (`make restart`)
6. **지정가 우선**: 시장가 직접 진입 금지 (re-peg 소진 후 잔량 폴백, 킬스위치·긴급 청산 제외)
7. **전액 95%×3x 배분**: 레짐 배분 없음, 항상 고정
8. **브랜치는 `main` 단일 운영**: 피처 브랜치 생성 금지 — 모든 커밋은 `main`에 직접

## 문서 규칙

코드 수정 시 **같은 커밋**에서 대응하는 `docs/` 문서를 갱신한다 (구조 → `architecture.md`, 운영 절차 → `operations.md`, 전략 → `strategy.md`, 안전 → `safety.md`). 결정이 번복되기 어려운 것이면 `docs/adr/`에 새 ADR을 추가한다. 갱신할 문서가 없으면 커밋 메시지에 `Docs: 없음`을 적는다. 자동 린트는 없다.

## 봇 구조와 시작 절차

`python -m bot.main` → `BotApp.start()` (`bot/app.py`):

1. 거래소 연결, 마진 isolated·레버리지 3x 설정, `executor.start()`(고아 주문 취소)
2. `bot_state`·쿨다운·킬스위치/고점 복원
3. 거래소 포지션 동기화 (`sync_position`)
4. REST로 확정 봉 1000개 로드, `supertrend_signals`의 `MAX(bar_ts)`로 놓친 봉 확인 후 평가
5. Telegram 명령·대시보드 시작, 시작 메시지 전송

이후 루프: 4h 경계+3초에 봉 처리, 15분마다 housekeeping(포지션·잔고·리스크·스냅샷), UTC 자정 일일 리포트. 상태는 전부 Postgres(`bot_state` 등). Redis·pub/sub·웹소켓은 없다. 자세한 구조: `docs/architecture.md`.

## 배포 및 운영

```bash
cd cryptoengine
git diff HEAD --stat                 # 배포 코드 = 커밋될 코드 (전략 변경은 배포와 커밋을 같은 단위로)
make restart                         # docker compose up -d --build --no-deps bot
docker compose logs --tail=100 bot | grep -E "candles_backfilled|missed_bar|position_state_divergence"
make test                            # python3 -m pytest tests/bot tests/e2e tests/unit/test_kill_switch.py -q
```

- 긴급 청산: Telegram `/emergency_close` → `make emergency` (= `python -m bot.cli emergency-close`) → Bybit 앱.
- 외부 감시: 호스트 cron `cryptoengine/scripts/watchdog.sh` (알림 전용).
- `.env`는 절대 읽어서 값을 출력하거나 커밋하지 않는다. 키 이름만 확인한다 (`sed -E 's/=.*//' .env`).
- 셸에 export된 `DB_PASSWORD` 등이 `.env`보다 우선하므로 `source .env` 하지 않는다.

## 빠른 참조

| 목적 | 경로 |
|---|---|
| 시스템 구조·테이블 | `docs/architecture.md` |
| 배포·운영·롤백 | `docs/operations.md` |
| 전략 사양·백테스트 | `docs/strategy.md` |
| 킬스위치·안전·한계 | `docs/safety.md` |
| 결정 기록 | `docs/adr/` |
| 봇 코드 / 계약 | `cryptoengine/bot/`, `cryptoengine/bot/interfaces.py` |
| Compose / Dockerfile | `cryptoengine/docker-compose.yml`, `cryptoengine/bot/Dockerfile` |
| 백테스트 진입점 | `backtest/README.md` |
