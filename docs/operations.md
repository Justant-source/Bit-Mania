# 운영

메인넷 소액 실전 운영 중이다(`BYBIT_TESTNET=false`). 모든 명령은 `cryptoengine/`에서 실행한다. 구조는 [architecture.md](architecture.md), 안전 정책은 [safety.md](safety.md).

## 기본 사실

| 항목 | 값 |
|---|---|
| 봉 마감 (UTC → KST) | 00→09, 04→13, 08→17, 12→21, 16→01, 20→05 시 |
| `ohlcv_history.timestamp` | 봉 **시작** 시각. 17:00 KST에 마감된 봉 = 04:00 UTC 행 |
| 일일 리포트 | 00:00 UTC (09:00 KST) |
| 지갑 (2026-08-29 청산 후 기록) | 약 238.88 USDT. 현재 값은 `/status` 또는 대시보드로 확인 |
| 잔고 게이트 | 없음. 옛 `EXPECTED_INITIAL_BALANCE_USD` 기동 검증은 새 봇에 없다 |

환경변수는 `cryptoengine/.env` (`.env.example` 참고). 셸에 `DB_PASSWORD` 등이 export돼 있으면 compose가 `.env`보다 셸 값을 쓴다 — `source .env` 하지 말 것.

## 배포 · 재시작

**배포는 포지션을 청산하지 않는다.** 재시작 시 봇은 거래소 포지션을 읽어 동기화하고, 놓친 봉이 있으면 평가한다(`app.py` 시작 절차).

```bash
cd cryptoengine
# 1. 변경 전 포지션 확인: Telegram /status  (또는 make status)
# 2. 커밋 상태 확인 — 배포하는 코드 = git에 기록될 코드여야 한다 (git diff HEAD 비어 있음)
git status -sb && git diff HEAD --stat
# 3. bot만 재빌드/재시작
make restart                  # = docker compose up -d --build --no-deps bot
# 4. 확인
docker compose logs --tail=100 bot | grep -E "candles_backfilled|missed_bar|position_state_divergence|kill_state_restored"
```

- 시작 직후 Telegram에 "봇 시작" 메시지(보유 포지션, 로드한 봉 수, 다음 봉 처리 시각, 진입 차단 여부)가 온다. 오지 않으면 로그 확인.
- 전략 로직 변경은 **배포와 커밋을 같은 작업 단위로** 묶는다. (2026-08 사례: 배포와 커밋이 9일 어긋나 git만 보고 라이브 동작을 오진함.)
- 봉 마감 직전·직후(경계 ±1분)와 주문 진행 중에는 재시작을 피한다. 종료 유예는 450초라 진행 중인 주문(최대 420초)은 끝까지 처리된 뒤 종료된다. 강제 종료로 미체결 주문이 남으면 다음 시작의 `executor.start()`가 고아 주문을 취소한다.
- 기동 시 `DB_PASSWORD`, `BYBIT_API_KEY`, `BYBIT_API_SECRET`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`가 없거나 `BYBIT_TESTNET`이 `false`가 아니면 코드 2로 종료한다 (`restart: unless-stopped`이므로 계속 재시작을 반복하며 로그에 `FATAL`).
- 다른 클론이 2026-08-29 이전 히스토리를 갖고 있으면 `git pull`로 병합하지 말고 재클론하거나 `reset --hard origin/main`.
- 공유 라이브러리(`shared/`)가 바뀌어도 이미지는 하나다: `bot`만 재빌드한다.

### 로그

```bash
make logs                                   # docker compose logs -f --tail=100 bot
docker compose logs --since=1h bot | grep -E '"level": "(error|critical)"'
```

로그는 stdout JSON(컨테이너 json-file, 10MB×5)이다. DB에는 로그가 없으므로 오래된 이벤트는 사라진다 — 조사에는 `orders`, `positions`, `supertrend_signals`, `portfolio_snapshots`를 쓴다.

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT id, size, entry_price, opened_at FROM positions WHERE closed_at IS NULL;"
```

## Telegram 명령

`TELEGRAM_CHAT_ID`로 지정한 채팅만 명령을 쓸 수 있다.

| 명령 | 동작 |
|---|---|
| `/status` | 에쿼티, 포지션, 진입 차단 여부, 킬스위치 레벨, 마지막 봉·신호, 낙폭, 하트비트 |
| `/report` | 일일 리포트(24h/7d/30d 에쿼티 변화, 포지션, 청산 거래 손익·수수료, 승패, 낙폭) |
| `/dashboard` | 대시보드 링크 버튼 (`DASHBOARD_URL` 필요) |
| `/emergency_close` | 2단계: 현재 포지션·에쿼티 표시 + "청산 실행"/"취소" 버튼. 60초 안에 누른 버튼만 유효, 1회용 |
| `/resume` | 2단계 동일. 수동 정지 해제 |
| `/help` | 도움말 |

신규 알림: 진입·청산 체결, 주문 거부(같은 사유 중복 제거), 킬스위치, 포지션 소멸/출현(스톱 체결·수동 거래), ERROR 이상 로그(120초 중복 제거), 봉 누락, 시작·종료.

## 긴급 청산

우선순위 순서:

1. **Telegram `/emergency_close`** → "청산 실행". 진입을 `/resume` 전까지 차단하고 reduce-only 시장가로 청산, 거래소가 flat인지 확인해 알린다.
2. **`make emergency`** (= `docker compose exec bot python -m bot.cli emergency-close`). 봇 컨테이너가 안 떠 있으면 `docker compose run --rm bot python -m bot.cli emergency-close`로 대체한다. 확인 프롬프트에 `yes`; 프롬프트 생략은 `--yes`. `manual_halt`를 DB에 기록하고(실행 중인 봇은 ≤60초 내 반영) 청산한 뒤 `positions` 행을 닫는다. 종료 코드 0 = flat 확인(또는 취소), 1 = 아직 포지션 남음.
3. **Bybit 앱/웹**에서 직접 청산. 봇·DB·네트워크가 모두 죽었을 때의 최후 수단이다. 진입 스톱로스는 거래소에 걸려 있다(포지션 단위).

> `docker compose stop/kill bot`은 청산하지 않는다 (SIGTERM은 포지션을 유지, SIGKILL도 마찬가지).

해제: Telegram `/resume`. CLI로 정지했다면 `docker compose exec bot python -m bot.cli resume`도 되지만, Telegram `/emergency_close`로 정지한 경우 실행 중 봇 메모리의 킬스위치 MANUAL 레벨은 `/resume`으로만 초기화된다 — Telegram으로 정지했으면 Telegram으로 해제한다.

## watchdog (호스트 cron, 외부 dead-man)

`cryptoengine/scripts/watchdog.sh`가 5분마다 (1) `bot` 컨테이너가 running이고 Docker 헬스가 unhealthy가 아닌지, (2) `bot_state['heartbeat']`의 `updated_at`이 25분 이내인지 확인한다. 연속 2회(≥5분) 실패하면 Telegram 메시지 1회, 이후 복구까지 조용, 복구 시 1회. 상태 파일은 `cryptoengine/.watchdog/`(gitignore). docker·postgres가 죽어 있어도 안전하게 동작한다(모든 호출에 timeout). 자격증명은 `.env`의 `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`만 읽는다.

설치(수동, cron을 돌리는 사용자가 docker 그룹이어야 함):

```cron
*/5 * * * * /home/justant/Data/Bit-Mania/cryptoengine/scripts/watchdog.sh >/dev/null 2>&1
```

한계: 호스트 자체가 꺼지거나 cron이 죽으면 감시도 멈춘다. 인터넷이 끊겨도 알림이 가지 않는다.

## 백업 · 복구

- 자동: `pg-backup` 컨테이너가 매일 02:00 KST에 `cryptoengine_YYYYMMDD_HHMMSS.sql.gz`를 `pg-backups` 볼륨에 만들고 7일 지난 것을 지운다 (`BACKUP_CRON`으로 변경).
- 즉시: `make backup`.
- 목록: `docker compose exec pg-backup ls -lh /backups`.
- 복구 (파괴적 — 포지션 확인 후; 봇은 멈춘다):

```bash
docker compose stop bot
docker compose exec pg-backup sh -c "gunzip -c /backups/<파일>.sql.gz | psql -h postgres -U cryptoengine -d cryptoengine"
docker compose up -d bot
```

덤프는 `DROP`을 포함하지 않으므로 기존 데이터가 남은 DB에 복구하면 충돌한다. 완전 복구는 새 DB(`docker compose down` 후 `pgdata` 볼륨 교체)에서 하고, 복구 후 봇이 `ensure_schema()`로 누락 테이블을 만든다. 볼륨 통째 백업이 필요하면 `docker compose stop postgres` 후 볼륨을 tar로 보관한다.

## 대시보드

`http://<호스트>:3000/?t=<DASHBOARD_TOKEN>` (포트는 `DASHBOARD_PORT`). 첫 방문에 유효한 `t`가 있으면 30일짜리 쿠키가 설정돼 이후 `t` 없이도 열린다. 토큰이 없거나 틀리면 403, `/healthz`만 인증 없이 열린다. `DASHBOARD_TOKEN`이 비어 있으면 대시보드는 기동하지 않는다. Postgres만 읽는 읽기 전용 페이지다. 포트는 모든 인터페이스에 바인딩된다(방화벽/프록시로 제한 권장). 평문 HTTP이므로 외부 공개 시에는 리버스 프록시(TLS) 뒤에 둔다.

`http://<호스트>:3000/supertrend`는 **원본 전략 비교 대시보드**(옛 Node 대시보드 복원)로 **토큰이 필요 없다**(옛 구성과 동일, 읽기 전용 `GET`만). 정적 파일은 `bot/web_static/supertrend/`에 원본 그대로이며 수정하지 않는다. JSON은 `/api/internal/supertrend/{expected,actual,compare,equity,status,candles,candles/in-progress}`. 옛 Redis 값 대신 `status`의 `strategy_status`·`live_position`은 봇 상태/거래소 포지션(10초 캐시), 진행 중 4h 봉은 거래소 REST(30초 캐시, `source:"exchange"`)에서 온다. 사이드바의 `/monitor`(Prometheus/Redis 인프라 모니터)는 이식하지 않아 403(토큰 필요 경로)을 돌려준다. `/`(요약)와 `/api/summary`는 계속 토큰 인증이다.

Telegram의 대시보드 버튼은 `DASHBOARD_URL`에 봇이 `?t=<DASHBOARD_TOKEN>`을 덧붙인 링크다. `DASHBOARD_URL`에는 토큰 없이 기본 주소만 적는다.

## replay 데이터

replay 백테스트(`tests/fixtures/_replay_supertrend.py`)는 Postgres `replay_ohlcv_4h`(Binance 현물 4h)와 `replay_reference_trades`를 읽는다.

- 봇이 4h 봉마다 `replay_data.sync`를 호출해 새 확정 봉을 채운다. 수동: `make replay-sync`.
- 실행: `make replay` (호스트에서 `DB_HOST=127.0.0.1`, `DB_PASSWORD`는 `.env`에서 읽음). 인자: `make replay ARGS="--until 2026-04-30"` (정본 기준선 재현).
- 시드/재적재(파일이 필요하므로 호스트에서): `cd cryptoengine && DB_HOST=127.0.0.1 DB_PASSWORD=... python3 -m bot.replay_data import-csv tests/fixtures/btc_4h_extended.csv`, `import-trades tests/fixtures/trades_7908.csv`, `export-csv <경로>`, `status`.
- `sync`는 새 봉만 추가하고 기존 봉은 덮어쓰지 않는다.

## 롤백

- **봇 코드**: 이전 커밋으로 `git revert`(브랜치는 `main` 하나) 후 `make restart`. 스키마 변경은 추가 전용(`IF NOT EXISTS`)이라 이전 코드도 그대로 돈다. 포지션은 그대로 유지된다.
- **옛 7-컨테이너 구성으로 복귀**: 컷오버 직전 커밋을 확인(`git log`)해 그 시점의 `docker-compose.yml`과 `services/`, `shared/`, `dashboard/`로 되돌린 뒤 `docker compose up -d --build`. 새 봇을 먼저 `docker compose stop bot`으로 멈춰 두 시스템이 동시에 주문하지 않게 한다. 옛 시스템은 Redis 볼륨·Prometheus 볼륨을 쓰며 컷오버 후 자동 삭제되지 않는다(필요하면 `docker volume ls`로 확인).

## 문제 해결

| 증상 | 확인 |
|---|---|
| 봇이 계속 재시작 | `docker compose logs bot` 의 `FATAL`(환경변수), Postgres 연결 |
| 봉이 안 들어옴 | `bar_missing` 알림, Bybit REST 상태 `curl https://api.bybit.com/v5/market/time` |
| `position_state_divergence` | 내부 믿음 ≠ 거래소. 자동 교정됨. 직전 `orders` 거부/유실 확인 |
| 진입이 안 됨 | `/status`의 차단 사유(`manual_halt` 또는 `portfolio_block_until`), `orders`의 거부 사유 |
| `housekeeping_failing` | 15분 주기 잔고/포지션 조회가 3회 연속 실패 — API 키·네트워크 |
| 주문 거부 알림 | 같은 봉에서 60초마다 잔량만 재시도(체결 또는 다음 봉까지). 사유가 `kill_switch`/`entries_blocked`/`dry_run`이면 재시도 없음 |

## 테스트용 DB

`make test`는 `bot-test-pg`(postgres:16, `127.0.0.1:55432`)라는 일회용 컨테이너를 띄워서 쓴다. 운영 DB(5432)는 건드리지 않는다. 옛 운영 스키마는 `cryptoengine/tests/fixtures/legacy_schema/`에 테스트 픽스처로만 남아 있다(새 스키마가 기존 DB와 호환되는지 검사).
