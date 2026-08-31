---
title: 70 Policy — 운영 Runbook · 배포 · 모니터링
last_updated: 2026-08-31
---

# 운영 Runbook · 배포 · 모니터링

> **2026-08-29 일자 SSOT**: 레거시 DB DROP, fail-closed 자격증명, 킬스위치 수신, 히스토리 재작성, 배포 순서 교정은 [ADR-0010](../90-adr/0010-ops-cleanup-20260829.md)에 모아 두었다. 이 문서는 그 결정을 **매일 쓰는 명령**으로 풀어 쓴다.

## §0. 2026-08-29 운영자가 알아야 할 것

| 항목 | 현재 값 / 규칙 |
|---|---|
| 지갑 / 할당 | ≈ **238.88 USDT** (당일 청산 후). 게이트 폴백 `EXPECTED_INITIAL_BALANCE_USD=238.88` |
| 포지션 | 플랫 (17:19 KST 청산). 다음 평가 창은 4h 종가 |
| 4h 종가 (KST) | 01:00 / 05:00 / 09:00 / **13:00** / **17:00** / **21:00** (UTC 00/04/08/12/16/20) |
| `ohlcv_history` 4h `timestamp` | **봉 시작**. 17:00 KST에 마감된 봉 = **04:00 UTC** 행 |
| 배포 순서 | 이미지 `build` 먼저 → ST를 orch **앞**에 `up` (60s 틱) |
| compose env | 셸에 `DB_PASSWORD` 등이 export돼 있으면 `.env` 파일보다 이긴다. **source .env 금지**, 필요 시 `unset` |
| 킬스위치 수신 | `PUBSUB NUMSUB ce:kill_switch` ≥ 1 (orchestrator). Redis CLI는 호스트 `.env`의 `REDIS_PASSWORD` |
| Git | `origin/main` = `9f5b116f`. 복구 태그 `legacy-archive-2026-08-29` = `2ee11756`. D9로 **모든 옛 해시 무효** |
| DB 크기 | ~306MB (018 적용). 분기물 테이블 DROP됨 |

당일 청산 누락 원인: Redis 재시작 후 EE/ST가 **죽은 pub/sub**을 유지 → `order:request` NUMSUB=0. 배포는 `--force-recreate`로 소켓을 새로 열 것.

---

## §1. 핵심 명령 (Makefile)

### 라이프사이클

```bash
# 전체 프로덕션 기동
cd ~/Data/Bit-Mania/cryptoengine
make up                              # docker compose up -d (인프라 + 서비스)

# 전체 중지
make down                            # docker compose down (포지션 유지)

# 시스템 상태 확인
make status                          # docker compose ps + 리소스 사용률
```

### 특정 서비스 재빌드 (포지션 유지)

```bash
# 단일 서비스 재빌드 (예: supertrend)
docker compose up -d --build --no-deps supertrend
docker compose up -d --build --no-deps execution-engine

# shared/ 변경 시 — 모든 의존 서비스 재빌드 (순서 중요)
docker compose build --no-cache \
  market-data \
  execution-engine \
  supertrend \
  strategy-orchestrator \
  telegram-bot

# 순차 재시작 (의존성 + start 레이스 — 2026-08-29)
# 이미지를 먼저 빌드해 EE 다운타임을 5분 미만으로 유지 (dead-man)
docker compose build market-data execution-engine supertrend strategy-orchestrator telegram-bot
docker compose up -d --no-deps market-data execution-engine telegram-bot
sleep 10
docker compose up -d --no-deps supertrend   # 3. 전략이 커맨드 채널을 구독
sleep 8
docker compose up -d --no-deps strategy-orchestrator  # 4. 그다음 start 발행
# 60초 안에 command_received / strategy_started 가 없으면 ST 로그 확인
sleep 60
docker compose logs --since=3m supertrend | grep -E "command_received|strategy_started"
```

### 로그 및 모니터링

```bash
# 실시간 로그 (Ctrl+C 종료)
docker compose logs -f supertrend
docker compose logs -f execution-engine

# 최근 50줄 로그
docker compose logs --tail=50 supertrend

# 에러/경고 필터
docker compose logs --tail=100 execution-engine | grep -E "ERROR|WARN|CRITICAL"
```

### 비상 청산

```bash
# 전 포지션 시장가 청산 + 신규 주문 차단
make -C cryptoengine emergency
```

`scripts/emergency_close_all.py`를 execution-engine 안에서 stdin 주입으로 실행한다
(이미지 재빌드 불필요). 신규 주문 차단 → 전략에 `stop(reason=kill_switch)` 발행 →
`on_stop()`이 청산 → `ce:positions:all`로 청산 확인(최대 60초). **청산이 확인되어야만**
전략·오케스트레이터 정지 단계로 넘어가며, 미확인 시 중단하고 수동 확인을 요구한다.

> ⚠️ `docker compose kill supertrend`로는 **청산되지 않는다.** SIGKILL이라 `on_stop()`이
> 실행되지 않으며, 정상 종료(`stop`)를 하더라도 사유가 `service_shutdown`이면
> `_SHUTDOWN_NO_LIQUIDATE`에 걸려 포지션을 보존한다(배포 시 포지션 보호 원칙).
> 청산이 목적이면 반드시 `make emergency` 또는 Telegram `/emergency_close`를 쓸 것.

재개하려면 `ce:kill_switch:active` 키를 삭제한 뒤 Supertrend를 올리고 오케스트레이터를 기동한다 (ADR-0010). 구독 확인:

```bash
set -a && source cryptoengine/.env && set +a
docker exec cryptoengine-redis-1 redis-cli -a "$REDIS_PASSWORD" --no-auth-warning PUBSUB NUMSUB ce:kill_switch
unset DB_PASSWORD REDIS_PASSWORD REDIS_URL
# 기대: 1 이상
```

---

## §2. execution-engine 재시작 게이트 (필수)

⚠️ **실사례 (2026-06-13)**: 잔고 게이트 미현행화 → Dead Man's Kill Switch 연쇄 발동  
⚠️ **실사례 (2026-08-04)**: 전원 장애 후 구식 `.env` 잔고 → 기동 거부 → Dead Man's 연쇄

### 자동 복구 (Redis equity baseline)

운영 중 `execution-engine`은 실잔고를 Redis `ce:phase5:equity_baseline`(TTL 없음)에 60초마다 저장한다.  
재기동 시 잔고 게이트는 **Redis 기준선 우선**, 없으면 `.env`의 `EXPECTED_INITIAL_BALANCE_USD` 폴백(허용 오차 5%).

- 전원 장애 후 Redis 볼륨이 살아 있으면 → 기준선 ≈ 장애 직전 잔고 → 자동 통과
- Redis wipe / 콜드스타트 → `.env` 수동 현행화 필요
- 다운타임 중 잔고가 기준선 대비 **5% 초과** 변동(입출금 등) → 여전히 시작 거부

### 재시작 전 체크리스트 (수동 / Redis 없을 때)

1. **현재 실잔고 확인**
   ```bash
   docker compose logs execution-engine | grep "잔고 검증" | tail -1
   # actual_usdt 값 메모
   ```

2. **.env 갱신** (Redis baseline 없거나 게이트 실패 시)
   ```bash
   # cryptoengine/.env
   EXPECTED_INITIAL_BALANCE_USD=<실잔고>    # 2026-08-29 청산 후 예: 238.88
   ```

3. **사전 확인**
   ```bash
   docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
     "SELECT id, symbol, size, entry_price, opened_at FROM positions WHERE closed_at IS NULL;"
   
   # Telegram
   /positions          # 현재 포지션 확인
   /balance            # 현재 잔고 확인
   ```

4. **환경 변수 검증**
   ```bash
   grep "BYBIT_TESTNET\|PHASE5_MODE\|EXPECTED_INITIAL_BALANCE" cryptoengine/.env
   # BYBIT_TESTNET=false  (메인넷)
   # PHASE5_MODE=true     (고급 모드)
   ```

5. **재시작 실행**
   ```bash
   docker compose up -d --build --no-deps execution-engine
   ```

6. **복구 확인** (1-2분 대기)
   ```bash
   docker compose logs --tail=20 execution-engine | grep -E "검증|baseline|ready|tasks launched"
   ```

7. **Dead Man's / Kill Switch 해제** (하트비트 단절로 KS가 발동된 경우)
   ```bash
   docker compose restart strategy-orchestrator
   ```

### Dead Man's Switch 대응

execution-engine 하트비트(TTL 300s)가 5분 이상 끊기면 orchestrator가 Kill Switch L3 자동 발동.

- **발동 상황**: 잔고 게이트 등으로 재시작이 5분 초과
- **자동 해제**: 쿨다운 후 (인메모리 상태, Phase 5 기본 60분)
- **즉시 복구**: `docker compose restart strategy-orchestrator` (인메모리 kill 상태 초기화)
  - 주의: equity history 상 **당일** 손실 한도(5% AND $10)를 이미 넘긴 상태면 재시작 직후 KS가 다시 발동할 수 있음
  - daily/weekly/monthly peak는 해당 기간 샘플로만 복원됨 (전체 history 최고가 사용 금지 — 2026-08-04 수정)
- **알림**: KS 신규 발동만 CRITICAL→Telegram. 쿨다운 유지 중 재로깅/Dead Man's 재_trigger 없음

---

## §3. 일상 모니터링

### 매일 확인 사항

```bash
# 1. 시스템 상태
docker compose ps

# 2. 주요 서비스 에러 (1시간 범위)
docker compose logs --since=1h strategy-orchestrator | grep -E "ERROR|CRITICAL"
docker compose logs --since=1h execution-engine | grep -E "ERROR|CRITICAL"

# 3. Telegram 명령
/status                   # 현재 포트폴리오 상태
/positions                # 열린 포지션 목록
/balance                  # 현재 잔고

# 4. 열린 포지션 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT id, symbol, size, entry_price, opened_at FROM positions WHERE closed_at IS NULL;"

# 5. 대시보드 확인
# http://localhost:3000/supertrend    — Supertrend 신호 vs 실제 거래
# http://localhost:3000/monitor       — 자산/Kill Switch/서비스 상태

# 6. 신호-주문 대조 감사 (미체결 사고 조기 감지)
python3 cryptoengine/scripts/audit_signal_order_mismatch.py --days 7
python3 cryptoengine/scripts/audit_signal_order_mismatch.py --alert  # Telegram 알림
```

### 주간 확인 사항

```bash
# 1. 주간 거래 현황
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT DATE(created_at), COUNT(*) as trades, SUM(pnl) as total_pnl
   FROM trades WHERE created_at > NOW() - INTERVAL '7 days'
   GROUP BY DATE(created_at) ORDER BY DATE DESC;"

# 2. 마진 비율 추이 (3x 기준, 증증금율 < 33% 정상)
# 대시보드: /monitor → Margin Ratio (Min 7d)

# 3. 디스크/메모리 사용
docker system df
docker compose exec postgres pg_database_size cryptoengine
```

### 월간 확인 사항

```bash
# 1. 월간 P&L 리포트
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT SUM(pnl) as total_pnl, COUNT(*) as trade_count, AVG(pnl) as avg_pnl
   FROM trades WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW());"

# 2. Kill Switch 이벤트 리뷰 (월간)
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT * FROM kill_switch_events WHERE triggered_at > NOW() - INTERVAL '1 month';"

# 3. 월간 성과 지표
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT DATE_TRUNC('month', created_at),
          COUNT(*) as trades,
          SUM(pnl) as monthly_pnl,
          MIN(equity) as min_equity,
          MAX(equity) as max_equity
   FROM daily_reports
   GROUP BY DATE_TRUNC('month', created_at)
   ORDER BY DATE_TRUNC DESC;"

# 4. 엣지 소멸 트립와이어 체크 (매월 1일, 2026-08-31~)
python3 backtest/scripts/analysis/tripwire_check.py extend-csv   # 라이브 4h 봉으로 리플레이 CSV 연장
python3 backtest/scripts/analysis/tripwire_check.py check        # T1(월간 워닝)/T2(블록 게이트) 판정, log.md 기록
#   ⚠️ 2026-08-31 기준 CSV↔ohlcv_history 가격 드리프트로 extend-csv가 중단 상태
#      (backtest/results/2026-08-31/csv_ohlcv_drift.md 원인 미규명, 해소 전까지 check 로그 미기록)
#   규칙·임계값: backtest/results/tripwire/PREREGISTRATION_TRIPWIRE.md (커밋 후 불변)

# 5. 실 체결 슬리피지 재실측 (신규 체결 10건 누적 또는 분기 1회)
python3 backtest/scripts/analysis/live_slippage.py
#   최근 실측: backtest/results/2026-08-31/live_slippage_report.md
```

---

## §4. 배포 절차 (Phase 5 메인넷)

### 배포 전 검증

```bash
# 1. 현재 상태 확인
docker compose ps
Telegram: /positions

# 2. 변경 파일 확인
git diff HEAD --name-only | head -20
git diff HEAD --name-only | grep shared/  # shared/ 변경 여부

# 3. 코드 변경 내용 확인
git log --oneline -5
git diff HEAD
```

### 배포 절차

```bash
# Step 1: 코드 변경 적용
cd ~/Data/Bit-Mania
git fetch origin
git status -sb
# 2026-08-29 D9 이후 히스토리가 재작성됐다. 다른 클론이 옛 main이면
# git pull 은 병합 재앙이다 — 재클론하거나 reset --hard origin/main.
git log --oneline -5

# Step 2: shared/ 변경 여부 판단
if git diff HEAD --name-only | grep -q shared/; then
  echo "shared/ 변경 포함 → 전체 서비스 재빌드"
  # 전체 재빌드 (위의 shared/ 변경 섹션 참조)
else
  echo "shared/ 변경 없음 → 변경 서비스만 재빌드"
  docker compose up -d --build --no-deps <변경된_서비스>
fi

# Step 3: 포지션 복구 확인 (supertrend인 경우)
sleep 60
docker compose logs --tail=20 supertrend | grep -E "복구|recovered|restored"

# Step 4: Telegram 알림 확인
# Telegram: 모니터링 → 배포 완료 알림 수신 확인
```

---

## §5. 미체결·발산 관련 ERROR 알림 대응 (2026-06-13~)

주문이 전략 신호대로 체결되지 않으면 다음 ERROR 알림이 Telegram으로 온다 (5분 중복제거):

| 알림 이벤트 | 의미 | 대응 |
|------------|------|------|
| `주문 거부 — 전략 신호 미체결` (`order_rejected`) | Safety 차단·실행 실패로 주문 거부. reason 필드에 사유 | reason 확인. exit이면 60초 후 1회 자동 재시도됨 — 재시도도 거부되면 `/positions` 확인 후 수동 청산 판단 |
| `position_state_divergence` | 전략 믿음 ≠ 거래소 실포지션 — 자동으로 실제값으로 교정됨 | 직전 거부/유실 이력 확인 (`orders` 테이블). 교정 후 다음 봉부터 정상 동작 |
| `pending_order_unresolved` | 주문 결과를 450초 내 확인 못 함 — 재동기화 수행됨 | execution-engine 로그 확인, 거래소 주문 내역 대조 |
| `bar_feed_stall` / `bar_gap_detected` | 4h 봉 마감 메시지 누락 — REST 백필 자동 수행 | market-data 서비스 상태 확인 |
| `market_ws_reconnecting` / `OHLCV 수집 중단` | WS 구독 실패 또는 수집 갭 >10분 | 아래 §5.1 절차 |

### §5.1 OHLCV 중단 / `market_ws_reconnecting` (2026-08-11)

**증상**: Telegram `WebSocket subscription failed` + `OHLCV 수집 중단: 마지막 업데이트 N분 전`.

**대표 원인 (과거)**: 만기 분기물을 core BTCUSDT와 같은 subscribe 배치에 넣어 Bybit이 전체 구독을 거부. **2026-08-29 D2** 이후 `quarterly_lifecycle.py` 삭제, 구독은 **BTCUSDT 4h만**.

**확인**:
```bash
docker compose logs --tail=50 market-data | grep -E 'subscription failed|handler not found|ohlcv'
docker compose exec -T postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT timeframe, MAX(timestamp), EXTRACT(EPOCH FROM (NOW()-MAX(timestamp)))/60 AS gap_min
   FROM ohlcv_history WHERE exchange='bybit' AND symbol='BTCUSDT' GROUP BY 1 ORDER BY 1;"
```

**복구** (포지션 청산 없음 — market-data만 재빌드):
```bash
docker compose up -d --build --no-deps market-data
docker compose logs --tail=30 market-data | grep -E 'subscribed|BTCUSDT|subscription failed'
```

---

## §6. 모니터링 (3계층)

### Layer 1: Telegram (실시간 알림)

**특징**: 진입/청산/Kill Switch/에러 실시간 알림 + 배치 처리 + 30분 하트비트

**자주 쓰는 명령어**:
```bash
/status          # 현재 포트폴리오 상태
/positions       # 열린 포지션
/balance         # 현재 잔고
/kill            # 비상 청산 (Kill Switch L4)
/acknowledge     # Kill Switch L2 확인
/resume          # Kill Switch 해제
/stats           # 월간 통계
```

### Layer 2: Bit-Mania 대시보드 (http://localhost:3000)

| 페이지 | 역할 |
|--------|------|
| `/supertrend` | 4h 신호 vs 실제 거래 비교 + 차트 + 지표 |
| `/monitor` | 자산 곡선 + Kill Switch + 서비스 헬스 + 인프라 메트릭 |

**주요 패널**:
- Supertrend 비교: 매 4h 봉 예상 진입(파랑) vs 실제 체결(초록/오렌지)
- 자산 곡선: 백테스트 예상 vs 실제 메인넷 (30일)
- Kill Switch: 현재 상태 + 발동 이력
- 서비스 헬스: 서비스별 마지막 응답 시간 + 에러 건수
- 인프라: CPU/메모리/디스크/Redis (Prometheus 연동)

### Layer 3: 핵심 메트릭 모니터링

| 지표 | 정상 범위 | 경고 임계값 | 긴급 |
|------|-----------|------------|------|
| Margin Ratio | > 5x | 3x~5x | < 1.5x |
| Heartbeat | < 30s | 30s~60s | > 60s |
| Redis 메모리 | < 150MB | 150~200MB | > 200MB |
| Daily P&L | -2% ~ +5% | < -3% | < -5% AND > $10 |

---

## §7. 백업 · 복구

```bash
# 자동 백업 (매일 02:00 KST)
# pg-backup 서비스가 자동으로 실행 — 7일 보존

# D1 (2026-08-29) 018 DROP 직전 pgdata 볼륨 tar:
#   compose 볼륨 백업 디렉터리 + ~/legacy-cleanup-20260829_pgdata.tar.gz (~1.5G)
# Postgres는 tar 동안 ~3분 정지됨.

# 수동 백업 (즉시)
docker compose exec postgres pg_dump -U cryptoengine cryptoengine > backup_manual_$(date +%Y%m%d).sql

# 백업 파일 목록
docker compose exec postgres ls -lh /backups/ 2>/dev/null || echo "로컬 백업 확인 필요"

# PostgreSQL 복구 (DESTRUCTIVE)
docker compose down
docker compose up -d postgres
docker compose exec postgres psql -U cryptoengine < backup_manual_20260615.sql
```

---

## §8. 문제 해결

### Limit 지정가 Re-peg 모니터링

Post-only 지정가 주문은 10초마다 최대 20회 재발행 후 시장가로 폴백.

```bash
# 로그 이벤트
docker compose logs --tail=200 execution-engine | grep -E "limit_repeg|postonly_rejected|limit_fallback"

# 폴백 빈도 집계
docker compose logs execution-engine | grep "limit_fallback_to_market" | wc -l
```

| 로그 msg | 의미 | 대응 |
|----------|------|------|
| `limit_repeg_attempt` | attempt N에서 best-bid/ask로 재발행 | 정상 (체결 대기 중) |
| `postonly_rejected` | Bybit이 taker 체결 방지로 즉시 거부 | 정상 (빠른 시장) |
| `limit_fallback_to_market` | 20회 미체결 → 시장가 폴백 | ⚠️ taker 수수료 발생. 빈도 높으면 검토 |

### 주문이 체결되지 않는 경우

```bash
# 1. Execution Engine 로그 확인
docker compose logs execution-engine --tail=50 | grep -E "order|error|timeout"

# 2. Bybit API 상태
curl https://api.bybit.com/v5/market/time

# 3. 거래소 직접 확인
# Bybit 웹 → [선물] → [주문 기록]

# 4. 신호 vs 주문 대조 감사
python3 cryptoengine/scripts/audit_signal_order_mismatch.py --days 1
```

### 서비스가 시작되지 않는 경우

```bash
# 1. 컨테이너 상태 확인
docker compose ps

# 2. 로그 확인 (최근 100줄)
docker compose logs <서비스명> --tail=100

# 3. 의존성 서비스 확인
docker compose exec postgres pg_isready
docker compose exec redis redis-cli ping

# 4. 환경변수 확인
docker compose config | grep -A 10 <서비스명>

# 5. 이미지 재빌드
docker compose build --no-cache <서비스명>
docker compose up -d <서비스명>
```

### 데이터베이스 연결 실패

```bash
# 1. PostgreSQL 상태 확인
docker compose exec postgres pg_isready

# 2. 연결 테스트
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c "SELECT 1"

# 3. 연결 풀 상태 (활성 연결 수)
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname='cryptoengine';"

# 4. 재시작
docker compose restart postgres
```

---

## §9. Kill Switch 대응 (4단계)

| 레벨 | 이름 | 트리거 | 동작 | 복구 |
|------|------|--------|------|------|
| 1 | STRATEGY | 개별 전략 손절 (일 -5%, 주 -10%, 월 -15%) | 해당 전략만 중지 + 포지션 청산 | **60분** cooldown 후 auto_resume (`orchestrator.yaml`) |
| 2 | PORTFOLIO | 포트폴리오 손실 (일 -5% AND $10, 주 -10% AND $20, 월 -15% AND $30) | **모든 전략 중지** + **전체 포지션 청산** | **60분** cooldown |
| 3 | SYSTEM | API 연결 실패, 하트비트 5분 미수신 | 시장가 청산 시도 | 자동 불가 — orch 재시작으로 인메모리 상태 초기화 |
| 4 | MANUAL | `ce:kill_switch` 외부 발행 / Telegram `/emergency_close` / `make emergency` | 즉시 청산 + 주문 차단 | **auto-resume 불가**. `ce:kill_switch:active` 삭제 + 서비스 재기동 |

### Kill Switch 발동 시 대응

```bash
# 1. 알림 수신 (Telegram)
# 예: 🚨 [Kill Switch L2] 포트폴리오 일일 손실 -5.2%

# 2. L2 응답 필수
/acknowledge                         # 또는 /ack

# 3. 자동 재개 대기
# → 1시간 쿨다운 후 자동 재개
# 또는 즉시 재개
/resume

# 4. 로그 확인
docker compose logs strategy-orchestrator | grep -E "kill|switch|level"
```

---

## 관련 문서

- `docs/70-policy/strategy.md` — Supertrend 전략 SSOT
- `docs/70-policy/safety.md` — Kill Switch 정책 및 임계값
- `docs/README.md` — Map of Content (시작점)
