---
title: 70 Policy — 운영 Runbook · 배포 · 모니터링
last_updated: 2026-06-15
---

# 운영 Runbook · 배포 · 모니터링

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

# 순차 재시작 (의존성 순서)
docker compose up -d --no-deps market-data        # 1. 데이터 제공자
sleep 15
docker compose up -d --no-deps execution-engine   # 2. 주문 엔진
sleep 10
docker compose up -d --no-deps strategy-orchestrator  # 3. 오케스트레이터
sleep 10
docker compose up -d --no-deps supertrend         # 4. 전략 (포지션 복구)
sleep 60
docker compose up -d --no-deps telegram-bot       # 5. 알림
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
# 전 포지션 강제 청산 (Kill Switch L4 발동)
make emergency                       # 또는 docker compose kill supertrend execution-engine
```

---

## §2. execution-engine 재시작 게이트 (필수)

⚠️ **실사례 (2026-06-13)**: 잔고 게이트 미현행화 → Dead Man's Kill Switch 연쇄 발동

### 재시작 전 체크리스트

1. **현재 실잔고 확인**
   ```bash
   docker compose logs execution-engine | grep "잔고 검증" | tail -1
   # actual_usdt 값 메모
   ```

2. **.env 갱신**
   ```bash
   # cryptoengine/.env
   EXPECTED_INITIAL_BALANCE_USD=<실잔고>    # 예: 185.31
   ```

3. **사전 확인**
   ```bash
   docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
     "SELECT status FROM positions WHERE status='open';"
   
   # Telegram
   /positions          # 현재 포지션 확인
   /balance            # 현재 잔고 확인
   ```

4. **환경 변수 검증**
   ```bash
   grep "BYBIT_TESTNET\|PHASE5_MODE" cryptoengine/.env
   # BYBIT_TESTNET=false  (메인넷)
   # PHASE5_MODE=true     (고급 모드)
   ```

5. **재시작 실행**
   ```bash
   docker compose up -d --build --no-deps execution-engine
   ```

6. **복구 확인** (1-2분 대기)
   ```bash
   docker compose logs --tail=20 execution-engine | grep -E "복구|검증|ready"
   ```

### Dead Man's Switch 대응

execution-engine 하트비트(TTL 300s)가 5분 이상 끊기면 orchestrator가 Kill Switch L3 자동 발동.

- **발동 상황**: 잔고 게이트 등으로 재시작이 5분 초과
- **자동 해제**: 4시간 쿨다운 후 (인메모리 상태)
- **즉시 복구**: `docker compose restart strategy-orchestrator` (인메모리 kill 상태 초기화)

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
  "SELECT id, symbol, size, entry_price, updated_at FROM positions WHERE status='open';"

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
git pull
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
| 1 | STRATEGY | 개별 전략 손절 (일 -5%, 주 -10%, 월 -15%) | 해당 전략만 중지 + 포지션 청산 | 4시간 쿨다운 후 자동 재개 |
| 2 | PORTFOLIO | 포트폴리오 손실 (일 -5% AND $10, 주 -10% AND $20, 월 -15% AND $30) | **모든 전략 중지** + **전체 포지션 청산** | 1시간 쿨다운 후 재개 |
| 3 | SYSTEM | API 연결 실패, DB/Redis 다운 | 시장가 청산 시도 → 실패 시 수동 개입 대기 | 자동 불가 (수동) |
| 4 | MANUAL | Telegram 명령 또는 SSH | 즉시 **모든 포지션 청산** | 수동 `/resume` |

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
