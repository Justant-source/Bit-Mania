---
title: 모니터링 및 알림 설정
category: policies/operations
related_code:
  - dashboard/src/
  - dashboard/docker-compose.yml
  - cryptoengine/services/telegram-bot/
  - cryptoengine/config/orchestrator.yaml
  - cryptoengine/config/prometheus/
last_updated: 2026-05-25
when_to_update: |
  - 대시보드 페이지/라우트 변경 시
  - Telegram 알림 형식/임계값 변경 시
  - Prometheus 메트릭 추가 시
  - alertEvaluator.ts 규칙 변경 시
---

# 모니터링 및 알림 설정

세 가지 계층으로 구성된 통합 모니터링 시스템:
1. **Bit-Mania 대시보드**: 전략 비교 + 시스템 모니터 (포트 3000, standalone Docker)
2. **Telegram**: alertEvaluator 기반 실시간 알림 + 상호작용
3. **Prometheus**: 인프라 메트릭 수집 (30일 보존)

```mermaid
graph TD
    subgraph sources["데이터 소스"]
        SVC["마이크로서비스들\n(execution-engine, funding-arb 등)"]
        SYS["호스트 시스템\n(CPU, 메모리, 디스크)"]
        RD["Redis\n(메모리, 클라이언트)"]
    end

    subgraph collectors["수집"]
        PROM["Prometheus :9090\n30일 보존"]
        NE["node-exporter :9100"]
        RE["redis-exporter :9121"]
    end

    subgraph visualization["시각화"]
        DB["Bit-Mania 대시보드 :3000\n전략 비교 + 시스템 모니터"]
    end

    subgraph alerts["알림"]
        TG["Telegram\nAlertDispatcher"]
    end

    SVC -->|"structlog JSON"| PROM
    SYS --> NE --> PROM
    RD --> RE --> PROM
    PROM --> DB
    SVC --> DB
    DB -->|"alertEvaluator 60s"| TG
    SVC -->|"30분 하트비트"| TG

    style TG fill:#2196f3,color:#fff
    style DB fill:#1f6feb,color:#fff
```

---

## Bit-Mania 대시보드

### 기동

```bash
# standalone compose (cryptoengine 메인 스택과 별도)
cd /home/justant/Data/Bit-Mania/dashboard
docker compose up -d --build

# 로그 확인
docker compose logs -f dashboard
```

### 접속

```
URL: http://localhost:3000
```

### 페이지

| 경로 | 설명 |
|------|------|
| `/` 또는 `/supertrend` | **전략 비교** — 예상 vs 실제 거래 메인 화면 |
| `/monitor` | **시스템 모니터** — 자산 곡선, Kill Switch, 서비스 헬스, 인프라 |

### 전략 비교 페이지 주요 정보

- **KPI**: 현재 포지션 / 신호 상태 / 다음 4h봉 ETA / 체결 일치율 / 평균 슬리피지 / 자본배분
- **가격 차트**: BTC 4h 캔들 + 예상 진입(파랑 ▲) / 예상 청산(파랑 ▼) vs 실제 체결(초록/오렌지)
- **자산 곡선**: 백테스트 기준 예상 자산 (파랑 점선) vs 실제 메인넷 자산 (초록 실선)
- **거래 비교 테이블**: bar_ts별 예상(시각/가격/수량) ↔ 실제(체결가/수량/슬리피지/타이밍) + matched/missed/extra 배지
- **지표 패널**: ST 방향, EMA 7/27/230, ATR(14) 최신값

### alertEvaluator (Grafana 알림 대체)

`dashboard/src/alertEvaluator.ts` — 60초마다 9개 규칙을 직접 평가, Redis `ce:alerts:grafana` 채널로 발행 → telegram-bot 수신.

평가 규칙: 펀딩레이트 스파이크, 자산 급락 >3%/15분, OHLCV 갭 >10분, Kill Switch 발동, Max Drawdown >10%, CPU/메모리/디스크/Redis 임계값.

---

## Telegram 알림 (AlertDispatcher)

**AlertDispatcher**: 배치, 레이트 리밋, 중복 제거 기능이 있는 고급 알림 시스템.

**특징**:
- 배치 처리: 동시 다중 알림을 하나로 묶기
- 레이트 리밋: 과도한 알림 방지 (분당 최대 10건)
- 중복 제거: 같은 알림 2회 이상 발송 금지
- TTL 임계값: 오래된 알림은 자동 삭제

**하트비트**: 30분 간격 정상 작동 신호 발송 (너무 많지 않게 조정)

```mermaid
sequenceDiagram
    participant ENG as execution-engine
    participant MKT as market-data
    participant REDIS as Redis
    participant ORC as orchestrator (watchdog)
    participant TG as telegram-bot

    loop 30초마다
        ENG->>REDIS: SETEX heartbeat:execution TTL=300s
        ENG->>+ENG: touch /tmp/heartbeat_ok
        MKT->>REDIS: SETEX heartbeat:market TTL=300s
    end

    loop 60초마다
        ORC->>REDIS: GET heartbeat:execution
        alt 정상 (TTL 남음)
            REDIS-->>ORC: TTL > 0 OK
        else 5분 미수신
            REDIS-->>ORC: nil (만료됨)
            ORC->>ORC: KillSwitch.trigger(SYSTEM)
            ORC->>TG: execution-engine 응답 없음
        end
    end
```

**정기 리포트**:
- **08:00 UTC** (= 17:00 KST): 일일 리포트 (어제의 P&L, 펀딩비)
- **20:00 UTC** (= 05:00 KST 다음날): 야간 리포트 (밤새 변동사항)

### 알림 유형

#### 1. Kill Switch 알림 (자동)

```
🚨 [Kill Switch L2] 포트폴리오 일일 손실 -5.2% 달성
쿨다운: 60분 후 자동 재개
수동 조작: /resume (즉시 재개) | /kill (전체 정지)
```

**응답 필요**: `/acknowledge` 또는 `/ack`

#### 2. 마진 경고 (자동)

```
⚠️ [마진 경고] Margin Ratio: 8.5x (< 10x)
포지션: 0.15 BTC @ $65,000
조치: 모니터링 권장
```

**응답 필요**: 없음 (정보성)

#### 3. 포트폴리오 스냅샷 (정기)

```
📊 [12:00 KST] 포트폴리오 상태
Equity: $10,250 (+2.5%)
Daily P&L: +$250
Strategies:
  - Funding Arb: $80 PnL
  - Adaptive DCA: 포지션 없음
```

**주기**: 매 4시간

#### 4. 거래 진입/청산 (자동)

```
📈 [Funding Arb 진입]
BTC: 0.15 @ $65,000
Basis: +0.25%
목표 수익: +$50-100

---

📊 [Funding Arb 청산]
보유기간: 24시간
P&L: +$75 (0.75%)
펀딩비: +$50 | 기저: +$25
```

#### 5. API 장애 알림 (자동)

```
🔴 [API 오류] Bybit 연결 실패 (Retry 1/3)
상태: 재연결 중...
포지션: 안전함
```

#### 6. 시스템 오류 (긴급)

```
🚨🚨 [긴급] 거래소 API 연결 실패 (3회 초과)
자동 복구 불가능
필요한 조치: /kill (포지션 청산) 또는 SSH 접속
```

### 자주 쓰는 명령어

| 명령 | 설명 |
|------|------|
| `/status` | 현재 포트폴리오 상태 |
| `/positions` | 열린 포지션 목록 |
| `/balance` | 현재 잔고 |
| `/funding` | 현재 펀딩레이트 |
| `/kill` | Kill Switch 수동 발동 (모든 포지션 청산) |
| `/acknowledge` or `/ack` | Kill Switch 알림 확인 |
| `/resume` | Kill Switch 해제 (전략 재개) |
| `/stats` | 월간 통계 |

---

## Prometheus 메트릭 수집

### 데이터소스

| 소스 | 수집 항목 | 업데이트 | 보존 |
|------|---------|---------|------|
| **node_exporter** | CPU, 메모리, 디스크, 네트워크 | 15초마다 | 30일 |
| **redis_exporter** | Redis 메모리, 키 수, 명령 수 | 15초마다 | 30일 |
| **애플리케이션** | 주문 건수, 에러율, API 레이턴시 | 60초마다 | 30일 |

### 핵심 메트릭

| 메트릭 | 의미 | 경고 임계값 |
|--------|------|------------|
| `node_cpu_usage_percent` | 호스트 CPU 사용률 | > 80% |
| `node_memory_usage_percent` | 호스트 메모리 사용률 | > 85% |
| `node_disk_free_gb` | 여유 디스크 공간 | < 5 GB |
| `redis_memory_used_bytes` | Redis 메모리 사용량 | > 500 MB |
| `redis_connected_clients` | Redis 연결 클라이언트 | > 50 |
| `bybit_api_latency_ms` | Bybit API 응답 시간 | > 1000 ms |

### PromQL 쿼리 예시

```promql
# CPU 사용률 (1시간 평균)
avg(rate(node_cpu_seconds_total[5m])) by (instance) * 100

# Redis 메모리 (MB)
redis_memory_used_bytes / 1024 / 1024

# 미체결 주문 수
rate(orders_pending_total[5m])
```

---

## 핵심 모니터링 지표

### 일일 체크 지표

| 지표 | 정상 범위 | 경고 임계값 | 확인 방법 |
|------|-----------|------------|---------|
| Portfolio Equity | > 초기값 | < 95% 초기값 | `/status` |
| Daily P&L | -2% ~ +5% | < -3% | 대시보드 /monitor |
| Margin Ratio | > 10x | 5x ~ 10x | 대시보드 /monitor |
| API Latency | < 200ms | > 1000ms | 대시보드 /monitor |
| Kill Switch Events | 0회 | 1회 이상 | 대시보드 /monitor |
| Open Positions | ≤ 5개 | > 5개 | `/positions` |

### 주간 체크 지표

| 지표 | 정상 범위 | 확인 방법 |
|------|-----------|---------|
| Weekly P&L | +1% ~ +3% | 대시보드 /monitor |
| Sharpe (7일) | > 1.0 | 대시보드 /monitor |
| Trade Count | ≥ 3회 | Telegram 통계 |
| API Success Rate | > 99% | 대시보드 /monitor |

### 월간 체크 지표

| 지표 | 정상 범위 | 확인 방법 |
|------|-----------|---------|
| Monthly CAGR | +2.9% (annualized +34.87%) | 대시보드 /monitor |
| Sharpe (30일) | > 2.0 | 대시보드 /monitor |
| Max Drawdown | < -5% | 대시보드 /monitor |
| Win Rate | > 60% | Telegram 통계 |

---

## 모니터링 자동화

### Cron 작업 (선택사항)

```bash
# 매일 09:00 KST에 일일 리포트 생성
0 9 * * * cd ~/Data/Bit-Mania/cryptoengine && \
  docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT DATE(created_at), SUM(pnl), COUNT(*) FROM trades GROUP BY DATE(created_at) ORDER BY DATE DESC LIMIT 7;" \
  >> /var/log/cryptoengine_daily.log
```

### 알림 커스터마이징

Telegram 봇 설정 파일에서:

```yaml
# services/telegram-bot/config.yaml
alerts:
  margin_ratio:
    warning_threshold: 10.0
    critical_threshold: 5.0
    enabled: true
  
  kill_switch:
    enabled: true
    require_ack: true
  
  trading:
    notify_entry: true
    notify_exit: true
    notify_pnl_snapshot: true
```

---

## 문제 상황별 모니터링

### 포지션이 없는데 수익이 없음

```bash
# 1. 펀딩레이트 확인
docker compose exec redis redis-cli GET market:funding:BTCUSDT | jq .

# 2. 진입 조건 확인
docker compose logs funding-arb --tail=100 | grep -E "entry|condition|funding"

# 3. 오케스트레이터 상태 확인
docker compose logs strategy-orchestrator --tail=50 | grep -E "funding|weight"
```

### 수익이 음수

```bash
# 1. 최근 거래 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT id, symbol, pnl, created_at FROM trades ORDER BY created_at DESC LIMIT 10;"

# 2. Kill Switch 이벤트 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT * FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 5;"

# 3. 마진비율 추이 확인
# Grafana: Margin Ratio 그래프
```

### API 레이턴시 높음

```bash
# 1. 거래소 상태 확인
curl -I https://api.bybit.com/v5/market/time

# 2. 네트워크 확인
ping api.bybit.com
traceroute api.bybit.com

# 3. 로컬 네트워크 부하 확인
docker stats --no-stream
```

---

## 모니터링 체크리스트 (일일)

```markdown
매일 아침 확인:
- [ ] `/status` 실행 (Equity 확인)
- [ ] 대시보드 Portfolio P&L http://localhost:3000/monitor (음수 추세 있나?)
- [ ] 대시보드 Kill Switch Events (발동 있었나?)
- [ ] Telegram 알림 수신 (밤새 장애 없었나?)

필요 시 조치:
- [ ] Kill Switch 발동 → 원인 분석
- [ ] Margin < 10x → 포지션 축소 검토
- [ ] API 장애 → 거래소 상태 확인
```

---

## 긴급 상황 대응

### Margin Ratio < 5x (긴급)

```bash
# 1. 즉시 포지션 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT * FROM positions WHERE status='open';"

# 2. 수동 포지션 축소 또는 청산
# Bybit 웹 → [선물] → [포지션] → [조정]

# 3. 모니터링 강화
watch -n 30 'docker compose logs --tail=10 funding-arb'
```

### Kill Switch 연속 발동

```bash
# 1. 원인 분석
docker compose logs --since=2h funding-arb execution-engine | grep -E "ERROR|kill|switch"

# 2. 시스템 상태 확인
docker compose ps
docker compose exec postgres pg_isready
docker compose exec redis redis-cli ping

# 3. 시스템 재시작
docker compose restart strategy-orchestrator funding-arb
```

---

## 관련 문서

- [runbook.md](runbook.md) — 운영 매뉴얼 (일상 운영)
- [../kill-switch.md](../kill-switch.md) — Kill Switch 정책
- [mainnet-switch.md](mainnet-switch.md) — 메인넷 모니터링 (Phase 5)
