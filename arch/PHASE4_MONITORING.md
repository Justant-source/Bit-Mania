# Phase 4 — 테스트넷 모니터링 가이드

> 목표: 테스트넷에서 실제 자동매매 시작 + 7개 시나리오 검증  
> 기간: Phase 4 완료 기준 — 최소 7일 무중단 운영 후 Phase 5 진행  
> 전제: `BYBIT_TESTNET=true` 유지 (Phase 5 진입 전까지 절대 변경 금지)

---

## 일일 체크 (매일 아침)

### 1. 전체 서비스 상태 확인

```bash
docker compose ps
```

기대 상태: 모든 서비스 `running (healthy)` 또는 `running`. `Restarting` 상태 서비스가 있으면 즉시 로그 확인.

```bash
# 특정 서비스 로그 확인
docker compose logs --tail=100 funding-arb
docker compose logs --tail=100 market-data
docker compose logs --tail=100 execution-engine
docker compose logs --tail=100 strategy-orchestrator
```

---

### 2. 오픈 포지션 확인

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT strategy_id, symbol, side, quantity, entry_price, status, opened_at FROM positions WHERE status='open';"
```

확인 포인트:
- `funding_arb` 전략의 BTCUSDT 포지션이 delta-neutral(spot long + perp short)인지
- 포지션 크기가 설정값 범위 내인지 (equity 5%, 최대 10,000 USD)

---

### 3. Kill Switch 이력

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT level, reason, triggered_at, resolved_at FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 5;"
```

확인 포인트:
- `resolved_at` 이 NULL 인 행이 있으면 Kill Switch 활성 상태 — 즉시 원인 파악 필요
- Level 4 (MANUAL) 발동 이력이 있으면 해제 전 반드시 포지션 점검

---

### 4. 펀딩비 수취 이력

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT strategy_id, symbol, rate, payment, position_size, collected_at FROM funding_payments ORDER BY collected_at DESC LIMIT 10;"
```

확인 포인트:
- 8h마다 수취 기록이 쌓이는지 (Bybit 펀딩비 지급 주기)
- `payment` 값이 양수인지 (음수면 펀딩비 지급 측)

---

### 5. 일별 성과 확인

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT date, total_pnl, funding_income, drawdown_pct, sharpe_ratio FROM daily_reports ORDER BY date DESC LIMIT 7;"
```

목표 지표:
- Sharpe Ratio >= 2.0
- 일일 최대 드로다운 < 1.0%
- 주간 드로다운 < 3.0%

---

### 6. 헬스체크 스크립트 실행

```bash
docker compose --profile backtest run --rm backtester python scripts/phase4_health_check.py
```

8개 항목을 자동 점검하고 `PASS/FAIL` 결과를 출력합니다.  
모든 항목 PASS 확인 후 당일 운영 계속 진행.

---

### 7. 레짐 대시보드 확인

```bash
# 브라우저에서 확인
open http://localhost:3000/regime
```

확인 포인트:
- 현재 레짐 표시 (confirmed_regime: trending/ranging/volatile)
- 5분 타임라인에서 raw 레짐 추이
- 확정 레짐 전환 이력 (regime_transitions 테이블)
- 오케스트레이터 가중치 전환 진행률 (5-cycle EMA)

---

### 8. 포지션 정합성 (Reconciliation) 확인

```bash
docker compose logs execution-engine | grep "reconcile"
```

확인 포인트:
- 10분마다 `POSITION_RECONCILE_OK` 로그 확인
- `POSITION_RECONCILE_MISMATCH` 발생 시 즉시 원인 파악
- `position:reconcile_event` Redis 채널로 불일치 세부정보 발행됨

---

### 9. 백업 상태 확인

```bash
make backup-list
```

확인 포인트:
- 당일 자동 백업 파일(02:00 KST) 존재 여부
- 7일 이내 백업 7개 이상 유지

---

## 7개 시나리오 체크리스트

Phase 4에서 아래 7개 시나리오를 모두 검증해야 Phase 5로 진행 가능합니다.

### 시나리오 1: 정상 진입/청산 사이클
- [ ] 펀딩비 >= 0.005% (연환산 ~15%) 조건 충족 시 자동 진입
- [ ] Spot long + Perp short 동시 체결 확인
- [ ] 포지션 오픈 후 `positions` 테이블에 기록 확인
- [ ] 펀딩비 역전 또는 수렴 시 자동 청산 확인

검증 쿼리:
```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT * FROM trades WHERE strategy_id='funding_arb' ORDER BY executed_at DESC LIMIT 20;"
```

---

### 시나리오 2: 펀딩비 수취 정상 동작
- [ ] 8h마다 funding_payments 테이블에 기록 생성
- [ ] Bybit 테스트넷 계정 잔고와 수취액 일치 여부 육안 확인 (Bybit 웹에서 확인)
- [ ] 연환산 수익률이 예상 범위(15~30%) 내인지 확인

검증 쿼리:
```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT DATE(collected_at) as day, SUM(payment) as daily_income, COUNT(*) as collections FROM funding_payments GROUP BY DATE(collected_at) ORDER BY day DESC LIMIT 7;"
```

---

### 시나리오 3: One-side Fill 복구 (편향 체결 대응)
- [ ] 한 쪽 레그만 체결되는 상황이 발생하면 3분 대기 후 자동 취소/복구
- [ ] 로그에서 `one_side_fill_recovery` 이벤트 확인
- [ ] 복구 후 포지션 잔여물이 없는지 확인 (`positions` WHERE status='partial')

검증 방법:
```bash
docker compose logs funding-arb | grep "one_side_fill_recovery"
```

---

### 시나리오 4: Kill Switch 발동 및 해제
- [ ] Telegram `/kill` 명령으로 Level 4 (MANUAL) Kill Switch 수동 발동
- [ ] 발동 즉시 모든 포지션 청산 확인
- [ ] `kill_switch_events` 테이블에 이벤트 기록 확인
- [ ] Telegram에서 알림 메시지 수신 확인
- [ ] `/resume` 명령으로 정상 재개 확인

검증 쿼리:
```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT * FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 5;"
```

---

### 시나리오 5: 드로다운 임계값 자동 정지
- [ ] Portfolio drawdown >= 1% (일일) 시 Level 2 Kill Switch 자동 발동 확인
- [ ] 발동 후 4h 쿨다운 후 자동 재개 확인 (또는 수동 재개)
- [ ] Telegram 알림으로 발동/해제 알림 수신 확인

트리거 임계값 확인:
```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT date, drawdown_pct FROM daily_reports ORDER BY date DESC LIMIT 7;"
```

---

### 시나리오 6: Windows 재시작 후 자동 복구
- [ ] Windows/WSL 재시작 후 `docker compose ps` 로 모든 서비스 자동 기동 확인
- [ ] `restart: always` 정책으로 모든 핵심 서비스 재시작 확인
- [ ] 포지션 상태가 DB에서 정상 복구되는지 확인
- [ ] market-data WebSocket 재연결 확인 (로그에서 `reconnected` 메시지)

검증 방법:
```bash
# 재시작 후 1분 내 실행
docker compose ps
docker compose logs --tail=50 market-data | grep -E "connect|start|error"
```

---

### 시나리오 7: Telegram 알림 정상 동작
- [ ] 포지션 진입/청산 시 Telegram 메시지 수신
- [ ] 펀딩비 수취 시 알림 수신
- [ ] Kill Switch 발동 시 긴급 알림 수신
- [ ] 일별 성과 리포트 수신 (설정된 시간에)
- [ ] `/status` 명령으로 현재 상태 조회 가능

테스트 방법: Telegram 봇에 `/status` 전송 후 응답 확인

---

## 주간 체크

### 성과 집계

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT date, total_pnl, funding_income, drawdown_pct, sharpe_ratio FROM daily_reports ORDER BY date DESC LIMIT 7;"
```

### 포지션 히스토리

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT strategy_id, symbol, side, entry_price, exit_price, pnl, opened_at, closed_at FROM positions WHERE status='closed' ORDER BY closed_at DESC LIMIT 20;"
```

### 전체 펀딩비 수익

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT symbol, SUM(payment) as total_income, COUNT(*) as total_collections, AVG(rate) as avg_rate FROM funding_payments WHERE collected_at >= NOW() - INTERVAL '7 days' GROUP BY symbol;"
```

---

## 비상 대응 절차

### 즉시 정지가 필요한 경우

```bash
# 방법 1: Telegram에서 /kill 전송 (권장)
# 방법 2: make 명령어
make emergency

# 방법 3: 직접 명령
docker compose stop funding-arb adaptive-dca execution-engine
```

### 특정 서비스만 재시작

```bash
docker compose up -d --no-deps --build funding-arb
docker compose up -d --no-deps --build market-data
```

### 전체 스택 재시작

```bash
docker compose down
docker compose up -d
```

---

## Phase 5 진입 조건

아래 조건을 모두 충족해야 소액 실전(Phase 5)으로 진행합니다.

- [ ] 7개 시나리오 체크리스트 전항목 완료
- [ ] 7일 이상 무중단 운영 (예상치 못한 Restarting 없음)
- [ ] 일일 최대 드로다운 < 1.0% 유지
- [ ] Sharpe Ratio >= 2.0 (백테스트 기준 일치)
- [ ] Kill Switch 자동 발동 후 정상 재개 1회 이상 확인
- [ ] Telegram 모든 알림 유형 수신 확인
- [ ] `BYBIT_TESTNET=false` 전환 명시적 승인 후 진행

---

---

## Phase 5 진입 전 최종 점검: `phase5_preflight.py`

```bash
docker compose --profile backtest run --rm backtester python scripts/phase5_preflight.py
```

8개 항목 자동 점검:
1. 환경변수 (`BYBIT_TESTNET`, API 키 존재 여부)
2. Bybit API 연결
3. 계정 잔고
4. 수수료 등급 확인
5. 최소 주문 크기 확인
6. 레버리지 설정 일치
7. 오픈 포지션 상태
8. DB 연결 및 최근 데이터

**모든 항목 PASS + 7개 시나리오 완료 후에만 `BYBIT_TESTNET=false` 전환 진행.**

---

*최종 업데이트: Phase 4 안전장치 및 모니터링 강화 완료 (2026-04-06)*
