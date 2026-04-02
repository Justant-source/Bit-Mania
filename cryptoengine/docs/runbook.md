# CryptoEngine 운영 매뉴얼 (Runbook)

## 목차

1. [시스템 시작/중지](#시스템-시작중지)
2. [일상 운영](#일상-운영)
3. [인시던트 대응](#인시던트-대응)
4. [Kill Switch 대응](#kill-switch-대응)
5. [데이터베이스 관리](#데이터베이스-관리)
6. [모니터링](#모니터링)
7. [문제 해결](#문제-해결)

---

## 시스템 시작/중지

### 전체 시스템 시작

```bash
# 1. 환경변수 확인
cp .env.example .env
# .env 파일에 실제 값 입력

# 2. 데이터베이스 초기화 (최초 1회)
python scripts/init_db.py

# 3. 히스토리 데이터 시드 (선택)
python scripts/seed_historical.py --months 6

# 4. 시스템 시작
docker compose up -d

# 5. 상태 확인
python scripts/health_check.py
```

### 전체 시스템 중지

```bash
# 그레이스풀 종료 (포지션 유지)
docker compose down

# 긴급 종료 (데이터 손실 가능)
docker compose kill
```

### 개별 서비스 재시작

```bash
docker compose restart market-data
docker compose restart execution-engine
docker compose restart strategy-orchestrator
```

---

## 일상 운영

### 매일 확인 사항

1. **시스템 상태 확인**
   ```bash
   python scripts/health_check.py
   ```

2. **텔레그램으로 상태 확인**
   - `/status` — 현재 포트폴리오 상태
   - `/positions` — 열린 포지션 목록

3. **Grafana 대시보드 확인** (http://localhost:3002)
   - 포트폴리오 추이
   - 전략별 P&L
   - Kill Switch 이벤트

4. **로그 확인**
   ```bash
   docker compose logs --tail=100 strategy-orchestrator
   docker compose logs --tail=100 execution-engine
   ```

### 주간 확인 사항

1. **백테스트 실행**
   ```bash
   docker compose run --rm backtester \
     python main.py --strategy combined --walk-forward
   ```

2. **거래 내역 내보내기**
   ```bash
   python scripts/export_trades.py \
     --start $(date -d '7 days ago' +%Y-%m-%d) \
     -o weekly_trades.csv
   ```

3. **디스크/메모리 사용량 확인**
   ```bash
   python scripts/health_check.py --json | jq '.checks[] | select(.name == "Disk Space" or .name == "Memory")'
   ```

### 월간 확인 사항

1. **월간 리포트 생성**
   ```bash
   python scripts/generate_monthly_report.py
   ```

2. **데이터베이스 정리**
   ```sql
   -- 90일 이전 OHLCV 히스토리 삭제
   DELETE FROM ohlcv_history WHERE timestamp < NOW() - INTERVAL '90 days';
   -- VACUUM
   VACUUM ANALYZE;
   ```

---

## 인시던트 대응

### 인시던트 레벨 정의

| 레벨 | 정의 | 대응 시간 | 예시 |
|------|------|-----------|------|
| P1 | 자금 손실 위험 | 즉시 | 거래소 API 장애 중 포지션 존재 |
| P2 | 기능 장애 | 1시간 | 전략 서비스 다운 |
| P3 | 성능 저하 | 4시간 | 레이턴시 증가 |
| P4 | 정보성 | 24시간 | 로그 경고 |

### P1 대응 절차

```
1. 텔레그램에서 /kill 실행 (모든 포지션 즉시 청산)
2. 시스템 로그 확인
   docker compose logs --since=30m
3. 거래소 API 상태 확인
   curl https://api.bybit.com/v5/market/time
4. 포지션 수동 확인 (Bybit 웹사이트)
5. 원인 파악 후 조치
6. 시스템 재시작
   docker compose restart
7. /resume 명령으로 거래 재개
8. 인시던트 보고서 작성
```

### P2 대응 절차

```
1. 영향 받는 서비스 확인
   docker compose ps
2. 서비스 재시작
   docker compose restart <서비스명>
3. 로그 확인
   docker compose logs --tail=200 <서비스명>
4. 포지션 영향 확인
   /positions 텔레그램 명령
5. 정상화 확인
   python scripts/health_check.py
```

---

## Kill Switch 대응

### L1 — 전략 레벨

- **증상**: 개별 전략 최대 낙폭 초과
- **동작**: 해당 전략만 중지, 포지션 청산
- **복구**: 쿨다운(4시간) 후 자동 재개
- **확인**:
  ```bash
  docker compose logs strategy-orchestrator | grep "kill-switch"
  ```

### L2 — 포트폴리오 레벨

- **증상**: 일일 -5%, 주간 -10%, 월간 -15% 낙폭
- **동작**: 전체 전략 중지, 모든 포지션 청산
- **복구**: 쿨다운(1시간) 후 자동 재개
- **조치**:
  1. 텔레그램 알림 확인
  2. 포트폴리오 상태 확인: `/status`
  3. 원인 분석 (시장 급락? 전략 오작동?)
  4. 필요 시 설정 조정

### L3 — 시스템 레벨

- **증상**: API 오류 다발, 인프라 장애
- **동작**: 시장가로 전량 청산
- **복구**: 쿨다운 후 자동 재개
- **조치**:
  1. 인프라 상태 확인: `python scripts/health_check.py`
  2. 거래소 API 상태 확인
  3. 네트워크 연결 확인
  4. Docker 컨테이너 상태 확인

### L4 — 수동 긴급

- **증상**: 운영자가 `/kill` 명령 실행
- **동작**: 즉시 전량 청산
- **복구**: 수동 `/resume`만 가능 (자동 복구 불가)
- **조치**:
  1. 상황 파악
  2. 문제 해결
  3. 텔레그램에서 `/resume` 실행

---

## 데이터베이스 관리

### 백업

```bash
# 일일 백업 (cronjob 권장)
pg_dump -U cryptoengine -d cryptoengine \
  -F c -f /backup/cryptoengine_$(date +%Y%m%d).dump

# 복원
pg_restore -U cryptoengine -d cryptoengine \
  /backup/cryptoengine_20260401.dump
```

### 마이그레이션

```bash
cd shared/db/migrations
alembic upgrade head
alembic history
```

### 성능 최적화

```sql
-- 인덱스 상태 확인
SELECT relname, indexrelname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- 테이블 크기 확인
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```

---

## 모니터링

### Grafana 알림 설정

1. 포트폴리오 낙폭 > 3% → 텔레그램 알림
2. API 오류 > 10건/분 → 텔레그램 알림
3. 서비스 다운 → 텔레그램 알림

### 핵심 메트릭

| 메트릭 | 정상 범위 | 경고 임계값 |
|--------|-----------|-------------|
| 일일 수익률 | -2% ~ +5% | < -3% |
| Sharpe 30일 | > 1.0 | < 0.5 |
| API 레이턴시 | < 200ms | > 1000ms |
| 미체결 주문 | < 10개 | > 30개 |
| 메모리 사용 | < 70% | > 85% |
| 디스크 사용 | < 80% | > 90% |

### 로그 레벨

```
DEBUG: 개발 환경만
INFO: 기본 운영 로그
WARNING: 주의 필요 (자동 복구 가능)
ERROR: 오류 발생 (수동 개입 필요 가능)
CRITICAL: Kill Switch 발동, 즉시 대응 필요
```

---

## 문제 해결

### 서비스가 시작되지 않는 경우

```bash
# 1. 컨테이너 상태 확인
docker compose ps

# 2. 로그 확인
docker compose logs <서비스명>

# 3. 의존성 서비스 확인 (PostgreSQL, Redis)
docker compose logs postgres
docker compose logs redis

# 4. 환경변수 확인
docker compose config
```

### 주문이 체결되지 않는 경우

1. Execution Engine 로그 확인
2. Bybit API 상태 확인
3. API 키 잔여 한도 확인
4. 잔고 확인: `/balance`

### 레짐 감지 오류

1. Market Data 서비스 로그 확인
2. OHLCV 데이터 수집 상태 확인
3. Redis에 저장된 레짐 확인:
   ```bash
   redis-cli GET market:regime:current
   ```

### 데이터베이스 연결 실패

```bash
# 1. PostgreSQL 상태 확인
docker compose exec postgres pg_isready

# 2. 연결 테스트
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c "SELECT 1"

# 3. 연결 풀 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT count(*) FROM pg_stat_activity WHERE datname = 'cryptoengine'"
```

---

## 비상 연락처

| 역할 | 연락처 | 대응 범위 |
|------|--------|-----------|
| 시스템 운영자 | 텔레그램 봇 | P1-P4 |
| 거래소 지원 | Bybit 서포트 | API 장애 |
