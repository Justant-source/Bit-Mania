---
last_updated: 2026-05-02
---

# SQL 쿼리 라이브러리

Wave 2 대시보드(Task 3~6)가 참조하는 SQL 쿼리 표준 모음.
각 섹션의 쿼리는 대시보드 패널에서 직접 사용 가능.

---

## A. Overview 대시보드용 (Task 3)

### A.1 Hero KPI

#### A.1.1 총 자산 (USD) + 어제 대비 변동
```sql
WITH latest AS (
  SELECT total_equity, snapshot_at
  FROM portfolio_snapshots
  ORDER BY snapshot_at DESC LIMIT 1
),
yesterday AS (
  SELECT total_equity
  FROM portfolio_snapshots
  WHERE snapshot_at <= NOW() - INTERVAL '24 hours'
  ORDER BY snapshot_at DESC LIMIT 1
)
SELECT
  l.total_equity AS current_equity,
  l.total_equity - y.total_equity AS delta_24h_usd,
  ((l.total_equity - y.total_equity) / NULLIF(y.total_equity, 0)) * 100 AS delta_24h_pct
FROM latest l, yesterday y;
```

#### A.1.2 일일 PnL
```sql
SELECT
  daily_return AS daily_return_pct,
  daily_pnl AS daily_pnl_usd
FROM daily_reports
WHERE date = CURRENT_DATE
LIMIT 1;
```

#### A.1.3 월 누적 PnL (MTD)
```sql
WITH month_start AS (
  SELECT total_equity AS start_equity
  FROM portfolio_snapshots
  WHERE snapshot_at >= date_trunc('month', CURRENT_DATE)
  ORDER BY snapshot_at ASC LIMIT 1
),
current_val AS (
  SELECT total_equity AS current_equity
  FROM portfolio_snapshots
  ORDER BY snapshot_at DESC LIMIT 1
)
SELECT
  c.current_equity - m.start_equity AS mtd_pnl_usd,
  ((c.current_equity - m.start_equity) / NULLIF(m.start_equity, 0)) * 100 AS mtd_pnl_pct
FROM month_start m, current_val c;
```

#### A.1.4 미실현 PnL + 오픈 포지션 수
```sql
SELECT
  COALESCE(SUM(unrealized_pnl), 0) AS total_unrealized_pnl,
  COUNT(*) AS open_position_count
FROM positions
WHERE closed_at IS NULL;
```

### A.2 Risk Status (P0 핵심)

#### A.2.1 청산까지 거리 (가장 위험한 포지션)
```sql
SELECT
  symbol,
  side,
  entry_price,
  current_price,
  CASE
    WHEN side = 'long' THEN
      ((current_price - liquidation_price) / NULLIF(current_price, 0)) * 100
    WHEN side = 'short' THEN
      ((liquidation_price - current_price) / NULLIF(current_price, 0)) * 100
  END AS liq_distance_pct
FROM positions
WHERE closed_at IS NULL
ORDER BY liq_distance_pct ASC NULLS LAST
LIMIT 1;
```
_주의: liquidation_price 컬럼이 없음. 계산식: liq_price = entry_price * (1 + (margin / size))_

#### A.2.2 마진 사용률 (전체 포지션 합산)
```sql
WITH margin_calc AS (
  SELECT
    COALESCE(SUM(unrealized_pnl), 0) as total_unrealized_pnl,
    (SELECT total_equity FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1) AS total_equity
  FROM positions
  WHERE closed_at IS NULL
)
SELECT
  CASE WHEN total_equity > 0 THEN 
    (total_unrealized_pnl / total_equity) * 100
  ELSE NULL END AS margin_ratio_pct
FROM margin_calc;
```

#### A.2.3 일일 손실 한도 진행률 (Phase 5: $-10 한도)
```sql
WITH today_pnl AS (
  SELECT
    (SELECT total_equity FROM portfolio_snapshots
     WHERE snapshot_at::date = CURRENT_DATE 
     ORDER BY snapshot_at ASC LIMIT 1) AS day_start,
    (SELECT total_equity FROM portfolio_snapshots
     ORDER BY snapshot_at DESC LIMIT 1) AS current_eq
)
SELECT
  current_eq - day_start AS today_pnl_usd,
  CASE
    WHEN current_eq - day_start < 0 THEN
      ABS(current_eq - day_start) / 10.0 * 100
    ELSE 0
  END AS loss_limit_progress_pct
FROM today_pnl;
```

#### A.2.4 Kill Switch 상태
```sql
SELECT level
FROM kill_switch_events
ORDER BY triggered_at DESC LIMIT 1;
```

---

## B. Performance & Risk 대시보드용 (Task 4)

### B.1 자산 곡선 + 어노테이션

#### B.1.1 자산 곡선 본체
```sql
SELECT
  snapshot_at AS time,
  total_equity AS equity
FROM portfolio_snapshots
WHERE snapshot_at >= $__timeFrom
  AND snapshot_at <= $__timeTo
ORDER BY snapshot_at;
```

#### B.1.2 Kill Switch 어노테이션
```sql
SELECT
  triggered_at AS time,
  CONCAT('Kill Switch L', level, ': ', reason) AS text,
  CONCAT('level_', level) AS tags
FROM kill_switch_events
WHERE triggered_at >= $__timeFrom
  AND triggered_at <= $__timeTo;
```

#### B.1.3 레짐 전환 어노테이션
```sql
SELECT
  detected_at AS time,
  CONCAT(from_regime, ' → ', to_regime) AS text,
  to_regime AS tags
FROM regime_transitions
WHERE detected_at >= $__timeFrom
  AND detected_at <= $__timeTo;
```

### B.2 분포 분석

#### B.2.1 일일 수익률 히스토그램용 데이터
```sql
SELECT daily_return AS daily_return_pct
FROM daily_reports
WHERE date >= $__timeFrom::date
  AND date <= $__timeTo::date
ORDER BY date;
```

#### B.2.2 레짐별 PnL 분해
```sql
WITH trade_with_regime AS (
  SELECT
    t.created_at,
    t.pnl,
    (SELECT to_regime FROM regime_transitions
     WHERE detected_at <= t.created_at
     ORDER BY detected_at DESC LIMIT 1) AS regime
  FROM trades t
  WHERE t.filled_at IS NOT NULL
    AND t.created_at >= $__timeFrom
    AND t.created_at <= $__timeTo
)
SELECT
  COALESCE(regime, 'unknown') AS regime,
  COUNT(*) AS trade_count,
  COALESCE(SUM(pnl), 0) AS total_pnl,
  COALESCE(AVG(pnl), 0) AS avg_pnl,
  CASE 
    WHEN COUNT(*) > 0 THEN
      SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100
    ELSE 0 
  END AS win_rate_pct
FROM trade_with_regime
GROUP BY regime
ORDER BY total_pnl DESC;
```

#### B.2.3 수수료 vs 펀딩수익 비교
```sql
SELECT
  date_trunc('day', f.collected_at) AS day,
  COALESCE(SUM(f.payment), 0) AS funding_received,
  COALESCE(SUM(t.fee), 0) AS fees_paid,
  COALESCE(SUM(f.payment), 0) - COALESCE(SUM(t.fee), 0) AS net
FROM funding_payments f
FULL OUTER JOIN trades t ON date_trunc('day', f.collected_at) = date_trunc('day', t.created_at)
WHERE f.collected_at >= $__timeFrom
  AND f.collected_at <= $__timeTo
GROUP BY day
ORDER BY day;
```

#### B.2.4 위험 지표 (Sharpe / Sortino / Calmar)
```sql
WITH returns AS (
  SELECT (daily_return / 100.0) AS r
  FROM daily_reports
  WHERE date >= CURRENT_DATE - INTERVAL '30 days'
),
stats AS (
  SELECT
    AVG(r) AS avg_return,
    STDDEV(r) AS std_dev,
    STDDEV(CASE WHEN r < 0 THEN r END) AS downside_std,
    MAX(0 - r) AS max_drawdown
  FROM returns
)
SELECT
  CASE WHEN std_dev > 0 THEN (avg_return / std_dev) * SQRT(365) ELSE 0 END AS sharpe_30d,
  CASE WHEN downside_std > 0 THEN (avg_return / downside_std) * SQRT(365) ELSE 0 END AS sortino_30d,
  CASE WHEN max_drawdown > 0 THEN (avg_return * 365) / max_drawdown ELSE 0 END AS calmar_30d
FROM stats;
```

---

## C. Strategies & Positions 대시보드용 (Task 5)

### C.1 전략 KPI 테이블
```sql
WITH strategy_pnl AS (
  SELECT
    strategy_id,
    COALESCE(SUM(pnl), 0) AS total_pnl,
    COUNT(*) AS trade_count
  FROM trades
  WHERE filled_at IS NOT NULL
  GROUP BY strategy_id
),
strategy_state AS (
  SELECT DISTINCT ON (strategy_id)
    strategy_id, allocated_capital, current_pnl
  FROM strategy_states
  ORDER BY strategy_id, updated_at DESC
)
SELECT
  s.strategy_id,
  COALESCE(s.allocated_capital, 0) AS allocated_capital,
  COALESCE(s.current_pnl, 0) AS unrealized_pnl,
  COALESCE(p.total_pnl, 0) AS realized_pnl_total,
  COALESCE(p.trade_count, 0) AS trade_count
FROM strategy_state s
LEFT JOIN strategy_pnl p ON s.strategy_id = p.strategy_id
ORDER BY s.strategy_id;
```

### C.2 오픈 포지션 (청산 위험 컬럼 포함, P0)
```sql
SELECT
  symbol,
  side,
  size,
  entry_price,
  current_price,
  unrealized_pnl,
  CASE WHEN unrealized_pnl IS NOT NULL THEN
    (unrealized_pnl / size / entry_price * leverage) * 100
  ELSE NULL END AS unrealized_pnl_pct,
  leverage,
  EXTRACT(EPOCH FROM (NOW() - opened_at)) / 3600 AS holding_hours
FROM positions
WHERE closed_at IS NULL
ORDER BY unrealized_pnl ASC NULLS LAST;
```

### C.3 FA 진입 조건 5개 (체크리스트)
```sql
-- 주의: fa_entry_conditions 테이블이 없음
-- 대신 현재 포지션의 진입 시간과 마켓 조건을 추론
SELECT
  symbol,
  'Position Entry' AS condition_name,
  opened_at AS check_time,
  unrealized_pnl AS current_state,
  'entry_conditions' AS status
FROM positions
WHERE closed_at IS NULL;
```

---

## D. Operations 대시보드용 (Task 6)

### D.1 서비스 헬스 (마지막 업데이트 경과 시간)
```sql
WITH last_seen AS (
  SELECT
    service,
    MAX(timestamp) AS last_event
  FROM service_logs
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  GROUP BY service
)
SELECT
  service,
  EXTRACT(EPOCH FROM (NOW() - last_event)) AS seconds_since_last_event,
  CASE 
    WHEN EXTRACT(EPOCH FROM (NOW() - last_event)) < 60 THEN 'healthy'
    WHEN EXTRACT(EPOCH FROM (NOW() - last_event)) < 300 THEN 'warning'
    ELSE 'critical'
  END AS health_status
FROM last_seen
ORDER BY seconds_since_last_event DESC;
```

### D.2 인프라 메트릭 (Prometheus PromQL)
```promql
# CPU 사용률
100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)

# 메모리 사용률
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100

# 디스크 사용률
(node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"})
  / node_filesystem_size_bytes{mountpoint="/"} * 100

# Redis 메모리
redis_memory_used_bytes / redis_memory_max_bytes * 100
```

### D.3 LogWriter 큐 모니터링
```sql
SELECT
  COUNT(*) AS total_logs_last_hour,
  SUM(CASE WHEN level_no >= 40 THEN 1 ELSE 0 END) AS error_count,
  ROUND(SUM(CASE WHEN level_no >= 40 THEN 1 ELSE 0 END)::float / COUNT(*) * 100, 2) AS error_pct
FROM service_logs
WHERE timestamp > NOW() - INTERVAL '1 hour';
```

### D.4 OHLCV 수집 갭 검사
```sql
WITH expected AS (
  SELECT generate_series(
    date_trunc('hour', NOW() - INTERVAL '24 hours'),
    date_trunc('hour', NOW()),
    '1 hour'::interval
  ) AS expected_hour
),
actual AS (
  SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS cnt
  FROM ohlcv
  WHERE timeframe = '1h'
    AND timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY hour
)
SELECT
  e.expected_hour AS time,
  COALESCE(a.cnt, 0) AS candles_collected,
  CASE
    WHEN COALESCE(a.cnt, 0) = 0 THEN 'gap'
    WHEN COALESCE(a.cnt, 0) < 1 THEN 'incomplete'
    ELSE 'ok'
  END AS status
FROM expected e
LEFT JOIN actual a ON e.expected_hour = a.hour
ORDER BY time;
```

---

## E. 공통 변수 (Template Variables)

### E.1 $time_range (interval 변수)
옵션: `1h, 6h, 24h, 7d, 30d, 90d`

### E.2 $symbol (포지션이 있는 심볼만)
```sql
SELECT DISTINCT symbol FROM positions WHERE closed_at IS NULL
UNION SELECT 'BTCUSDT' AS symbol
ORDER BY symbol;
```

### E.3 $strategy_id
```sql
SELECT DISTINCT strategy_id FROM strategy_states ORDER BY strategy_id;
```

---

## 검증 결과

| 쿼리 | 상태 | 비고 |
|---|---|---|
| A.1.1 | ✓ 검증 완료 | portfolio_snapshots.total_equity 확인 |
| A.1.2 | ✓ 검증 완료 | daily_reports.daily_return, daily_pnl 확인 |
| A.1.3 | ✓ 검증 완료 | MTD 계산 가능 |
| A.1.4 | ✓ 검증 완료 | positions.closed_at 조건 사용 |
| A.2.1 | ⚠ 부분 | liquidation_price 없음, 청산 거리 계산 필요 |
| A.2.2 | ✓ 검증 완료 | unrealized_pnl 기반 마진 비율 계산 |
| A.2.3 | ✓ 검증 완료 | 일일 손실 진행률 계산 가능 |
| A.2.4 | ✓ 검증 완료 | kill_switch_events.level 확인 |
| B.1.1 | ✓ 검증 완료 | portfolio_snapshots 시계열 |
| B.1.2 | ✓ 검증 완료 | kill_switch_events 어노테이션 |
| B.1.3 | ✓ 검증 완료 | regime_transitions 테이블 존재 |
| B.2.1 | ✓ 검증 완료 | daily_reports.daily_return |
| B.2.2 | ✓ 검증 완료 | regime_transitions + trades 조인 |
| B.2.3 | ✓ 검증 완료 | funding_payments + trades 조인 |
| B.2.4 | ✓ 검증 완료 | daily_reports 기반 통계 |
| C.1 | ✓ 검증 완료 | strategy_states + trades 조인 |
| C.2 | ✓ 검증 완료 | positions 스키마 확인 |
| C.3 | ⚠ 미구현 | fa_entry_conditions 테이블 없음 |
| D.1 | ✓ 검증 완료 | service_logs 시계열 |
| D.2 | ✓ 검증 완료 | Prometheus 메트릭 확인 |
| D.3 | ✓ 검증 완료 | service_logs 오류율 계산 |
| D.4 | ✓ 검증 완료 | ohlcv 테이블 시계열 검사 |
