---
last_updated: 2026-05-02
---

# Prometheus 메트릭 감사 결과

감사일: 2026-05-02

## 핵심 메트릭 수집 상태

| 메트릭 | 상태 | Series 수 | 비고 |
|---|---|---|---|
| node_cpu_seconds_total | ✓ 수집 중 | 64 | CPU 성능 모니터링 가능 |
| node_memory_MemAvailable_bytes | ✓ 수집 중 | 1 | 메모리 압박 모니터링 가능 |
| node_filesystem_avail_bytes | ✓ 수집 중 | 1 | 디스크 공간 감시 가능 |
| redis_memory_used_bytes | ✓ 수집 중 | 1 | Redis 메모리 모니터링 가능 |

## 감사 상세

### 1. CPU 메트릭 (node_cpu_seconds_total)
- **상태**: 완벽
- **Series**: 64개 (멀티코어 시스템, 모드별 분류)
- **용도**: Operations 대시보드 — CPU 사용률 계산
- **PromQL**:
  ```promql
  100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
  ```

### 2. 메모리 메트릭 (node_memory_MemAvailable_bytes)
- **상태**: 완벽
- **Series**: 1개
- **용도**: Operations 대시보드 — 메모리 압박도 표시
- **PromQL**:
  ```promql
  (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
  ```

### 3. 디스크 메트릭 (node_filesystem_avail_bytes)
- **상태**: 완벽
- **Series**: 1개 (루트 마운트)
- **용도**: Operations 대시보드 — 디스크 사용률 경고
- **PromQL**:
  ```promql
  (node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"})
    / node_filesystem_size_bytes{mountpoint="/"} * 100
  ```

### 4. Redis 메모리 (redis_memory_used_bytes)
- **상태**: 완벽
- **Series**: 1개
- **용도**: Operations 대시보드 — Redis 메모리 효율성
- **PromQL**:
  ```promql
  redis_memory_used_bytes / redis_memory_max_bytes * 100
  ```

## Operations 대시보드 영향

모든 핵심 메트릭이 정상 수집 중입니다.

### 인프라 헬스 패널 구성
- **Top Left**: CPU 사용률 (Gauge)
- **Top Right**: 메모리 사용률 (Gauge)
- **Bottom Left**: 디스크 사용률 (Gauge)
- **Bottom Right**: Redis 메모리 (Gauge)

### 알람 임계값 (권장)
| 메트릭 | Warning | Critical |
|---|---|---|
| CPU | 70% | 85% |
| Memory | 75% | 90% |
| Disk | 80% | 95% |
| Redis | 80% | 95% |

## Prometheus 엔드포인트
- **URL**: http://localhost:9090
- **node-exporter**: http://node-exporter:9100/metrics
- **redis-exporter**: Docker Compose 내장 (포트 9121)
- **prometheus.yml**: `cryptoengine/config/prometheus/prometheus.yml`

## 추가 메트릭 발견

감사 중 다음 메트릭들도 수집 중 확인:
- `node_load1`, `node_load5`, `node_load15` — 시스템 로드
- `redis_commands_processed_total` — Redis 처리량
- `redis_connected_clients` — 동시 연결 수

이들은 향후 Advanced 대시보드에 활용 가능합니다.

## 감사 결론

**전체 상태: ✓ PASS**

Wave 2 대시보드(Task 3~6)에 필요한 모든 Prometheus 메트릭이 정상 수집 중입니다.
Operations 대시보드(Task 6)는 즉시 구성 가능합니다.
