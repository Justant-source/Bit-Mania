# Legacy Optimizer Scripts (v4~v7)

**Deprecated.** 이 스크립트들은 SQLite 큐(`queue.sqlite3`) 기반이며 Phase A(2026-05-17)에서 구축한 PG-native 파이프라인(`backtest/scripts/optimization/pg_*`)으로 완전 대체되었다.

현재 환경(`backtest-postgres` PostgreSQL)에서는 실행 불가 — 이력·알고리즘 참조용 보존.

## 파일 목록

| 파일 | 설명 |
|---|---|
| `v4_generate_grid.py` | 216 combo SQLite 큐 생성 |
| `v4_worker.py` | SQLite WAL 기반 워커 |
| `v4_master.py` | ProcessPoolExecutor + v4_aggregate 호출 |
| `v4_aggregate.py` | 집계, Tier 1-4, 이중 랭킹 |
| `v5_{...}.py` | v5 (324 combo), plateau 추가 |
| `v5_2_{...}.py` | v5_2 (1296 combo), recent window, 3-way cross-val |
| `v6_{...}.py` | v6 (225 combo), 비대칭 TP/SL, Tier A/B/C |
| `v7_leverage_sweep.py` | v7 레버리지 비교 (3x archive 확정 스윕) |

## 대체 사용법

신규 스윕은 PG 파이프라인으로:

```bash
DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
$DC python3 /app/scripts/optimization/pg_generate_grid.py --sweep <id> --grid-json '...'
$DC python3 /app/scripts/optimization/pg_master.py --sweep <id> --workers 6
```
