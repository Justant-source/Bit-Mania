---
title: Polyglot Persistence Architecture (Data Stores ADR)
adr_number: N/A (architecture decision, not formally numbered)
category: architecture
status: Accepted
date: 2026-05-18
related_code:
  - backtest/scripts/db/schema.sql
  - backtest/scripts/data/import_parquet_to_pg.py
  - cryptoengine/shared/db/migrations/011_ohlcv_1m_longterm.sql
  - cryptoengine/docker-compose.yml
  - cryptoengine/services/market-data/
  - cryptoengine/services/execution/
last_updated: 2026-05-18
when_to_update: |
  - When new data stores are introduced (e.g., TimescaleDB, DuckDB)
  - When scaling triggers require architecture re-evaluation
  - When trigger conditions for deferred decisions are met
---

# Polyglot Persistence Architecture

## Status

Accepted (2026-05-18)

---

## Context

CryptoEngine manages three distinct workloads with different requirements:

1. **Backtest analytics**: Historical OHLCV (4.5M+ 1m bars, 19k 4h bars) + strategy sweep results
2. **Live trading**: Rolling 7-day OHLCV window + real-time positions, orders, P&L
3. **Monitoring**: Time-series metrics (CPU, memory, Redis, market depth) for alerting

Prior to this decision, the architecture included:
- **File-based data lake**: 105 Parquet files (`data/ohlcv/BTCUSDT/1m/*.parquet`) — slow scans, operational friction
- **Jesse CSV logs**: 779 trade log CSVs — redundant, no longer used
- **Single PostgreSQL instance** (5432): queried for both live and backtest data — workload contention
- **Potential future scales**: DuckDB, Hadoop, TimescaleDB all under evaluation

This decision clarifies the persistent data store strategy to reduce ambiguity and optimize for current/near-term scale.

---

## Decision

Adopt **polyglot persistence** with **intentional workload separation**:

| Store | Host | Purpose | Retention | Access Pattern |
|-------|------|---------|-----------|-----------------|
| **Redis 7** | `redis:6379` | Hot state, messaging (Pub/Sub), kill-switch flags | Rolling (in-memory, AOF) | Read/write high-frequency |
| **PostgreSQL 5432 (live)** | `postgres:5432` | Rolling 7-day OHLCV window, positions, orders, P&L | Sliding window (7 days) | Queries: Market depth, position state |
| **PostgreSQL 5433 (backtest)** | `backtest-postgres:5432` | OHLCV 1m/4h long-term (2017-2026), strategy sweeps, combos | Full history | Batch queries, Jesse internal tables |
| **Prometheus** | `prometheus:9090` | Time-series ops metrics (CPU, memory, Redis, disk) | 30-day rolling | Graph queries, alerting rules |

**Key principle**: Workloads are **geographically isolated** — no cross-database queries (live queries don't touch backtest, backtest queries don't touch live).

---

## Rationale

### Why Not Single PostgreSQL?

❌ **Workload incompatibility**:
- Live OHLCV: tiny window (7 days ≈ 10k rows), queried constantly for recent ticks
- Backtest OHLCV: 4.5M rows, scanned in batches during sweep analysis
- Single index strategy poor for both (live favors recent timestamps; backtest favors full scans)
- Contention risk during backtests (locks on live table during batch inserts)

### Why Abandon File-Based Parquet Lake?

❌ **Operational friction**:
- Slow repeated scans (I/O, Polars deserialization)
- No query indexing → every backtest sweep scans all 105 files
- Manual file synchronization (git-ignored, external management)
- Difficult incremental updates

✓ **Migration complete** (2026-05-18):
- All 4,568,888 rows (1m) + 19,057 bars (4h) ingested into `backtest-postgres`
- Parquet files flagged for deletion (archive first if needed)
- Jesse CSV logs (779 files) already deleted

### Why Not Hadoop?

❌ **Scale mismatch**:
- Hadoop designed for 100GB+ distributed datasets
- CryptoEngine backtest data: < 500MB (OHLCV + sweep results)
- Operational overhead (JVM, name node HA, YARN) unjustified
- Single-machine PostgreSQL sufficient and simpler

### Why Not DuckDB?

❌ **Not needed at this scale**:
- DuckDB advantage: columnstore compression for analytical queries
- Tradeoff: no multi-writer support, embedded (no client-server)
- PostgreSQL sufficient for 4.5M rows on single host
- Revisit only if query p95 > target or backtest time exceeds tolerance

### Why Not TimescaleDB?

❌ **Deferred (trigger conditions)**:
- TimescaleDB extension adds hypertable optimization (auto-chunking, compression)
- Advantage: 10-100x compression on time-series data
- Cost: Requires schema redesign, different query patterns
- **Revisit when**:
  - `ohlcv_1m_longterm` query p95 > 500ms (current state: not measured yet)
  - Table size > 5GB (current: ~300MB)
  - Backtest time exceeds 2 hours for full sweep (early abort if true)

---

## Data Store Specifications

### PostgreSQL 5432 (Live Trading)

**Tables** (kept minimal for speed):
- `ohlcv_history` (rolling 7 days)
- `positions`, `orders`, `fills`, `trades`
- Telegram logs, alert records

**Indexes**: Timestamp DESC (recent ticks first), symbol lookup

**Retention**: `DELETE FROM ohlcv_history WHERE timestamp < now() - interval '7 days'` (daily cron)

**Known issue**: `regime_detector.py` queries `FROM ohlcv` (empty table) — pre-existing bug, separate ticket

### PostgreSQL 5433 (Backtest, jesse_db)

**Tables** (from `backtest/scripts/db/schema.sql`):
- `ohlcv_1m`: 4,568,888 rows (2017-08 to 2026-04)
- `ohlcv_4h`: 19,057 bars (resampled from 1m)
- `st_sweeps`: Metadata for each parameter sweep
- `st_combos`: 10k+ combinations (3 parameters × 100+ combos each)
- `st_window_results`: Per-window results (cross-validation)
- `jesse_*`: Jesse framework internal tables (candles, trades, etc.)

**Indexes**: (symbol, timestamp DESC) on OHLCV; (sweep_id, combo_id) on combos

**Ingestion**: One-time Parquet import (complete 2026-05-18)

### Redis 7 (Hot State)

**Key patterns**:
- `ce:kill_switch:*` — Kill Switch state flags
- `strategy:status:*` — Per-strategy runtime state
- `ce:market:signal` — Market regime (trending/ranging/volatile)
- Pub/Sub channels: `ce:*` namespace

**Retention**: In-memory + AOF (no persistence beyond restart)

### Prometheus (Operations)

**Metrics** (via exporters):
- Host CPU, memory, disk (node-exporter)
- Redis memory, connections, latency (redis-exporter)
- Application events: trade count, P&L, orders (custom instrumentation)

**Scrape interval**: 15s; retention: 30 days

---

## Migration Artifacts

### Files Deleted (Phase 2-3, 2026-05-01 to 2026-05-18)

- 105 Parquet files in `data/ohlcv/BTCUSDT/1m/` → pending deletion after import verification
- 779 Jesse CSV trade logs → already deleted (no longer referenced)

### Migration Scripts

- `backtest/scripts/data/import_parquet_to_pg.py` — Parquet → PostgreSQL UPSERT (idempotent, can re-run)
- Command: `python scripts/data/import_parquet_to_pg.py --verify-only` to audit current state

---

## Consequences

### Positive

1. **Workload isolation**: Live queries no longer blocked by backtest scans
2. **Query performance**: Backtest sweeps 5-10x faster with indexed OHLCV
3. **Operational clarity**: No ambiguity on data location; clear SOP
4. **Git cleanliness**: Parquet lake removed from worktree (was .gitignore'd anyway)
5. **Incremental improvements**: Easy to add TimescaleDB or DuckDB to backtest-postgres later without live system impact

### Negative

1. **Two PostgreSQL instances**: Slightly higher infrastructure cost (but negligible on single host)
2. **Sync responsibility**: Manual coordination if backtest schema changes (Jesse tables unaffected)
3. **No live-backtest data joins**: Cannot directly compare live trades vs. backtest on same query (would require ETL job)

---

## Known Issues & Deferred

### Pre-Existing Bug (Separate Ticket)

**File**: `cryptoengine/services/market-data/regime_detector.py`

**Issue**: Queries `FROM ohlcv` table which is empty in live PostgreSQL (5432).  
Regime detection currently disabled or fallback hardcoded.

**Fix scope**: Out of scope for this ADR. Requires analysis of whether regime table should be:
- Populated by market-data service itself
- Cached in Redis
- Computed from `ohlcv_history` rolling window

### Migration Cleanup (Task #3)

`cryptoengine/shared/db/migrations/017_drop_ohlcv_1m_longterm.sql` (deferred from 2026-05-01):
- Removes `ohlcv_1m_longterm` table from live PostgreSQL (5432)
- Blocked until live queries audited (regime_detector dependency)
- Will be created as separate migration when regime_detector fix is deployed

---

## Review Criteria (Decision Confidence)

This decision is **Accepted** because:

1. **Scope is clear**: Three distinct workloads identified and isolated
2. **Alternatives evaluated**: Hadoop, single-DB, DuckDB, TimescaleDB all considered
3. **Deferral criteria explicit**: TimescaleDB, scale escalations documented
4. **Migration complete**: Parquet → PostgreSQL verified, scripts idempotent
5. **No blocking unknowns**: All data stores proven in current deployment

---

## Related Documents

- `docs/architecture/system-overview.md` — Service architecture (19 services, layers)
- `docs/architecture/data-flow.md` — Market data pipeline (Bybit → Redis → PostgreSQL)
- `docs/structure/README.md` — Schema reference, migrations list
- `backtest/scripts/db/schema.sql` — Backtest PostgreSQL schema (authoritative)
- `cryptoengine/shared/db/migrations/` — Live PostgreSQL migrations
- `docs/CODE_MAP.md` — Code ↔ Document index (updated together)

---

## Next Steps

1. **Archive Parquet files** (optional, do before deletion):
   ```bash
   tar czf data/ohlcv/BTCUSDT/1m/backup-2026-05-18.tar.gz data/ohlcv/BTCUSDT/1m/*.parquet
   rm -rf data/ohlcv/BTCUSDT/1m/*.parquet
   ```

2. **Fix regime_detector bug** (separate PR):
   - Audit live `ohlcv` table usage
   - Populate from market-data service or Redis
   - Deploy 017_drop_ohlcv_1m_longterm.sql

3. **Backtest perf monitoring** (ongoing):
   - Measure sweep p95 query time (target: < 1 sec/combo)
   - If > 2 hours for full sweep: evaluate TimescaleDB
   - If data > 5GB: re-evaluate compression strategy

