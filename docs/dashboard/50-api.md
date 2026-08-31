---
title: L5 API — Dashboard REST
last_updated: 2026-08-29
---

# L5 API — Dashboard REST

## §4. Dashboard REST API

Dashboard(`dashboard/src/routes/`) 제공 엔드포인트:

### 4.1 내부 API (포트 3000)

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/internal/portfolio` | 포트폴리오 상태 |
| GET | `/api/internal/positions` | 열린 포지션 |
| GET | `/api/internal/trades?limit=&strategy=` | 거래 이력 |
| POST | `/api/internal/kill-switch` | Kill Switch 수동 발동 |
| POST | `/api/internal/resume` | Kill Switch 해제 |
| GET | `/api/internal/supertrend/candles` | 4h OHLCV만 (`ohlcv_history`). 차트 EMA는 라이브 `supertrend_signals` 우선 |
| GET | `/api/internal/supertrend/candles/in-progress` | 미확정 4h 봉 (`cache:ohlcv:bybit:BTCUSDT:4h`) |
| GET | `/api/internal/supertrend/status` | `is_running`, 할당 자본, `last_tick`, 최신 신호. **권위 상태** |
| GET | `/compare` | 신호 vs 체결 비교 |
| GET | `/equity` | 자산 곡선 |
| GET | `/status` | 전략 상태 |

**알려진 깨진 계약 (2026-08-29 실사, D3와 무관)**:
- `GET /api/pnl` — `FROM daily_pnl` 테이블이 없어 **500**. Supertrend UI는 호출하지 않음.
- `GET /api/internal/monitor/positions` — `positions`/`strategy_states` 스키마 불일치로 **500**일 수 있음. `/api/positions`는 Redis 캐시 히트 시 200.

`llm_judgments` / `llm_reports`는 DROP하면 `/api/llm*`가 500이 된다 — 018에서 유지.

### 4.2 공개 API (포트 3001)

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/public/status` | 시스템 상태 (제한) |
| GET | `/api/public/performance` | 성과 요약 (제한) |

---
