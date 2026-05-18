---
title: Code → Docs 역인덱스
last_updated: 2026-05-19
---

# Code → Docs 역인덱스

코드 파일을 수정할 때 함께 업데이트해야 할 문서를 찾는 인덱스.

## 전략

| 코드 | 문서 |
|------|------|
| `services/strategies/base_strategy.py` | [test/strategies/](test/strategies/) |
| `services/strategies/supertrend/**` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) · [policies/btc-only.md](policies/btc-only.md) |
| `config/strategies/supertrend.yaml` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) |

## 실행 / 주문

| 코드 | 문서 |
|------|------|
| `services/execution/**` | [architecture/data-flow.md](architecture/data-flow.md) · [policies/operations/runbook.md](policies/operations/runbook.md) |
| `services/orchestrator/**` | [architecture/system-overview.md](architecture/system-overview.md) |
| `services/market-data/**` | [architecture/data-flow.md](architecture/data-flow.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/market-data/collector.py` | [architecture/data-flow.md](architecture/data-flow.md) |
| `services/market-data/binance_collector.py` | [architecture/data-flow.md](architecture/data-flow.md) |
| `services/market-data/okx_collector.py` | [architecture/data-flow.md](architecture/data-flow.md) |
| `services/market-data/quarterly_lifecycle.py` | [architecture/data-flow.md](architecture/data-flow.md) |

## 안전장치

| 코드 | 문서 |
|------|------|
| `shared/kill_switch.py` | [policies/kill-switch.md](policies/kill-switch.md) |
| `shared/log_events.py` | [structure/README.md](structure/README.md) |
| `services/telegram-bot/**` | [policies/emergency-manual-close.md](policies/emergency-manual-close.md) · [structure/services.md](structure/services.md) |
| `config/telegram.yaml` | [policies/emergency-manual-close.md](policies/emergency-manual-close.md) |

## 인프라

| 코드 | 문서 |
|------|------|
| `docker-compose.yml` | [structure/services.md](structure/services.md) · [policies/operations/runbook.md](policies/operations/runbook.md) |
| `shared/db/migrations/*.sql` | [structure/README.md](structure/README.md) · [architecture/data-stores.md](architecture/data-stores.md) |
| `shared/db/migrations/012_quarterly_perp_spread.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `shared/db/migrations/013_multi_exchange.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `shared/db/migrations/016_supertrend_signals.sql` | [structure/README.md](structure/README.md) |
| `shared/db/migrations/017_drop_ohlcv_1m_longterm.sql` (deferred) | [architecture/data-stores.md](architecture/data-stores.md) |
| `.env` | [policies/operations/runbook.md](policies/operations/runbook.md) |

## 대시보드 & 백필

| 코드 | 문서 |
|------|------|
| `dashboard/src/**` (standalone FE+BE) | [policies/operations/monitoring.md](policies/operations/monitoring.md) · [policies/operations/runbook.md](policies/operations/runbook.md) |
| `dashboard/src/public/css/tokens.css` | SSOT 디자인 토큰 — `dashboard/design/index.html` 에서 추출 |
| `dashboard/src/public/css/components.css` | SSOT 컴포넌트 라이브러리 — `dashboard/design/index.html` 에서 추출 |
| `dashboard/src/public/css/dashboard.css` | 페이지 특수 오버라이드 (app-shell 전체화면, 모달, 인프라 게이지 등) |
| `dashboard/src/public/js/theme.js` | 라이트/다크 토글 + localStorage — `bm:themechange` 이벤트 발행 |
| `dashboard/src/public/js/monitor-dashboard.js` | 시스템 모니터 프론트엔드 — palette() 기반 Plotly 토큰 연동 |
| `dashboard/src/public/js/supertrend-dashboard.js` | 전략 비교 프론트엔드 — palette() 기반 Plotly 토큰 연동 |
| `dashboard/src/public/monitor.html` | 시스템 모니터 페이지 — app-shell + 새 디자인 시스템 |
| `dashboard/src/public/supertrend.html` | 전략 비교 페이지 — app-shell + 새 디자인 시스템 |
| `dashboard/docker-compose.yml` | [policies/operations/monitoring.md](policies/operations/monitoring.md) |
| `dashboard/design/index.html` | 디자인 SSOT — 토큰·컴포넌트·적용 예시 |
| `scripts/backfill_supertrend_signals.py` | [structure/services.md](structure/services.md) |

## 공유 라이브러리

| 코드 | 문서 |
|------|------|
| `shared/exchange/bybit.py` | [policies/leverage-limits.md](policies/leverage-limits.md) (MAX_LEVERAGE: 3x) |
| `shared/exchange/**` | [structure/README.md](structure/README.md) |
| `shared/kill_switch.py` | [policies/kill-switch.md](policies/kill-switch.md) · [structure/README.md](structure/README.md) |
| `shared/models/**` | [structure/README.md](structure/README.md) |

## 백테스트 데이터 저장소

| 코드 | 문서 |
|------|------|
| `backtest/scripts/db/schema.sql` | [architecture/data-stores.md](architecture/data-stores.md) |
| `docker-compose.yml` (backtest-postgres service) | [architecture/data-stores.md](architecture/data-stores.md) · [structure/services.md](structure/services.md) |

## 백테스트 코드

백테스트 관련 코드 ↔ 문서 매핑은 별도 관리:
→ [backtest/docs/CODE_MAP.md](../backtest/docs/CODE_MAP.md)
