---
title: Code → Docs 역인덱스
last_updated: 2026-05-18
---

# Code → Docs 역인덱스

코드 파일을 수정할 때 함께 업데이트해야 할 문서를 찾는 인덱스.

## 전략

| 코드 | 문서 |
|------|------|
| `services/strategies/base_strategy.py` | [test/strategies/](test/strategies/) |
| `services/strategies/supertrend/**` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/strategies/funding-arb/**` | [policies/strategies/funding-arb.md](policies/strategies/funding-arb.md) (폐기됨, 히스토리용) |
| `services/strategies/adaptive-dca/**` | [backtest/docs/strategies/003_adaptive_dca.md](../backtest/docs/strategies/003_adaptive_dca.md) |
| `config/strategies/supertrend.yaml` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) |
| `config/strategies/funding-arb.yaml` | [policies/strategies/funding-arb.md](policies/strategies/funding-arb.md) (폐기됨) |
| `config/strategies/adaptive-dca.yaml` | [backtest/docs/strategies/003_adaptive_dca.md](../backtest/docs/strategies/003_adaptive_dca.md) |
| `backtest/strategies/multi_symbol_funding_arb.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) · [policies/btc-only.md](policies/btc-only.md) |
| `backtest/strategies/cross_exchange_fa.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `backtest/strategies/dynamic_threshold_fa.py` | [backtest/docs/strategies/001_funding_arb.md](../backtest/docs/strategies/001_funding_arb.md) |
| `backtest/configs/multi_symbol_fa.yaml` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `backtest/scripts/data/coinalyze_client.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `backtest/scripts/data/fetch_multi_symbol.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `backtest/scripts/data/fetch_multi_exchange.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |

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
| `services/telegram-bot/**` | [policies/emergency-manual-close.md](policies/emergency-manual-close.md) |

## 인프라

| 코드 | 문서 |
|------|------|
| `docker-compose.yml` | [structure/services.md](structure/services.md) · [policies/operations/runbook.md](policies/operations/runbook.md) |
| `shared/db/migrations/*.sql` | [structure/README.md](structure/README.md) |
| `shared/db/migrations/012_quarterly_perp_spread.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `shared/db/migrations/013_multi_exchange.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `shared/db/migrations/016_supertrend_signals.sql` | [structure/README.md](structure/README.md) |
| `.env` | [policies/operations/runbook.md](policies/operations/runbook.md) |

## 대시보드

| 코드 | 문서 |
|------|------|
| `services/dashboard/src/routes/supertrend.ts` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/routes/monitor.ts` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/alertEvaluator.ts` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/public/supertrend.html` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/public/monitor.html` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/public/js/supertrend-dashboard.js` | [structure/services.md](structure/services.md) |
| `services/dashboard/src/public/js/monitor-dashboard.js` | [structure/services.md](structure/services.md) |
| `scripts/backfill_supertrend_signals.py` | [structure/services.md](structure/services.md) |

## 공유 라이브러리

| 코드 | 문서 |
|------|------|
| `shared/exchange/bybit.py` | [policies/leverage-limits.md](policies/leverage-limits.md) (MAX_LEVERAGE: 3x) |
| `shared/exchange/**` | [structure/README.md](structure/README.md) |
| `shared/kill_switch.py` | [policies/kill-switch.md](policies/kill-switch.md) · [structure/README.md](structure/README.md) |
| `shared/models/**` | [structure/README.md](structure/README.md) |

## 백테스트 코드

백테스트 관련 코드 ↔ 문서 매핑은 별도 관리:
→ [backtest/docs/CODE_MAP.md](../backtest/docs/CODE_MAP.md)
