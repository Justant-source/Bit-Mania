---
title: Code → Docs 역인덱스
last_updated: 2026-05-12
---

# Code → Docs 역인덱스

코드 파일을 수정할 때 함께 업데이트해야 할 문서를 찾는 인덱스.

## 전략

| 코드 | 문서 |
|------|------|
| `services/strategies/base_strategy.py` | [test/strategies/](test/strategies/) |
| `services/strategies/funding-arb/**` | [test/strategies/001_funding_arb.md](test/strategies/001_funding_arb.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/strategies/adaptive-dca/**` | [test/strategies/003_adaptive_dca.md](test/strategies/003_adaptive_dca.md) |
| `config/strategies/funding-arb.yaml` | [test/strategies/001_funding_arb.md](test/strategies/001_funding_arb.md) · [policies/strategies/funding-arb.md](policies/strategies/funding-arb.md) |
| `config/strategies/adaptive-dca.yaml` | [test/strategies/003_adaptive_dca.md](test/strategies/003_adaptive_dca.md) |
| `services/jesse_engine/strategies/multi_symbol_funding_arb.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/jesse_engine/strategies/cross_exchange_fa.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `services/jesse_engine/strategies/dynamic_threshold_fa.py` | [test/strategies/001_funding_arb.md](test/strategies/001_funding_arb.md) |
| `services/jesse_engine/configs/multi_symbol_fa.yaml` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `services/jesse_engine/scripts/data/coinalyze_client.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `services/jesse_engine/scripts/data/fetch_multi_symbol.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `services/jesse_engine/scripts/data/fetch_multi_exchange.py` | [ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md](ADR/002.%20Multi-symbol%20FA%20도입%20검토_2026-05-02.md) |
| `services/jesse_engine/scripts/rolling_window_analysis.py` | [test/rolling_window_test.md](test/rolling_window_test.md) |
| `services/jesse_engine/scripts/README.md` | [test/backtest-skillset.md](test/backtest-skillset.md) |
| `services/jesse_engine/scripts/build_v4_dashboard.py` | [test/rolling_window_test.md](test/rolling_window_test.md) · [test/strategies/backtest/*.md](test/strategies/backtest/) |
| `services/jesse_engine/scripts/param_sweep_v2.py` | [test/param_sweep_v2.md](test/param_sweep_v2.md) · [test/strategies/backtest/*.md](test/strategies/backtest/) |
| `services/jesse_engine/scripts/param_sweep_v3.py` | [test/param_sweep_v3.md](test/param_sweep_v3.md) · [test/strategies/backtest/*.md](test/strategies/backtest/) |

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
| `.env` | [policies/operations/runbook.md](policies/operations/runbook.md) |

## 공유 라이브러리

| 코드 | 문서 |
|------|------|
| `shared/exchange/**` | [structure/README.md](structure/README.md) |
| `shared/kill_switch.py` | [policies/kill-switch.md](policies/kill-switch.md) · [structure/README.md](structure/README.md) |
| `shared/models/**` | [structure/README.md](structure/README.md) |
