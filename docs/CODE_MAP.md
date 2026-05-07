---
title: Code → Docs 역인덱스
last_updated: 2026-05-02
---

# Code → Docs 역인덱스

코드 파일을 수정할 때 함께 업데이트해야 할 문서를 찾는 인덱스.

## 전략

| 코드 | 문서 |
|------|------|
| `services/strategies/base_strategy.py` | [strategies/](strategies/) |
| `services/strategies/funding-arb/**` | [strategies/funding_arb.md](strategies/funding_arb.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/strategies/adaptive-dca/**` | [strategies/adaptive_dca.md](strategies/adaptive_dca.md) |
| `config/strategies/funding-arb.yaml` | [strategies/funding_arb.md](strategies/funding_arb.md) · [policies/strategies/funding-arb.md](policies/strategies/funding-arb.md) |
| `config/strategies/adaptive-dca.yaml` | [strategies/adaptive_dca.md](strategies/adaptive_dca.md) |
| `services/jesse_engine/strategies/multi_symbol_funding_arb.py` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/jesse_engine/strategies/cross_exchange_fa.py` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) |
| `services/jesse_engine/strategies/dynamic_threshold_fa.py` | [strategies/funding_arb.md](strategies/funding_arb.md) |
| `services/jesse_engine/configs/multi_symbol_fa.yaml` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) |
| `services/jesse_engine/scripts/data/coinalyze_client.py` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) |
| `services/jesse_engine/scripts/data/fetch_multi_symbol.py` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) |
| `services/jesse_engine/scripts/data/fetch_multi_exchange.py` | [adrs/ADR-003-multi-symbol-funding-arb.md](adrs/ADR-003-multi-symbol-funding-arb.md) |

## 실행 / 주문

| 코드 | 문서 |
|------|------|
| `services/execution/**` | [architecture/data-flow.md](architecture/data-flow.md) · [runbook.md](runbook.md) |
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
| `docker-compose.yml` | [structure/services.md](structure/services.md) · [runbook.md](runbook.md) |
| `shared/db/migrations/*.sql` | [structure/README.md](structure/README.md) |
| `shared/db/migrations/012_quarterly_perp_spread.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `shared/db/migrations/013_multi_exchange.sql` | [architecture/data-flow.md](architecture/data-flow.md) |
| `.env` | [runbook.md](runbook.md) |

## 공유 라이브러리

| 코드 | 문서 |
|------|------|
| `shared/exchange/**` | [structure/README.md](structure/README.md) |
| `shared/kill_switch.py` | [policies/kill-switch.md](policies/kill-switch.md) · [structure/README.md](structure/README.md) |
| `shared/models/**` | [structure/README.md](structure/README.md) |
