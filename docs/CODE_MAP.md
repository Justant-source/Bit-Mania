---
title: Code → Docs 역인덱스
last_updated: 2026-06-14 (Bybit 네이티브 4h 재빌드: 메트릭 갱신 137.64%/1.349/-73.29%, 360 trades)
---

# Code → Docs 역인덱스

코드 파일을 수정할 때 함께 업데이트해야 할 문서를 찾는 인덱스.

## 전략

| 코드 | 문서 |
|------|------|
| `services/strategies/base_strategy.py` | [test/strategies/](test/strategies/) |
| `services/strategies/supertrend/**` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) · [policies/btc-only.md](policies/btc-only.md) |
| `services/strategies/supertrend/indicators.py` | Jesse 2.1.2 정본 포팅 — `_atr_jesse`(Wilder ATR, period-1 시드) + 밴드 리셋 절 + gated flip → `compute_supertrend`; `compute_ema`(close[0] 시드 재귀 ≡ jesse_rust.ema), `compute_atr`(≡ `_atr_jesse` ≡ jesse_rust.atr). **TA-Lib 제거**(2026-06-14 백테스트 정합) |
| `tests/unit/test_supertrend_parity.py` | 라이브 지표 ↔ Jesse 2.1.2(#7908) 골든 5체크(supertrend·EMA·ATR·deque(1000) 윈도우·진입필터); 골든 `tests/fixtures/golden_supertrend.json`은 `_gen_golden_supertrend.py`로 backtester 이미지(Jesse 2.1.2)에서 생성 |
| `services/strategies/supertrend/strategy.py` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) §주문 확정·상태 동기화 — pending 추적·`order:result:{id}` 구독·`_sync_position_from_exchange`(매 봉 신호 전)·exit 거부 60s 후 1회 재시도·봉 워치독/갭 백필·`min(할당자본, equity)` 사이징. `supertrend_signals` 테이블에 `st_line` 컬럼 저장; `entry_ok=bool(...)` numpy bool_ → Python bool 변환 필수 (asyncpg DataError 방지); `CANDLE_LOOKBACK=1000`(dir_ema 240 시드 정합, 2026-06-14 300→1000) |
| `scripts/backfill_supertrend_signals.py` | 정본 ST 재계산 후 `st_dir`·`st_line` upsert (DO UPDATE) |
| `config/strategies/supertrend.yaml` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) |

## 실행 / 주문

| 코드 | 문서 |
|------|------|
| `services/execution/**` | [architecture/data-flow.md](architecture/data-flow.md) · [policies/operations/runbook.md](policies/operations/runbook.md) |
| `services/execution/engine.py` | [architecture/data-flow.md](architecture/data-flow.md) §3.5 — 거부 ERROR 알림 + strategy_id 전파(`_publish_rejection`), ORDER_TIMEOUT 420s + 타임아웃 시 취소 후 거부 (블라인드 재시도 제거) |
| `services/execution/order_manager.py` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) §주문 실행 방식 — 재페그 부분체결 누적 추적(잔량만 재발주), CancelledError 시 미체결 주문 정리, 재시작 시 고아 주문 취소(`_restore_inflight_orders`) |
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
| `services/execution/safety.py` | [policies/strategies/supertrend.md](policies/strategies/supertrend.md) (SAFETY_LEVERAGE_LIMIT env, reduce_only 마진·레버리지 면제 — 2026-05-27 버그픽스: `_check_leverage_limit`에 `reduce_only` 조기 반환 추가, exit 주문이 implied leverage 초과로 잘못 차단되던 문제 수정) |
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
| `dashboard/src/public/css/dashboard.css` | 페이지 특수 오버라이드 (app-shell 전체화면, 모달, 인프라 게이지 등) · `--c-purple` 토큰(라이트/다크) — LWC 이전 후 차트에서 미사용 |
| `dashboard/src/public/js/theme.js` | 라이트/다크 토글 + localStorage — `bm:themechange` 이벤트 발행 |
| `dashboard/src/public/js/monitor-dashboard.js` | 시스템 모니터 프론트엔드 — palette() 기반 Plotly 토큰 연동 |
| `dashboard/src/public/js/supertrend-dashboard.js` | 전략 비교 프론트엔드 — TradingView Lightweight Charts 엔진(네이티브 pan/zoom/터치/자동스케일), 캔들+EMA 3선+ST 밴드+진입/종료 마커+클릭 모달; 플로팅 크로스헤어 툴팁(검은 카드·가격 내림차순, `subscribeCrosshairMove`+`param.seriesData`); 우측 축 현재가 티커만 유지(`lastValueVisible:true`); 진행 중 4h 봉 1분 폴링(updateInProgressCandle·_emaStep) |
| `dashboard/src/routes/supertrend.ts` | 대시보드 supertrend 라우터 — `/candles`·`/compare`·`/equity`·`/status`. `/compare`: 신호 쿼리에 `actual_exit_price`·`actual_exit_at`·`delay_note` 포함; `actual_exit_at IS NOT NULL`이면 타이밍 윈도우 대신 직접 매칭(status="matched"), orders 쿼리에 `original_signal_ts` 포함 및 `filled_delayed` 상태 인식 |
| `dashboard/src/public/monitor.html` | 시스템 모니터 페이지 — app-shell + 새 디자인 시스템 |
| `dashboard/src/public/supertrend.html` | 전략 비교 페이지 — 가격 차트 강화(밴드+EMA+마커+모달) |
| `dashboard/docker-compose.yml` | [policies/operations/monitoring.md](policies/operations/monitoring.md) |
| `dashboard/design/index.html` | 디자인 SSOT — 토큰·컴포넌트·적용 예시 |
| `scripts/backfill_supertrend_signals.py` | [structure/services.md](structure/services.md) |
| `scripts/manual_mainnet_test.py` | [policies/operations/runbook.md](policies/operations/runbook.md) (메인넷 매수/매도 1회 트리거 테스트) |
| `scripts/manual_close_delayed.py` | 지연 청산 복구 스크립트 — safety 버그 등으로 rejected된 exit 신호를 수동으로 체결하고 DB에 지연 이력(delay_reason, original_signal_ts) 기록; orders·supertrend_signals 양쪽 갱신 |
| `scripts/audit_signal_order_mismatch.py` | [policies/operations/runbook.md](policies/operations/runbook.md) §일상 운영 — supertrend_signals(expected_action) vs orders(filled) 대조: signal_no_fill/qty_mismatch/unmatched_fill 감지, `--alert`로 Telegram 발행 |

## 공유 라이브러리

| 코드 | 문서 |
|------|------|
| `shared/exchange/bybit.py` | [policies/leverage-limits.md](policies/leverage-limits.md) (MAX_LEVERAGE: 3x) · [policies/strategies/supertrend.md](policies/strategies/supertrend.md) (PostOnlyRejected, fetch_order_result, price_to_precision) |
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
