"""
shared/log_events.py — 서비스 전체 표준 이벤트명 상수 정의
모든 서비스에서 자유 문자열 대신 이 상수를 사용한다.
"""

# === 서비스 생명주기 ===
SERVICE_STARTED = "service_started"
SERVICE_STOPPING = "service_stopping"
SERVICE_STOPPED = "service_stopped"
SERVICE_HEALTH_OK = "service_health_ok"
SERVICE_HEALTH_FAIL = "service_health_fail"
SERVICE_RECONNECTED = "service_reconnected"

# === 시장 데이터 ===
MARKET_WS_CONNECTED = "market_ws_connected"
MARKET_WS_DISCONNECTED = "market_ws_disconnected"
MARKET_WS_RECONNECTING = "market_ws_reconnecting"
MARKET_OHLCV_STORED = "market_ohlcv_stored"
MARKET_FUNDING_RATE = "market_funding_rate"
MARKET_REGIME_CHANGED = "market_regime_changed"
MARKET_TICKER_RECEIVED = "market_ticker_received"

# === 전략 ===
STRATEGY_STARTED = "strategy_started"
STRATEGY_STOPPED = "strategy_stopped"
STRATEGY_PAUSED = "strategy_paused"
STRATEGY_RESUMED = "strategy_resumed"
STRATEGY_TICK = "strategy_tick"
STRATEGY_SIGNAL = "strategy_signal"
STRATEGY_REBALANCE = "strategy_rebalance"
STRATEGY_CIRCUIT_BREAKER = "strategy_circuit_breaker"

# === 펀딩비 차익거래 (FA) ===
FA_ENTRY_CONDITION_MET = "fa_entry_condition_met"
FA_POSITION_OPENED = "fa_position_opened"
FA_POSITION_CLOSED = "fa_position_closed"
FA_FUNDING_COLLECTED = "fa_funding_collected"
FA_HEDGE_DRIFT = "fa_hedge_drift"
FA_HEDGE_REBALANCED = "fa_hedge_rebalanced"
FA_ONE_SIDE_FILL = "fa_one_side_fill"
FA_ONE_SIDE_RECOVERY = "fa_one_side_recovery"
FA_REINVEST = "fa_reinvest"

# === DCA ===
DCA_PURCHASE = "dca_purchase"
DCA_MULTIPLIER_CALC = "dca_multiplier_calc"
DCA_TAKE_PROFIT = "dca_take_profit"

# === 주문 실행 ===
ORDER_SUBMITTED = "order_submitted"
ORDER_RECEIVED = "order_received"
ORDER_SAFETY_PASSED = "order_safety_passed"
ORDER_SAFETY_FAILED = "order_safety_failed"
ORDER_SENT = "order_sent"
ORDER_FILLED = "order_filled"
ORDER_PARTIALLY_FILLED = "order_partially_filled"
ORDER_CANCELLED = "order_cancelled"
ORDER_REJECTED = "order_rejected"
ORDER_RETRY = "order_retry"
ORDER_TIMEOUT = "order_timeout"
ORDER_DUPLICATE_SKIPPED = "order_duplicate_skipped"

# === Kill Switch ===
KILL_SWITCH_TRIGGERED = "kill_switch_triggered"
KILL_SWITCH_RESUMED = "kill_switch_resumed"
KILL_SWITCH_COOLDOWN = "kill_switch_cooldown"
KILL_SWITCH_MANUAL_RESET = "kill_switch_manual_reset"

# === 오케스트레이터 ===
ORCH_CYCLE_START = "orch_cycle_start"
ORCH_WEIGHT_CHANGED = "orch_weight_changed"
ORCH_CAPITAL_ALLOCATED = "orch_capital_allocated"
ORCH_DRAWDOWN_WARNING = "orch_drawdown_warning"
ORCH_CONFIG_RELOADED = "orch_config_reloaded"
ORCH_DEAD_MAN_SWITCH = "orch_dead_man_switch"

# === LLM 어드바이저 ===
LLM_ANALYSIS_START = "llm_analysis_start"
LLM_ANALYSIS_COMPLETE = "llm_analysis_complete"
LLM_WEIGHT_SUGGESTION = "llm_weight_suggestion"
LLM_API_ERROR = "llm_api_error"

# === 텔레그램 ===
TELEGRAM_COMMAND_RECEIVED = "telegram_command_received"
TELEGRAM_NOTIFICATION_SENT = "telegram_notification_sent"
TELEGRAM_HEARTBEAT = "telegram_heartbeat"

# === 포지션 정합성 ===
POSITION_RECONCILE_OK = "position_reconcile_ok"
POSITION_RECONCILE_MISMATCH = "position_reconcile_mismatch"
POSITION_RECONCILE_FIXED = "position_reconcile_fixed"

# === 수수료 ===
FEE_TIER_UPDATED = "fee_tier_updated"
FEE_TIER_MISMATCH = "fee_tier_mismatch"

# === 인프라 ===
DB_POOL_CREATED = "db_pool_created"
DB_POOL_CLOSED = "db_pool_closed"
DB_QUERY_SLOW = "db_query_slow"
REDIS_CONNECTED = "redis_connected"
REDIS_DISCONNECTED = "redis_disconnected"
REDIS_RECONNECTING = "redis_reconnecting"
REDIS_PUBLISH_FAILED = "redis_publish_failed"
