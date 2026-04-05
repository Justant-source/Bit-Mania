.PHONY: up down logs test test-unit backtest status migrate emergency monthly-report

COMPOSE := docker compose
DEV_COMPOSE := $(COMPOSE) -f docker-compose.yml -f docker-compose.dev.yml

# ── Lifecycle ─────────────────────────────────────────────────

up: ## Start all services in production mode
	$(COMPOSE) up -d --build --remove-orphans
	@echo "CryptoEngine is running. Dashboard: http://localhost:3000 | Grafana: http://localhost:3002"

up-dev: ## Start all services in development mode with hot reload
	$(DEV_COMPOSE) up -d --build --remove-orphans
	@echo "CryptoEngine (dev) is running."

down: ## Stop all services and remove containers
	$(COMPOSE) down

down-clean: ## Stop all services and remove volumes (DESTROYS DATA)
	$(COMPOSE) down -v

restart: ## Restart all services
	$(COMPOSE) restart

# ── Logging ───────────────────────────────────────────────────

logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

logs-%: ## Tail logs for a specific service (e.g., make logs-market-data)
	$(COMPOSE) logs -f --tail=100 $*

# ── Testing ───────────────────────────────────────────────────

test: ## Run the full test suite across all services
	$(COMPOSE) run --rm --no-deps market-data pytest -v /app/tests/
	$(COMPOSE) run --rm --no-deps strategy-orchestrator pytest -v /app/tests/
	$(COMPOSE) run --rm --no-deps execution-engine pytest -v /app/tests/
	$(COMPOSE) run --rm --no-deps funding-arb pytest -v /app/tests/
	$(COMPOSE) run --rm --no-deps adaptive-dca pytest -v /app/tests/

test-unit: ## Run unit tests only (no external dependencies)
	$(COMPOSE) run --rm --no-deps market-data pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps strategy-orchestrator pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps execution-engine pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps funding-arb pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps adaptive-dca pytest -v -m unit /app/tests/

fetch-history-3y: ## Download 3 years of OHLCV + funding rate data from Bybit (2023-04-01 ~ now)
	$(COMPOSE) --profile backtest run --rm backtester \
		python scripts/fetch_real_ohlcv.py \
		--start 2023-04-01 --end 2026-04-01 \
		--timeframes 15m,1h,4h

fetch-history-3y-funding: ## Download 3 years of funding rate history
	$(COMPOSE) --profile backtest run --rm backtester \
		python scripts/seed_historical.py \
		--exchange bybit --symbol BTCUSDT \
		--data-type funding \
		--start 2023-04-01 --end 2026-04-01

backtest: ## Run backtester (single strategy, 3-year default)
	$(COMPOSE) --profile backtest run --rm backtester

backtest-scenarios: ## Run all scenarios including 3-year backtests (6m + yearly + 3y)
	$(COMPOSE) --profile backtest run --rm backtester python run_scenarios.py

backtest-3y: ## Run combined strategy backtest over full 3-year period
	$(COMPOSE) --profile backtest run --rm backtester \
		python main.py --strategy combined \
		--start 2023-04-01 --end 2026-03-31 \
		--walk-forward --wf-train-days 365 --wf-test-days 90

# ── Operations ────────────────────────────────────────────────

status: ## Show status of all services
	@echo "=== Container Status ==="
	@$(COMPOSE) ps
	@echo ""
	@echo "=== Resource Usage ==="
	@docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" $$($(COMPOSE) ps -q 2>/dev/null) 2>/dev/null || true

migrate: ## Run database migrations
	$(COMPOSE) run --rm execution-engine alembic upgrade head

emergency: ## EMERGENCY: Close all positions and halt trading immediately
	@echo "!!! EMERGENCY SHUTDOWN !!!"
	@echo "Sending kill signal to all strategy services..."
	$(COMPOSE) exec execution-engine python -m engine.emergency_close_all
	$(COMPOSE) stop funding-arb adaptive-dca strategy-orchestrator
	@echo "All strategies stopped. Execution engine still running for position monitoring."
	@echo "Review positions at http://localhost:3000"

monthly-report: ## Generate monthly performance report
	$(COMPOSE) run --rm backtester python -m reports.monthly_report

resilience-test: ## Run automated resilience tests (kill+restart each core service, verify recovery)
	@echo "=== CryptoEngine Resilience Test ==="
	@echo "WARNING: This will briefly kill core services."
	@read -p "Proceed? [y/N] " confirm; [ "$$confirm" = "y" ] || exit 0
	python scripts/resilience_test.py

resilience-test-market-data: ## Test only market-data restart + gap backfill
	python scripts/resilience_test.py --service market-data

resilience-test-execution: ## Test only execution-engine restart + position preservation
	python scripts/resilience_test.py --service execution-engine

backup: ## Trigger manual PostgreSQL backup now
	@echo "=== Manual backup triggered ==="
	$(COMPOSE) exec pg-backup sh /scripts/pg_backup.sh
	@echo "=== Backup complete. Files in pg-backups volume ==="

backup-list: ## List all existing PostgreSQL backups
	$(COMPOSE) exec pg-backup sh -c "ls -lh /backups/cryptoengine_*.sql.gz 2>/dev/null || echo 'No backups found'"

backup-restore: ## Restore from latest backup (DESTRUCTIVE — confirm first)
	@echo "!!! WARNING: This will overwrite the current database !!!"
	@read -p "Latest backup to restore (filename only, e.g. cryptoengine_20260406_170000.sql.gz): " f; \
	$(COMPOSE) exec pg-backup sh -c "gunzip -c /backups/$$f | psql -h postgres -U cryptoengine -d cryptoengine"

# ── Help ──────────────────────────────────────────────────────

help: ## Show this help message
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
