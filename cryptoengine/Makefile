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
	$(COMPOSE) run --rm --no-deps grid-trading pytest -v /app/tests/
	$(COMPOSE) run --rm --no-deps adaptive-dca pytest -v /app/tests/

test-unit: ## Run unit tests only (no external dependencies)
	$(COMPOSE) run --rm --no-deps market-data pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps strategy-orchestrator pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps execution-engine pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps funding-arb pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps grid-trading pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps adaptive-dca pytest -v -m unit /app/tests/

backtest: ## Run the backtester against historical data
	$(COMPOSE) --profile backtest run --rm backtester

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
	$(COMPOSE) stop funding-arb grid-trading adaptive-dca strategy-orchestrator
	@echo "All strategies stopped. Execution engine still running for position monitoring."
	@echo "Review positions at http://localhost:3000"

monthly-report: ## Generate monthly performance report
	$(COMPOSE) run --rm backtester python -m reports.monthly_report

# ── Help ──────────────────────────────────────────────────────

help: ## Show this help message
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
