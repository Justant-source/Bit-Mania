.PHONY: up down logs test test-unit status migrate emergency lint-docs

COMPOSE := docker compose
DEV_COMPOSE := $(COMPOSE) -f docker-compose.yml -f docker-compose.dev.yml

# ── Lifecycle ─────────────────────────────────────────────────

up: ## Start all services in production mode
	$(COMPOSE) up -d --build --remove-orphans
	@echo "CryptoEngine is running. (Dashboard is a separate compose project — see bitmania-dashboard.)"

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

test-unit: ## Run unit tests only (no external dependencies)
	$(COMPOSE) run --rm --no-deps market-data pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps strategy-orchestrator pytest -v -m unit /app/tests/
	$(COMPOSE) run --rm --no-deps execution-engine pytest -v -m unit /app/tests/

# NOTE: The real backtest stack lives in backtest/docker/docker-compose.yml
# (separate compose project, `cryptoengine-backtest`). Legacy targets that
# invoked `fetch_real_ohlcv.py`, `seed_historical.py`, `run_scenarios.py`,
# and `main.py --strategy combined` were removed 2026-08-29 — those scripts
# no longer exist anywhere in the repo (deleted in a prior cleanup). To run
# a backtest today:
#   cd backtest/docker && docker compose --profile backtest run --rm backtester \
#     python /app/scripts/runners/run_external_backtest.py

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
	@echo "긴급 청산 스크립트를 execution-engine 안에서 실행한다 (stdin 주입 — 재빌드 불필요)"
	@echo "청산이 확인되어야만 서비스 정지 단계로 넘어간다. 미확인 시 여기서 중단된다."
	$(COMPOSE) exec -T execution-engine python - < scripts/emergency_close_all.py
	@echo "청산 확인됨. 전략·오케스트레이터를 정지한다 (execution-engine은 모니터링 위해 유지)."
	$(COMPOSE) stop supertrend strategy-orchestrator
	@echo "완료. 대시보드에서 확인: http://localhost:3000/supertrend"
	@echo "재개하려면 ce:kill_switch:active 키를 삭제한 뒤 서비스를 기동할 것."

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

lint-docs: ## Lint docs/ tree against C4 SSOT standard (6 checks, stdlib only)
	cd .. && python3 scripts/lint_docs.py

# ── Help ──────────────────────────────────────────────────────

help: ## Show this help message
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
