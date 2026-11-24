.PHONY: help up down restart logs status test test-db emergency backup replay replay-sync

COMPOSE := docker compose

up: ## Build and start postgres + bot + pg-backup
	$(COMPOSE) up -d --build --remove-orphans

down: ## Stop the stack (positions stay open on the exchange)
	$(COMPOSE) down

restart: ## Rebuild and restart only the bot (positions are never closed)
	$(COMPOSE) up -d --build --no-deps bot

logs: ## Tail bot logs
	$(COMPOSE) logs -f --tail=100 bot

status: ## Container state, health and bot status (position, equity, halt, heartbeat age)
	@$(COMPOSE) ps
	@echo ""
	@$(COMPOSE) exec -T bot python -m bot.cli status 2>/dev/null || echo "(bot container not running)"

test-db: ## Throwaway Postgres for the tests (127.0.0.1:55432, never the production DB)
	@docker inspect bot-test-pg >/dev/null 2>&1 || docker run -d --name bot-test-pg \
	  -e POSTGRES_PASSWORD=test -p 127.0.0.1:55432:5432 postgres:16-alpine >/dev/null
	@docker start bot-test-pg >/dev/null
	@until docker exec bot-test-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

test: test-db ## Unit and e2e tests on the host
	python3 -m pytest tests/bot tests/e2e tests/unit/test_kill_switch.py -q

emergency: ## EMERGENCY: halt entries and close the position (asks for confirmation)
	@if [ -n "$$($(COMPOSE) ps -q --status running bot 2>/dev/null)" ]; then \
	  $(COMPOSE) exec bot python -m bot.cli emergency-close; \
	else \
	  echo "bot container is not running - using a one-off container"; \
	  $(COMPOSE) run --rm bot python -m bot.cli emergency-close; \
	fi

backup: ## Run a PostgreSQL backup now (stored in the pg-backups volume)
	$(COMPOSE) exec pg-backup sh /scripts/pg_backup.sh

replay: ## Replay backtest on the Postgres data (extra args: make replay ARGS="--until 2026-04-30")
	@DB_HOST=127.0.0.1 DB_PASSWORD="$$(grep '^DB_PASSWORD=' .env | cut -d= -f2-)" \
	  python3 tests/fixtures/_replay_supertrend.py $(ARGS)

replay-sync: ## Fetch missing closed Binance spot 4h bars into replay_ohlcv_4h
	$(COMPOSE) exec bot python -m bot.replay_data sync

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-14s %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
