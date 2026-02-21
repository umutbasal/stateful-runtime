COMPOSE := docker compose -f docker-compose.yml
API_BASE ?= http://localhost:8080
METRICS_BASE ?= http://localhost:9090
FOLLOWING ?= user-1,user-2,user-3,user-4,user-5

.PHONY: up down logs ps wait-ready feed timeline metrics smoke test

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f redpanda runtime fake-ingest

ps:
	$(COMPOSE) ps

wait-ready:
	@echo "waiting for runtime readiness..."
	@for i in $$(seq 1 60); do \
		if curl -fsS "$(API_BASE)/readyz" >/dev/null; then \
			echo "runtime ready"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "runtime did not become ready in time"; \
	exit 1

feed: wait-ready
	@curl -fsS "$(API_BASE)/v1/feed?user_id=user-1&following=$(FOLLOWING)&limit=20"; echo

timeline: wait-ready
	@curl -fsS "$(API_BASE)/v1/users/user-1/timeline?limit=20"; echo

metrics: wait-ready
	@curl -fsS "$(METRICS_BASE)/metrics" | rg "stateful_runtime_(http_requests_total|ingestion_messages_total|store_entities_total)" || true

smoke: up wait-ready
	@echo "warming fake ingestion for 6 seconds..."
	@sleep 6
	@echo "feed response:"
	@curl -fsS "$(API_BASE)/v1/feed?user_id=user-1&following=$(FOLLOWING)&limit=20"; echo
	@echo "timeline response:"
	@curl -fsS "$(API_BASE)/v1/users/user-1/timeline?limit=20"; echo

test: smoke
