SHELL := /bin/bash
PYTHON ?= python3.11
VENV ?= .venv
BIN := $(VENV)/bin
COMPOSE ?= docker compose
EVENTS ?= 50000
TOPICS := telemetry workorder_cdc

.PHONY: help up down demo produce pipeline compact test test-local test-integration lint format install clean

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

up: ## start Redpanda + Spark/Delta container and create topics
	$(COMPOSE) up -d --build --wait
	$(COMPOSE) exec redpanda rpk topic create $(TOPICS) -p 3 || true

down: ## stop containers (keeps ./data)
	$(COMPOSE) down

produce: ## send $(EVENTS) synthetic events to Redpanda
	$(COMPOSE) exec spark python scripts/generate_events.py --events $(EVENTS) --bootstrap redpanda:9092

pipeline: ## bronze -> silver -> gold (availableNow triggers), prints gold rows
	$(COMPOSE) exec spark python -m lakehouse.run all

demo: produce pipeline ## produce events, run the pipeline, print gold

compact: ## OPTIMIZE/ZORDER + VACUUM silver and gold
	$(COMPOSE) exec spark python -m lakehouse.compact

$(BIN)/pytest:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install -q --upgrade pip
	$(BIN)/pip install -q -e ".[dev]"

install: $(BIN)/pytest ## local venv with pinned deps

test: ## unit + replay tests: inside the spark container if `make up` ran, else local venv
	@if $(COMPOSE) ps --status running --services 2>/dev/null | grep -qx spark; then \
		$(COMPOSE) exec spark pytest -m "not integration"; \
	else \
		$(MAKE) test-local; \
	fi

test-local: install ## same tests on a local SparkSession (needs Java 17 + Python 3.11)
	PYSPARK_PYTHON=$(BIN)/python $(BIN)/pytest -m "not integration"

test-integration: install ## end-to-end via Kafka; run `make up` first
	KAFKA_BOOTSTRAP=localhost:19092 PYSPARK_PYTHON=$(BIN)/python $(BIN)/pytest -m integration

lint: install ## ruff lint + format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format: install
	$(BIN)/ruff format .
	$(BIN)/ruff check --fix .

clean: ## remove local lake data, checkpoints and caches
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
