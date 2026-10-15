PYTHON ?= python3.12
VENV ?= .venv
DAYS ?= 7
SEED ?= 42

ifeq ($(wildcard $(VENV)/bin/python),)
RUN :=
else
RUN := $(VENV)/bin/
endif

.DEFAULT_GOAL := help

.PHONY: help install generate pipeline full-refresh sql report charts demo test test-unit \
	lint format clean docker-build docker-demo docker-test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

install: ## Create .venv and install the package with dev and docs extras
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev,docs]"

generate: ## Write DAYS batches of synthetic landing data (SEED controls the output)
	$(RUN)lakeforge generate --days $(DAYS) --seed $(SEED)

pipeline: ## Process every unprocessed landing batch through Bronze, Silver and Gold
	$(RUN)lakeforge run

full-refresh: ## Rebuild every layer from landing data
	$(RUN)lakeforge run --full-refresh

sql: ## Run the analytical SQL examples against Gold
	$(RUN)lakeforge sql sql/*.sql

report: ## Print the per-batch data-quality manifest
	$(RUN)lakeforge report

charts: ## Render docs/images charts from the current Gold output
	$(RUN)python scripts/render_charts.py

demo: generate pipeline sql ## Generate data, run the pipeline and the SQL examples

test: ## Run the full test suite, including end-to-end pipeline tests
	$(RUN)pytest

test-unit: ## Run tests without the slower end-to-end pipeline tests
	$(RUN)pytest -m "not integration"

lint: ## Lint and check formatting
	$(RUN)ruff check .
	$(RUN)ruff format --check .

format: ## Apply formatting and safe lint fixes
	$(RUN)ruff format .
	$(RUN)ruff check --fix .

clean: ## Delete generated lakehouse data and tool caches
	rm -rf data/landing data/bronze data/silver data/gold data/quarantine data/_state data/_staging
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log

docker-build: ## Build the container image (Python 3.12 + Java 17 + Spark)
	docker compose build

docker-demo: ## Run the demo inside the container; data is written to ./data
	docker compose run --rm lakeforge make demo DAYS=$(DAYS) SEED=$(SEED)

docker-test: ## Run lint and tests inside the container
	docker compose run --rm lakeforge sh -c "make lint && make test"
