.PHONY: bootstrap lint test validate-schemas clean help

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
BENCH := $(VENV)/bin/python -m bench.cli

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Create venv and install dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

lint: ## Run basic syntax checks
	$(VENV)/bin/python -m py_compile bench/cli.py
	@echo "Lint OK"

test: ## Run unit tests
	@$(PYTEST) -q --tb=short; status=$$?; \
	if [ $$status -ne 0 ] && [ $$status -ne 5 ]; then exit $$status; fi

validate-schemas: ## Validate all JSON schemas
	$(BENCH) validate-schemas

clean: ## Remove build artifacts and caches
	rm -rf $(VENV) build/ dist/ *.egg-info .pytest_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
