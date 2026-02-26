# ELT Pipeline - Makefile
# Usage: make setup | run | test | lint | docker-up | docker-down

.PHONY: setup run test lint docker-up docker-down ingest transform app figures clean

# Default target
help:
	@echo "ELT Pipeline - Real-time Crypto Prices"
	@echo ""
	@echo "  make setup      Create venv, install deps (pip or uv)"
	@echo "  make run        Run full pipeline: ingest → transform"
	@echo "  make ingest     Run ingestion only"
	@echo "  make transform  Run transformation only"
	@echo "  make app        Start Streamlit dashboard"
	@echo "  make test       Run pytest"
	@echo "  make lint       Run ruff check and format"
	@echo "  make docker-up  Build and run with Docker Compose"
	@echo "  make docker-down Stop containers"
	@echo "  make figures    Generate correlation matrix and trend line (docs/)"
	@echo "  make clean      Remove cache and build artifacts"

setup:
	@echo "Setting up project..."
	@if command -v uv >/dev/null 2>&1; then \
		uv venv && . .venv/bin/activate && uv pip install -r requirements.txt; \
	else \
		python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt; \
	fi
	@echo "Copy .env.example to .env and set API keys if needed."
	@echo "Done. Activate with: source .venv/bin/activate"

run: ingest transform
	@echo "Pipeline complete."

ingest:
	python -m src.ingestion.main

transform:
	python -m src.transformation.main

app:
	streamlit run src/app/dashboard.py

test:
	pytest tests/ -v

lint:
	ruff check src tests
	ruff format src tests --check

lint-fix:
	ruff check src tests --fix
	ruff format src tests

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

figures:
	PYTHONPATH=. python scripts/generate_readme_figures.py

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
