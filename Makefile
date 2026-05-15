PYTHON := .venv/bin/python
DBT    := .venv/bin/dbt

.PHONY: up down logs ps ingest produce consume test \
        dbt-deps dbt-run dbt-test dbt-freshness \
        airflow-build airflow-init airflow-up \
        dashboard clean setup help

# ── Setup ─────────────────────────────────────────────────────────────────────
setup:
	cp -n .env.example .env || true
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip -q
	$(PYTHON) -m pip install -r requirements.txt

# ── Docker ────────────────────────────────────────────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v

# ── Ingestion ─────────────────────────────────────────────────────────────────
ingest:
	$(PYTHON) -m ingestion.ingest

produce:
	$(PYTHON) -m ingestion.producer

consume:
	$(PYTHON) -m ingestion.consumer

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps:
	cd dbt && ../$(DBT) deps --profiles-dir .

dbt-run:
	cd dbt && ../$(DBT) run --profiles-dir .

dbt-test:
	cd dbt && ../$(DBT) test --profiles-dir . --select mlb_pipeline

dbt-freshness:
	cd dbt && ../$(DBT) source freshness --profiles-dir .

# ── Airflow ───────────────────────────────────────────────────────────────────
airflow-build:
	docker compose build airflow-init airflow-webserver airflow-scheduler

airflow-init:
	docker compose run --rm airflow-init

airflow-up:
	docker compose up -d airflow-init airflow-webserver airflow-scheduler \
	                      postgres-exporter kafka-exporter

# ── Dashboard ─────────────────────────────────────────────────────────────────
dashboard:
	docker compose up -d dashboard

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo " To run the full stack:  docker compose up -d"
	@echo " Airflow handles the pipeline automatically on an hourly schedule."
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "Docker (primary interface):"
	@echo "  make up          Start all services"
	@echo "  make down        Stop all services"
	@echo "  make clean       Stop + wipe volumes (full reset)"
	@echo "  make logs        Follow logs"
	@echo "  make ps          Show service status"
	@echo ""
	@echo "Local dev only (requires: make setup first):"
	@echo "  make setup       Create .venv + install Python deps locally"
	@echo "  make ingest      Run MLB API ingestion locally"
	@echo "  make produce     Publish to Kafka locally"
	@echo "  make consume     Consume from Kafka locally"
	@echo "  make dbt-deps    Install dbt packages locally"
	@echo "  make dbt-run     Run dbt models locally"
	@echo "  make dbt-test    Run dbt schema tests locally"
	@echo ""
