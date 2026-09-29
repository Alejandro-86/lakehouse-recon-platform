.PHONY: install up down bootstrap produce ingest transform transform-break reconcile snapshots maintain demo test lint typecheck ci clean

install:
	pip install -e ".[dev,dbt]"

up:
	docker compose up -d
	@echo "waiting for Trino..."
	@until curl -sf http://localhost:8080/v1/info | grep -q '"starting":false'; do sleep 2; done
	@echo "Trino ready on :8080, Kafka on :9092, MinIO console on :9001"

down:
	docker compose down -v

bootstrap:
	lakehouse bootstrap

produce:
	lakehouse produce --count 5000

ingest:
	lakehouse ingest

transform:
	cd dbt/lakehouse && DBT_PROFILES_DIR=. dbt build

# Deliberately breaks the gold layer to prove the controls catch it.
transform-break:
	cd dbt/lakehouse && DBT_PROFILES_DIR=. dbt build --vars 'inject_break: true'

reconcile:
	lakehouse reconcile

snapshots:
	lakehouse snapshots

maintain:
	lakehouse maintain

demo: bootstrap produce ingest transform reconcile

test:
	pytest tests/ -v -m "not integration"

test-integration:
	pytest tests/ -v -m integration

lint:
	ruff check src/ tests/

typecheck:
	mypy src/ --ignore-missing-imports

ci: lint typecheck test

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache dbt/lakehouse/target dbt/lakehouse/logs
