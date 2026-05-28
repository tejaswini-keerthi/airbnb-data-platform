.PHONY: setup install lint test dbt-run dbt-test infra-plan infra-apply

setup:
	uv sync

install:
	uv sync --extra dev

lint:
	uv run ruff check .
	uv run mypy pipeline/ ingestion/

test:
	uv run pytest tests/ -v --cov=pipeline

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-snapshot:
	cd dbt && dbt snapshot

infra-plan:
	cd infrastructure/terraform && terraform plan

infra-apply:
	cd infrastructure/terraform && terraform apply