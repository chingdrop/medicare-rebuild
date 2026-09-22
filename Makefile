# One-command synthetic demo. See docs/demo.md.
# Uses the throwaway dev SQL Server container from docker-compose.yml (dev-only credentials).

SEED ?=
PATIENTS ?=
FAULT ?=
GEN_ARGS = $(if $(SEED),--seed $(SEED)) $(if $(PATIENTS),--patients $(PATIENTS)) $(if $(FAULT),--inject-fault $(FAULT))

.PHONY: demo reconcile demo-down schema migrate

schema:
	uv run python -m tools.generate_schema

migrate:
	uv run alembic upgrade head

demo:
	docker compose up -d mssql
	uv run python -m tools.synthetic_data --out demo_data $(GEN_ARGS)
	uv run python -m tools.synthetic_data.demo --data-dir demo_data --output-dir demo_output

reconcile:
	uv run python -m tools.reconcile --data-dir demo_data --output-dir demo_output

demo-down:
	docker compose down
	rm -rf demo_data demo_output
