# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An ETL pipeline that rebuilds the data architecture for a medical company's remote physician monitoring (diabetes/hypertension telehealth) billing. It extracts patient/device/reading data from legacy SQL databases and a SharePoint CSV export, standardizes it with pandas, and loads it into a new "GPS" SQL Server database whose schema correctly tracks Medicare billing CPT codes (99202, 99453, 99454, 99457, 99458 — computed in `src/medicare_rebuild/billing.py`, a pandas/ORM port of the original `sql/stored_procedures/batch_medcode_99XXX.sql`; see decision 0014).

## Commands

```sh
uv sync                        # install deps + create .venv
uv run pytest                  # unit tests (default; mocks all external systems)
uv run pytest -m integration   # integration tests (needs a real SQL Server, see below)
uv run pytest tests/test_dataframe_utils.py::test_standardize_state  # single test
uv run ruff check .            # lint
uv run ruff format .           # format
uv run mypy                    # type check (src/ only, per [tool.mypy] files config)
uv run medicare-rebuild         # run the ETL pipeline (console script -> __main__:main)
uv run python -m medicare_rebuild  # equivalent
uv run alembic upgrade head     # create/update a real GPS database's schema (make migrate)
```

CI (`.github/workflows/ci.yml`) runs `test` (ruff + mypy + unit pytest) and `integration-test` (real `mssql` service container) as separate jobs on every push/PR.

### Integration tests

`tests/integration/` exercises `DatabaseManager` and `DataImporter` against a **real** SQL Server rather than mocks (MS Graph/Tenovi HTTP calls are still mocked via `requests_mock`). Needs:

1. `docker compose up -d` — starts a disposable `mcr.microsoft.com/mssql/server` container.
2. ODBC Driver 18 for SQL Server installed locally (`brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18` on macOS).
3. `uv run pytest -m integration`

`tests/integration/conftest.py` creates a dedicated database per test session and skips gracefully if no server is reachable — connection details default to `docker-compose.yml`'s and are overridable via `INTEGRATION_DB_HOST`/`PORT`/`USER`/`PASSWORD`. On Apple Silicon the SQL Server image only runs via x86_64 emulation (no native arm64 build exists); GitHub's runners are x86_64 natively.

`sql/schema.sql` is generated from `src/medicare_rebuild/models.py` and `legacy_models.py` (the schema of record, see decision 0015) via `make schema`; a test (`tests/test_generate_schema.py`) fails if it drifts from the models. The integration tests, and the demo, build their databases from the same classes via `metadata.create_all()`, so there is only one definition of the GPS schema to keep in sync.

Alembic migrations for the GPS database (only -- not the legacy source databases) live in `alembic/`, targeting `GpsBase.metadata`; `make migrate` (`alembic upgrade head`) is how a real GPS database is created or updated (see decision 0016). The demo and integration tests still use `metadata.create_all()`, not Alembic -- a throwaway database has no schema history to migrate from. `tests/integration/test_alembic_integration.py` runs the migration chain against a real database and fails if it drifts from `models.py` (`alembic check`).

## Architecture

### Two databases, two schemas, one pipeline

`DataImporter` (`src/medicare_rebuild/__main__.py`) reads from **legacy source databases** (env vars `LEGACY_SQL_*`, e.g. `LEGACY_SQL_SP_NOTES`, `LEGACY_SQL_SP_TIME`, `LEGACY_SQL_SP_FULFILLMENT`, `LEGACY_SQL_SP_READINGS`) — these use old capitalized table/column names (`Glucose_Readings`, `Blood_Pressure_Readings`, `Medical_Notes`, `Time_Log`, `Fulfillment_All`; see `legacy_models.py` and `queries.py`'s `get_*_stmt` functions) — and writes into the new **GPS database** (env vars `GPS_SQL_*`), which uses lowercase snake_case tables defined in `models.py` (`patient`, `patient_address`, `patient_insurance`, `medical_necessity`, `patient_status`, `emergency_contact`, `device`, `user`, `patient_note`, `glucose_reading`, `blood_pressure_reading`, plus the lookup tables). `DatabaseManager` (`utils/db_utils.py`) is instantiated once per source database plus once for the GPS target (`self.gps`), and `DataImporter` holds one SQLAlchemy `Session` (`self.session`) across its `import_*_data` calls.

Patient/device/reading data crosses from the old schema's `sharepoint_id` (or `Vendor` name) to the new schema's auto-generated identity `patient_id`/`device_id`/`vendor_id` by inserting the parent ORM objects and calling `session.flush()`, which populates their identity attribute immediately (see decision 0015) — no separate `SELECT`-and-merge step. Every `import_*_data` method in `DataImporter` follows this insert-parent-then-resolve-then-insert-children order; a row whose parent identity can't be resolved (an orphan source row, or one belonging to a rejected patient) is dropped, not inserted with a null foreign key (`_drop_unresolved` in `__main__.py`).

Lookup-table foreign keys (`temp_status_type` → `patient_status_type_id`, `temp_user` → `user_id`, `temp_note_type` → `note_type_id`) are resolved the same way, but are allowed to stay `NULL` when nothing matches: a `{name: id}` dict is built from one query per lookup table and applied with `.map()` (via `_map_id`, which avoids pandas' float/NaN upcast of the id column) before the row is constructed, replacing the four `UPDATE ... WHERE temp_X IS NOT NULL` statements the code used to run once at the end of `import_all_data()`.

### `billing.py` — billing computation

`create_billing_report()` (`__main__.py`) calls `billing.run_billing(session, end_date)` then `billing.build_billing_report(session, start_date, end_date)`. Each CPT code has an `apply_*` function that is a faithful pandas/ORM port of one `sql/stored_procedures/batch_medcode_*.sql` file (see decision 0014); the procedures themselves stay in `sql/stored_procedures/` for reference but the pipeline no longer calls them. Every `apply_*` function does its own query-compute-insert-commit, matching the original's one-procedure-per-`EXEC` granularity, so a later rule's "already has a code" check sees what an earlier one just inserted — `run_billing` calls them in the same fixed order `create_billing_report()` used to call the procedures in (see `docs/billing-rules.md`: How the rules interact). The pandas logic itself is factored into small, pure helpers (`_qualifying_by_minutes`, `_qualifying_by_reading_days`, `_qualifying_99458`, `_since`, `_in_report_window`) that take plain DataFrames and return plain DataFrames, so the documented boundary cases (899/900 s for 99202, 1199/1200 s for 99457, 16-day and 30-day-window edges for 99453/99454, the report's midnight-of-end-date exclusion) are tested directly in `tests/test_billing.py` without a database.

### `utils/dataframe_utils.py` — three function categories

- **Standardize** (`standardize_*`): clean a single value (state name -> 2-letter code via `keyword_search`/`enums.state_abbreviations`, phone numbers, MBI/insurance IDs, weight/height parsing, etc). Most return `str | float`, returning `np.nan` (not raising) when the input doesn't parse — callers apply these with `.apply()` over a Series.
- **Create** (`create_*_df`): slice one wide patient DataFrame into the six separate DataFrames matching the GPS schema's separate tables (patient/address/insurance/med_necessity/status/emcontacts).
- **Normalize** (`normalize_*`): apply the standardize functions across a raw DataFrame's relevant columns, then rename SharePoint/legacy column names to the GPS schema's column names. `normalize_patients` is the biggest and drives most of `check_patient_db_constraints`'s expectations afterward.

`check_patient_db_constraints` filters out rows that would violate DB column-length constraints (e.g. an unrecognized state string is too long once title-cased instead of being resolved to a 2-letter code) — this silently drops rows, it doesn't clean them further.

### `utils/api_utils.py`

`MSGraphApi`/`TenoviApi` wrap `RestAdapter` (`utils/rest_adapter.py`). `RestAdapter` builds request URLs with `urljoin(base_url, endpoint)`, not string concatenation — every `base_url` here **must** end with a trailing slash and every `endpoint` passed to `.get()`/`.post()`/etc. **must not** start with a leading slash, or `urljoin` silently drops the base URL's own path segment (e.g. Tenovi's `/clients/{domain}` or Graph's `/v1.0`) and produces a wrong-but-live URL. `RestAdapter` raises `requests.HTTPError` on 4xx/5xx after retries (it does not swallow and return `None`) — callers are expected to let that propagate.

### Inlined helpers (`utils/rest_adapter.py`, `utils/atomic_io.py`, `utils/tabular_io.py`)

These were copied from the `py-shared-tools` library (v1.3.1) so the repo is self-contained: no second repository, submodule or git dependency is needed. Only what the pipeline uses was copied: `RestAdapter`/`RestAdapterConfig` (HTTP), `atomic_io.ensure_dir` (idempotent directory creation) with `atomic_write`, and `tabular_io.write_structured_file` (DataFrame -> xlsx, used by `DataImporter.snap_dataframe` and `create_billing_report`). `logger.py`'s own `setup_logger` is deliberately **not** replaced by that library's `logging_setup` — the shared version only attaches a console handler, while this repo needs the per-name persistent file handler + colorlog formatting `setup_logger` provides. `requires-python = ">=3.12"` was originally set because `py-shared-tools` requires it; it was left unchanged when the code was inlined.

### Logging

`logger.setup_logger(name, level)` is idempotent by checking `logger.handlers` (not `logger.hasHandlers()`, which also checks ancestor/root loggers and would wrongly no-op if the root logger already has a handler attached from elsewhere). Writes both a colorized console stream and a per-name file under `logs/{name}_logfile.log` (gitignored).

### Required environment variables (loaded via `.env` / `python-dotenv`)

GPS target DB: `GPS_SQL_USERNAME`, `GPS_SQL_PASSWORD`, `GPS_SQL_HOST`, `GPS_SQL_DB`.
Legacy source DBs (shared username/password, separate DB names): `LEGACY_SQL_USERNAME`, `LEGACY_SQL_PASSWORD`, `LEGACY_SQL_HOST`, `LEGACY_SQL_SP_NOTES`, `LEGACY_SQL_SP_TIME`, `LEGACY_SQL_SP_FULFILLMENT`, `LEGACY_SQL_SP_READINGS`.
Azure AD (MS Graph): `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, plus `AZURE_GROUP_ID` (the group whose members are imported into the `user` table).
All are read via `os.environ[...]` (not `.get()`), so a missing var fails fast with a `KeyError` naming it rather than silently passing `None` into `pyodbc`.

`main()` (`__main__.py`) currently hardcodes its billing period date ranges (`import_all_data("2025-01-01", "2025-02-28", ...)`) rather than deriving them — `helpers.get_last_month_billing_cycle()` exists but isn't wired into `main()` yet.
