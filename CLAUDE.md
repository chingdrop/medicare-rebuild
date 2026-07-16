# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An ETL pipeline that rebuilds the data architecture for a medical company's remote physician monitoring (diabetes/hypertension telehealth) billing. It extracts patient/device/reading data from legacy SQL databases and a SharePoint CSV export, standardizes it with pandas, and loads it into a new "GPS" SQL Server database whose schema correctly tracks Medicare billing CPT codes (99202, 99453, 99454, 99457, 99458 — see `sql/stored_procedures/batch_medcode_99XXX.sql`).

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
```

CI (`.github/workflows/ci.yml`) runs `test` (ruff + mypy + unit pytest) and `integration-test` (real `mssql` service container) as separate jobs on every push/PR.

### Integration tests

`tests/integration/` exercises `DatabaseManager` and `DataImporter` against a **real** SQL Server rather than mocks (MS Graph/Tenovi HTTP calls are still mocked via `requests_mock`). Needs:

1. `docker compose up -d` — starts a disposable `mcr.microsoft.com/mssql/server` container.
2. ODBC Driver 18 for SQL Server installed locally (`brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18` on macOS).
3. `uv run pytest -m integration`

`tests/integration/conftest.py` creates a dedicated database per test session and skips gracefully if no server is reachable — connection details default to `docker-compose.yml`'s and are overridable via `INTEGRATION_DB_HOST`/`PORT`/`USER`/`PASSWORD`. On Apple Silicon the SQL Server image only runs via x86_64 emulation (no native arm64 build exists); GitHub's runners are x86_64 natively.

There's no `schema.sql` in this repo — `tests/integration/test_data_importer_integration.py` reconstructs the minimal GPS-database schema it needs by reading the DataFrame column names the code actually writes (`create_patient_df`, `create_patient_address_df`, etc.), not from an authoritative source. If those functions' output columns change, the test schema needs updating too.

## Architecture

### Two databases, two schemas, one pipeline

`DataImporter` (`src/medicare_rebuild/__main__.py`) reads from **legacy source databases** (env vars `LCH_SQL_*`, e.g. `LCH_SQL_SP_NOTES`, `LCH_SQL_SP_TIME`, `LCH_SQL_SP_FULFILLMENT`, `LCH_SQL_SP_READINGS`) — these use old capitalized table/column names (`Glucose_Readings`, `Blood_Pressure_Readings`, `Medical_Notes`, `Time_Log`, `Fulfillment_All`; see `queries.py`'s `get_*_stmt` constants) — and writes into the new **GPS database** (env vars `LCH_SQL_GPS_*`), which uses lowercase snake_case tables (`patient`, `patient_address`, `patient_insurance`, `medical_necessity`, `patient_status`, `emergency_contact`, `device`, `user`, `patient_note`, `glucose_reading`, `blood_pressure_reading`). `DatabaseManager` (`utils/db_utils.py`) is instantiated once per source database plus once for the GPS target (`self.gps`).

Patient/device/reading data crosses from the old schema's `sharepoint_id` (or `Vendor` name) to the new schema's auto-generated identity `patient_id`/`device_id`/`vendor_id` via `add_id_col()` (`utils/dataframe_utils.py`) — insert first, then `SELECT id, sharepoint_id FROM <table>` (`queries.py`'s `get_patient_id_stmt`/`get_device_id_stmt`/`get_vendor_id_stmt`) and merge the result back in before inserting dependent rows. Every `import_*_data` method in `DataImporter` follows this insert-parent-then-resolve-then-insert-children order.

A few `UPDATE ... WHERE temp_X IS NOT NULL` statements (`update_patient_note_stmt`, `update_patient_status_stmt`, `update_user_stmt`, `update_user_note_stmt` in `queries.py`) resolve `temp_*` staging columns (`temp_status_type`, `temp_user`, `temp_note_type`) into real FK columns (`patient_status_type_id`, `user_id`) — these run once at the end of `import_all_data()`, not per-DataImporter-method, because SQLAlchemy requires multi-statement SQL to be a stored procedure or a single `UPDATE ... SET x = (SELECT ...)`, not ad hoc multi-statement batches (see `JOURNEY.md`).

### `utils/dataframe_utils.py` — three function categories

- **Standardize** (`standardize_*`): clean a single value (state name -> 2-letter code via `keyword_search`/`enums.state_abbreviations`, phone numbers, MBI/insurance IDs, weight/height parsing, etc). Most return `str | float`, returning `np.nan` (not raising) when the input doesn't parse — callers apply these with `.apply()` over a Series.
- **Create** (`create_*_df`): slice one wide patient DataFrame into the six separate DataFrames matching the GPS schema's separate tables (patient/address/insurance/med_necessity/status/emcontacts).
- **Normalize** (`normalize_*`): apply the standardize functions across a raw DataFrame's relevant columns, then rename SharePoint/legacy column names to the GPS schema's column names. `normalize_patients` is the biggest and drives most of `check_patient_db_constraints`'s expectations afterward.

`check_patient_db_constraints` filters out rows that would violate DB column-length constraints (e.g. an unrecognized state string is too long once title-cased instead of being resolved to a 2-letter code) — this silently drops rows, it doesn't clean them further.

### `utils/api_utils.py`

`MSGraphApi`/`TenoviApi` wrap `shared_tools.rest_adapter.RestAdapter`. `RestAdapter` builds request URLs with `urljoin(base_url, endpoint)`, not string concatenation — every `base_url` here **must** end with a trailing slash and every `endpoint` passed to `.get()`/`.post()`/etc. **must not** start with a leading slash, or `urljoin` silently drops the base URL's own path segment (e.g. Tenovi's `/clients/{domain}` or Graph's `/v1.0`) and produces a wrong-but-live URL. `RestAdapter` raises `requests.HTTPError` on 4xx/5xx after retries (it does not swallow and return `None`) — callers are expected to let that propagate.

### `shared_tools` (py-shared-tools, git dependency pinned in `[tool.uv.sources]`)

Used for: `RestAdapter`/`RestAdapterConfig` (HTTP), `atomic_io.ensure_dir` (idempotent directory creation — used instead of a local reimplementation), `tabular_io.write_structured_file` (DataFrame -> xlsx, used by `DataImporter.snap_dataframe` and `create_billing_report`). `logger.py`'s own `setup_logger` is deliberately **not** replaced by `shared_tools.logging_setup` — the shared version only attaches a console handler, while this repo needs the per-name persistent file handler + colorlog formatting `setup_logger` provides. `requires-python = ">=3.12"` exists specifically because py-shared-tools requires it.

### Logging

`logger.setup_logger(name, level)` is idempotent by checking `logger.handlers` (not `logger.hasHandlers()`, which also checks ancestor/root loggers and would wrongly no-op if the root logger already has a handler attached from elsewhere). Writes both a colorized console stream and a per-name file under `logs/{name}_logfile.log` (gitignored).

### Required environment variables (loaded via `.env` / `python-dotenv`)

GPS target DB: `LCH_SQL_GPS_USERNAME`, `LCH_SQL_GPS_PASSWORD`, `LCH_SQL_GPS_HOST`, `LCH_SQL_GPS_DB`.
Legacy source DBs (shared username/password, separate DB names): `LCH_SQL_USERNAME`, `LCH_SQL_PASSWORD`, `LCH_SQL_HOST`, `LCH_SQL_SP_NOTES`, `LCH_SQL_SP_TIME`, `LCH_SQL_SP_FULFILLMENT`, `LCH_SQL_SP_READINGS`.
Azure AD (MS Graph): `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.
All are read via `os.environ[...]` (not `.get()`), so a missing var fails fast with a `KeyError` naming it rather than silently passing `None` into `pyodbc`.

`main()` (`__main__.py`) currently hardcodes its billing period date ranges (`import_all_data("2025-01-01", "2025-02-28", ...)`) rather than deriving them — `helpers.get_last_month_billing_cycle()` exists but isn't wired into `main()` yet.
