# Design decisions

These records explain why the pipeline is built the way it is. Each one is a short note in the style of [MADR](https://adr.github.io/madr/): context, the decision, alternatives considered, consequences, and links to the code and tests that pin the behaviour.

They were written from what this repository records: the code, the stored procedures, the tests, the docs and the commit history. Where the repository does not say why something was chosen, or what else was considered, the record says so with a `TODO` marker for the maintainer to fill in instead of guessing.

Decisions are not edited to hide a change of mind. A record that is later replaced is marked as superseded and points to its replacement; it is not deleted.

| # | Decision | Summary |
|---|----------|---------|
| [0001](0001-staged-etl-in-pandas.md) | Stage the pipeline as extract, transform in pandas, load | Read into DataFrames, transform with pandas functions, load into SQL Server in dependency order. |
| [0002](0002-billing-rules-in-stored-procedures.md) | Implement billing rules as SQL Server stored procedures | Billing codes are computed by T-SQL stored procedures; Python only calls them and exports the report. |
| [0003](0003-medical-code-rows-carry-the-date-of-service.md) | Record billable events as timestamped medical-code rows | A code is a timestamped `medical_code` row; the report derives the date of service from it. |
| [0004](0004-database-assigned-keys-and-full-reset.md) | Let the database assign keys; make runs repeatable by full reset | Database identity keys, `temp_*` columns resolved at the end, and a full reset per run. |
| [0005](0005-validate-in-transform-and-drop-bad-patient-rows.md) | Validate in the transform stage and drop rows that break column limits | Normalize, then drop patient rows that break column limits, with no reporting. |
| [0006](0006-source-extraction-and-environment-credentials.md) | Extract from a CSV export, legacy SQL and Graph; supply credentials by environment | Patient CSV export, legacy SQL and Microsoft Graph as sources; all credentials from environment variables. |
| [0007](0007-sqlalchemy-engine-and-raw-sql.md) | Use SQLAlchemy as an engine and session layer with raw SQL, not ORM models | SQLAlchemy engines and sessions with raw SQL and pandas I/O; no ORM models. |
| [0008](0008-mocked-unit-tests-and-real-sql-server-integration-tests.md) | Test with mocked unit tests and integration tests against a real SQL Server | Mocked unit tests by default; integration tests against a disposable SQL Server. |
| [0009](0009-synthetic-only-data-and-deterministic-generator.md) | Use only synthetic data, generated deterministically with a by-construction manifest | Synthetic data only, generated deterministically with an expected-results manifest built by construction. |
| [0010](0010-shared-http-client-and-failing-loudly.md) | Use the shared REST adapter and let HTTP failures stop the run | Shared REST adapter; HTTP errors raise and stop the run. |
| [0011](0011-uv-src-layout-and-python-312.md) | Manage dependencies with uv, use a src layout, require Python 3.12 | uv and `uv.lock`, `src/` layout, Python 3.12 or newer. |
| [0012](0012-lint-type-check-and-test-in-ci.md) | Lint, type-check and test on every push and pull request | Ruff, mypy and tests in CI and pre-commit; a separate integration job. |
