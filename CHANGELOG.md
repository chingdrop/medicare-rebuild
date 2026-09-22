# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project uses [Semantic Versioning](https://semver.org/). Documented history begins at
the start of this rebuild (`4199c05`, 2025-01-15); this changelog does not cover
anything before that commit.

<!-- markdownlint-disable MD024 -->

## [Unreleased]

### Added

- `src/medicare_rebuild/models.py` and `legacy_models.py`: SQLAlchemy declarative
  models that are now the GPS schema of record, and Core table definitions for the
  legacy source tables. `sql/schema.sql` is generated from them (`make schema`),
  checked for drift by a test. See
  [decision 0015](docs/decisions/0015-full-orm-schema-of-record.md).
- `src/medicare_rebuild/billing.py`: billing computation ported from the T-SQL stored
  procedures to pandas and the ORM, with unit tests (`tests/test_billing.py`) pinning
  the exact boundary cases `docs/billing-rules.md` documents. See
  [decision 0014](docs/decisions/0014-pandas-billing-rules.md).
- `alembic/`: versioned migrations for the GPS database, generated from
  `models.py`. `make migrate` (`alembic upgrade head`) creates or updates a real GPS
  database; `tests/integration/test_alembic_integration.py` guards the migration
  chain against drifting from the models. See
  [decision 0016](docs/decisions/0016-alembic-migrations-for-the-gps-database.md).

### Changed

- `DataImporter`'s load path now inserts through the ORM and resolves identity and
  lookup-table foreign keys via `session.flush()` and small dict lookups, replacing
  `add_id_col`, the three `get_*_id_stmt` queries, and the four deferred
  `UPDATE ... WHERE temp_X IS NOT NULL` statements. The demo and integration tests
  build their GPS databases from the same models instead of independently
  reconstructed DDL.
- `create_billing_report()`'s internals now call `billing.run_billing()` and
  `billing.build_billing_report()` instead of executing the `batch_medcode_*` and
  `create_billing_report` stored procedures; its public signature is unchanged. A
  billing error now raises and stops the run instead of being logged and swallowed.
- `DatabaseManager.create_engine`'s connection-URL construction moved into a standalone
  `build_mssql_url()` (`db_utils.py`) so `alembic/env.py` can build the same URL from
  the same `GPS_SQL_*` variables without duplicating it.

### Removed

- `add_id_col` (`utils/dataframe_utils.py`) and the deferred-`UPDATE` SQL in
  `queries.py`; the pipeline's own `reset_all_billing_tables` stored-procedure call,
  replaced by `models.reset_all_data`.
- The demo's stored-procedure installation (`tools/synthetic_data/schema.py`'s
  `PROCEDURES` list and `procedure_sql()`); the demo database no longer installs any
  stored procedure. `sql/stored_procedures/*.sql` remain in the repository for
  reference (see `sql/stored_procedures/README.md`), but nothing calls them anymore.

## [1.0.0] - 2026-09-22

### Added

- Staged ETL pipeline: extract patient, device, reading and note data from legacy SQL
  Server tables, a SharePoint patient export and a Microsoft Graph user directory;
  transform with pandas `standardize_*`/`create_*`/`normalize_*` functions; load into
  a new SQL Server ("GPS") schema. See [decision 0001](docs/decisions/0001-staged-etl-in-pandas.md)
  and [README: What it does](README.md#what-it-does).
- Billing-rule stored procedures for CPT codes 99202, 99453, 99454, 99457 and 99458,
  and a billing report grouped by date of service. See
  [docs/billing-rules.md](docs/billing-rules.md) and
  [decision 0002](docs/decisions/0002-billing-rules-in-stored-procedures.md).
- The GPS database schema and entity-relationship diagrams
  ([docs/erd/](docs/erd/)), and [decision 0003](docs/decisions/0003-medical-code-rows-carry-the-date-of-service.md)
  on how the schema records date of service.
- A two-layer test suite: mocked unit tests (default) plus integration tests against
  a disposable SQL Server container (`docker-compose.yml`), run as separate CI jobs.
  See [decision 0008](docs/decisions/0008-mocked-unit-tests-and-real-sql-server-integration-tests.md).
- A deterministic synthetic data generator and one-command demo (`make demo`) that
  runs the full pipeline against generated data with a by-construction
  expected-results manifest and a PASS/FAIL summary; no credentials or real data
  required. See [docs/demo.md](docs/demo.md) and
  [decision 0009](docs/decisions/0009-synthetic-only-data-and-deterministic-generator.md).
- Post-run reconciliation (`make reconcile`) that independently checks row
  conservation, key integrity, cross-source consistency, rejection accounting,
  billing lineage and report totals, plus optional fault injection
  (`--inject-fault`) to prove the checks catch real problems. See
  [docs/reconciliation.md](docs/reconciliation.md).
- Architecture decision records covering why the pipeline is built the way it is,
  indexed at [docs/decisions/](docs/decisions/README.md).
- Portfolio documentation: [provenance and data boundary](docs/provenance-and-data-boundary.md),
  [data handling](docs/data-handling.md), [architecture](docs/architecture.md),
  [configuration](docs/configuration.md), an [engineering narrative](docs/narrative.md),
  and [SECURITY.md](SECURITY.md).
- CI: lint, type-check, unit test, integration test and security jobs (pip-audit,
  gitleaks over full history, a tracked-data-file guard), plus CodeQL scanning and
  weekly Dependabot updates. See
  [decision 0012](docs/decisions/0012-lint-type-check-and-test-in-ci.md).
- `.env.example` listing every configuration variable with a placeholder value.

### Changed

- Inlined the REST adapter, directory helper and DataFrame writer previously
  imported from a separate git-dependency library into `medicare_rebuild.utils`,
  removing the external dependency so a fresh clone needs no second repository. See
  [decision 0013](docs/decisions/0013-inline-the-shared-helpers.md).
- Dependency management moved to [uv](https://docs.astral.sh/uv/) with a committed
  lock file, a `src/` package layout, and a Python 3.12 floor. See
  [decision 0011](docs/decisions/0011-uv-src-layout-and-python-312.md).

### Removed

- `JOURNEY.md` removed from the working tree; its reusable content was generalized
  into [docs/narrative.md](docs/narrative.md). The original file remains readable in
  git history.

### Fixed

- `DatabaseManager.execute_query` committed a transaction before fetching its result
  rows, which broke against a real SQL Server cursor but was invisible to the mocked
  unit tests; found by adding the integration test suite. See
  [decision 0008](docs/decisions/0008-mocked-unit-tests-and-real-sql-server-integration-tests.md).
- State-name and Medicare Beneficiary Identifier standardization bugs (case-sensitive
  keyword matching; MBI dashes not stripped before pattern matching), caught while
  adding unit test coverage.

<!--
No git tag exists yet for this repository. The [1.0.0] link below compares the
first commit to the current tip of `main`, which will keep moving until a `v1.0.0`
tag is actually cut - at that point, replace it with a fixed compare or release link,
and add an [Unreleased] link comparing v1.0.0...main.
-->

[1.0.0]: https://github.com/chingdrop/medicare-rebuild/compare/4199c05...main
