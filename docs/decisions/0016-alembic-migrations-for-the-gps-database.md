# 0016. Manage the GPS schema with Alembic migrations

Status: Accepted (2026-09-22)

## Context

`models.py` became the GPS schema of record in [decision 0015](0015-full-orm-schema-of-record.md):
`sql/schema.sql` is generated from it, and both the demo and the integration tests build
their databases from it via `GpsBase.metadata.create_all()`. That covers a fresh,
throwaway database well, but says nothing about a database that already exists and
already holds data: `create_all()` only creates tables that are missing, it does not
alter a table whose columns no longer match the models, and there was no tool in this
repository for taking an existing GPS database from one version of the schema to the
next.

## Decision

[Alembic](https://alembic.sqlalchemy.org/) manages the GPS database's schema going
forward. `alembic/env.py` targets `GpsBase.metadata` only -- the legacy source
databases are a reconstruction of a system this repository does not own (see
`legacy_models.py`), so there is nothing meaningful to migrate there. It reads
connection details from the same `GPS_SQL_*` environment variables every other entry
point in this repository reads, through a small `build_mssql_url()` helper factored out
of `DatabaseManager.create_engine` so there is exactly one place a connection URL gets
assembled from those variables, not two. The one migration committed so far
(`alembic/versions/*_initial_schema.py`) was generated with `alembic revision
--autogenerate` against an empty database and reproduces the current schema exactly --
confirmed by `alembic check` reporting no drift, and pinned by
`tests/integration/test_alembic_integration.py`.

The demo and integration tests are **not** changed to use Alembic: they still build
their GPS database directly from `GpsBase.metadata.create_all()`. A migration chain
exists to take a real, persistent database from one schema version to the next; a test
or demo database is thrown away and rebuilt every run, so there is no "next version" to
migrate from and `create_all()` remains the faster, simpler tool for that job.

## Alternatives considered

Keeping `create_all()`/`drop_all()` as the only schema-management mechanism everywhere,
including for a real deployment, was the status quo and not chosen: it has no way to
alter an existing table in place, no ordered history of what changed and when, and no
rollback path, all of which matter once a GPS database holds real data across multiple
deployments -- none of which describes the demo or integration-test databases, which is
why those two keep `create_all()` rather than adopting Alembic too.

## Consequences

- A model change now needs a paired migration (`alembic revision --autogenerate -m
  "..."`, reviewed, then committed) the same way it already needs `make schema` to
  regenerate `sql/schema.sql`; two generated artifacts now have to be kept in sync with
  `models.py`, not one. `tests/test_generate_schema.py` and
  `tests/integration/test_alembic_integration.py` each guard one of them, independently.
- `alembic.ini` deliberately leaves `sqlalchemy.url` unset; `alembic/env.py` builds the
  connection from `GPS_SQL_*` instead, so there is no credential-bearing connection
  string to accidentally commit.
- `DatabaseManager.create_engine`'s URL construction moved into a standalone
  `build_mssql_url()` function in `db_utils.py` so `env.py` could reuse it without
  duplicating it or instantiating a full `DatabaseManager`.
- A real deployment now runs `make migrate` (`alembic upgrade head`) once against a
  fresh GPS database instead of relying on some other, undocumented DDL step; see
  [docs/configuration.md](../configuration.md).

## Evidence

- [`alembic/env.py`](../../alembic/env.py), [`alembic/versions/`](../../alembic/versions/),
  [`alembic.ini`](../../alembic.ini)
- [`build_mssql_url`](../../src/medicare_rebuild/utils/db_utils.py)
- [`test_alembic_integration.py`](../../tests/integration/test_alembic_integration.py)
- [`docs/configuration.md`](../configuration.md) (Setting up the GPS database schema)
