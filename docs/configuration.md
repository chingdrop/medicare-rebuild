# Configuration

What you need to run the pipeline against real sources. None of this is needed for the [synthetic demo](demo.md), which supplies its own throwaway values.

## Credentials

- Service Account on the Microsoft SQL Server hosting the new database.
- Service Account on the Microsoft SQL Servers hosting the old databases.
- Azure Active Directory (AD) application credentials.

## Environment variables

Loaded from a `.env` file (via python-dotenv) or the environment. All are read with `os.environ[...]`, so a missing variable fails immediately with a `KeyError` naming it.

| Group | Variables |
|-------|-----------|
| GPS target database | `GPS_SQL_USERNAME`, `GPS_SQL_PASSWORD`, `GPS_SQL_HOST`, `GPS_SQL_DB` |
| Legacy source databases (shared username, password and host) | `LEGACY_SQL_USERNAME`, `LEGACY_SQL_PASSWORD`, `LEGACY_SQL_HOST`, plus one database name each: `LEGACY_SQL_SP_NOTES`, `LEGACY_SQL_SP_TIME`, `LEGACY_SQL_SP_FULFILLMENT`, `LEGACY_SQL_SP_READINGS` |
| Azure AD (Microsoft Graph) | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_GROUP_ID` (the group whose members are imported into the `user` table) |

## Setting up the GPS database schema

A real GPS database is created with Alembic, not `CREATE TABLE` statements run by hand:

```sh
make migrate            # or: uv run alembic upgrade head
```

This reads the same `GPS_SQL_*` variables as everything else and applies every
migration in [`alembic/versions/`](../alembic/versions/) up to the latest. See
[decision 0016](decisions/0016-alembic-migrations-for-the-gps-database.md) and
[`alembic/README.md`](../alembic/README.md). The demo and integration tests don't use
this -- they build an ephemeral database directly from `medicare_rebuild.models`, which
is faster and needs no migration history for a database that gets thrown away anyway.

## Running against real sources

```sh
uv run medicare-rebuild
```

- The patient export must be at `data/Patient_Export.csv`, relative to the working directory.
- `main()` currently hardcodes its date windows: import `2025-01-01` to `2025-02-28`, billing report `2025-02-01` to `2025-02-28`.
- Requires the ODBC Driver 18 for SQL Server; see [Tech stack](../README.md#tech-stack).
