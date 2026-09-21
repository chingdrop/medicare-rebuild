# 0007. Use SQLAlchemy as an engine and session layer with raw SQL, not ORM models

Status: Accepted (2025-01-21, `1876b04`, `60131d0`)

## Context

The pipeline reads several SQL Server databases and writes one, mixing pandas bulk writes with stored procedure calls.

## Decision

`DatabaseManager` wraps SQLAlchemy at engine and session level: one `mssql+pyodbc` engine per database (ODBC Driver 18), `fast_executemany` switched on by an event listener, pandas `read_sql` and `to_sql` for bulk data, and raw SQL through `session.execute(text(...))` with a session opened and closed per call. There are no mapped classes.

## Alternatives considered

None recorded for ORM versus raw SQL. Connection handling changed once: "add connect and close connections" (`a4783c5`) was reverted (`5d4605d`, 2025-02-01).

<!-- TODO(craig): why raw SQL and pandas rather than ORM models. -->

## Consequences

- `execute_query` logs errors and returns `None` rather than raising, so a failing procedure produces no codes and no exception.
- Rows are fetched before committing. Committing first broke real SQL Server cursors and was found by the integration suite (`8ed3799`).
- The port is fixed at 1433 in `create_engine`; non-default ports go in the host string (`host,port`).
- Column mismatches surface at run time, not at import.

## Evidence

- [`db_utils.py`](../../src/medicare_rebuild/utils/db_utils.py)
- [`test_execute_query_handles_error`](../../tests/test_db_utils.py)
- [`test_execute_query_rolls_back_on_error`](../../tests/integration/test_db_utils_integration.py)
