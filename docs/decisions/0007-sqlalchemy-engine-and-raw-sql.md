# 0007. Use SQLAlchemy as an engine and session layer with raw SQL, not ORM models

Status: Superseded by [0015](0015-full-orm-schema-of-record.md) (2025-01-21, `1876b04`, `60131d0`; superseded 2026-09-22). The "no mapped classes" part of this decision no longer holds for the GPS load path; `DatabaseManager`'s engine/session layer, `execute_query`, and `to_sql` for the legacy demo tables are unaffected and still described accurately below.

## Context

The pipeline reads several SQL Server databases and writes one, mixing pandas bulk writes with stored procedure calls.

## Decision

`DatabaseManager` wraps SQLAlchemy at engine and session level: one `mssql+pyodbc` engine per database (ODBC Driver 18), `fast_executemany` switched on by an event listener, pandas `read_sql` and `to_sql` for bulk data, and raw SQL through `session.execute(text(...))` with a session opened and closed per call. There are no mapped classes.

## Alternatives considered

Using SQLAlchemy at all was the author's own preferred direction, brought in for proper connection and session management from Python rather than hand-rolled `pyodbc` calls. Full ORM-mapped classes were not part of that goal: the billing rules themselves were required to stay in SQL (see [0002](0002-billing-rules-in-stored-procedures.md)), so there was no logic left on the Python side that ORM models would have served. Connection handling changed once beyond that: "add connect and close connections" (`a4783c5`) was reverted (`5d4605d`, 2025-02-01).

## Consequences

- `execute_query` logs errors and returns `None` rather than raising, so a failing procedure produces no codes and no exception.
- Rows are fetched before committing. Committing first broke real SQL Server cursors and was found by the integration suite (`8ed3799`).
- The port is fixed at 1433 in `create_engine`; non-default ports go in the host string (`host,port`).
- Column mismatches surface at run time, not at import.

## Evidence

- [`db_utils.py`](../../src/medicare_rebuild/utils/db_utils.py)
- [`test_execute_query_handles_error`](../../tests/test_db_utils.py)
- [`test_execute_query_rolls_back_on_error`](../../tests/integration/test_db_utils_integration.py)
