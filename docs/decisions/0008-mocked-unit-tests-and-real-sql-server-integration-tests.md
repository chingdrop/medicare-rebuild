# 0008. Test with mocked unit tests and integration tests against a real SQL Server

Status: Accepted (unit tests 2025-03-26, `b568b15`; integration suite 2026-07-16, `efee409`)

## Context

Unit tests that mock every collaborator are fast but cannot catch driver-level bugs. `execute_query` committed before fetching rows; the mocked tests passed and the first real query failed (`8ed3799`). The repo has no schema file to build a test database from.

## Decision

Two layers. Unit tests mock external systems and run by default. Integration tests run against a disposable SQL Server container (`docker-compose.yml`), with only Microsoft Graph mocked, marked `integration` and excluded by default. They skip if no server is reachable. CI runs the layers as separate jobs.

## Alternatives considered

Mocked unit tests alone, the state before `efee409`, which its message calls insufficient. No other alternative is recorded.

## Consequences

- Integration tests need Docker and the ODBC driver.
- Their schema is inferred from the DataFrame columns the code writes, so it must be updated when those columns change.
- The original integration tests covered only the user and patient import paths. The synthetic demo test later added readings, notes, devices and the billing procedures.
- Both layers run on every push and pull request.

## Evidence

- [`docker-compose.yml`](../../docker-compose.yml), [`tests/integration/conftest.py`](../../tests/integration/conftest.py)
- [`pyproject.toml`](../../pyproject.toml) (`integration` marker, `addopts`), [`ci.yml`](../../.github/workflows/ci.yml)
- [`test_db_utils_integration.py`](../../tests/integration/test_db_utils_integration.py)
