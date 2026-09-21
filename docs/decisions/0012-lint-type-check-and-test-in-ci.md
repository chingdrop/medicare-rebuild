# 0012. Lint, type-check and test on every push and pull request

Status: Accepted (2026-07-16, `e552828`; ruff `368477e`, mypy `ab71410`; pre-commit 2026-07-17, `7e63dbc`)

## Context

Ruff found a real cross-version bug: backslash escapes inside f-string expressions, a syntax error before Python 3.12 (`368477e`). Mypy started at 79 errors (`ab71410`).

## Decision

The CI `test` job runs `ruff check`, `ruff format --check`, `mypy` (over `src/` only) and the unit tests. A separate `integration-test` job runs the integration suite against a SQL Server service container. A pre-commit config runs the same ruff and mypy hooks locally.

## Alternatives considered

Mypy was first configured to ignore pandas import errors because "full pandas-stubs adoption is a much larger effort than this pass warrants" (`ab71410`). Pandas-stubs was added later and surfaced five real errors (`114115a`).

## Consequences

- Type checking covers `src/` only. `tools/` and the tests are linted but not type-checked.
- Formatting is enforced in CI, not just suggested.
- A commit can fail CI on formatting alone (`e38c40c`).

## Evidence

- [`ci.yml`](../../.github/workflows/ci.yml), [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml)
- [`pyproject.toml`](../../pyproject.toml) (`[tool.ruff]`, `[tool.mypy]`)
