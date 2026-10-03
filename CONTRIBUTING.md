# Contributing

How to set up, check and document a change. The project's own guide for working in the
code is [CLAUDE.md](CLAUDE.md); why it is built this way is in
[docs/decisions/](docs/decisions/README.md).

## Prerequisites

- [uv](https://docs.astral.sh/uv/). It reads `.python-version` (3.12) and installs that
  Python if needed; `requires-python` is `>=3.12`.
- For the integration tests and `make demo` only: Docker, and the ODBC Driver 18 for SQL
  Server (macOS: `brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18`).

## Setup

```sh
uv sync                                         # dependencies + .venv, from uv.lock
uv run pre-commit install                       # ruff, mypy, gitleaks and the data-file guard on commit
git config blame.ignoreRevsFile .git-blame-ignore-revs   # blame skips pure reformat commits
```

## Before opening a pull request

Run what CI runs (`.github/workflows/ci.yml`):

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy                      # src/ only
uv run pytest --cov              # unit tests; fails below the coverage floor
uv run pytest -m integration     # needs `docker compose up -d` and the ODBC driver
```

`make demo` runs the whole pipeline on synthetic data and `make reconcile` audits the
run; see [docs/demo.md](docs/demo.md) and [docs/reconciliation.md](docs/reconciliation.md).

## Data: synthetic only

This repository holds synthetic data only. Real patient data, credentials, tenant IDs
and client configuration must never be committed, in files or in history.

- The `no-data-files` pre-commit hook, and the matching "No data files" step in CI, block
  CSV, Excel, database and backup files. Generated demo data lives in the git-ignored
  `demo_data/` and `demo_output/`.
- gitleaks scans staged changes in the pre-commit hook and the full history in CI.
- New test data: see
  [docs/provenance-and-data-boundary.md](docs/provenance-and-data-boundary.md#contributing-test-data-safely).

## Tests

- Unit tests mock every external system; integration tests in `tests/integration/` run
  against a real SQL Server ([decision 0008](docs/decisions/0008-mocked-unit-tests-and-real-sql-server-integration-tests.md)).
- Layout: once there are more than 10 test modules outside `integration/`, tests mirror
  the code they test (`tests/utils/`, `tests/vendor/`, `tests/tools/`), with tests of
  top-level modules at the top. Every test directory has an `__init__.py`.
- pytest's default import mode is kept: tests import `tools` from the repository root.
- Coverage: branch coverage is on, and the floor (`fail_under` in `pyproject.toml`) is
  the measured baseline minus 2, rounded down. When the baseline climbs past floor + 4,
  raise the floor in the same pull request.

## Vendored code

`src/medicare_rebuild/vendor/` holds copies of modules from
[py-shared-tools](https://github.com/chingdrop/py-shared-tools) under their original
names. Fix them upstream first, then re-copy; don't let the copies drift
([decision 0013](docs/decisions/0013-inline-the-shared-helpers.md)).

## Decisions (ADRs)

A design decision gets a short record in `docs/decisions/`, numbered next in sequence,
in the existing MADR style: Status, Context, Decision, Alternatives considered,
Consequences, Evidence. Add it to the table in
[docs/decisions/README.md](docs/decisions/README.md). Decisions are not edited to
change their meaning: a replaced decision is marked superseded and points to its
replacement.

## Changelog

Add a line under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md), in the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) sections (`Added`, `Changed`,
`Fixed`, `Removed`). Version numbers are set at release, not per change.
