# 0011. Manage dependencies with uv, use a src layout, require Python 3.12

Status: Accepted (2026-07-16, `680c585`). The `py-shared-tools` dependency behind the Python floor was later removed ([0013](0013-inline-the-shared-helpers.md)); the floor itself was not changed.

## Context

The project had a `setup.py` and a `requirements.txt`. The declared dependencies included one unused package that fails to build without a local PostgreSQL. The entry script sat outside the package, so it was missing from the built wheel (`6b0aa57`).

## Decision

Dependencies live in `pyproject.toml` with a committed `uv.lock`, and CI installs with `uv sync --locked`. Code is in a `src/` layout under the `medicare_rebuild` package, with a `medicare-rebuild` console script. `requires-python` is `>=3.12`.

## Alternatives considered

`setup.py` and `requirements.txt` were removed as superseded (`bc836b8`). The Python floor was raised from 3.10 to 3.12 because `py-shared-tools` requires it; "the alternative was not adding the dependency at all" (`394eed7`), which dropped 3.10 and 3.11 support.

## Consequences

- Contributors need uv, or must install from the lock file another way.
- Python 3.12 or newer only.
- One lock file drives local runs, CI and the demo.

## Evidence

- [`pyproject.toml`](../../pyproject.toml), [`uv.lock`](../../uv.lock)
- [`Makefile`](../../Makefile), [`ci.yml`](../../.github/workflows/ci.yml)
- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`main`, the console-script target)
