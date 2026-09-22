# 0013. Inline the shared helpers so the repository is self-contained

Status: Accepted (2026-09-21)

## Context

The pipeline used three helpers from a separate library, `py-shared-tools`, installed as a git dependency pinned to a tag ([0010](0010-shared-http-client-and-failing-loudly.md)): a REST adapter, a directory helper and a DataFrame writer. A fresh clone therefore needed a second repository fetched at install time, and the library set the Python floor ([0011](0011-uv-src-layout-and-python-312.md)).

## Decision

Copy only the code the pipeline uses into the main package (`utils/rest_adapter.py`, `utils/atomic_io.py`, `utils/tabular_io.py`), with their tests, and remove the dependency. Public names are unchanged, so call sites changed only by import path. Each copied file records its origin and authorship.

## Alternatives considered

The alternatives are not recorded in the repository.

<!-- TODO(craig): why inline rather than publish the library to PyPI, use a submodule, or keep the git dependency. -->

## Consequences

- Setup is one command (`uv sync`) with no second repository.
- Two copies exist if the library is still used elsewhere; fixes must be made in both, or the copies will drift.
- The library is licensed GPL-3.0 and this repository is MIT. <!-- TODO(craig): confirm the licence for the copied files. -->
- `requires-python` stays at 3.12; lowering it was not tested.
- `certifi` and `urllib3` became direct dependencies, because the copied adapter imports them.

## Evidence

- [`rest_adapter.py`](../../src/medicare_rebuild/utils/rest_adapter.py), [`atomic_io.py`](../../src/medicare_rebuild/utils/atomic_io.py), [`tabular_io.py`](../../src/medicare_rebuild/utils/tabular_io.py)
- [`test_rest_adapter.py`](../../tests/test_rest_adapter.py), [`test_atomic_io.py`](../../tests/test_atomic_io.py), [`test_tabular_io.py`](../../tests/test_tabular_io.py)
- [`pyproject.toml`](../../pyproject.toml) (no git source)
