# 0017. Adopt the shared Python tooling standard

Status: Accepted (2026-10-03)

## Context

This repository is one of four the author maintains with the same shape: a `src/`
package managed with uv ([0011](0011-uv-src-layout-and-python-312.md)), ruff and mypy in
CI and pre-commit ([0012](0012-lint-type-check-and-test-in-ci.md)), and code copied from
`py-shared-tools` ([0013](0013-inline-the-shared-helpers.md)). Each had drifted into its
own settings: build backend, line length, rule set, mypy flags, where copied code lives,
test layout and how the coverage floor is set. Aligning them on one standard means a
change of habit in one repository applies to all four.

## Decision

- **Packaging.** Build with hatchling instead of setuptools, with the wheel's package set
  explicitly (`src/medicare_rebuild`). Hatchling's sdist takes everything not
  gitignored, so an explicit sdist exclude list keeps local data, logs, `.env` files and
  data-file types out regardless of `.gitignore`. A `.python-version` file pins local
  runs to 3.12; `requires-python` stays `>=3.12`.
- **Ruff.** Line length 120. The rule set adds `SIM` (flake8-simplify) to
  `E, F, I, B, UP, S`. `E501` is enforced outside tests instead of ignored globally:
  at 120 characters only 11 lines exceeded it and were re-wrapped. Tests keep the
  exemption for long fixture rows and expected strings.
- **mypy.** Adds `warn_unused_configs`, `warn_redundant_casts`, `warn_unused_ignores`,
  `strict_equality` and `check_untyped_defs`, still over `src/` only.
- **Vendored code.** Copies from `py-shared-tools` live in `src/medicare_rebuild/vendor/`
  under their original module and symbol names, so they read as copies to diff against
  the library. Changes go upstream first, then are re-copied. `logger.py` is project
  code and stays outside it.
- **Test layout.** Once there are more than 10 test modules outside
  `tests/integration/`, tests mirror the code they test (`tests/utils/`,
  `tests/vendor/`, `tests/tools/`), with tests of top-level modules at the top.
- **pytest import mode.** The default (`prepend`) is kept. The tests import the
  repository-root `tools` package, which resolves because the default mode puts the
  root on `sys.path`; every test directory has an `__init__.py` so mirrored module names
  don't collide.
- **Coverage.** Branch coverage on. The floor is `fail_under` in `pyproject.toml`, so
  local runs enforce it as CI does: the measured baseline minus 2, rounded down, and
  raised in the same pull request once the baseline exceeds floor + 4.

## Alternatives considered

- **Keep setuptools.** It worked, but each of the four repositories would keep its own
  packaging config; hatchling needs one table for a `src/` layout.
- **Ruff's default line length of 88**, or keeping `E501` ignored globally. 88 made the
  rule unenforceable here (80 long lines were too many to rewrap, hence the global
  ignore); at 120 it can be enforced with 11 fixes.
- **Leave the copied helpers in `utils/`.** Mixed in with project code, they read as
  this repository's own and invite local edits that drift from the library.
- **pytest's `importlib` import mode.** It avoids `sys.path` changes, but the tests'
  `import tools` would then need the repository root added some other way.
- **Keep the coverage floor as a CI flag.** A local `pytest --cov` would not enforce it.

## Consequences

- One reformat commit touched 41 files; it is listed in `.git-blame-ignore-revs`.
- The built wheel's package files are unchanged; its metadata no longer includes
  `top_level.txt`.
- `check_untyped_defs` found a real gap: `DatabaseManager.read_sql`/`to_sql` passed an
  engine that is `None` until `create_engine()` runs; they now raise a clear error.
- The branch-coverage baseline (74.71%) is not comparable with the earlier line-only
  figure (69.76%); the floor moves from 68 to 72.
- Anything vendored must be fixed in `py-shared-tools` first.

## Evidence

- [`pyproject.toml`](../../pyproject.toml) (`[build-system]`, `[tool.hatch.*]`,
  `[tool.ruff]`, `[tool.mypy]`, `[tool.coverage.*]`), [`.python-version`](../../.python-version)
- [`vendor/`](../../src/medicare_rebuild/vendor/__init__.py)
- [`tests/utils/`](../../tests/utils/), [`tests/vendor/`](../../tests/vendor/), [`tests/tools/`](../../tests/tools/)
- [`ci.yml`](../../.github/workflows/ci.yml) (test job), [`.git-blame-ignore-revs`](../../.git-blame-ignore-revs)
- [`CONTRIBUTING.md`](../../CONTRIBUTING.md)
