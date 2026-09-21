# 0010. Use the shared REST adapter and let HTTP failures stop the run

Status: Accepted (2026-07-16, `5c73d2a`)

## Context

`api_utils.py` carried a hand-written REST wrapper (about 280 lines that duplicated the shared package, per the commit). It set no timeout, retried only connection failures, and on an HTTP error logged and returned `None`.

## Decision

`MSGraphApi` and `TenoviApi` delegate to `shared_tools.rest_adapter.RestAdapter` from `py-shared-tools`, a git dependency pinned to a tag. It raises `requests.HTTPError` on 4xx/5xx after retries, so a failed call stops the run.

## Alternatives considered

Keeping the old wrapper, which returned `None` on errors. The commit chose raising because a failed call "should stop the run loudly rather than silently produce missing data downstream". `setup_logger` was deliberately not moved to the shared package, which only attaches a console handler.

## Consequences

- Default 10-second timeout, and retries on 429 and 5xx.
- Base URLs must end with a slash and endpoints must not start with one, because URLs are built with `urljoin` (CLAUDE.md).
- The project depends on a git-pinned package, which also sets the Python floor (see 0011).

## Evidence

- [`api_utils.py`](../../src/medicare_rebuild/utils/api_utils.py)
- [`test_get_group_members_raises_on_http_error`](../../tests/test_api_utils.py), `test_get_readings_raises_on_http_error` (same file)
- [`pyproject.toml`](../../pyproject.toml) (`[tool.uv.sources]`)
