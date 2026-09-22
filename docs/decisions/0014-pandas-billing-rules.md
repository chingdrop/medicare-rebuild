# 0014. Port billing rules from SQL stored procedures to pandas

Status: Accepted (2026-09-22)

## Context

Billing computation lived in T-SQL stored procedures (`sql/stored_procedures/batch_medcode_*.sql`,
`create_billing_report.sql`; see [0002](0002-billing-rules-in-stored-procedures.md)), a
direction set for the original project rather than a technical choice made on this
repository's own terms (see [docs/narrative.md](../narrative.md)). The rules could only be
exercised end to end against a real SQL Server, through the synthetic demo; there was no
way to unit-test a single rule's boundary condition (the 899/900-second cutoff for 99202,
say) without a database.

## Decision

`src/medicare_rebuild/billing.py` is a faithful pandas/ORM port of every `batch_medcode_*`
procedure and `create_billing_report`. Each CPT code keeps its own `apply_*` function,
called in the same fixed order `create_billing_report()` used to call the procedures in,
and each does its own query-compute-insert-commit -- matching the original's
one-procedure-per-`EXEC` granularity, which is what lets a later rule's "already has a
code" check see what an earlier one just committed. The pandas computation itself is
factored into small, pure helpers (`_qualifying_by_minutes`, `_qualifying_by_reading_days`,
`_qualifying_99458`, `_since`, `_in_report_window`) that take plain DataFrames and return
plain DataFrames, independent of any session, so `tests/test_billing.py` can hand-build the
documented boundary cases directly. This is a technology migration, not a rules rewrite:
every documented behavior in [docs/billing-rules.md](../billing-rules.md), including its
known gaps and quirks (the rolling windows, 99202's unescalated time band, the report's
midnight-of-end-date truncation), is preserved exactly, verified against the demo's
pre-existing report content hash.

The stored procedures stay in `sql/stored_procedures/` and in git history, for a portfolio
reader to compare the original SQL against the pandas port side by side; nothing in this
repository -- the pipeline, the demo, the integration tests -- calls them anymore.

## Alternatives considered

Keeping the procedures and running pandas alongside them, so the two could be compared on
every run, was on the table and not chosen: it would have doubled the database work for a
comparison this repository's tests already do a better job of (see Consequences), and it
would have meant permanently carrying two implementations of the same rules rather than
replacing one with the other.

## Consequences

- `DatabaseManager.execute_query` is no longer called anywhere in the pipeline; a billing
  error now raises and stops the run, rather than being logged and swallowed the way a
  failing stored procedure was (see [0007](0007-sqlalchemy-engine-and-raw-sql.md)'s
  Consequences). `execute_query` itself is unchanged and still available.
- `tools/synthetic_data/schema.py` no longer installs any stored procedure in the demo
  database; `PROCEDURES`/`procedure_sql()` are gone.
- Billing logic is unit-testable without a database for the first time -- `tests/test_billing.py`
  pins the exact boundary cases `docs/billing-rules.md` documents (899/900/1799/1800 s for
  99202, 1199/1200 s for 99457, 39/40/60/80/100 minutes for 99458, the 16-day and 30-day
  window edges for 99453/99454, and the report's midnight-of-end-date exclusion), on top of
  the existing integration/demo coverage that exercises the real database end to end.
- `call_time_seconds` (`patient_note.call_time_seconds`) is a `FLOAT` column in the schema
  of record ([`models.py`](../../src/medicare_rebuild/models.py)), not the `INTEGER` an
  earlier revision of `docs/billing-rules.md` stated; `FLOOR(SUM(call_time_seconds))/60`
  (99202) and `SUM(call_time_seconds)/60` (99457) are real division on that sum, not
  integer division. This was already true of the pre-ORM reconstructed schema and does not
  change any demo scenario's outcome (values are whole seconds throughout), but the pandas
  port's `_qualifying_by_minutes` helper implements the real-division behavior explicitly,
  which is what surfaced the stale claim; `docs/billing-rules.md` is corrected accordingly.
- `docs/billing-rules.md`'s evidence columns now cite `billing.py` functions instead of
  `.sql` line numbers.

## Evidence

- [`billing.py`](../../src/medicare_rebuild/billing.py)
- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`create_billing_report`)
- [`test_billing.py`](../../tests/test_billing.py)
- [`test_every_named_scenario_behaves_as_designed`](../../tests/integration/test_demo_integration.py)
- `make demo`'s report content hash, unchanged across this migration (see
  [docs/demo.md](../demo.md))
