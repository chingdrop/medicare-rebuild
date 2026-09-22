# 0002. Implement billing rules as SQL Server stored procedures

Status: Superseded by [0014](0014-pandas-billing-rules.md) (2025-01-23, `387726d`; report
moved into a procedure 2025-02-03, `086f222`; superseded 2026-09-22). The procedures
themselves stay in `sql/stored_procedures/` for reference; the pipeline no longer calls
them, and the record below describes what was true at the time, not the pipeline as it
stands now.

## Context

After loading, each patient's readings and notes must become billing codes (99202, 99453, 99454, 99457, 99458) stamped with a date, and a report must be produced.

## Decision

Each code has a T-SQL procedure (`batch_medcode_*`) that reads the loaded tables and inserts rows into `medical_code`. Python only calls the procedures in a fixed order from `create_billing_report()` and writes the result to Excel.

## Alternatives considered

SQL was a direction set for this project, not a choice made on technical grounds recorded here: doing the rules in SQL was requested, while pandas was the author's own preferred direction for a later version. History shows logic moving into SQL over time: the table resets (`e35ee05`, `b3bec50`), the report query (`086f222`) and the 99458 calculation, which was folded into one query (`970aeb6`, 2025-02-03) and split into CTEs again on 2025-02-13 (`007c8ec`, "to fix grouping anomaly").

## Consequences

- Rules run next to the data. Their windows are measured back from the report end date.
- Codes are recomputed from scratch on every run (`reset_medical_code_tables`).
- The rules cannot be exercised without a SQL Server. They are covered by the synthetic-demo scenarios, not by dedicated unit tests.
- Changing a rule means editing a procedure file. See [docs/billing-rules.md](../billing-rules.md) for the exact behaviour.

## Evidence

- [`sql/stored_procedures/`](../../sql/stored_procedures/) (`batch_medcode_*.sql`)
- [`create_billing_report()`](../../src/medicare_rebuild/__main__.py)
- [`test_every_named_scenario_behaves_as_designed`](../../tests/integration/test_demo_integration.py)
