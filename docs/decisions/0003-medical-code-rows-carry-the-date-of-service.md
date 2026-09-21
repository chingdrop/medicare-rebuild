# 0003. Record billable events as timestamped medical-code rows

Status: Accepted (2025-02-06, `cf85f11`)

## Context

The project's stated aim is an accurate date of service for each billable event. Some codes depend on the devices that produced the readings.

## Decision

There is no separate date-of-service entity. Each awarded code is a row in `medical_code` (patient, code type, `timestamp_applied`), drawn as a history table in the ERD and linked to devices through `medical_code_device`. The report derives `DateOfService` as the date part of `timestamp_applied`.

## Alternatives considered

None recorded. Commit `02df5bb` (2025-02-13) split the report query into CTEs "for grouping anomalies"; that is the only recorded change to how the report groups rows.

<!-- TODO(craig): whether a dedicated date-of-service table was considered. -->

## Consequences

- The date is the timestamp of the triggering reading or note, not a visit date.
- Codes on the same day share one report row, with a count per code.
- Only codes stamped between the report start and midnight at the start of the end date appear.
- Codes are deleted and recomputed each run, so no billed history is kept.

## Evidence

- [Patient Billing ERD](../erd/4_patient_billing_erd.png)
- [`create_billing_report.sql`](../../sql/stored_procedures/create_billing_report.sql) (L11, L20-22)
- [docs/billing-rules.md](../billing-rules.md)
- [`test_every_named_scenario_behaves_as_designed`](../../tests/integration/test_demo_integration.py) (scenario S13 pins the end-date edge)
