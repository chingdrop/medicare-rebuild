# 0005. Validate in the transform stage and drop rows that break column limits

Status: Accepted (2025-01-31, `4cb3f2e`)

## Context

Patient values can be longer than the target columns allow (phone, SSN, state, ZIP, Medicare and payer IDs). The target schema is not in this repo.

## Decision

The transform stage normalizes values, then `check_patient_db_constraints` drops any patient row whose values are too long for the target columns. Dropped rows are not logged or reported.

## Alternatives considered

A failed-patient export was written for a while (`e4d42ac`, 2025-02-05, "add failed data exports as a new dataframe") and then switched off (`bdf23e8`, 2025-02-21, "disable failed patient export file creation"). The helper is still in `dataframe_utils.py`, commented out. It was meant to surface which patients failed to bill, but time became a constraint and the client's priority was getting the billing report itself ready, so it was disabled rather than finished. The limit values (11, 9, 2, 5, 30) are not independently derived; they mirror the real target database's original column widths from its schema design, which is not reproduced in this repository.

## Consequences

- A rejected patient also loses their devices, readings and notes, because those rows can no longer find a patient to link to.
- Nothing in the report marks a rejected patient. The synthetic demo's summary takes rejection reasons from its manifest, not from the pipeline.
- The check filters; it does not repair. Other bad values are normalized to NULL instead (for example a malformed email).

## Evidence

- [`check_patient_db_constraints`](../../src/medicare_rebuild/utils/dataframe_utils.py) and the commented-out `patient_check_failed_data`
- [`test_check_patient_db_constraints`](../../tests/test_dataframe_utils.py)
- [`test_manifest_rejections_match_the_real_normalisation`](../../tests/test_synthetic_data.py)
