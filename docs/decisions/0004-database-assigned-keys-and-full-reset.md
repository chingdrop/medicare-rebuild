# 0004. Let the database assign keys; make runs repeatable by full reset

Status: Accepted (2025-01-23, `975c5f6`; reset moved into a procedure 2025-02-03, `e35ee05`). Partially amended by [0015](0015-full-orm-schema-of-record.md) (2026-09-22): the database still assigns identity keys and each run still starts from a full reset, but the *mechanism* described below (`add_id_col`, deferred `UPDATE` statements, the reset as a stored procedure) has been replaced. See that record for the current mechanism.

## Context

Legacy data is keyed by SharePoint ID and vendor name; the new schema uses identity keys. Some references, such as user names, cannot be resolved until the rows they point at exist. Runs must be repeatable.

## Decision

The database assigns identity keys. Python inserts parent rows first, reads back the key pairs (`get_patient_id_stmt` and similar) and merges them onto child rows with `add_id_col`. Unresolved references are stored in `temp_*` columns and resolved by `UPDATE` statements at the end of `import_all_data()`, because SQLAlchemy needs multi-statement SQL as a procedure or a single `UPDATE` (CLAUDE.md). Each run starts with `reset_all_billing_tables`, which deletes rows and reseeds identities.

## Alternatives considered

Assigning keys in Python (rather than relying on the database's identity columns) was the author's own preferred direction; keeping them in SQL was the direction set for this project. Commit `c4022b9` (2025-02-18, "move update queries to main") moved the `UPDATE` calls from `billing_report.py` into `main.py`.

## Consequences

- Re-runnable by wiping, not by upsert. There are no incremental loads.
- Child rows with no matching parent are dropped by the merge, silently.
- Lookup tables (vendor, note type, status type, code type) must already exist. They are not defined in this repo.

## Evidence

- [`reset_all_billing_tables.sql`](../../sql/stored_procedures/reset_all_billing_tables.sql)
- [`add_id_col`](../../src/medicare_rebuild/utils/dataframe_utils.py), [`queries.py`](../../src/medicare_rebuild/queries.py)
- [`test_add_id_col`](../../tests/test_dataframe_utils.py), [`test_import_patient_data`](../../tests/integration/test_data_importer_integration.py)
