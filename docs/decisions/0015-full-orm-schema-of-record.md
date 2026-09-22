# 0015. Adopt SQLAlchemy declarative models as the schema of record

Status: Accepted (2026-09-22)

## Context

The repository had no authoritative schema file ([0007](0007-sqlalchemy-engine-and-raw-sql.md)): the GPS database's shape existed only as inference from the columns the code wrote, reconstructed independently in two places (the demo's `tools/synthetic_data/schema.py` and the integration test's own DDL) that had to be kept in sync by hand. Loading data used a separate insert-then-`SELECT`-and-merge step (`add_id_col`) to resolve each row's identity key, and lookup-table foreign keys were resolved after the fact by four deferred `UPDATE` statements, because SQLAlchemy Core requires multi-statement SQL to be a stored procedure or a single `UPDATE ... SET x = (SELECT ...)`.

## Decision

Declarative ORM models in `src/medicare_rebuild/models.py` (GPS) and `src/medicare_rebuild/legacy_models.py` (the legacy source tables, as plain Core `Table` objects rather than declarative classes, since the real source system has no primary key on them and forcing one would misrepresent a schema this repo doesn't own) are now the schema of record. `sql/schema.sql` is generated from them (`make schema`), and the demo builds its databases from the same classes via `metadata.create_all()`. `DataImporter` inserts parent rows and reads their SQLAlchemy-assigned identity keys back via `session.flush()`, and resolves lookup-table foreign keys (note type, coach, status type) with a small dict built from one query, applied before insert — both replacing their previous mechanisms.

## Alternatives considered

Two narrower options were on the table and not chosen: keeping the models as a schema definition only, with the load path left on raw SQL/`to_sql`; and using the ORM for reads only, with bulk loads staying on `to_sql`. Full ORM was chosen instead, covering the whole load path.

## Consequences

- `add_id_col`, `queries.py`'s `get_patient_id_stmt`/`get_device_id_stmt`/`get_vendor_id_stmt`, and the four deferred `UPDATE ... WHERE temp_X IS NOT NULL` statements are gone.
- The demo's GPS schema is authoritative-by-construction now, not a separate reconstruction; the same is not true of the legacy source tables, which remain a reconstruction of a system outside this repository.
- The 5 legacy-source extraction queries in `queries.py` are Core `select()` functions bound to `legacy_models.py`'s tables, not raw SQL strings with `?` placeholders.
- Real foreign key constraints now exist in the generated schema (the old hand-written reconstruction had none), which the demo's `orphan-fk` fault has to explicitly disable (`ALTER TABLE ... NOCHECK CONSTRAINT ALL`) before it can inject a deliberately broken row — the same thing a real DBA would need to do.
- `DatabaseManager.execute_query`/`read_sql`/`to_sql` are unchanged and still used: `execute_query` for the billing stored procedures and `DBCC CHECKIDENT` reseeds, `to_sql` for loading the legacy demo database, whose tables have no ORM identity to resolve.

## Evidence

- [`models.py`](../../src/medicare_rebuild/models.py), [`legacy_models.py`](../../src/medicare_rebuild/legacy_models.py), [`sql/schema.sql`](../../sql/schema.sql), [`tools/generate_schema.py`](../../tools/generate_schema.py)
- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`DataImporter`, `reset_all_data` from `models.py`)
- [`test_generate_schema.py`](../../tests/test_generate_schema.py), [`test_import_patient_data`](../../tests/integration/test_data_importer_integration.py)
