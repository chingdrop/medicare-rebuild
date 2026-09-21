# 0001. Stage the pipeline as extract, transform in pandas, load

Status: Accepted (2025-01-16, `e8c0fba`)

## Context

Data comes from a SharePoint list export (CSV), five legacy SQL Server tables and a Microsoft Graph user directory. None of it matches the new schema: values need standardizing, one wide patient row becomes six tables, and legacy keys must be replaced before loading.

## Decision

`DataImporter` reads each source into DataFrames (`get_*_data`), pandas functions in `dataframe_utils.py` standardize, split and normalize the values, and `import_*_data` methods load them into SQL Server in dependency order. `import_all_data()` runs the sequence.

## Alternatives considered

None recorded as alternatives. History shows structure experiments: a class-based importer (`dd164a7`, 2025-01-31) was reverted to functions the next day (`35873bd`) and a class returned in `d229b65` (2025-03-06); no reasons are recorded.

<!-- TODO(craig): why the stages are kept separate, and what else was considered (for example transforming in SQL). -->

## Consequences

- Data sits in memory as DataFrames between stages.
- Files are not the interchange format. Only the patient export is a file; the rest is read straight from SQL and Graph. Extract and normalize happen together inside each `get_*` method.
- Optional per-stage Excel snapshots (`snap=True`) dump intermediate DataFrames. <!-- TODO(craig): what the snapshots are for. -->
- The transform functions can be tested without a database.

## Evidence

- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`DataImporter`, `import_all_data`)
- [`dataframe_utils.py`](../../src/medicare_rebuild/utils/dataframe_utils.py)
- [`test_pipeline_produces_internally_consistent_dataframes`](../../tests/integration/test_pipeline_integration.py)
