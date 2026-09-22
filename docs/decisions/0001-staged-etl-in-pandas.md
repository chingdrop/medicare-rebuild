# 0001. Stage the pipeline as extract, transform in pandas, load

Status: Accepted (2025-01-16, `e8c0fba`)

## Context

Data comes from a SharePoint list export (CSV), five legacy SQL Server tables and a Microsoft Graph user directory. None of it matches the new schema: values need standardizing, one wide patient row becomes six tables, and legacy keys must be replaced before loading. The legacy SQL Server was also in active production, serving the business as it continued to operate, and its data was too broken to transform in place without risking that operation.

## Decision

`DataImporter` reads each source into DataFrames (`get_*_data`), pandas functions in `dataframe_utils.py` standardize, split and normalize the values, and `import_*_data` methods load them into SQL Server in dependency order. `import_all_data()` runs the sequence.

## Alternatives considered

Transforming in place on the legacy SQL Server was ruled out: that server was live production infrastructure the business continued to operate on, and its data was too broken to transform there without disrupting that operation. A separate extract-then-transform step was the only way to work on the data without touching the source.

History shows structure experiments beyond that: a class-based importer (`dd164a7`, 2025-01-31) was reverted to functions the next day (`35873bd`) and a class returned in `d229b65` (2025-03-06); no reasons are recorded for those.

## Consequences

- Data sits in memory as DataFrames between stages.
- Files are not the interchange format. Only the patient export is a file; the rest is read straight from SQL and Graph. Extract and normalize happen together inside each `get_*` method.
- Optional per-stage Excel snapshots (`snap=True`) dump intermediate DataFrames. Originally added to check how the data was being manipulated at each stage; later repurposed as a debugging aid.
- The transform functions can be tested without a database.

## Evidence

- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`DataImporter`, `import_all_data`)
- [`dataframe_utils.py`](../../src/medicare_rebuild/utils/dataframe_utils.py)
- [`test_pipeline_produces_internally_consistent_dataframes`](../../tests/integration/test_pipeline_integration.py)
