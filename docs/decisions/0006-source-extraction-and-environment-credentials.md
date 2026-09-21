# 0006. Extract from a CSV export, legacy SQL and Graph; supply credentials by environment

Status: Accepted (Graph client 2025-02-18, `e13ba2f`; environment variables from 2025-01-16, `00ea3f7`)

## Context

Patients come from a SharePoint list, notes, devices and readings from legacy SQL Server databases, and staff from a Microsoft Graph group. The pipeline needs credentials for all of them.

## Decision

The patient export is read from `data/Patient_Export.csv`. Legacy tables are queried directly (`queries.py`). Users come from a Graph group through `MSGraphApi`, using an Azure AD app. Every connection detail and secret is read from environment variables (`.env` via python-dotenv, `os.environ[...]` so a missing one raises). `.env` is git-ignored.

## Alternatives considered

The Tenovi vendor API was used as the readings source (`38e0248`, 2025-02-26) and then reverted to the legacy database (`04276e2`, 2025-03-06, "revert to original data source"). No reason is recorded. `TenoviApi` remains but the pipeline does not call it.

<!-- TODO(craig): why the SharePoint data is a downloaded CSV rather than an API read, and why the legacy database won over the vendor API. -->

## Consequences

- The SharePoint step is manual: build a view, download it, place the file.
- A real run needs network access to Graph and the legacy servers. The demo replaces Graph with a local file.
- Variables are listed in [docs/configuration.md](../configuration.md).

## Evidence

- [`__main__.py`](../../src/medicare_rebuild/__main__.py) (`get_user_data`, `get_patient_data`), [`api_utils.py`](../../src/medicare_rebuild/utils/api_utils.py)
- [`test_get_group_members`](../../tests/test_api_utils.py), [`test_import_user_data`](../../tests/integration/test_data_importer_integration.py)
- [`.gitignore`](../../.gitignore)
