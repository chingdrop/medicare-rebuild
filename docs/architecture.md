# Architecture

The pipeline's stages in detail. For the one-screen overview and diagram, see the [README](../README.md); for the schema diagrams, see [Data model](../README.md#data-model).

> **Inlined helpers.** `utils/rest_adapter.py`, `utils/atomic_io.py` and `utils/tabular_io.py` were factored out of a separate shared library (`py-shared-tools`) and copied into this repository, so it is self-contained and needs no second repository to install. See [decision 0013](decisions/0013-inline-the-shared-helpers.md).

## Pipeline stages

### Extraction

- **SharePoint Data** (patients): Data is extracted by creating a view in SharePoint and filtering for the relevant fields. The data is then downloaded as a CSV file (`data/Patient_Export.csv`).
- **SQL Data** (notes, time log, devices, readings): Data is retrieved from the legacy SQL databases by executing the necessary queries to fill the final database schema.
- **Microsoft Graph** (users): Members of an Azure AD group are read through the Graph API and loaded into the `user` table.

### Transformation

Path - [`src/medicare_rebuild/utils/dataframe_utils.py`](../src/medicare_rebuild/utils/dataframe_utils.py)

Data transformation is handled using a set of organized functions in Python.

- **Standardize Functions**: These methods clean and transform data within a Pandas DataFrame.
- **Create Functions**: Methods designed to structure and separate patient data from the SharePoint list.
- **Normalize Functions**: Apply standardization functions to specific fields in the DataFrame.

Additional functions included:

- Enforce database value constraints.
- Assign identity values to specific fields in the new database schema.

### Load

Once transformed, the data is loaded into a new Microsoft SQL Server database via SQLAlchemy declarative models (`src/medicare_rebuild/models.py`), which are the schema of record — see [decision 0015](decisions/0015-full-orm-schema-of-record.md). The new schema and entity relationships allow for the accurate recording of service dates for billable Medicare services.

The schema design is shown in the [Data model](../README.md#data-model) section of the README (diagrams in [`docs/erd/`](erd/)); the generated DDL is [`sql/schema.sql`](../sql/schema.sql).

**Stored Procedures** are used to query and insert entries into the medical code table, ensuring that services performed are recorded with the correct Medicare codes.

### Report

Path - [`sql/stored_procedures/create_billing_report.sql`](../sql/stored_procedures/create_billing_report.sql)

- Create a billing report that groups the patients by the count of recorded medical codes and the date of service.
