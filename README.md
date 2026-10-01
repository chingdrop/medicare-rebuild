# Medicare-Rebuild

An ETL pipeline that rebuilds the data architecture for a healthcare provider's remote patient monitoring program billed to Medicare, recording an accurate 'date of service' for each billable event.

[![CI](https://github.com/chingdrop/medicare-rebuild/actions/workflows/ci.yml/badge.svg)](https://github.com/chingdrop/medicare-rebuild/actions/workflows/ci.yml)
![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## About this project

- **What it does:** an ETL pipeline for remote-patient-monitoring billing. It reads patient, device, reading and note data from legacy SQL Server databases, a SharePoint list export and a Microsoft Graph user directory, standardizes it with pandas, and loads it into a new SQL Server schema that records a date of service for each billable event. Pandas rules then assign the Medicare CPT codes 99202, 99453, 99454, 99457 and 99458 and produce a billing report.
- **Context:** the work was motivated by a healthcare provider running a remote patient monitoring program billed to Medicare. This repository is a cleaned-up version of the pipeline built for that program, published as a portfolio project with client-specific material removed.
- **Data:** all data in this repository is synthetic. Test fixtures use fictional names, `example.com` emails and placeholder IDs, and the SQL files contain schema and queries only, no data.
- **Boundary:** no real patient data, credentials, tenant identifiers, or client configuration appear in the code or in the commit history of this repository's branches.
- **Try it:** run the whole pipeline on generated synthetic data with `make demo` - see [docs/demo.md](docs/demo.md).
- **More:** see [docs/provenance-and-data-boundary.md](docs/provenance-and-data-boundary.md) for provenance, the data boundary, and how to contribute test data safely.

## What it does

- **Sources in:** the patient export (a SharePoint list downloaded as `Patient_Export.csv`), five legacy SQL Server tables (`Medical_Notes`, `Time_Log`, `Fulfillment_All`, `Glucose_Readings`, `Blood_Pressure_Readings`), and the user directory from Microsoft Graph.
- **Transformation:** pandas `standardize_*`, `create_*` and `normalize_*` functions in `dataframe_utils.py`, then `check_patient_db_constraints` drops rows that would violate the target column limits.
- **Load:** `DataImporter.import_*_data` inserts through the SQLAlchemy ORM (`models.py`), resolving legacy `SharePoint_ID` and vendor names to identity keys via `session.flush()`.
- **Billing rules:** `src/medicare_rebuild/billing.py` applies CPT codes 99202, 99453, 99454, 99457 and 99458 in pandas, a faithful port of the original `batch_medcode_*` stored procedures (see [docs/billing-rules.md](docs/billing-rules.md)).
- **Report out:** `create_billing_report` produces `data/Billing_Report.xlsx`, one row per patient per date of service with a count for each code.

```mermaid
flowchart LR
    subgraph src["Sources"]
        SP["SharePoint export<br/>data/Patient_Export.csv"]
        LG["Legacy SQL Server<br/>Medical_Notes, Time_Log,<br/>Fulfillment_All, Glucose_Readings,<br/>Blood_Pressure_Readings"]
        MG["Microsoft Graph<br/>Azure AD group members"]
    end
    subgraph py["Python: src/medicare_rebuild"]
        EX["Extract<br/>DataImporter.get_*_data"]
        TR["Transform<br/>utils/dataframe_utils.py<br/>standardize, create, normalize,<br/>check_patient_db_constraints"]
        LD["Load<br/>DataImporter.import_*_data<br/>models.py ORM classes"]
    end
    subgraph gps["GPS database (SQL Server)"]
        DB[("patient, device, reading,<br/>note and user tables")]
        BP["Billing rules<br/>billing.py run_billing"]
        RP["billing.py build_billing_report"]
    end
    OUT["data/Billing_Report.xlsx"]
    SP --> EX
    LG --> EX
    MG --> EX
    EX --> TR --> LD --> DB --> BP --> RP --> OUT
```

## Try it in 60 seconds

Runs the whole pipeline on generated synthetic data: no credentials, no real data, no network during the run.

You need Docker (running), [uv](https://docs.astral.sh/uv/), `make`, and the ODBC Driver 18 for SQL Server (macOS: `brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18`).

```sh
git clone https://github.com/chingdrop/medicare-rebuild.git
cd medicare-rebuild
uv sync
make demo
```

Expected output (trimmed; a first run also downloads the SQL Server image and Python dependencies):

```text
Synthetic demo - seed 20250228, 200 patients, 46 named scenarios
Rows: source -> loaded
  users                         8 ->     8
  patients                    200 ->   196
  devices                     109 ->   103
  glucose readings            904 ->   807
  blood pressure readings     507 ->   507
  patient notes               158 ->   153
...
Billing codes        applied   in report
  99202                  13         13
  99453                  46         44
  99454                  44         43
  99457                  34         34
  99458                  18         18
...
RESULT: PASS (27/27 checks)
Report: demo_output/Billing_Report.xlsx
```

Full output, how the demo works, and how to reset it (`make demo-down`): [docs/demo.md](docs/demo.md). To check that a run is complete and consistent: [docs/reconciliation.md](docs/reconciliation.md).

## Results

- **Demo run** (synthetic data, default seed): 196 of 200 patients loaded (4 rejected by the pipeline's constraint checks), 89 billing-report rows, and codes applied 99202 x13, 99453 x46, 99454 x44, 99457 x34, 99458 x18, with all 27 checks against the expected-results manifest passing.
- **Original scale:** approximately 22,000 Medicare-eligible patients, completed within a 3-month timeframe.

## Data model

The GPS database's tables, drawn from [`models.py`](src/medicare_rebuild/models.py), the schema of record ([decision 0015](docs/decisions/0015-full-orm-schema-of-record.md)); the generated DDL is [`sql/schema.sql`](sql/schema.sql). Solid lines are real foreign keys. Dotted lines and entities marked *(design only)* come from the original schema design ([`docs/erd/`](docs/erd/)) but were not built as tables: most survive as a `temp_*` text column holding the raw value, and the fulfillment tables do not exist at all. Every foreign key column is nullable, and `temp_*` columns without a matching key stay unresolved text.

### Patient

```mermaid
erDiagram
    patient_status_type ||--o{ patient_status : classifies
    user ||--o{ patient : "assigned to"
    patient ||--o{ patient_address : has
    address_state |o..o{ patient_address : "temp_state"
    marital_status |o..o{ patient : "temp_marital_status"
    user |o..o{ patient_status : "changed by (temp_user)"
    patient ||--o{ patient_status : "status history"
    language |o..o{ patient : "preferred_language"
    patient ||--o{ emergency_contact : has
    race |o..o{ patient : "temp_race"

    language["language (design only)"] {
    }
    race["race (design only)"] {
    }
    marital_status["marital_status (design only)"] {
    }
    address_state["address_state (design only)"] {
    }
    patient {
        int patient_id PK
        int user_id FK
        int sharepoint_id "legacy SharePoint ID"
        string first_name
        string last_name
        string middle_name
        string name_suffix
        string full_name
        string nick_name
        datetime2 date_of_birth
        string sex
        string email
        string phone_number
        string social_security
        string temp_race
        string temp_marital_status
        string preferred_language
        int weight_lbs
        int height_in
        string temp_user
    }
    user {
        int user_id PK
        string first_name
        string last_name
        string display_name
        string email
        string user_principal_name "matches notes' AZURE_UPN"
        string ms_entra_id
    }
    patient_address {
        int patient_address_id PK
        int patient_id FK
        string street_address
        string city
        string temp_state
        string zipcode
    }
    emergency_contact {
        int emergency_contact_id PK
        int patient_id FK
        string full_name
        string phone_number
        string relationship
    }
    patient_status {
        int patient_status_id PK
        int patient_id FK
        int patient_status_type_id FK
        string temp_status_type
        datetime2 modified_date
        string temp_user
    }
    patient_status_type {
        int patient_status_type_id PK
        string name
    }
```

### Patient health

```mermaid
erDiagram
    device ||--o{ blood_pressure_reading : records
    dx_code |o..o{ medical_necessity : "temp_dx_code"
    patient ||--o{ device : uses
    device ||--o{ glucose_reading : records
    vendor ||--o{ device : supplies
    patient ||--o{ medical_necessity : "diagnosed with"

    dx_code["dx_code (design only)"] {
    }
    device {
        int device_id PK
        int patient_id FK
        int vendor_id FK
        string hardware_uuid
        string name
    }
    vendor {
        int vendor_id PK
        string name
    }
    glucose_reading {
        int glucose_reading_id PK
        int device_id FK
        datetime2 recorded_datetime
        datetime2 received_datetime "drives 99453/99454 day counts"
        float glucose_reading
        bit is_manual
        string temp_device
    }
    blood_pressure_reading {
        int blood_pressure_reading_id PK
        int device_id FK
        datetime2 recorded_datetime
        datetime2 received_datetime "drives 99453/99454 day counts"
        float systolic_reading
        float diastolic_reading
        bit is_manual
        string temp_device
    }
    medical_necessity {
        int medical_necessity_id PK
        int patient_id FK
        datetime2 evaluation_datetime "on-board date"
        string temp_dx_code
    }
```

### Patient notes

```mermaid
erDiagram
    patient ||..o{ patient_comment : "about"
    note_type ||--o{ patient_note : classifies
    user ||..o{ patient_comment : writes
    user ||--o{ patient_note : writes
    patient ||--o{ patient_note : "about"

    patient_note {
        int patient_note_id PK
        int patient_id FK
        int user_id FK
        int note_type_id FK
        nvarchar note_content
        datetime2 note_datetime
        float call_time_seconds "drives 99202/99457/99458"
        datetime2 start_call_datetime
        datetime2 end_call_datetime
        bit is_manual
        string temp_user
        string temp_note_type
    }
    note_type {
        int note_type_id PK
        string name
    }
    patient_comment["patient_comment (design only)"] {
    }
```

### Patient billing

A billable event is a timestamped `medical_code` row: `timestamp_applied` is the date of service the billing report groups by ([decision 0003](docs/decisions/0003-medical-code-rows-carry-the-date-of-service.md)). `medical_code_device` links a code to the devices whose readings earned it; in this pipeline only 99453 writes these links, to every device the patient has.

```mermaid
erDiagram
    patient ||--o{ patient_insurance : "covered by"
    patient ||--o{ medical_necessity : "diagnosed with"
    patient ||--o{ medical_code : "billed"
    medical_code_type ||--o{ medical_code : classifies
    medical_code ||--o{ medical_code_device : "based on"
    device ||--o{ medical_code_device : "based on"

    patient_insurance {
        int patient_insurance_id PK
        int patient_id FK
        string medicare_beneficiary_id
        string primary_payer_id
        string primary_payer_name
        string secondary_payer_id
        string secondary_payer_name
    }
    medical_code {
        int med_code_id PK
        int patient_id FK
        int med_code_type_id FK
        datetime2 timestamp_applied "date of service"
    }
    medical_code_type {
        int med_code_type_id PK
        string name "99202, 99453, 99454, 99457, 99458"
    }
    medical_code_device {
        int medical_code_device_id PK
        int med_code_id FK
        int device_id FK
    }
```

### Patient fulfillment (design only)

Orders and resupply were part of the original design but have no tables in `models.py` and are not loaded by this pipeline; only `patient`, `device` and `vendor` exist.

```mermaid
erDiagram
    vendor ||--o{ device : supplies
    order_device }o..|| device : "shipped in"
    order ||..o{ order_resupply : includes
    order ||..o{ order_device : includes
    order_status_type ||..o{ order : classifies
    device ||..o{ resupply : "resupplied by"
    patient ||..o{ order : places
    resupply ||..o{ order_resupply : "shipped in"
```

## Scope

The project covers the following:

- **Extraction** - Data is extracted from various sources including SharePoint Lists and legacy SQL databases.
- **Transformation** - Data is transformed by standardizing patient billing information and instrument readings.
- **Loading** - The transformed data is loaded into a new SQL database that enforces entity relationships and accurately records the 'date of service' for billable services.

The monitoring program focused mainly on *diabetes* and *hypertension*.

### Billing rules

[`src/medicare_rebuild/billing.py`](src/medicare_rebuild/billing.py) assigns five CPT codes from the loaded data in pandas, a faithful port of the original stored procedures in [`sql/stored_procedures/`](sql/stored_procedures/) (kept for reference; see [decision 0014](docs/decisions/0014-pandas-billing-rules.md)). 99453 and 99454 come from at least 16 distinct days of device readings (for 99454, within a rolling 30 days). 99457 and 99458 come from 20-minute blocks of note call time in a rolling month, with up to three 99458. 99202 comes from Initial Evaluation notes totalling 15 to under 30 minutes. Exact conditions, windows, interactions, worked examples and known gaps are in [docs/billing-rules.md](docs/billing-rules.md).

## Process

The pipeline extracts patients from a SharePoint CSV export, notes, devices and readings from legacy SQL databases, and users from Microsoft Graph. Pandas functions standardize and normalize the data, then it is loaded into a new SQL Server schema; pandas rules assign the billing codes and build the report. Stage-by-stage detail is in [docs/architecture.md](docs/architecture.md).

## Configuration

Running against real sources needs service accounts for the old and new SQL Servers, Azure AD application credentials, and a set of `GPS_SQL_*`, `LEGACY_SQL_*` and `AZURE_*` environment variables. The demo needs none of these. See [docs/configuration.md](docs/configuration.md).

## Design decisions

Why the pipeline is built the way it is - staged ETL, a full SQLAlchemy ORM schema, billing rules in pandas, database-assigned keys, the testing approach and more - is recorded in short decision records. See [docs/decisions/](docs/decisions/README.md). For a first-person account of building it - what broke, and what I'd change now - see [docs/narrative.md](docs/narrative.md).

## Security and data handling

The repository holds synthetic data only. How credentials, connections, logging and data-file guards work (and what a real deployment would still need) is in [docs/data-handling.md](docs/data-handling.md); to report a vulnerability, see [SECURITY.md](SECURITY.md).

## Limitations, roadmap and changelog

What this reference implementation doesn't cover, and what's traceable as a possible next step, is in [docs/limitations-and-roadmap.md](docs/limitations-and-roadmap.md). Notable changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## Tech stack

Versions come from [`pyproject.toml`](pyproject.toml).

- **Python** 3.12 or newer.
- **ODBC Driver 18**: Required for connecting to Microsoft SQL Server.
- **SQLAlchemy**: SQL toolkit and ORM. Declarative models (`src/medicare_rebuild/models.py`) are the schema of record and drive the load path; billing computation also runs through the ORM (`billing.py`), not raw SQL (see [Design decisions](#design-decisions)).
  - **pyodbc**: Used for ODBC connections.
  - **Alembic**: versioned migrations for the GPS database, generated from the same declarative models (`alembic/`, `make migrate`).
- **Pandas**: A library for data manipulation and analysis.
  - **NumPy**: Numeric support for Pandas.
  - **openpyxl**: Used by Pandas for Excel file operations.
- **Requests**: A library for making HTTP requests.
- **python-dotenv**: Loads environment variables from a `.env` file.
- **colorlog**: Colored console logging.
- **certifi** and **urllib3**: TLS certificates and retry support for the REST adapter (`utils/rest_adapter.py`).

## Testing

Install dependencies with `uv sync`, then:

```sh
uv run pytest              # unit tests only (default)
uv run pytest -m integration   # integration tests only
```

Unit tests mock all external systems and need nothing else installed.

Integration tests exercise `DatabaseManager` and `DataImporter` against a real
SQL Server instance (MS Graph calls are still mocked). To run them
locally:

1. `docker compose up -d` to start a disposable SQL Server container.
2. Install the ODBC Driver 18 for SQL Server (e.g. `brew install
   microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18`
   on macOS, or see [Microsoft's Linux install docs](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)).
3. `uv run pytest -m integration`

Connection details default to the `docker-compose.yml` values and can be
overridden with `INTEGRATION_DB_HOST`, `INTEGRATION_DB_PORT`,
`INTEGRATION_DB_USER`, and `INTEGRATION_DB_PASSWORD`. Tests skip automatically
if no server is reachable. CI runs both suites on every push and pull request.
