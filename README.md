# Medicare-Rebuild

Repository: [medicare-rebuild - GitHub](https://github.com/chingdrop/medicare-rebuild)

The Medicare-Rebuild project is a Python-based solution designed to rebuild the data architecture for a medical company specializing in remote physician monitoring. The goal of this rebuild is to accurately record the 'date of service' for services provided, with a focus on Medicare billing for telehealth and remote monitoring.

This project was completed within a 3-month timeframe and involves the extraction, transformation, and loading (ETL) of data for approximately 22,000 Medicare-eligible patients.

## Scope

The project covers the following:

- **Extraction** - Data is extracted from various sources including SharePoint Lists and unorganized SQL databases.
- **Transformation** - Data is transformed by standardizing patient billing information and instrument readings.
- **Loading** - The transformed data is loaded into a new SQL database that enforces entity relationships and accurately records the 'date of service' for billable services.

The client medical company focused mainly on remote physician monitoring for *diabetes* and *hypertension*.

Path - `/sql/stored_procedures/batch_medcode_99XXX.sql`

Below are the main Medicare CPT codes developed for this project:

- **99202** - The initial telehealth visit from the nurse practitioner.
- **99453** - Initial device setup for vital monitoring instrument (after 16 distinct days of device testing).
- **99454** - Repeated device usage (after 16 distinct days of device testing).
- **99457** - The initial 20 minutes of patient interaction.
- **99458** - Repeated patient interaction, billed in increments of 20 minutes (can be applied up to 3 times).

## Process

### Extraction

- **SharePoint Data**: Data is extracted by creating a view in SharePoint and filtering for the relevant fields. The data is then downloaded as a CSV file.
- **SQL Data**: Data is retrieved from various SQL databases by executing the necessary queries to fill the final database schema.

### Transformation

Path - `/utils/dataframe_utils.py`

Data transformation is handled using a set of organized functions in Python.

- **Standardize Functions**: These methods clean and transform data within a Pandas DataFrame.
- **Create Functions**: Methods designed to structure and separate patient data from the SharePoint list.
- **Normalize Functions**: Apply standardization functions to specific fields in the DataFrame.

Additional functions included:

- Enforce database value constraints.
- Assign identity values to specific fields in the new database schema.

### Load

Once transformed, the data is loaded into a new Microsoft SQL Server database. The new schema and entity relationships allow for the accurate recording of service dates for billable Medicare services.

Path - `/docs/erd/*_erd.png`

The following entities are defined in the database:

- **Patient Information** - [[1_patient_erd.png]]
- **Patient Health** - [[2_patient_health_erd.png]]
- **Patient Time** - [[3_patient_time_erd.png]]
- **Patient Billing** - [[4_patient_billing_erd.png]]
- **Patient Fulfillment** - [[5_patient_fulfillment_erd.png]]

**Stored Procedures** are used to query and insert entries into the medical code table, ensuring that services performed are recorded with the correct Medicare codes.

### Report

Path - `/sql/stored_procedures/create_billing_report.sql`

- Create a billing report that groups the patients by the count of recorded medical codes and the date of service.

## Materials

### Credentials

- Service Account on the Microsoft SQL Server hosting the new database.
- Service Account on the Microsoft SQL Servers hosting the old databases.
- Azure Active Directory (AD) application credentials.

### Requirements

- **ODBC Driver 18**: Required for connecting to Microsoft SQL Server.
- **SQLAlchemy**: A micro ORM for SQL execution.
  - **pyodbc**: Used for ODBC connections.
- **Pandas**: A library for data manipulation and analysis.
  - **openpyxl**: Used by Pandas for Excel file operations.
- **Requests**: A library for making HTTP requests.
