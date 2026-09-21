# Data handling

This page separates what the repository actually does from advice for a real deployment. Part A is checked against the code, configuration and CI; Part B is general guidance that this repository does not implement.

The repository contains no real data. Everything in it and in the demo is synthetic (see [provenance-and-data-boundary.md](provenance-and-data-boundary.md)).

## Part A. Implemented in this repository

### Fields that would be sensitive in a real deployment

The transform functions in [`dataframe_utils.py`](../src/medicare_rebuild/utils/dataframe_utils.py) (`create_*_df`) and the schema diagrams in [`erd/`](erd/) show what the target database holds per patient: name, date of birth, sex, email, phone number, a `social_security` column, race, marital status, language, weight and height; street address; emergency contacts (name, phone, relationship); Medicare beneficiary ID and payer IDs; diagnosis codes; free-text clinical notes; glucose and blood pressure readings; and device identifiers. Staff names and emails come from the Microsoft Graph user directory.

### Credentials

- Every connection detail and secret is read from environment variables with `os.environ[...]`, so a missing one raises immediately ([`__main__.py`](../src/medicare_rebuild/__main__.py)). The variables are listed in [configuration.md](configuration.md).
- `main()` loads a `.env` file with python-dotenv (`__main__.py`, `load_dotenv()`).
- [`.gitignore`](../.gitignore) ignores `.env` and `.env.*` and keeps `!.env.example` trackable. No `.env.example` file exists in the repository. <!-- TODO(craig): add a .env.example with placeholder values, or remove the .gitignore exception. -->
- The only passwords in the repository are the throwaway SQL Server `sa` password in [`docker-compose.yml`](../docker-compose.yml), the CI workflow and the test defaults. They protect disposable local and CI containers holding synthetic data.

### Connection security

- SQL Server: [`db_utils.py`](../src/medicare_rebuild/utils/db_utils.py) builds a `mssql+pyodbc` URL with ODBC Driver 18 and sets `TrustServerCertificate=yes`, which makes the client accept any server certificate without validating it. It does not set `Encrypt`, so the driver default applies. <!-- TODO(craig): confirm the ODBC Driver 18 default for Encrypt, and what a real deployment would set. --> The setting suits a disposable container with a self-signed certificate and does not suit a real deployment (see Part B). The port is fixed at 1433.
- HTTP APIs: the Microsoft login and Graph endpoints and the Tenovi endpoint are hard-coded `https://` URLs ([`api_utils.py`](../src/medicare_rebuild/utils/api_utils.py)).
- Error text: `create_engine` sets `hide_parameters=True` so SQL error messages do not include bound row values (`db_utils.py`).

### What logging emits

[`logger.py`](../src/medicare_rebuild/logger.py) writes to the console and to `logs/<name>_logfile.log` (git-ignored). `main()` sets the level to debug. The code in `src/` emits:

- SQL text without bound values, table names and row and column counts (`db_utils.py`, `__main__.py`); no row contents, and no `print`, `head()`, `to_string()` or `to_csv()` to the console.
- Exception messages from failed queries (`db_utils.py`), which no longer include bound values.
- **Credential exposure, open:** the shared REST adapter (an external package, `py-shared-tools`) logs request parameters and bodies at debug level. The Microsoft token request body contains the Azure AD client secret, and `main()` runs at debug level and passes its logger to the API client. <!-- TODO(craig): decide on a fix, for example passing the adapter a logger set above debug level. -->
- The pipeline writes row-level files to git-ignored locations: `data/Billing_Report.xlsx`, and per-stage snapshots in `data/snaps/` when `snap=True` (off by default). Nothing in the code encrypts, rotates or deletes them.

### Guards against committing data

- [`.gitignore`](../.gitignore) ignores `data/`, `logs/`, `demo_data/`, `demo_output/` and spreadsheet, CSV, Parquet, PDF, archive and SQL Server backup and data files.
- [`.pre-commit-config.yaml`](../.pre-commit-config.yaml): a local hook blocks staged data and database files (`.csv`, `.xlsx`, `.xls`, `.bak`, `.bacpac`, `.mdf`, `.ldf`, `.parquet`, `.db`, `.sqlite`) with no exceptions; plus gitleaks, `detect-private-key` and a 500 KB file-size limit.
- [`ci.yml`](../.github/workflows/ci.yml): a security job runs a "No data files" check over tracked files, gitleaks over the full history, and `pip-audit` on the locked dependencies, weekly and on every push and pull request. Workflows have read-only permissions and third-party actions are pinned to commit SHAs. [`codeql.yml`](../.github/workflows/codeql.yml) runs CodeQL, and [`dependabot.yml`](../.github/dependabot.yml) proposes weekly updates.

### Synthetic-only demo

[`tools/synthetic_data/`](../tools/synthetic_data/) generates all demo data, and [`safety.py`](../tools/synthetic_data/safety.py) fails the run if output contains a real-looking SSN, Medicare ID, phone number or email. The demo runs against a local container using the throwaway password. See [demo.md](demo.md).

## Part B. Deployment guidance (not implemented by this repository)

This repository demonstrates none of the following operationally. It is guidance for anyone adapting the code to real data.

- **Compliance first.** Handling real patient data needs a business associate agreement with each vendor involved and a compliance and security review before any real data is loaded.
- **Least privilege.** Use separate database accounts: read-only for the legacy sources, and a load account limited to the target database. Do not use an administrator account such as `sa`.
- **Encryption in transit.** Use certificates the client can validate and require encryption; do not set `TrustServerCertificate=yes`.
- **Encryption at rest.** Encrypt the databases, backups and the disk holding `data/`, snapshots, reports and logs.
- **Access logging.** Log who accessed or exported patient data, separately from application debug logs, and review it.
- **Secret management.** Keep secrets in a managed secret store with rotation instead of `.env` files, and keep them out of logs.
- **Minimum necessary.** Load only the fields billing needs; consider omitting the social security column.
- **Retention and disposal.** Define how long logs, snapshots and report files are kept, and delete them securely.
