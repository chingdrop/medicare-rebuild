# Provenance and data boundary

This page explains where this project comes from, what data it does and does not contain, and the rules for keeping it that way. It is written for people reading the repository as a portfolio piece. Start with the [README](../README.md) for what the pipeline does.

## Provenance

- **Context.** The pipeline addresses a healthcare provider running a remote patient monitoring program billed to Medicare. That kind of program needs billing data that records the date of service for each billable event (device setup, device use, and time spent with the patient). The source data lived in several legacy SQL databases and SharePoint list exports.
- **This repository.** It is a cleaned-up version of the pipeline built for that program, prepared for publication as a portfolio project. Client data, credentials, identifiers and client-specific configuration were removed or replaced with neutral names and environment variables. It is not an independent reimplementation.
- **Carried over.** The pipeline's code, schema design, stored procedures and billing logic.
- **Not carried over.** Patient data, credentials, tenant or directory identifiers, hostnames, and client-specific configuration. The code reads such values from environment variables at run time; none are stored in the repository.

## Data boundary

**In this repository:**

- Source code for the pipeline (`src/`).
- Database logic as SQL: stored procedures and queries only, with no rows of data (`sql/`).
- Schema diagrams (`docs/erd/`).
- Tests whose fixtures are written inline in Python with fictional values: `John Doe`-style names, `example.com` emails, sequential placeholder phone and SSN patterns, `123 Main St`, and documentation-style Medicare IDs.
- A `docker-compose.yml` for a disposable local SQL Server with a placeholder password.

**Must never be committed:**

| Category | Examples |
|---|---|
| Real patient or claims data | names, dates of birth, Medicare Beneficiary Identifiers, SSNs, addresses, phone numbers, device readings, clinical notes |
| Exports and extracts | CSV, Excel and Parquet files, including the pipeline's own snapshot and billing-report outputs (written to `data/`, which is git-ignored) |
| Database backups and files | `.bak`, `.bacpac`, `.mdf`, `.ldf` |
| Credentials | `.env` files, passwords, API keys, client secrets, tokens |
| Tenant or client identifiers | tenant, directory-group and application IDs, server and database hostnames, names of the organizations involved |

**Why.** Patient data is protected health information, and committing it is a privacy incident that cannot be fully undone: git history is copied by clones, forks and caches. Credentials and tenant identifiers let others reach or enumerate real systems. Client identifiers would reveal the organizations and people behind the data. The patterns in [`.gitignore`](../.gitignore) block the common file types as a safety net, but they cannot detect real values written into code or docs, so review your diff before you push.

## Contributing test data safely

1. **Generate it, don't copy it.** Write fixtures inline in Python, as the existing tests do (see [`test_pipeline_integration.py`](../tests/integration/test_pipeline_integration.py)). Data files such as `*.csv` and `*.xlsx` are git-ignored on purpose.
2. **Make it recognizably fake.** Use names like John Doe, emails at `example.com`, phone numbers in the 555-01xx range or obvious sequences, SSN-shaped values in invalid ranges or obvious sequences, addresses like `123 Main St`, and Medicare IDs in the shape of CMS's published examples.
3. **Never mask a real record.** Scrambling, redacting or "changing a few digits" of a real row still derives from real data. If a bug comes from real data, write a new row by hand that reproduces the failure.
4. **Keep secrets out.** Tests use placeholders such as `test-tenant-id`. Real values belong only in a local, git-ignored `.env`.
5. **Read your diff.** Before pushing, confirm that every value you added is one you invented.

## Scope

The billing logic here is a reference implementation for demonstration. It encodes one reading of publicly documented Medicare remote patient monitoring rules (for example, 16 distinct days of readings for CPT 99453 and 99454, and 20-minute time increments for 99457 and 99458) and may not match current CMS rules, payer policy, or any organization's compliance requirements. It is not billing, coding, legal or compliance advice. CPT is a registered trademark of the American Medical Association.
