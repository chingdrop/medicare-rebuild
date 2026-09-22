# Stored procedures

Nothing in this directory is called by the pipeline anymore. These files are kept for
reference — a portfolio reader can compare the original T-SQL against the Python that
replaced it.

- `batch_medcode_*.sql`, `create_billing_report.sql`, `reset_medical_code_tables.sql` —
  billing computation. Ported to `src/medicare_rebuild/billing.py`; see
  [decision 0014](../../docs/decisions/0014-pandas-billing-rules.md) and
  [docs/billing-rules.md](../../docs/billing-rules.md).
- `reset_all_billing_tables.sql` — the full-database reset that used to run once per
  pipeline run. Ported to `models.reset_all_data`; see
  [decision 0015](../../docs/decisions/0015-full-orm-schema-of-record.md).
- `get_my_queue.sql`, `get_patient_overview.sql`, `search_patient_list.sql` — never
  called by this pipeline. They came from the original system this repository was built
  from and are unrelated to the ETL/billing flow described in the docs above.
