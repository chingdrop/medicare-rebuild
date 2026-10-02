# Limitations and roadmap

## Known limitations

Everything below is already documented elsewhere in this repository; this page
collects it into one place rather than repeating it.

| Limitation | What it means in practice | Documented in |
|---|---|---|
| Fixed CPT code set | Only 99202, 99453, 99454, 99457 and 99458 are computed. No other E/M or remote-monitoring codes exist here. | [billing-rules.md](billing-rules.md) (Purpose and scope; Rules at a glance) |
| No payer-specific rule variation | The rules encode one reading of the thresholds; there is no per-payer branching, coverage check, or claim edit. | [billing-rules.md: Known gaps and assumptions](billing-rules.md#known-gaps-and-assumptions) ("Payer-specific checks are absent") |
| Several rule behaviors diverge from real billing intent | Enrollment date is unused, windows are rolling rather than calendar-month in places, codes have no memory across runs, internal notes aren't distinguished from patient-facing ones, and 99202 doesn't escalate above 30 minutes. Each is explained individually. | [billing-rules.md: Known gaps and assumptions](billing-rules.md#known-gaps-and-assumptions) |
| Reconciliation checks the pipeline's own load, not the source's correctness | `make reconcile` confirms every source row is accounted for and internally consistent; it cannot confirm the source data itself is correct, or that the billing thresholds match current policy. | [reconciliation.md: What reconciliation cannot prove](reconciliation.md#what-reconciliation-cannot-prove) |
| The demo runs at a reduced synthetic scale | The generator defaults to 200 patients; the original program this pipeline was built for ran at roughly 22,000. Reconciliation reads whole tables into memory, which suits demo scale. | [tools/synthetic_data/config.py](../tools/synthetic_data/config.py) (`DEFAULT_PATIENTS`); [README: Results](../README.md#results); [reconciliation.md: What reconciliation cannot prove](reconciliation.md#what-reconciliation-cannot-prove) |
| SQL Server-specific | The schema is SQLAlchemy declarative models and billing is pandas/ORM (`billing.py`), both more portable in principle than the hand-written T-SQL they replaced, but the reset helpers each still issue one raw, SQL-Server-specific statement with no ORM equivalent (`DBCC CHECKIDENT`), and `DatabaseManager` connects through `mssql+pyodbc` and ODBC Driver 18 specifically. Porting to another RDBMS would still need rework, not a config change. | [models.py](../src/medicare_rebuild/models.py) (`reset_all_data`); [billing.py](../src/medicare_rebuild/billing.py) (`clear_medical_codes`); [db_utils.py](../src/medicare_rebuild/utils/db_utils.py) |
| Production hardening is guidance only, not implemented | Least-privilege database accounts, encryption in transit and at rest, access logging, managed secrets, and data retention/disposal are all described but not built. | [data-handling.md: Part B](data-handling.md#part-b-deployment-guidance-not-implemented-by-this-repository) |
| The demo's legacy source schema is reconstructed, not authoritative | The GPS schema is now generated from `models.py` and is authoritative-by-construction. The legacy source tables remain a reconstruction, inferred from the columns `queries.py` reads, since the real source system is not part of this repository. | [demo.md: What is reconstructed or replaced](demo.md#what-is-reconstructed-or-replaced) |

## Possible next steps

Open bugs, features and housekeeping are tracked in one place,
[`TODO.md`](../TODO.md) at the repository root, rather than as `TODO(craig)` markers
scattered through the docs and code. That list includes every limitation above that
has a concrete next step.

These are directions, not commitments, and nothing there should be read as a release
plan.
