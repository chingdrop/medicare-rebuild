# Reconciliation

`make reconcile` proves a finished demo run is complete and consistent. It runs after
the pipeline, reads the raw source files and queries the loaded database itself, and
does not use the pipeline's own summary. Everything is synthetic; reports contain
counts and synthetic or surrogate IDs only, never field values.

## What is verified, and why it matters

| Check | Question it answers | Property |
|-------|---------------------|----------|
| 1 row conservation | Is every source row loaded, merged, rejected or dropped for a documented reason? Shows the equation with real numbers per source. | completeness |
| 2a key uniqueness | Are primary and natural keys (patient ID, device hardware ID, user IDs, one address/insurance/status per patient) unique? | integrity |
| 2b foreign keys | Does every foreign key point at a row that exists? | integrity |
| 2c required fields | Are the fields a loaded row must have all populated? | integrity |
| 3 cross-source consistency | Does a patient present in several sources resolve to exactly one loaded entity? | integrity |
| 4 rejection accounting | Does every rejected patient have a reason code, with counts by reason? | completeness |
| 5 billing lineage | Does each billed code trace back to source rows in its window, stamped at the latest one, and does the set of codes match the generator's by-construction manifest? | traceability |
| 6 report totals | Do the billing report's counts equal the database's? | consistency |

Rules are not re-implemented. Check 5 uses the rules table in
[billing-rules.md](billing-rules.md) only to choose which source rows to trace for each
code (readings or notes, and the window). Whether a patient *qualifies* is settled by
the manifest comparison.

Two limits of what the pipeline records shaped the design:

- **Rejected rows are not recorded by the pipeline.** It drops them silently
  ([ADR 0005](decisions/0005-validate-in-transform-and-drop-bad-patient-rows.md)). Reconcile
  derives reason codes itself: it re-runs the pipeline's own `normalize_patients` on the
  source file and assigns a code per violated length limit (`tools/reconcile/reasons.py`).
  The limits are declared once there and cross-checked against the pipeline's
  `check_patient_db_constraints` on every run, so drift is reported as a failure.
- **Readings link to a patient only through their device.** The pipeline writes no patient
  key on reading rows, so lineage resolves a reading's patient via its device. (Building this
  check showed the demo's reconstructed reading tables had an unused `patient_id` column;
  it has been removed from `schema.py`.)

## Commands

```sh
make demo          # run the pipeline on synthetic data (see demo.md)
make reconcile     # check the run; exits non-zero if any check fails
```

Outputs, both git-ignored, in `demo_output/`: `reconcile.txt` (the summary below) and
`reconcile.json` (the same results, machine-readable). Without `make`:

```sh
uv run python -m tools.reconcile --data-dir demo_data --output-dir demo_output
uv run python -m tools.reconcile --sample 25    # trace 25 billed codes instead of all
```

By default every billed code is traced, which is cheap at demo size. Running twice on the
same seed gives identical output.

## Real output: a clean run

The unedited output of `make reconcile` straight after `make demo`:

```text
uv run python -m tools.reconcile --data-dir demo_data --output-dir demo_output
Reconciliation - counts and synthetic IDs only
Seed 20250228

Row conservation (source = loaded + merged + dispositions):
  patients                 source   200 = loaded 196 + duplicates merged 0 + PATIENT_REJECTED 4  [unexplained 0]
                           rejected by reason: EMERGENCY_PHONE_1_LENGTH 1, PHONE_LENGTH 1, STATE_LENGTH 1, ZIP_LENGTH 1
  devices                  source   109 = loaded 103 + duplicates merged 0 + EXCLUDED_BY_SOURCE_QUERY 1 + NO_PATIENT_IN_EXPORT 1 + PATIENT_REJECTED 4  [unexplained 0]
  glucose readings         source   904 = loaded 823 (of which 16 extra from device fan-out) + duplicates merged 0 + OUTSIDE_EXTRACT_WINDOW 1 + NO_PATIENT_IN_EXPORT 16 + PATIENT_REJECTED 64 + NO_DEVICE_ON_FILE 16  [unexplained 0]
  blood pressure readings  source   507 = loaded 523 (of which 16 extra from device fan-out) + duplicates merged 0 + OUTSIDE_EXTRACT_WINDOW 0 + NO_PATIENT_IN_EXPORT 0 + PATIENT_REJECTED 0 + NO_DEVICE_ON_FILE 0  [unexplained 0]
  patient notes            source   158 = loaded 153 + duplicates merged 0 + OUTSIDE_EXTRACT_WINDOW 0 + NO_PATIENT_IN_EXPORT 1 + PATIENT_REJECTED 4  [unexplained 0]
  users                    source     8 = loaded 8 + duplicates merged 0  [unexplained 0]
  patient_address          expected   196 = loaded 196  [unexplained 0]
  patient_insurance        expected   196 = loaded 196  [unexplained 0]
  patient_status           expected   196 = loaded 196  [unexplained 0]
  medical_necessity        expected   393 = loaded 393  [unexplained 0]
  emergency_contact        expected   278 = loaded 278  [unexplained 0]

Checks:
  [PASS] 1 row conservation: every source row is accounted for
  [PASS] 2a key uniqueness: 24 keys unique
  [PASS] 2b foreign keys: 18 relationships have no orphans
  [PASS] 2c required fields: no required field is empty after load
  [PASS] 3 cross-source consistency: 152 patients in 2+ sources each resolve to one loaded entity
  [PASS] 4 rejection accounting: 4 rejected patients, each with a reason code
  [PASS] 5 billing lineage: 155 billed codes traced to source rows; manifest agrees
  [PASS] 6 report totals: report equals the database: 89 rows, 152 codes

RECONCILIATION: PASS (8/8 checks)
```

## Real output: an injected fault

Faults are optional and off by default. `make demo FAULT=orphan-fk` generates the same data,
runs the same pipeline and passes its own checks, then corrupts the loaded database
afterwards. It changes neither the pipeline nor the source files. `make reconcile` then
catches it (the row-conservation section is identical to the clean run and is omitted here):

```text
uv run python -m tools.reconcile --data-dir demo_data --output-dir demo_output
Reconciliation - counts and synthetic IDs only
Seed 20250228
Injected fault(s) applied after the run: orphan-fk
...
Checks:
  [PASS] 1 row conservation: every source row is accounted for
  [PASS] 2a key uniqueness: 24 keys unique
  [FAIL] 2b foreign keys: orphans: blood_pressure_reading.device_id -> device
         IDs: blood_pressure_reading:1
  [PASS] 2c required fields: no required field is empty after load
  [PASS] 3 cross-source consistency: 152 patients in 2+ sources each resolve to one loaded entity
  [PASS] 4 rejection accounting: 4 rejected patients, each with a reason code
  [PASS] 5 billing lineage: 155 billed codes traced to source rows; manifest agrees
  [PASS] 6 report totals: report equals the database: 89 rows, 152 codes

RECONCILIATION: FAIL (7/8 checks)
make: *** [reconcile] Error 1
```

| `FAULT=` | What is done after the run | Check that catches it |
|----------|----------------------------|-----------------------|
| `drop-rows` | delete three glucose readings of a patient with no billed codes | 1 row conservation |
| `duplicate-keys` | give two devices the same hardware ID | 2a key uniqueness |
| `orphan-fk` | point one blood pressure reading at a device that does not exist | 2b foreign keys |
| `report-off-by-one` | add one to a code count in the billing report file | 6 report totals |

Each fault trips exactly its own check and no other; the integration tests assert this.
Run `make demo-down` to reset.

## What reconciliation cannot prove

- **That the source data is correct.** It shows every source row is accounted for, not that
  the rows are true.
- **That billing policy is current.** Lineage confirms codes trace to source rows; the
  thresholds themselves come from the procedures and the manifest, and payer rules change.
- **That its own assumptions match a real deployment.** The required-field set and the
  length limits behind the reason codes are reconcile's own declarations. The real schema is
  not in this repository, and the demo's tables are reconstructed.
- **Anything the pipeline did not load.** A source row dropped for a reason reconcile does not
  know shows up as an unexplained difference, which is a failure by design.
- **Scale.** Tables are read into memory, which suits demo size.
- **Anything outside the run it was given.** It checks the data and report of one run; it does
  not detect problems in earlier runs.
