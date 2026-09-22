# Synthetic-data demo

Run the whole pipeline - extract, transform, load, billing rules, report - from a fresh
clone, with no credentials, no network access during the run, and no real data.

```sh
uv sync
make demo
```

That is the entire demo. It ends with a pass/fail comparison against expectations that
were written down when the data was generated.

## What it proves, and what it does not

**It shows that**, given source data with known structure, the pipeline as committed in
`src/medicare_rebuild/` and the stored procedures in `sql/stored_procedures/` produce
exactly the outcomes the data was built to produce: which patients are loaded and which
are rejected, which billing codes are applied and when, and what the billing report
contains. The data includes threshold cases (just below, at, just above), window edges,
duplicates, out-of-order dates and malformed fields; see the [scenario table](#scenarios).

**It does not show that**

- the pipeline is correct against real data or the real production schema. The GPS and
  legacy tables are *reconstructed* for the demo (see [What is reconstructed](#what-is-reconstructed-or-replaced));
- the billing thresholds match Medicare's rules. The expected values encode what the
  stored procedures are written to do, not what CMS requires;
- the Microsoft Graph client works. It is replaced by a local file for the run
  (it is covered by `tests/test_api_utils.py` instead).

## Requirements

Setup needs a network connection once; the run itself does not use one.

| Need | Why | Notes |
|------|-----|-------|
| Docker, with the daemon running | disposable SQL Server container | first run pulls `mcr.microsoft.com/mssql/server:2022-latest`. On Apple Silicon the image runs under x86_64 emulation. |
| [uv](https://docs.astral.sh/uv/) | Python environment | `uv sync` downloads dependencies once. |
| ODBC Driver 18 for SQL Server | how Python talks to SQL Server | macOS: `brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18` |
| `make` | the one-command entry point | the copy shipped with macOS and Linux is enough. |

The SQL Server `sa` password (`IntegrationTest_Passw0rd!`) is the throwaway value already
in `docker-compose.yml`. It is **dev-only**: it protects a disposable local container
holding synthetic data and nothing else. Do not reuse it anywhere real.

## Commands

| Command | What it does |
|---------|--------------|
| `make demo` | Starts the container, generates data into `demo_data/`, builds the two demo databases, runs the pipeline, writes the report to `demo_output/`, prints the summary, exits non-zero on FAIL. |
| `make demo-down` | Stops and removes the container and deletes `demo_data/` and `demo_output/`. |
| `make reconcile` | After `make demo`: checks the run is complete and consistent (row conservation, key integrity, billing lineage, report totals) and exits non-zero on any failure. See [reconciliation.md](reconciliation.md). |

`demo_data/` and `demo_output/` are git-ignored. Generated CSV/XLSX files are never committed.

Outputs in `demo_output/`: `Billing_Report.xlsx` (the report the pipeline wrote),
`summary.txt` (what is printed below), `checks.json`, and `work/` (the directory the
pipeline ran in).

## Real output

This is the unedited output of `make demo` from an actual run (with the container image
already pulled):

```text
docker compose up -d mssql
 Container medicare-rebuild-mssql-1 Running 
uv run python -m tools.synthetic_data --out demo_data  
Generated 200 synthetic patients (seed 20250228) in demo_data/ - 46 named scenarios
uv run python -m tools.synthetic_data.demo --data-dir demo_data --output-dir demo_output
Synthetic demo - seed 20250228, 200 patients, 46 named scenarios

Rows: source -> loaded
  users                         8 ->     8
  patients                    200 ->   196
  devices                     109 ->   103
  glucose readings            904 ->   823
  blood pressure readings     507 ->   523
  patient notes               158 ->   153

Rejected patients (4) - reasons are from the manifest;
the pipeline drops these rows silently, along with their devices, readings and notes:
  ID 1039: phone number longer than 11 digits
  ID 1040: state not recognised (longer than 2 characters)
  ID 1041: ZIP code longer than 5 characters
  ID 1042: emergency contact number longer than 11 digits
Other rows not loaded:
  excluded by the source query (Resupply flag set): 1 device
  no matching patient in the export: 1 device, 16 glucose reading, 1 patient note
  patient has no device on file: 16 glucose reading
  recorded outside the extract window: 1 glucose reading
Rows loaded twice: readings for the 1 multi-device patient are duplicated once per device (known limitation, see docs/demo.md)

Duplicates: 16 duplicate reading rows and 1 duplicate note row, loaded as-is (the pipeline does not merge them)

Billing codes        applied   in report
  99202                  13         13
  99453                  46         44
  99454                  44         43
  99457                  34         34
  99458                  18         18
  report rows: 89   report content hash: fab1462d5f2e8eb2

Checks against the manifest:
  [PASS] source row counts (7 checks)
  [PASS] loaded row counts (11 checks)
  [PASS] rejected patients
  [PASS] billing codes applied (patient, code, timestamp)
  [PASS] billing report rows
  [PASS] references resolved (note types, authors, coaches, statuses) (4 checks)
  [PASS] no SQL errors logged by the pipeline
  [PASS] named scenarios behave as designed (46 scenarios)

RESULT: PASS (27/27 checks)
Report: demo_output/Billing_Report.xlsx
```

Running it again produces the same summary; the `report content hash` line is a digest
of the report's rows, so identical hashes mean identical results.

## Changing the data, and resetting

```sh
make demo SEED=42 PATIENTS=300     # different data, still deterministic
make demo-down                     # reset everything
```

- `SEED` (default `20250228`): the same seed always produces byte-identical files, given the
  Faker version pinned in `uv.lock`. A different seed gives different names, dates and
  filler patients. The named scenarios keep their shape, and the expectations are rebuilt for
  the new data, so the demo still ends in PASS.
- `PATIENTS` (default `200`, minimum `45`): patients in the export. The named scenarios
  come first; the rest are filler patients drawn from a fixed mix of profiles.

Without `make`:

```sh
docker compose up -d mssql
uv run python -m tools.synthetic_data --seed 42 --patients 300 --out demo_data
uv run python -m tools.synthetic_data.demo --data-dir demo_data --output-dir demo_output
```

## What the synthetic data looks like

Everything is invented and marked as such:

- Names come from [Faker](https://faker.readthedocs.io/) with a fixed seed; addresses come
  from a fixed list of fictional streets in fictional towns, with ZIP codes in the
  unassigned `000xx` range. Coincidental resemblance to a real person is possible but
  unintended.
- Phone numbers are `555-01xx`; email addresses are `@example.com`.
- Medicare IDs are `SYN-000123` (not MBI-shaped). Social Security numbers are left blank.
- Every patient's nickname is `SYNTHETIC-<id>`, and every clinical note starts with
  `SYNTHETIC note:`. Device names carry `(SYNTHETIC)`.
- The generator scans its own output and fails if it finds a non-`9xx` SSN pattern, an
  MBI-shaped value, a phone number outside `555-01xx`, or an email outside `example.com`
  (`tools/synthetic_data/safety.py`; unit-tested).

## How it works

```text
tools/synthetic_data/            demo tooling, outside the installed package
  generator.py, scenarios.py     data + expectations, written together
  schema.py                      reconstructed demo tables + which stored procedures to apply
  demo.py                        runs the real pipeline, compares with the manifest
demo_data/                       generated: Patient_Export.csv, users.json, legacy/*.csv, manifest.json
```

1. **Generate.** Each scenario builds its source rows *and* states the outcome it expects
   (for example: "16 distinct days of readings, so 99453 and 99454 stamped at the last
   reading"). Expectations are written by construction; nothing re-implements the billing
   rules to compute them. They are written to `demo_data/manifest.json`.
2. **Load.** Two throwaway databases are created: a *legacy* database holding the source
   tables the pipeline reads, and a *GPS* database with the target tables. The stored
   procedures are applied verbatim from `sql/stored_procedures/`.
3. **Run.** The unmodified pipeline runs (`import_all_data`, then `create_billing_report`,
   with the same date windows `main()` uses).
4. **Compare.** Row counts, rejected patients, every applied billing code (patient, code,
   timestamp), the report rows, resolved references, and per-scenario behaviour are checked
   against the manifest. The pipeline swallows SQL errors (it logs and returns), so the runner
   also fails if any error was logged.

### What is reconstructed or replaced

- **Tables.** The repo has no authoritative schema. The demo tables in `schema.py` are
  *reconstructed, not authoritative*: inferred from the columns the pipeline writes, the
  columns the stored procedures read, and the ERDs in `docs/erd/`. Constraints and column
  widths are guesses that are just permissive enough to run.
- **Microsoft Graph.** `DataImporter.get_user_data` builds an `MSGraphApi` internally, which
  would call `login.microsoftonline.com` and `graph.microsoft.com`. During the demo only,
  the runner swaps that one class for a stand-in that reads `demo_data/users.json`
  (`unittest.mock.patch` inside the runner process). `get_user_data` itself runs unmodified,
  and the `AZURE_*` variables are set to obvious placeholders that are never used.
- **Patient notes.** `main()` and `import_all_data()` never import patient notes (that step
  was dropped in a refactor), so as written they cannot produce 99202, 99457 or 99458. The
  runner calls `get_patient_note_data` / `import_patient_note_data` itself, then re-runs the
  two note `UPDATE` statements. This is the only orchestration difference from `main()`.

No file under `src/` or `sql/` is modified by the demo.

## Behaviour the demo pins down

These are properties of the pipeline as committed. The demo documents them; it does not
change them.

- **Multi-device patients get duplicated readings** (S07). Readings are joined to the device
  table on `patient_id`, so a patient with two devices has every reading loaded once per
  device. Billing is unaffected (it counts distinct days), but row counts are inflated.
  See [billing-rules.md](billing-rules.md#known-gaps-and-assumptions).
- **A code can be applied yet fall outside the report** (S10, S13). The report keeps codes
  stamped up to midnight at the *start* of the end date, so a reading received at 00:20 on the
  last day is coded but not reported. Similarly, S14: data recorded after midnight on the end
  date is never extracted.
- **Duplicate notes double-count time** (S35). Nothing de-duplicates notes, so a note present
  twice is counted twice toward 99457.
- **Rejected patients disappear silently** (S39-S42). Rows violating column limits are
  filtered out with no log line, along with their devices, readings and notes; the reasons
  in the summary come from the manifest.

## Scenarios

Each scenario is one patient (S46 is deliberately absent from the patient export). "Expected
codes" is what the manifest states; the run must match it exactly.

| # | Scenario | Rule | Edge case | Expected codes | Flags |
|---|----------|------|-----------|----------------|-------|
| S01 | `rpm_bg_16_days` | 99453 + 99454 | threshold (16) | 99453, 99454 |  |
| S02 | `rpm_bg_15_days` | 99453 + 99454 | just below threshold | none |  |
| S03 | `rpm_bg_17_days` | 99453 + 99454 | just above threshold | 99453, 99454 |  |
| S04 | `rpm_bg_many_per_day` | 99453 + 99454 | counts days, not readings | none |  |
| S05 | `rpm_bp_16_days` | 99453 + 99454 | threshold (16), bp procedures | 99453, 99454 |  |
| S06 | `rpm_bp_15_days` | 99453 + 99454 | just below threshold, bp procedures | none |  |
| S07 | `rpm_multi_device` | 99453 + 99454 | one code per patient; readings duplicated by device join | 99453, 99454 | reading_fanout |
| S08 | `rpm_window_inside` | 99454 | 30-day window edge, inside | 99453, 99454 |  |
| S09 | `rpm_window_outside` | 99454 | 30-day window edge, outside (99453 only) | 99453 |  |
| S10 | `rpm_january_only` | 99453 | code applied outside report period | 99453 (not in report) |  |
| S11 | `rpm_duplicate_readings` | 99453 + 99454 | duplicate source rows | 99453, 99454 | duplicate_rows_loaded_as_is |
| S12 | `rpm_out_of_order` | 99453 + 99454 | out-of-order dates | 99453, 99454 |  |
| S13 | `rpm_received_after_midnight` | report | report end-date edge: coded but not reported | 99453, 99454 (not in report) | report_end_date_edge |
| S14 | `rpm_recorded_after_cutoff` | extract window | extract end-date edge: 16th day never loaded | none |  |
| S15 | `rpm_null_values` | 99453 + 99454 | missing fields | 99453, 99454 |  |
| S16 | `rpm_readings_no_device` | load | dependent rows dropped | none |  |
| S17 | `rpm_resupply_device` | load | source query excludes resupply rows | 99453, 99454 |  |
| S18 | `ie_nurse_practitioner` | 99202 | time forced to 15 min | 99202 |  |
| S19 | `ie_899_seconds` | 99202 | just below 15 min | none |  |
| S20 | `ie_900_seconds` | 99202 | threshold (15 min) | 99202 |  |
| S21 | `ie_1799_seconds` | 99202 + 99457 | just below 30 min | 99202, 99457 |  |
| S22 | `ie_1800_seconds` | 99457 | at 30 min: no 99202 | 99457 |  |
| S23 | `ie_two_notes` | 99202 | time summed across notes | 99202 |  |
| S24 | `ie_registered_nurse` | 99202 | note type forced to Initial Evaluation | 99202 |  |
| S25 | `time_1199_seconds` | 99457 | just below 20 min | none |  |
| S26 | `time_1200_seconds` | 99457 | threshold (20 min) | 99457 |  |
| S27 | `time_three_notes` | 99457 | time summed across notes | 99457 |  |
| S28 | `time_window_inside` | 99457 | 1-month window edge, inside | 99457 |  |
| S29 | `time_window_outside` | 99457 | 1-month window edge, outside | none |  |
| S30 | `time_39_minutes` | 99457 / 99458 | 99458 not yet earned | 99457 |  |
| S31 | `time_40_minutes` | 99457 / 99458 | first 99458 | 99457, 99458 |  |
| S32 | `time_60_minutes` | 99457 / 99458 | two 99458 | 99457, 99458 x2 |  |
| S33 | `time_80_minutes` | 99457 / 99458 | three 99458 | 99457, 99458 x3 |  |
| S34 | `time_100_minutes` | 99457 / 99458 | 99458 capped at three | 99457, 99458 x3 |  |
| S35 | `time_duplicate_notes` | 99457 | duplicate source rows double-count time | 99457 | duplicate_rows_loaded_as_is, call_time_double_counted |
| S36 | `time_no_time_log` | 99457 | missing fields; HTML markup in note text | none |  |
| S37 | `time_alert_notes` | 99457 | note type forced to Alert; any type counts | 99457 |  |
| S38 | `rpm_and_time_same_day` | all codes | one report row carries four codes | 99453, 99454, 99457, 99458 |  |
| S39 | `reject_phone` | load | patient rejected, dependents dropped | none (patient rejected) |  |
| S40 | `reject_state` | load | patient rejected, dependents dropped | none (patient rejected) |  |
| S41 | `reject_zip` | load | patient rejected, dependents dropped | none (patient rejected) |  |
| S42 | `reject_emergency_phone` | load | patient rejected, dependents dropped | none (patient rejected) |  |
| S43 | `malformed_email` | load | loads with NULL email | none |  |
| S44 | `messy_formatting` | load | values normalised, not rejected | none |  |
| S45 | `missing_optional_fields` | load | blank diagnosis still yields a report row | 99453, 99454 |  |
| S46 | `orphan_source_rows` | load | no matching patient: nothing loads | none (no patient in export) |  |

## Timing

Measured in a copy of the tree with no local state (no `.venv`, no `demo_data/`, no
`demo_output/`), on an Apple Silicon Mac with the SQL Server image and the `uv` package
cache already present: `uv sync` under 1 second and `make demo` about 13 seconds, including
starting the container. A first run on a clean machine adds the image pull (well over a
gigabyte) and the dependency downloads; that time was not measured.
