# TODO

Open items for this repo: bugs to fix, features to build, and housekeeping. This is the
one list. Anything that would once have been left as a `TODO(craig)` comment in the code
or docs goes here instead (the last such marker, in
[`docs/limitations-and-roadmap.md`](docs/limitations-and-roadmap.md), was moved here), and
items are checked off as they land.

## Bugs

- [ ] **Microsoft Graph members are not paged.** `MSGraphApi.get_group_members`
  (`src/medicare_rebuild/utils/api_utils.py`) makes one request and ignores
  `@odata.nextLink`, so a staff group larger than one page (100 by default) silently
  loses users, and their patients and notes resolve to no user.
- [ ] **A patient with two devices of the same type gets each reading once per device.**
  Readings carry no device ID, so `_resolve_device_readings` can't tell which glucose
  meter took a reading (e.g. a replacement meter). Billing counts distinct days, so codes
  are unaffected, but row counts are inflated. No demo scenario covers it.
- [ ] **Check `standardize_device_type` against the real device names.** Its keywords
  ("gluc"/"BG" and "pressure"/"BP"/"cuff") were chosen without the real `Device_Name`
  vocabulary; a device whose name matches neither gets no readings (logged as a count).
- [ ] **A code stamped exactly at midnight on the 1st appears in two months' reports.**
  Every end bound is an inclusive midnight (`<=`), inherited from the original SQL, so the
  boundary instant falls in both windows. An exclusive end bound fixes it but changes the
  pinned demo edges (S10, S13, S14). See `docs/billing-rules.md`, Known gaps.
- [ ] **Duplicate source rows are loaded and counted.** Nothing de-duplicates notes or
  readings, so a note present twice counts its time twice toward 99457 (S35); duplicate
  readings load twice but don't change day counts (S11).
- [ ] **Patients missing an address, insurance or diagnosis row vanish from the report.**
  `build_billing_report` inner-joins those tables, so codes applied to such a patient are
  never reported, with no warning.

## Billing features

From `docs/billing-rules.md`, Known gaps and assumptions, unless noted.

- [ ] **Remember billed codes between runs.** Codes are deleted and recomputed every run,
  so one-time codes (99453, 99202) are billed again in every later run. Keep a persistent
  "already billed" flag per patient and code.
- [ ] **Use the enrollment date.** `On-board Date` is loaded into
  `medical_necessity.evaluation_datetime` but never read; a date of service should only be
  billable for the 30 days following it.
- [ ] **Exclude internal notes from billed time.** Every note counts toward 99457 and
  99458; internal notes shouldn't. Needs an "internal" note type in the lookup tables and
  the source mapping.
- [ ] **Escalate 99202 to 99203-99205.** An initial evaluation of 30 minutes or more
  currently earns no visit code at all (S22).
- [ ] **Model 99453 as a setup event.** It is awarded on 16 reading days as a proxy, not
  on a recorded device setup or patient education event, and has no 30-day window.
- [ ] **Decide calendar month vs rolling windows.** 99457/99458 use a rolling month by
  design; outside sources describe them as calendar-month services. Record the decision
  as an ADR either way.
- [ ] **Re-check the code rules against current CMS policy.** The CY 2026 fee schedule
  revises the remote monitoring codes (new 99445, revised 99454); the 99458 cap of three
  and the 15-to-under-30-minute band for 99202 also need a primary source.
- [ ] **Define "day" with a timezone.** Reading days are the stored `received_datetime`'s
  date with no timezone conversion.
- [ ] **Payer-specific checks.** Nothing checks coverage, medical necessity, ordering
  provider, frequency limits across providers, or claim edits.
- [ ] **A dedicated date-of-service entity** instead of deriving it from
  `medical_code.timestamp_applied` (`docs/narrative.md`, "What I'd change now";
  decision 0003).

## Pipeline features

- [ ] **Audit trail for rejected rows.** Patients that fail `check_patient_db_constraints`
  are dropped silently (decision 0005); write them, with a reason, somewhere an operator
  can review, rather than relying on the demo's manifest and the reconciliation tool.
- [ ] **Fulfillment tables.** Orders, order status, resupply and their join tables are in
  the original design (`docs/erd/5_patient_fulfillment_erd.png`) but have no models.
- [ ] **Run the integration tests against the Alembic-built schema too.** The demo and
  integration tests use `metadata.create_all()`; only `test_alembic_integration.py`
  exercises the migration chain.

## Production hardening

Described in `docs/data-handling.md` Part B, none of it implemented.

- [ ] Business associate agreements before any real patient data.
- [ ] Least-privilege database accounts: read-only for the legacy sources, a load account
  for the GPS database.
- [ ] Encryption in transit (`Encrypt=yes` with validated certificates) and at rest
  (databases, backups, and the disk holding `data/`, snapshots, reports and logs).
- [ ] Access logging for who reads or exports patient data, separate from debug logs.
- [ ] A managed secret store with rotation instead of `.env` files.
- [ ] Minimum necessary: load only what billing needs; consider dropping `social_security`.
- [ ] Retention and secure disposal for logs, snapshots and report files.

## Repo hygiene

- [ ] `main` has no branch protection or rulesets. If you add protection, require the CI
  check names: `lint`, `typecheck`, `test`, `integration-test`, `security`, `analyze`
  and `CodeQL`.
- [ ] Optionally tighten mypy toward `strict`, one flag at a time. `pyproject.toml`
  records the current counts: `--strict` reports 42 errors in `src/`, and `tools/` isn't
  type-checked (8 errors).
