"""Assembles scenario plans into source files and an expected-results manifest."""

import json
import random
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import faker
import pandas as pd

from tools.synthetic_data import config as cfg
from tools.synthetic_data.factory import PATIENT_CSV_COLUMNS, Factory
from tools.synthetic_data.model import Note, Plan
from tools.synthetic_data.scenarios import SCENARIOS, build_filler

LEGACY_DIR = "legacy"
FILES = {
    "patients": "Patient_Export.csv",
    "users": "users.json",
    "notes": f"{LEGACY_DIR}/Medical_Notes.csv",
    "time_log": f"{LEGACY_DIR}/Time_Log.csv",
    "devices": f"{LEGACY_DIR}/Fulfillment_All.csv",
    "glucose": f"{LEGACY_DIR}/Glucose_Readings.csv",
    "bp": f"{LEGACY_DIR}/Blood_Pressure_Readings.csv",
    "manifest": "manifest.json",
    "readme": "README_SYNTHETIC.txt",
}
FIRST_NOTE_ID = 500001
ORPHAN_KEY = "orphan_source_rows"

README_TEXT = f"""{cfg.SYNTHETIC_MARKER} DATA
This directory is produced by tools/synthetic_data. Every value is invented.
No real people, patients, payers or organisations are represented; any resemblance
to a real person is coincidence. It is regenerated on demand and is not committed.
"""


def minimum_patients() -> int:
    """Named scenarios that are patients in the export (the orphan case is not one)."""
    return sum(1 for s in SCENARIOS if s.key != ORPHAN_KEY)


def _ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _hms(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def build_plans(seed: int, patients: int) -> list[Plan]:
    if patients < minimum_patients():
        raise ValueError(
            f"--patients must be at least {minimum_patients()} to cover every scenario"
        )
    f = Factory(seed)
    plans: list[Plan] = []
    sid = cfg.FIRST_PATIENT_ID
    for scenario in SCENARIOS:
        if scenario.key == ORPHAN_KEY:
            # An ID that is deliberately absent from the patient export.
            plan = scenario.build(f, cfg.ORPHAN_PATIENT_ID)
        else:
            plan = scenario.build(f, sid)
            sid += 1
        plan.scenario = scenario.key
        plans.append(plan)
    while sum(p.in_export for p in plans) < patients:
        plans.append(build_filler(f, sid))
        sid += 1
    return plans


def _users() -> list[dict]:
    """Shaped like MS Graph group-member objects (what get_group_members returns)."""
    users = []
    for i, name in enumerate(cfg.STAFF):
        given, _, surname = name.partition(" ")
        slug = "".join(ch for ch in name.lower() if ch.isalnum())
        users.append(
            {
                "@odata.type": "#microsoft.graph.user",
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"synthetic-user-{i}")),
                "displayName": name,
                "givenName": given,
                "surname": surname or "Synthetic",
                "mail": f"{slug}@example.com",
                "userPrincipalName": f"{slug}@example.com",
            }
        )
    return users


def _tables(plans: list[Plan], seed: int) -> dict[str, pd.DataFrame]:
    rng = random.Random(seed + 1)  # independent stream for shuffling source rows

    patients = pd.DataFrame(
        [p.row for p in plans if p.in_export], columns=PATIENT_CSV_COLUMNS
    )

    devices, glucose, bp, notes, time_log = [], [], [], [], []
    note_ids: dict[int, int] = {}
    next_id = FIRST_NOTE_ID
    for p in plans:
        sid = p.patient_id
        for d in p.devices:
            devices.append(
                {
                    "Vendor": d.vendor,
                    "Device_ID": d.hardware_id,
                    "Device_Name": d.name,
                    "Patient_ID": sid,
                    "Resupply": int(d.resupply),
                }
            )
        for r in p.readings:
            model = (
                "Tenovi Glucometer" if r.kind == "bg" else "Omron Blood Pressure Cuff"
            )
            base = {
                "SharePoint_ID": sid,
                "Device_Model": model,
                "Time_Recorded": _ts(r.recorded),
                "Time_Recieved": _ts(
                    r.received
                ),  # column spelling matches the legacy table
            }
            if r.kind == "bg":
                glucose.append(
                    {
                        **base,
                        "BG_Reading": r.values[0],
                        "Manual_Reading": _bit(r.manual),
                    }
                )
            else:
                bp.append(
                    {
                        **base,
                        "BP_Reading_Systolic": r.values[0],
                        "BP_Reading_Diastolic": r.values[1],
                        "Manual_Reading": _bit(r.manual),
                    }
                )
        for n in p.notes:
            if n.same_as is not None:
                note_id = note_ids[id(n.same_as)]
            else:
                note_id = next_id
                next_id += 1
                note_ids[id(n)] = note_id
                if n.seconds is not None:
                    time_log.append(_time_log_row(sid, n, note_id))
            notes.append(
                {
                    "SharePoint_ID": sid,
                    "Notes": n.body,
                    "TimeStamp": _ts(n.timestamp),
                    "AZURE_UPN": n.upn,
                    "Time_Note": n.note_type,
                    "Note_ID": note_id,
                }
            )

    def shuffled(rows: list[dict], cols: list[str] | None = None) -> pd.DataFrame:
        rows = list(rows)
        rng.shuffle(rows)
        return pd.DataFrame(rows, columns=cols)

    return {
        "patients": patients,
        "devices": shuffled(devices),
        "glucose": shuffled(glucose),
        "bp": shuffled(bp),
        "notes": shuffled(notes),
        "time_log": shuffled(time_log),
    }


def _bit(value: bool | None) -> int | None:
    return None if value is None else int(value)


def _time_log_row(sid: int, n: Note, note_id: int) -> dict:
    assert n.seconds is not None
    return {
        "SharPoint_ID": sid,  # spelling matches the legacy table
        "Recording_Time": _hms(n.seconds),
        "AZURE_UPN": n.upn,
        "Notes": n.note_type,
        "Auto_Time": int(n.auto_time),
        "Start_Time": _ts(n.timestamp - timedelta(seconds=n.seconds)),
        "End_Time": _ts(n.timestamp),
        "Note_ID": note_id,
    }


# -- expected results ---------------------------------------------------------


def _in_report(applied_at: datetime) -> bool:
    return cfg.REPORT_START <= applied_at <= cfg.REPORT_END


def _in_extract(when: datetime) -> bool:
    return cfg.IMPORT_START <= when <= cfg.IMPORT_END


def _expected(plans: list[Plan]) -> dict:
    loaded = Counter()
    source = Counter()
    excluded: dict[tuple[str, str], int] = defaultdict(int)
    rejected = []
    dup = {"reading_rows": 0, "note_rows": 0}
    multi_device = 0
    codes_applied: Counter = Counter()
    codes_reported: Counter = Counter()
    applied, report = [], defaultdict(Counter)

    source["users"] = len(cfg.STAFF)
    loaded["user"] = len(cfg.STAFF)

    for p in plans:
        sid = p.patient_id
        is_loaded = p.in_export and p.rejected_reason is None
        drop_reason = (
            "no matching patient in the export"
            if not p.in_export
            else f"patient rejected: {p.rejected_reason}"
        )
        dup["reading_rows"] += p.duplicate_readings
        dup["note_rows"] += p.duplicate_notes

        if p.in_export:
            source["patients"] += 1
        if is_loaded:
            loaded["patient"] += 1
            loaded["patient_address"] += 1
            loaded["patient_insurance"] += 1
            loaded["patient_status"] += 1
            loaded["medical_necessity"] += p.dx_rows
            loaded["emergency_contact"] += p.emergency_contacts
        else:
            if p.in_export:
                rejected.append({"id": sid, "reason": p.rejected_reason})
                excluded[("patient", drop_reason)] += 1

        live_devices = [d for d in p.devices if not d.resupply]
        if is_loaded and len(live_devices) > 1 and p.readings:
            multi_device += 1
        for d in p.devices:
            source["devices"] += 1
            if d.resupply:
                excluded[
                    ("device", "excluded by the source query (Resupply flag set)")
                ] += 1
            elif not is_loaded:
                excluded[("device", drop_reason)] += 1
            else:
                loaded["device"] += 1

        for r in p.readings:
            table = "glucose_reading" if r.kind == "bg" else "blood_pressure_reading"
            source[table] += 1
            if not _in_extract(r.recorded):
                excluded[(table, "recorded outside the extract window")] += 1
            elif not is_loaded:
                excluded[(table, drop_reason)] += 1
            elif not live_devices:
                excluded[(table, "patient has no device on file")] += 1
            else:
                loaded[table] += len(live_devices)

        for n in p.notes:
            source["patient_note"] += 1
            if n.same_as is None and n.seconds is not None:
                source["time_log"] += 1
            if not _in_extract(n.timestamp):
                excluded[("patient_note", "outside the extract window")] += 1
            elif not is_loaded:
                excluded[("patient_note", drop_reason)] += 1
            else:
                loaded["patient_note"] += 1

        if not is_loaded:
            assert not p.expected_codes, "a rejected/orphan patient cannot have codes"
            continue
        for c in p.expected_codes:
            codes_applied[c.code] += 1
            applied.append({"id": sid, "code": c.code, "applied_at": _ts(c.applied_at)})
            if _in_report(c.applied_at):
                codes_reported[c.code] += 1
                report[(sid, c.applied_at.date().isoformat())][c.code] += 1

    report_rows = [
        {
            "ID": sid,
            "DateOfService": d,
            **{c: counts.get(c, 0) for c in cfg.BILLING_CODES},
        }
        for (sid, d), counts in sorted(report.items())
    ]
    return {
        "source_rows": dict(source),
        "loaded_rows": dict(loaded),
        "excluded_rows": [
            {"table": t, "reason": why, "count": n}
            for (t, why), n in sorted(excluded.items())
        ],
        "rejected_patients": rejected,
        "duplicates": {
            "duplicate_reading_rows": dup["reading_rows"],
            "duplicate_note_rows": dup["note_rows"],
            "note": "duplicates are loaded as-is; the pipeline does not merge them",
        },
        "multi_device_patients": multi_device,
        "codes_applied": {c: codes_applied.get(c, 0) for c in cfg.BILLING_CODES},
        "codes_in_report": {c: codes_reported.get(c, 0) for c in cfg.BILLING_CODES},
        "applied_codes": sorted(
            applied, key=lambda a: (a["id"], a["code"], a["applied_at"])
        ),
        "report_rows": report_rows,
    }


def _manifest(plans: list[Plan], seed: int, requested: int) -> dict:
    scenario_rows = []
    by_key = {s.key: s for s in SCENARIOS}
    for i, s in enumerate(SCENARIOS, start=1):
        plan = next(p for p in plans if p.scenario == s.key)
        scenario_rows.append(
            {
                "id": f"S{i:02d}",
                "key": s.key,
                "rule": s.rule,
                "summary": s.summary,
                "edge": s.edge,
                "patient_id": plan.patient_id,
                "in_export": plan.in_export,
                "rejected_reason": plan.rejected_reason,
                "flags": plan.flags,
                "expected_codes": [
                    {
                        "code": c.code,
                        "applied_at": _ts(c.applied_at),
                        "in_report": _in_report(c.applied_at),
                    }
                    for c in plan.expected_codes
                ],
            }
        )
    assert len(scenario_rows) == len(by_key), "scenario keys must be unique"
    return {
        "generator": {
            "version": cfg.GENERATOR_VERSION,
            "faker_version": faker.VERSION,
            "seed": seed,
            "patients_requested": requested,
            "note": "expected values are stated by construction, not computed by the billing rules",
        },
        "windows": {
            "extract_start": _ts(cfg.IMPORT_START),
            "extract_end": _ts(cfg.IMPORT_END),
            "report_start": _ts(cfg.REPORT_START),
            "report_end": _ts(cfg.REPORT_END),
        },
        "scenarios": scenario_rows,
        "expected": _expected(plans),
    }


# -- output -------------------------------------------------------------------


def generate(seed: int, patients: int, out_dir: Path | str) -> dict:
    """Write the synthetic source files and manifest to `out_dir`; return the manifest."""
    out = Path(out_dir)
    plans = build_plans(seed, patients)
    tables = _tables(plans, seed)
    manifest = _manifest(plans, seed, patients)

    (out / LEGACY_DIR).mkdir(parents=True, exist_ok=True)
    for key in ("patients", "notes", "time_log", "devices", "glucose", "bp"):
        tables[key].to_csv(out / FILES[key], index=False, lineterminator="\n")
    users = {"value": _users()}
    (out / FILES["users"]).write_text(json.dumps(users, indent=2) + "\n")
    (out / FILES["manifest"]).write_text(json.dumps(manifest, indent=2) + "\n")
    (out / FILES["readme"]).write_text(README_TEXT)
    return manifest
