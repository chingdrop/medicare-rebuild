"""Unit tests for tools/reconcile with small hand-built inputs (no database)."""

import json
from datetime import datetime

import pandas as pd

from tools.reconcile import checks, reasons
from tools.reconcile.checks import PRIMARY_KEYS
from tools.reconcile.model import Loaded, Settings, Source
from tools.reconcile.run import Reconciliation, render_json, render_text
from tools.synthetic_data import config as cfg
from tools.synthetic_data.generator import generate, minimum_patients

CFG = Settings(
    extract_start=datetime(2025, 1, 1),
    extract_end=datetime(2025, 2, 28),
    report_start=datetime(2025, 2, 1),
    report_end=datetime(2025, 2, 28),
)

COLUMNS = {
    "user": ["user_id", "ms_entra_id", "display_name"],
    "patient": [
        "patient_id",
        "sharepoint_id",
        "first_name",
        "last_name",
        "date_of_birth",
        "user_id",
    ],
    "patient_address": ["patient_address_id", "patient_id"],
    "patient_insurance": ["patient_insurance_id", "patient_id"],
    "medical_necessity": ["medical_necessity_id", "patient_id"],
    "patient_status_type": ["patient_status_type_id"],
    "patient_status": ["patient_status_id", "patient_id", "patient_status_type_id"],
    "emergency_contact": ["emergency_contact_id", "patient_id"],
    "vendor": ["vendor_id"],
    "device": ["device_id", "hardware_uuid", "patient_id", "vendor_id"],
    "glucose_reading": [
        "glucose_reading_id",
        "device_id",
        "recorded_datetime",
        "received_datetime",
    ],
    "blood_pressure_reading": [
        "blood_pressure_reading_id",
        "device_id",
        "recorded_datetime",
        "received_datetime",
    ],
    "note_type": ["note_type_id", "name"],
    "patient_note": [
        "patient_note_id",
        "patient_id",
        "note_datetime",
        "note_content",
        "temp_user",
        "note_type_id",
        "user_id",
    ],
    "medical_code_type": ["med_code_type_id", "name"],
    "medical_code": [
        "med_code_id",
        "patient_id",
        "med_code_type_id",
        "timestamp_applied",
    ],
    "medical_code_device": ["medical_code_device_id", "med_code_id", "device_id"],
}


def table(name: str, rows: list[dict] | None = None) -> pd.DataFrame:
    return pd.DataFrame(rows or [], columns=COLUMNS[name])


def ts(day: int, hour: int = 10) -> str:
    return f"2025-02-{day:02d} {hour:02d}:00:00"


def world() -> tuple[Source, Loaded, dict]:
    """A tiny consistent world: patients 1 and 2 load; patient 3 is rejected."""
    src = Source(
        patients=pd.DataFrame({"ID": [1, 2, 3]}),
        devices=pd.DataFrame(
            {
                "Patient_ID": [1, 2, 3, 9],
                "Vendor": ["Tenovi", "Omron", "Tenovi", "Tenovi"],
                "Resupply": [0, 0, 0, 1],
            }
        ),
        glucose=pd.DataFrame(
            {
                "SharePoint_ID": [1, 1, 1, 3],
                "Time_Recorded": [ts(1), ts(2), "2024-12-01 09:00:00", ts(3)],
                "Time_Recieved": [ts(1), ts(2), "2024-12-01 09:05:00", ts(3)],
            }
        ),
        bp=pd.DataFrame(
            {"SharePoint_ID": [], "Time_Recorded": [], "Time_Recieved": []}
        ),
        notes=pd.DataFrame(
            {
                "SharePoint_ID": [1, 2],
                "TimeStamp": [ts(5), ts(6)],
                "AZURE_UPN": ["Coach Alpha", "Coach Bravo"],
            }
        ),
        users=pd.DataFrame({"id": ["u1", "u2"]}),
    )
    t = {
        "user": table(
            "user",
            [
                {"user_id": 1, "ms_entra_id": "a", "display_name": "x"},
                {"user_id": 2, "ms_entra_id": "b", "display_name": "y"},
            ],
        ),
        "patient": table(
            "patient",
            [
                {
                    "patient_id": 10,
                    "sharepoint_id": 1,
                    "first_name": "f",
                    "last_name": "l",
                    "date_of_birth": ts(1),
                    "user_id": 1,
                },
                {
                    "patient_id": 11,
                    "sharepoint_id": 2,
                    "first_name": "f",
                    "last_name": "l",
                    "date_of_birth": ts(1),
                    "user_id": 2,
                },
            ],
        ),
        "patient_address": table(
            "patient_address",
            [
                {"patient_address_id": 1, "patient_id": 10},
                {"patient_address_id": 2, "patient_id": 11},
            ],
        ),
        "patient_insurance": table(
            "patient_insurance",
            [
                {"patient_insurance_id": 1, "patient_id": 10},
                {"patient_insurance_id": 2, "patient_id": 11},
            ],
        ),
        "medical_necessity": table(
            "medical_necessity",
            [
                {"medical_necessity_id": 1, "patient_id": 10},
                {"medical_necessity_id": 2, "patient_id": 11},
            ],
        ),
        "patient_status_type": table(
            "patient_status_type", [{"patient_status_type_id": 1}]
        ),
        "patient_status": table(
            "patient_status",
            [
                {"patient_status_id": 1, "patient_id": 10, "patient_status_type_id": 1},
                {"patient_status_id": 2, "patient_id": 11, "patient_status_type_id": 1},
            ],
        ),
        "emergency_contact": table(
            "emergency_contact", [{"emergency_contact_id": 1, "patient_id": 10}]
        ),
        "vendor": table("vendor", [{"vendor_id": 1}]),
        "device": table(
            "device",
            [
                {
                    "device_id": 100,
                    "hardware_uuid": "d1",
                    "patient_id": 10,
                    "vendor_id": 1,
                },
                {
                    "device_id": 101,
                    "hardware_uuid": "d2",
                    "patient_id": 11,
                    "vendor_id": 1,
                },
            ],
        ),
        "glucose_reading": table(
            "glucose_reading",
            [
                {
                    "glucose_reading_id": 1,
                    "device_id": 100,
                    "recorded_datetime": ts(1),
                    "received_datetime": ts(1),
                },
                {
                    "glucose_reading_id": 2,
                    "device_id": 100,
                    "recorded_datetime": ts(2),
                    "received_datetime": ts(2),
                },
            ],
        ),
        "blood_pressure_reading": table("blood_pressure_reading"),
        "note_type": table("note_type", [{"note_type_id": 1, "name": "Follow-Up"}]),
        "patient_note": table(
            "patient_note",
            [
                {
                    "patient_note_id": 1,
                    "patient_id": 10,
                    "note_datetime": ts(5),
                    "note_content": "n",
                    "temp_user": "Coach Alpha",
                    "note_type_id": 1,
                    "user_id": 1,
                },
                {
                    "patient_note_id": 2,
                    "patient_id": 11,
                    "note_datetime": ts(6),
                    "note_content": "n",
                    "temp_user": "Coach Bravo",
                    "note_type_id": 1,
                    "user_id": 2,
                },
            ],
        ),
        "medical_code_type": table(
            "medical_code_type",
            [
                {"med_code_type_id": 1, "name": "99457"},
                {"med_code_type_id": 2, "name": "99454"},
            ],
        ),
        "medical_code": table(
            "medical_code",
            [
                {
                    "med_code_id": 1,
                    "patient_id": 10,
                    "med_code_type_id": 2,
                    "timestamp_applied": ts(2),
                }
            ],
        ),
        "medical_code_device": table("medical_code_device"),
    }
    report = pd.DataFrame(
        {
            "ID": [1],
            "DateOfService": ["2025-02-02"],
            "99202": [0],
            "99453": [0],
            "99454": [1],
            "99457": [0],
            "99458": [0],
        }
    )
    derived = {
        "patient_address": 2,
        "patient_insurance": 2,
        "patient_status": 2,
        "medical_necessity": 2,
        "emergency_contact": 1,
    }
    assert set(t) == set(PRIMARY_KEYS)
    return src, Loaded(t, report), derived


REASONS = {3: ["PHONE_LENGTH"]}


# -- 1. row conservation -------------------------------------------------------


def test_conservation_balances_and_shows_the_equation():
    src, loaded, derived = world()
    r = checks.check_row_conservation(src, loaded, CFG, REASONS, derived)
    assert r.ok
    s = r.details["sources"]
    assert s["patients"] == {
        **s["patients"],
        "source": 3,
        "loaded": 2,
        "unexplained": 0,
    }
    assert s["patients"]["dispositions"] == {"PATIENT_REJECTED": 1}
    assert s["devices"]["source"] == 4 and s["devices"]["loaded"] == 2
    assert s["devices"]["dispositions"] == {
        "EXCLUDED_BY_SOURCE_QUERY": 1,
        "NO_PATIENT_IN_EXPORT": 0,
        "PATIENT_REJECTED": 1,
    }
    assert s["glucose readings"]["dispositions"]["OUTSIDE_EXTRACT_WINDOW"] == 1
    assert s["glucose readings"]["dispositions"]["PATIENT_REJECTED"] == 1
    assert s["glucose readings"]["unexplained"] == 0


def test_conservation_fails_when_a_loaded_row_goes_missing():
    src, loaded, derived = world()
    loaded.tables["glucose_reading"] = loaded.tables["glucose_reading"].iloc[:1]
    r = checks.check_row_conservation(src, loaded, CFG, REASONS, derived)
    assert not r.ok
    assert r.details["sources"]["glucose readings"]["unexplained"] == 1
    assert "glucose readings" in r.summary


def test_conservation_counts_device_fanout_as_a_documented_disposition():
    src, loaded, derived = world()
    t = loaded.tables
    # A second device for patient 1 loads each of their readings twice.
    t["device"] = pd.concat(
        [
            t["device"],
            table(
                "device",
                [
                    {
                        "device_id": 102,
                        "hardware_uuid": "d3",
                        "patient_id": 10,
                        "vendor_id": 1,
                    }
                ],
            ),
        ]
    )
    t["glucose_reading"] = pd.concat(
        [
            t["glucose_reading"],
            t["glucose_reading"].assign(device_id=102, glucose_reading_id=[3, 4]),
        ]
    )
    src.devices = pd.concat(
        [
            src.devices,
            pd.DataFrame({"Patient_ID": [1], "Vendor": ["Omron"], "Resupply": [0]}),
        ]
    )
    r = checks.check_row_conservation(src, loaded, CFG, REASONS, derived)
    assert r.ok
    assert r.details["sources"]["glucose readings"]["loaded_from_fanout"] == 2


def test_conservation_fails_for_a_rejected_patient_who_was_loaded():
    src, loaded, derived = world()
    extra = table(
        "patient",
        [
            {
                "patient_id": 12,
                "sharepoint_id": 3,
                "first_name": "f",
                "last_name": "l",
                "date_of_birth": ts(1),
                "user_id": 1,
            }
        ],
    )
    loaded.tables["patient"] = pd.concat([loaded.tables["patient"], extra])
    r = checks.check_row_conservation(src, loaded, CFG, REASONS, derived)
    assert not r.ok
    assert 3 in r.violations


# -- 2. key integrity -----------------------------------------------------------


def test_keys_pass_on_clean_data_and_fail_on_a_duplicate_natural_key():
    _, loaded, _ = world()
    assert checks.check_key_uniqueness(loaded).ok
    loaded.tables["device"].loc[1, "hardware_uuid"] = "d1"
    r = checks.check_key_uniqueness(loaded)
    assert not r.ok
    assert r.details["rows_involved_by_key"] == {"device.hardware_uuid": 2}
    assert sorted(r.violations) == ["device:100", "device:101"]


def test_foreign_keys_flag_orphans_but_not_unset_references():
    _, loaded, _ = world()
    loaded.tables["patient_note"].loc[0, "note_type_id"] = (
        None  # unset is not an orphan
    )
    assert checks.check_foreign_keys(loaded).ok
    loaded.tables["glucose_reading"].loc[0, "device_id"] = 999
    r = checks.check_foreign_keys(loaded)
    assert not r.ok
    assert r.details["orphans_by_relationship"] == {
        "glucose_reading.device_id -> device": 1
    }
    assert r.violations == ["glucose_reading:1"]


def test_required_fields_flag_empty_values():
    _, loaded, _ = world()
    assert checks.check_required_fields(loaded).ok
    loaded.tables["patient"].loc[0, "last_name"] = None
    r = checks.check_required_fields(loaded)
    assert not r.ok and r.details["empty_by_field"] == {"patient.last_name": 1}


# -- 3. cross-source consistency --------------------------------------------------


def test_a_patient_in_several_sources_resolves_to_one_entity():
    src, loaded, _ = world()
    r = checks.check_cross_source(src, loaded, {3})
    assert r.ok
    assert r.details["patients_in_2plus_sources"] == 3
    assert r.details["excluded_because_rejected"] == 1
    assert r.details["resolving_to_exactly_one"] == 2


def test_cross_source_fails_when_a_patient_loads_twice_or_repeats_in_the_export():
    src, loaded, _ = world()
    dup = loaded.tables["patient"].iloc[[0]].assign(patient_id=99)
    loaded.tables["patient"] = pd.concat([loaded.tables["patient"], dup])
    r = checks.check_cross_source(src, loaded, {3})
    assert not r.ok and r.violations == [1]
    src2, loaded2, _ = world()
    src2.patients = pd.DataFrame({"ID": [1, 1, 2, 3]})
    assert not checks.check_cross_source(src2, loaded2, {3}).ok


# -- 4. rejection accounting -------------------------------------------------------


def test_every_missing_patient_needs_a_reason():
    src, loaded, _ = world()
    ok = checks.check_rejection_accounting(src, loaded, REASONS, agrees=True)
    assert ok.ok and ok.details["by_reason"] == {"PHONE_LENGTH": 1}
    bad = checks.check_rejection_accounting(src, loaded, {}, agrees=True)
    assert not bad.ok and bad.violations == [3]


def test_rejection_accounting_fails_when_reasons_disagree_with_the_pipeline():
    src, loaded, _ = world()
    assert not checks.check_rejection_accounting(src, loaded, REASONS, agrees=False).ok


def test_reason_codes_follow_the_declared_limits():
    normalized = pd.DataFrame(
        {
            "sharepoint_id": [1, 2],
            "phone_number": ["1" * 12, "1" * 10],
            "social_security": ["", ""],
            "temp_state": ["CA", "ATLANTIS"],
            "zipcode": ["12345", "12345"],
            "emergency_phone_number": ["", ""],
            "emergency_phone_number2": ["", ""],
            "medicare_beneficiary_id": ["", ""],
            "primary_payer_id": ["", ""],
            "secondary_payer_id": ["", ""],
        }
    )
    by_index = reasons.rejection_reasons(normalized)
    assert reasons.reasons_by_patient_id(normalized, by_index) == {
        1: ["PHONE_LENGTH"],
        2: ["STATE_LENGTH"],
    }


def test_reason_derivation_agrees_with_the_pipeline_on_generated_data(tmp_path):
    generate(cfg.DEFAULT_SEED, minimum_patients() + 10, tmp_path)
    normalized = reasons.normalize(reasons.read_export(tmp_path / "Patient_Export.csv"))
    by_index = reasons.rejection_reasons(normalized)
    assert by_index  # the generated data includes rejected patients
    assert reasons.agrees_with_pipeline(normalized, by_index)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    expected = {r["id"] for r in manifest["expected"]["rejected_patients"]}
    assert set(reasons.reasons_by_patient_id(normalized, by_index)) == expected


def test_drift_between_declared_limits_and_the_pipeline_is_detected(
    tmp_path, monkeypatch
):
    generate(cfg.DEFAULT_SEED, minimum_patients() + 10, tmp_path)
    normalized = reasons.normalize(reasons.read_export(tmp_path / "Patient_Export.csv"))
    monkeypatch.setattr(
        reasons,
        "LIMITS",
        [(c, n + 3 if c == "phone_number" else n, k) for c, n, k in reasons.LIMITS],
    )
    assert not reasons.agrees_with_pipeline(
        normalized, reasons.rejection_reasons(normalized)
    )


# -- 5. billing lineage -------------------------------------------------------------


def lineage(loaded, src, expected=None, sample=None):
    return checks.check_billing_lineage(src, loaded, CFG, expected, sample)


def test_lineage_traces_a_code_to_its_source_rows():
    src, loaded, _ = world()
    r = lineage(loaded, src, expected=[(1, "99454", ts(2))])
    assert r.ok
    assert r.details["codes_traced"] == 1
    assert r.details["with_backing_rows"] == r.details["backing_rows_in_source"] == 1
    assert r.details["manifest"]["billed"] == 1


def test_lineage_fails_when_a_code_has_no_backing_rows():
    src, loaded, _ = world()
    loaded.tables["glucose_reading"] = loaded.tables["glucose_reading"].iloc[0:0]
    r = lineage(loaded, src)
    assert not r.ok and r.violations == ["1:99454"]


def test_lineage_fails_when_a_backing_row_is_not_in_the_source():
    src, loaded, _ = world()
    src.glucose = src.glucose.iloc[1:]  # the reading that backs the code is gone
    src.glucose = src.glucose[src.glucose["SharePoint_ID"] != 1]
    assert not lineage(loaded, src).ok


def test_lineage_fails_when_the_code_is_not_stamped_at_its_latest_row():
    src, loaded, _ = world()
    loaded.tables["medical_code"].loc[0, "timestamp_applied"] = ts(20)
    assert not lineage(loaded, src).ok


def test_lineage_reports_disagreement_with_the_manifest():
    src, loaded, _ = world()
    r = lineage(loaded, src, expected=[(1, "99454", ts(2)), (2, "99457", ts(6))])
    assert not r.ok
    assert r.details["manifest"]["expected_but_not_billed"] == 1
    assert r.violations == ["2:99457"]


def test_lineage_can_sample():
    src, loaded, _ = world()
    assert lineage(loaded, src, sample=1).details["codes_traced"] == 1


# -- 6. report totals ------------------------------------------------------------------


def test_report_totals_match_the_database():
    _, loaded, _ = world()
    r = checks.check_report_totals(loaded, CFG)
    assert r.ok and r.details["report_rows"] == r.details["database_rows"] == 1


def test_report_totals_fail_when_a_count_is_off_by_one():
    _, loaded, _ = world()
    loaded.report.loc[0, "99454"] = 2
    r = checks.check_report_totals(loaded, CFG)
    assert not r.ok and "99454" in r.summary


def test_report_totals_fail_when_a_row_is_missing():
    _, loaded, _ = world()
    loaded.report = loaded.report.iloc[0:0]
    assert not checks.check_report_totals(loaded, CFG).ok


# -- output ------------------------------------------------------------------------------


def test_output_carries_counts_and_ids_but_no_row_values():
    src, loaded, derived = world()
    loaded.tables["patient"].loc[0, "first_name"] = "SentinelFirstName"
    results = [
        checks.check_row_conservation(src, loaded, CFG, REASONS, derived),
        checks.check_key_uniqueness(loaded),
    ]
    rec = Reconciliation(results, seed=1, faults=[])
    text, payload = render_text(rec), render_json(rec)
    assert "SentinelFirstName" not in text + payload
    assert json.loads(payload)["counts_and_synthetic_ids_only"] is True
