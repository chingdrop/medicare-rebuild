"""Unit tests for tools/synthetic_data (the demo's data generator). No database needed."""

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from medicare_rebuild.utils.dataframe_utils import (
    check_patient_db_constraints,
    normalize_patients,
)
from tools.synthetic_data import config as cfg
from tools.synthetic_data.generator import (
    FILES,
    generate,
    minimum_patients,
)
from tools.synthetic_data.safety import scan_dir, scan_text
from tools.synthetic_data.scenarios import SCENARIOS


def _hashes(directory: Path) -> dict[str, str]:
    return {
        str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }


@pytest.fixture(scope="module")
def generated(tmp_path_factory) -> tuple[Path, dict]:
    out = tmp_path_factory.mktemp("synthetic")
    manifest = generate(cfg.DEFAULT_SEED, cfg.DEFAULT_PATIENTS, out)
    return out, manifest


def test_same_seed_produces_identical_files(generated, tmp_path):
    out, _ = generated
    generate(cfg.DEFAULT_SEED, cfg.DEFAULT_PATIENTS, tmp_path)
    assert _hashes(out) == _hashes(tmp_path)


def test_different_seed_changes_the_data(generated, tmp_path):
    out, _ = generated
    generate(cfg.DEFAULT_SEED + 1, cfg.DEFAULT_PATIENTS, tmp_path)
    assert _hashes(out)[FILES["patients"]] != _hashes(tmp_path)[FILES["patients"]]


def test_patient_count_is_respected(tmp_path):
    generate(1, 80, tmp_path)
    assert len(pd.read_csv(tmp_path / FILES["patients"])) == 80


def test_too_few_patients_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="at least"):
        generate(1, minimum_patients() - 1, tmp_path)


def test_no_forbidden_patterns_in_output(generated):
    out, _ = generated
    assert scan_dir(out) == []


@pytest.mark.parametrize(
    "text, kind",
    [
        ("ssn 123-45-6789", "ssn_outside_9xx"),
        ("ssn 987-65-4321", None),  # 9xx area numbers are never issued
        ("mbi 1EG4-TE5-MK73", "mbi_shape"),
        ("mbi SYN-000123", None),
        ("call (415) 555-0142", None),
        ("call (415) 867-5309", "phone_not_555_01xx"),
        ("call 415-555-0242", "phone_not_555_01xx"),
        ("mail a@example.com", None),
        ("mail a@company.org", "email_not_example_com"),
    ],
)
def test_safety_scanner_flags_only_real_looking_values(text, kind):
    assert [k for _, k in scan_text(text)] == ([kind] if kind else [])


def test_synthetic_markers_are_visible(generated):
    out, _ = generated
    patients = pd.read_csv(out / FILES["patients"], dtype=str)
    assert patients["Nickname"].str.startswith(cfg.SYNTHETIC_MARKER).all()
    assert patients["Medicare ID number"].str.startswith("SYN-").all()
    emails = patients["Email"].dropna().str.strip().str.lower()
    with_at = emails[emails.str.contains("@")]
    assert with_at.str.endswith("@example.com").all()
    assert (
        len(patients) - len(with_at) == 2
    )  # the two deliberate missing/malformed cases
    notes = pd.read_csv(out / FILES["notes"])
    assert notes["Notes"].str.contains(cfg.SYNTHETIC_MARKER).all()
    # Optional identifiers the demo does not need are left blank rather than invented.
    assert patients["Social Security"].isna().all()


def test_every_scenario_is_present_in_the_manifest_and_data(generated):
    out, manifest = generated
    keys = [s["key"] for s in manifest["scenarios"]]
    assert keys == [s.key for s in SCENARIOS]
    assert len(set(keys)) == len(keys)

    export_ids = set(pd.read_csv(out / FILES["patients"])["ID"])
    source_ids: set[int] = set()
    for name in ("notes", "devices", "glucose", "bp"):
        col = "SharePoint_ID" if name != "devices" else "Patient_ID"
        source_ids |= set(pd.read_csv(out / FILES[name])[col])

    for s in manifest["scenarios"]:
        pid = s["patient_id"]
        if s["in_export"]:
            assert pid in export_ids, s["key"]
        else:
            assert pid not in export_ids and pid in source_ids, s["key"]
        assert set(s["flags"]) <= {
            "reading_fanout",
            "report_end_date_edge",
            "duplicate_rows_loaded_as_is",
            "call_time_double_counted",
        }


def test_every_billing_code_and_edge_case_is_covered(generated):
    _, manifest = generated
    coded = {c["code"] for s in manifest["scenarios"] for c in s["expected_codes"]}
    assert coded == set(cfg.BILLING_CODES)
    rules = " ".join(s["rule"] for s in manifest["scenarios"])
    for code in cfg.BILLING_CODES:
        assert code in rules
    flags = {f for s in manifest["scenarios"] for f in s["flags"]}
    assert {"reading_fanout", "report_end_date_edge"} <= flags
    edges = " ".join(s["edge"] for s in manifest["scenarios"])
    for phrase in (
        "just below",
        "just above",
        "window edge",
        "duplicate source rows",
        "out-of-order",
        "missing fields",
        "patient rejected",
    ):
        assert phrase in edges, phrase
    # A coded-but-unreported case exists, so the report check is meaningful.
    assert any(
        not c["in_report"] for s in manifest["scenarios"] for c in s["expected_codes"]
    )


def test_manifest_rejections_match_the_real_normalisation(generated):
    """The manifest's rejected patients must be exactly the rows the pipeline's own
    normalise + constraint-check functions drop (no database involved)."""
    out, manifest = generated
    df = pd.read_csv(
        out / FILES["patients"],
        dtype={"Phone Number": "str", "Social Security": "str", "Zip code": "str"},
        parse_dates=["DOB", "On-board Date"],
    )
    kept = check_patient_db_constraints(normalize_patients(df))
    dropped = set(df["ID"]) - set(kept["sharepoint_id"])
    assert dropped == {r["id"] for r in manifest["expected"]["rejected_patients"]}


def test_manifest_is_valid_json_with_expected_sections(generated):
    out, _ = generated
    manifest = json.loads((out / FILES["manifest"]).read_text())
    assert {"generator", "windows", "scenarios", "expected"} <= manifest.keys()
    assert {
        "source_rows",
        "loaded_rows",
        "excluded_rows",
        "rejected_patients",
        "applied_codes",
        "report_rows",
    } <= manifest["expected"].keys()
