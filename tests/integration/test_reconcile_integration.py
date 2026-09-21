"""Runs the demo, then reconciliation, against the real SQL Server container: a clean
run must pass every check, and each injected fault must trip its specific check."""

import re

import pandas as pd
import pytest

from tools.reconcile.__main__ import main as reconcile_main
from tools.reconcile.run import run
from tools.synthetic_data import config as cfg
from tools.synthetic_data.demo import run_demo
from tools.synthetic_data.faults import EXPECTED_CHECK, FAULTS
from tools.synthetic_data.generator import generate, minimum_patients

pytestmark = pytest.mark.integration

PATIENTS = minimum_patients() + 20
ALL_CHECKS = {
    "1 row conservation",
    "2a key uniqueness",
    "2b foreign keys",
    "2c required fields",
    "3 cross-source consistency",
    "4 rejection accounting",
    "5 billing lineage",
    "6 report totals",
}


def demo_and_reconcile(base, faults=None):
    data, out = base / "data", base / "out"
    generate(cfg.DEFAULT_SEED, PATIENTS, data, faults)
    demo = run_demo(data, out)
    return data, out, demo, run(data, out)


@pytest.fixture(scope="module")
def clean(tmp_path_factory, test_database):
    # `test_database` only provides the skip-if-no-server behaviour.
    return demo_and_reconcile(tmp_path_factory.mktemp("clean"))


def test_a_clean_run_passes_every_check(clean):
    _, _, demo, rec = clean
    assert demo.passed
    assert {r.name for r in rec.results} == ALL_CHECKS
    assert rec.passed, [(r.name, r.summary) for r in rec.results if not r.ok]


def test_every_source_row_is_accounted_for(clean):
    _, _, _, rec = clean
    conservation = next(r for r in rec.results if r.name.startswith("1 "))
    sources = conservation.details["sources"]
    assert {s["unexplained"] for s in sources.values()} == {0}
    assert sources["patients"]["dispositions"]["PATIENT_REJECTED"] > 0
    assert sources["patients"]["rejected_by_reason"]
    assert sources["glucose readings"]["loaded_from_fanout"] > 0  # multi-device patient


def test_reconciliation_is_reproducible(clean):
    data, out, _, _ = clean
    first = ((out / "reconcile.txt").read_text(), (out / "reconcile.json").read_text())
    run(data, out)
    assert first == (
        (out / "reconcile.txt").read_text(),
        (out / "reconcile.json").read_text(),
    )


def test_reports_contain_no_row_level_values(clean):
    data, out, _, _ = clean
    export = pd.read_csv(data / "Patient_Export.csv", dtype=str)
    text = (out / "reconcile.txt").read_text() + (out / "reconcile.json").read_text()
    for column in (
        "Email",
        "Phone Number",
        "Mailing Address",
        "DOB",
        "Medicare ID number",
    ):
        for value in export[column].dropna().unique():
            assert value not in text, column
    for last_name in export["Last Name"].dropna().unique():
        assert not re.search(rf"\b{re.escape(last_name)}\b", text)


def test_the_command_line_exits_zero_on_a_clean_run(clean):
    data, out, _, _ = clean
    assert reconcile_main(["--data-dir", str(data), "--output-dir", str(out)]) == 0


@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_each_injected_fault_trips_its_specific_check(tmp_path, test_database, fault):
    data, out, demo, rec = demo_and_reconcile(tmp_path, [fault])
    assert demo.passed  # the fault is applied after the demo's own checks
    assert not rec.passed
    assert rec.failing() == {EXPECTED_CHECK[fault]}
    assert reconcile_main(["--data-dir", str(data), "--output-dir", str(out)]) == 1
