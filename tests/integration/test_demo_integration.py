"""Runs the whole synthetic demo (generate -> load -> pipeline -> billing report)
against the real SQL Server container and checks the result against the manifest."""

import copy

import pytest

from tools.synthetic_data import config as cfg
from tools.synthetic_data.demo import compare, run_demo
from tools.synthetic_data.generator import generate, minimum_patients

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def demo(tmp_path_factory, test_database):
    # `test_database` only matters for its skip-if-no-server behaviour; the demo
    # creates and drops its own databases.
    base = tmp_path_factory.mktemp("demo")
    generate(cfg.DEFAULT_SEED, minimum_patients() + 20, base / "data")
    return run_demo(base / "data", base / "output")


def test_demo_passes_every_check(demo):
    failed = [f"{c.name}: {c.detail}" for c in demo.checks if not c.ok]
    assert not failed, failed
    assert demo.passed
    assert "RESULT: PASS" in demo.summary


def test_every_named_scenario_behaves_as_designed(demo):
    assert demo.scenarios_ok
    assert all(demo.scenarios_ok.values()), [
        k for k, ok in demo.scenarios_ok.items() if not ok
    ]


def test_report_was_written(demo):
    assert demo.report_path is not None and demo.report_path.exists()
    assert demo.actual.report_rows


def test_comparison_actually_detects_differences(demo):
    """Guards against a vacuous PASS: perturb the manifest and the checks must fail."""
    broken = copy.deepcopy(demo.manifest)
    broken["expected"]["loaded_rows"]["patient"] += 1
    broken["expected"]["applied_codes"].pop()
    broken["expected"]["report_rows"].pop()
    checks, _ = compare(broken, demo.actual)
    failing = {c.name for c in checks if not c.ok}
    assert "loaded rows: patient" in failing
    assert "billing codes applied (patient, code, timestamp)" in failing
    assert "billing report rows" in failing
