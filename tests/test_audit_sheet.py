"""The audit sheet is built only from a run's own output files: every figure on it
comes from reconcile.json, checks.json or the generator's manifest."""

import json

from tools.audit_sheet import build


def _source(source, loaded, **dispositions):
    return {
        "source": source,
        "loaded": loaded,
        "loaded_from_fanout": 0,
        "duplicates_merged": 0,
        "dispositions": dispositions,
        "unexplained": source - loaded - sum(dispositions.values()),
    }


def _write_run(tmp_path, *, report_ok=True):
    data, out = tmp_path / "data", tmp_path / "out"
    data.mkdir()
    out.mkdir()
    sources = {
        "patients": _source(10, 9, PATIENT_REJECTED=1)
        | {"rejected_by_reason": {"ZIP_LENGTH": 1}},
        "users": _source(2, 2),
        "devices": _source(5, 4, PATIENT_REJECTED=1),
        "glucose readings": _source(40, 37, NO_MATCHING_DEVICE=3),
        "blood pressure readings": _source(20, 20),
        "patient notes": _source(6, 6),
    }
    by_code = {"99202": 1, "99453": 2, "99454": 2, "99457": 1, "99458": 0}
    checks = [
        {
            "name": "1 row conservation",
            "ok": True,
            "summary": "every source row is accounted for",
            "details": {"sources": sources},
        },
        {
            "name": "5 billing lineage",
            "ok": True,
            "summary": "6 billed codes traced",
            "details": {"codes_in_database": 7, "codes_traced": 6},
        },
        {
            "name": "6 report totals",
            "ok": report_ok,
            "summary": "report equals the database",
            "details": {"report_rows": 4, "report_by_code": by_code},
        },
    ]
    (out / "reconcile.json").write_text(json.dumps({"seed": 42, "checks": checks}))
    (out / "checks.json").write_text(
        json.dumps([{"name": "a", "ok": True}, {"name": "b", "ok": False}])
    )
    (data / "manifest.json").write_text(
        json.dumps(
            {
                "windows": {
                    "extract_start": "2025-01-01 00:00:00",
                    "extract_end": "2025-02-28 00:00:00",
                    "report_start": "2025-02-01 00:00:00",
                    "report_end": "2025-02-28 00:00:00",
                },
                "expected": {"codes_applied": by_code | {"99458": 1}},
            }
        )
    )
    return data, out


def test_sheet_figures_come_from_the_run_files(tmp_path):
    page = build(*_write_run(tmp_path))

    assert "seed <b>42</b>" in page
    assert ">83</div>" in page  # total source rows: 10 + 2 + 5 + 40 + 20 + 6
    assert "0 unexplained" in page
    assert "40</span>" in page and "37</span>" in page  # glucose source, loaded
    assert "3 with no device of the reading's type" in page
    assert "1 rejected by validation" in page and "ZIP code too long" in page
    assert "6<small> / 7</small>" in page  # codes traced / in database
    assert ">1</span><small> / 2</small>" in page  # demo checks passed / total


def test_a_failed_check_is_shown_as_failed(tmp_path):
    page = build(*_write_run(tmp_path, report_ok=False))
    assert "&#10007; FAIL" in page
    assert ">2</span><small> / 3</small>" in page  # reconciliation checks passed
