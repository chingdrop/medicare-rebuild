"""Read the raw source files and the loaded database for reconciliation."""

import json
import warnings
from pathlib import Path

import pandas as pd

from tools.reconcile.checks import PRIMARY_KEYS
from tools.reconcile.model import Loaded, Settings, Source
from tools.synthetic_data import config as cfg
from tools.synthetic_data.generator import FILES

REPORT_NAME = "Billing_Report.xlsx"


def settings() -> Settings:
    """The windows the demo runs with (same values `main()` hardcodes)."""
    return Settings(
        extract_start=cfg.IMPORT_START,
        extract_end=cfg.IMPORT_END,
        report_start=cfg.REPORT_START,
        report_end=cfg.REPORT_END,
    )


def load_source(data_dir: Path) -> Source:
    legacy = data_dir / "legacy"
    users = json.loads((data_dir / FILES["users"]).read_text())["value"]
    return Source(
        patients=pd.read_csv(
            data_dir / FILES["patients"],
            dtype={"Phone Number": "str", "Social Security": "str", "Zip code": "str"},
            parse_dates=["DOB", "On-board Date"],
        ),
        devices=pd.read_csv(legacy / "Fulfillment_All.csv"),
        glucose=pd.read_csv(legacy / "Glucose_Readings.csv"),
        bp=pd.read_csv(legacy / "Blood_Pressure_Readings.csv"),
        notes=pd.read_csv(legacy / "Medical_Notes.csv"),
        users=pd.DataFrame(users),
    )


def load_loaded(output_dir: Path) -> Loaded:
    # Imported here so the pure checks and their unit tests need no database driver.
    from tools.synthetic_data.demo import GPS_DB, _read

    with warnings.catch_warnings():
        # pandas warns about raw pyodbc connections; the demo's reader uses one.
        warnings.simplefilter("ignore", UserWarning)
        tables = {
            name: _read(f"SELECT * FROM [{name}]", GPS_DB)  # noqa: S608
            for name in PRIMARY_KEYS
        }
    report = pd.read_excel(output_dir / REPORT_NAME)
    report.columns = [str(c) for c in report.columns]
    return Loaded(tables, report)


def load_expected_codes(data_dir: Path) -> list[tuple[int, str, str]]:
    manifest = json.loads((data_dir / FILES["manifest"]).read_text())
    return [
        (int(c["id"]), c["code"], c["applied_at"])
        for c in manifest["expected"]["applied_codes"]
    ]
