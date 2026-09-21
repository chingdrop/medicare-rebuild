"""Plain containers for reconciliation inputs and results."""

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd


@dataclass
class Source:
    """The raw source files, as the pipeline's extract stage would read them."""

    patients: pd.DataFrame  # Patient_Export.csv
    devices: pd.DataFrame  # Fulfillment_All
    glucose: pd.DataFrame  # Glucose_Readings
    bp: pd.DataFrame  # Blood_Pressure_Readings
    notes: pd.DataFrame  # Medical_Notes
    users: pd.DataFrame  # users.json


@dataclass
class Loaded:
    """What is in the target database and the billing report after the run."""

    tables: dict[str, pd.DataFrame]
    report: pd.DataFrame


@dataclass
class Settings:
    extract_start: datetime
    extract_end: datetime
    report_start: datetime
    report_end: datetime
    device_vendors: tuple[str, ...] = ("Tenovi", "Omron")


@dataclass
class CheckResult:
    name: str
    ok: bool
    summary: str
    details: dict = field(default_factory=dict)
    # Surrogate or synthetic IDs only, capped; never field values.
    violations: list = field(default_factory=list)
