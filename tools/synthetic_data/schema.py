"""Demo-only database setup.

The GPS tables come from `medicare_rebuild.models` -- the schema of record (see
decision 0015) -- via `GpsBase.metadata.create_all()`, so the demo's GPS database is
authoritative-by-construction, not a separate reconstruction. The legacy source tables
come from `medicare_rebuild.legacy_models` the same way; those ARE a reconstruction
(inferred from the columns `queries.py` reads and the ERDs in docs/erd), since the real
source system is not part of this repository and was never claimed to be authoritative.

No stored procedures are installed: billing is computed in `medicare_rebuild.billing`
(see decision 0014). `sql/stored_procedures/` is kept for reference, but nothing in
this pipeline runs it, demo included.
"""

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from medicare_rebuild.legacy_models import LegacyMetadata
from medicare_rebuild.models import (
    GpsBase,
    MedicalCodeType,
    NoteType,
    PatientStatusType,
    Vendor,
)
from tools.synthetic_data import config as cfg

LOOKUP_SEEDS: dict[type, list[str]] = {
    Vendor: cfg.VENDORS,
    NoteType: cfg.NOTE_TYPES,
    PatientStatusType: cfg.PATIENT_STATUSES,
    MedicalCodeType: cfg.BILLING_CODES,
}

# legacy table -> (generated CSV, datetime columns)
LEGACY_LOADS = {
    "Medical_Notes": ("Medical_Notes.csv", ["TimeStamp"]),
    "Time_Log": ("Time_Log.csv", ["Start_Time", "End_Time"]),
    "Fulfillment_All": ("Fulfillment_All.csv", []),
    "Glucose_Readings": ("Glucose_Readings.csv", ["Time_Recorded", "Time_Recieved"]),
    "Blood_Pressure_Readings": (
        "Blood_Pressure_Readings.csv",
        ["Time_Recorded", "Time_Recieved"],
    ),
}


def create_gps_schema(engine: Engine) -> None:
    GpsBase.metadata.create_all(engine)


def create_legacy_schema(engine: Engine) -> None:
    LegacyMetadata.create_all(engine)


def seed_lookups(session: Session) -> None:
    """Insert the rows the pipeline resolves temp_* columns against."""
    for model, names in LOOKUP_SEEDS.items():
        session.add_all(model(name=n) for n in names)
    session.commit()
