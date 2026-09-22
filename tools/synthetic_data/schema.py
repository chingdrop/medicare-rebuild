"""Demo-only database setup.

The GPS tables come from `medicare_rebuild.models` -- the schema of record (see
decision 0015) -- via `GpsBase.metadata.create_all()`, so the demo's GPS database is
authoritative-by-construction, not a separate reconstruction. The legacy source tables
come from `medicare_rebuild.legacy_models` the same way; those ARE a reconstruction
(inferred from the columns `queries.py` reads and the ERDs in docs/erd), since the real
source system is not part of this repository and was never claimed to be authoritative.

The billing stored procedures are NOT reconstructed: they are applied verbatim from
sql/stored_procedures/.
"""

from pathlib import Path

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

PROCEDURES_DIR = Path(__file__).resolve().parents[2] / "sql" / "stored_procedures"

# Only the billing procedures the pipeline still calls (see decision 0014). Resetting
# all GPS tables is now the Python function medicare_rebuild.models.reset_all_data.
# batch_medcode_99454.sql (an older combined version) and the three query helpers were
# never invoked by the Python code even before that.
PROCEDURES = [
    "reset_medical_code_tables",
    "batch_medcode_99202",
    "batch_medcode_99453_bg",
    "batch_medcode_99453_bp",
    "batch_medcode_99454_bg",
    "batch_medcode_99454_bp",
    "batch_medcode_99457",
    "batch_medcode_99458",
    "create_billing_report",
]

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


def procedure_sql(name: str) -> str:
    # The repo's .sql files start with a UTF-8 BOM.
    return (PROCEDURES_DIR / f"{name}.sql").read_text(encoding="utf-8-sig")


def create_gps_schema(engine: Engine) -> None:
    GpsBase.metadata.create_all(engine)


def create_legacy_schema(engine: Engine) -> None:
    LegacyMetadata.create_all(engine)


def seed_lookups(session: Session) -> None:
    """Insert the rows the pipeline resolves temp_* columns against."""
    for model, names in LOOKUP_SEEDS.items():
        session.add_all(model(name=n) for n in names)
    session.commit()
