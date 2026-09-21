"""Demo-only database setup.

The repo has no authoritative schema (see CLAUDE.md), so the tables below are
RECONSTRUCTED, NOT AUTHORITATIVE: they are inferred from the columns the pipeline
writes (`create_*_df`, `normalize_*`), the columns the stored procedures read, and
the ERDs in docs/erd. Constraints, foreign keys and column widths are guesses that
are just permissive enough to run the pipeline. The stored procedures themselves
are NOT reconstructed: they are applied verbatim from sql/stored_procedures/.
"""

from pathlib import Path

from tools.synthetic_data import config as cfg

PROCEDURES_DIR = Path(__file__).resolve().parents[2] / "sql" / "stored_procedures"

# Only the procedures the pipeline actually calls. batch_medcode_99454.sql (a combined
# version) and the three query helpers are never invoked by the Python code.
PROCEDURES = [
    "reset_all_billing_tables",
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

GPS_TABLES = [
    """CREATE TABLE [user] (
        user_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name VARCHAR(100), last_name VARCHAR(100), display_name VARCHAR(200),
        email VARCHAR(200), ms_entra_id VARCHAR(100))""",
    """CREATE TABLE patient (
        patient_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name VARCHAR(100), last_name VARCHAR(100), middle_name VARCHAR(100),
        name_suffix VARCHAR(20), full_name VARCHAR(200), nick_name VARCHAR(100),
        date_of_birth DATETIME2, sex VARCHAR(10), email VARCHAR(200),
        phone_number VARCHAR(20), social_security VARCHAR(20), temp_race VARCHAR(50),
        temp_marital_status VARCHAR(50), preferred_language VARCHAR(50),
        weight_lbs INT, height_in INT, sharepoint_id INT,
        temp_user VARCHAR(100), user_id INT NULL)""",
    """CREATE TABLE patient_address (
        patient_address_id INT IDENTITY(1,1) PRIMARY KEY,
        street_address VARCHAR(200), city VARCHAR(100), temp_state VARCHAR(10),
        zipcode VARCHAR(10), patient_id INT)""",
    """CREATE TABLE patient_insurance (
        patient_insurance_id INT IDENTITY(1,1) PRIMARY KEY,
        medicare_beneficiary_id VARCHAR(20), primary_payer_id VARCHAR(50),
        primary_payer_name VARCHAR(100), secondary_payer_id VARCHAR(50),
        secondary_payer_name VARCHAR(100), patient_id INT)""",
    """CREATE TABLE medical_necessity (
        medical_necessity_id INT IDENTITY(1,1) PRIMARY KEY,
        evaluation_datetime DATETIME2, temp_dx_code VARCHAR(20), patient_id INT)""",
    """CREATE TABLE patient_status_type (
        patient_status_type_id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(50))""",
    """CREATE TABLE patient_status (
        patient_status_id INT IDENTITY(1,1) PRIMARY KEY,
        temp_status_type VARCHAR(50), modified_date DATETIME2, temp_user VARCHAR(100),
        patient_id INT, patient_status_type_id INT NULL)""",
    """CREATE TABLE emergency_contact (
        emergency_contact_id INT IDENTITY(1,1) PRIMARY KEY,
        full_name VARCHAR(200), phone_number VARCHAR(20), relationship VARCHAR(50),
        patient_id INT)""",
    """CREATE TABLE vendor (
        vendor_id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(50))""",
    """CREATE TABLE device (
        device_id INT IDENTITY(1,1) PRIMARY KEY,
        hardware_uuid VARCHAR(100), name VARCHAR(200), patient_id INT, vendor_id INT)""",
    """CREATE TABLE glucose_reading (
        glucose_reading_id INT IDENTITY(1,1) PRIMARY KEY,
        temp_device VARCHAR(100), recorded_datetime DATETIME2, received_datetime DATETIME2,
        glucose_reading FLOAT, is_manual BIT, device_id INT)""",
    """CREATE TABLE blood_pressure_reading (
        blood_pressure_reading_id INT IDENTITY(1,1) PRIMARY KEY,
        temp_device VARCHAR(100), recorded_datetime DATETIME2, received_datetime DATETIME2,
        systolic_reading FLOAT, diastolic_reading FLOAT, is_manual BIT,
        device_id INT)""",
    """CREATE TABLE note_type (
        note_type_id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100))""",
    """CREATE TABLE patient_note (
        patient_note_id INT IDENTITY(1,1) PRIMARY KEY,
        note_content NVARCHAR(MAX), note_datetime DATETIME2, temp_user VARCHAR(100),
        temp_note_type VARCHAR(100), call_time_seconds FLOAT, is_manual BIT,
        start_call_datetime DATETIME2, end_call_datetime DATETIME2,
        patient_id INT, note_type_id INT NULL, user_id INT NULL)""",
    """CREATE TABLE medical_code_type (
        med_code_type_id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(20))""",
    """CREATE TABLE medical_code (
        med_code_id INT IDENTITY(1,1) PRIMARY KEY,
        patient_id INT, med_code_type_id INT, timestamp_applied DATETIME2)""",
    """CREATE TABLE medical_code_device (
        medical_code_device_id INT IDENTITY(1,1) PRIMARY KEY,
        med_code_id INT, device_id INT)""",
]

LOOKUP_SEEDS = {
    "vendor": cfg.VENDORS,
    "note_type": cfg.NOTE_TYPES,
    "patient_status_type": cfg.PATIENT_STATUSES,
    "medical_code_type": cfg.BILLING_CODES,
}

# Legacy source tables. Spellings such as SharPoint_ID and Time_Recieved are the
# legacy databases' own (see queries.py), kept as-is.
LEGACY_TABLES = [
    """CREATE TABLE Medical_Notes (
        SharePoint_ID INT, Notes NVARCHAR(MAX), TimeStamp DATETIME2,
        AZURE_UPN VARCHAR(100), Time_Note VARCHAR(100), Note_ID INT)""",
    """CREATE TABLE Time_Log (
        SharPoint_ID INT, Recording_Time VARCHAR(8), AZURE_UPN VARCHAR(100),
        Notes VARCHAR(100), Auto_Time BIT, Start_Time DATETIME2, End_Time DATETIME2,
        Note_ID INT)""",
    """CREATE TABLE Fulfillment_All (
        Vendor VARCHAR(50), Device_ID VARCHAR(100), Device_Name VARCHAR(200),
        Patient_ID INT, Resupply BIT)""",
    """CREATE TABLE Glucose_Readings (
        SharePoint_ID INT, Device_Model VARCHAR(100), Time_Recorded DATETIME2,
        Time_Recieved DATETIME2, BG_Reading FLOAT, Manual_Reading BIT)""",
    """CREATE TABLE Blood_Pressure_Readings (
        SharePoint_ID INT, Device_Model VARCHAR(100), Time_Recorded DATETIME2,
        Time_Recieved DATETIME2, BP_Reading_Systolic FLOAT, BP_Reading_Diastolic FLOAT,
        Manual_Reading BIT)""",
]

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


def seed_statements() -> list[str]:
    """INSERTs for the lookup tables the pipeline resolves names against."""
    out = []
    for table, names in LOOKUP_SEEDS.items():
        values = ", ".join(f"('{n}')" for n in names)
        # Table and value names come from constants in config.py, not from input.
        out.append(f"INSERT INTO {table} (name) VALUES {values}")  # noqa: S608
    return out
