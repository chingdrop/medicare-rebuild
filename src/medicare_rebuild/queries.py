"""SQL for the legacy source databases, as SQLAlchemy Core selects against the table
definitions in legacy_models.py, so DataImporter's extract methods read through the
same schema-of-record path as the GPS load side (see decision 0015).

These are functions, not module-level constants, because the date-range filter has to
be bound to real values per call (the pipeline no longer builds a `?`-placeholder
string and passes `params=(start, end)` separately at execution time — the bound
values are part of the returned Select itself).
"""

from datetime import datetime

from sqlalchemy import Select, select

from medicare_rebuild.legacy_models import (
    blood_pressure_readings,
    fulfillment_all,
    glucose_readings,
    medical_notes,
    time_log,
)


def get_bg_readings_stmt(start: datetime, end: datetime) -> Select:
    t = glucose_readings
    return select(
        t.c.SharePoint_ID,
        t.c.Device_Model,
        t.c.Time_Recorded,
        t.c.Time_Recieved,
        t.c.BG_Reading,
        t.c.Manual_Reading,
    ).where(t.c.Time_Recorded >= start, t.c.Time_Recorded <= end)


def get_bp_readings_stmt(start: datetime, end: datetime) -> Select:
    t = blood_pressure_readings
    return select(
        t.c.SharePoint_ID,
        t.c.Device_Model,
        t.c.Time_Recorded,
        t.c.Time_Recieved,
        t.c.BP_Reading_Systolic,
        t.c.BP_Reading_Diastolic,
        t.c.Manual_Reading,
    ).where(t.c.Time_Recorded >= start, t.c.Time_Recorded <= end)


def get_fulfillment_stmt() -> Select:
    t = fulfillment_all
    return select(t.c.Vendor, t.c.Device_ID, t.c.Device_Name, t.c.Patient_ID).where(
        t.c.Resupply == 0, t.c.Vendor.in_(["Tenovi", "Omron"])
    )


def get_notes_log_stmt(start: datetime, end: datetime) -> Select:
    t = medical_notes
    return select(
        t.c.SharePoint_ID,
        t.c.Notes,
        t.c.TimeStamp,
        t.c.AZURE_UPN,
        t.c.Time_Note,
        t.c.Note_ID,
    ).where(t.c.TimeStamp >= start, t.c.TimeStamp <= end)


def get_time_log_stmt(start: datetime, end: datetime) -> Select:
    t = time_log
    return select(
        t.c.SharPoint_ID,
        t.c.Recording_Time,
        t.c.AZURE_UPN,
        t.c.Notes,
        t.c.Auto_Time,
        t.c.Start_Time,
        t.c.End_Time,
        t.c.Note_ID,
    ).where(t.c.End_Time >= start, t.c.End_Time <= end)
