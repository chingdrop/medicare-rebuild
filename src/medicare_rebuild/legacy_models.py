"""The legacy source tables, as SQLAlchemy Core table definitions.

These are plain `Table` objects, not declarative ORM classes like `models.py`'s GPS
schema. The pipeline only ever reads from them (`SELECT`) — it never inserts, updates,
or tracks their row identity — and, as reconstructed from the queries in `queries.py`
and the columns they return, none of them has a primary key in the real source system.
Declarative ORM classes require a primary key to map identity, so forcing a synthetic
one onto these tables would misrepresent a schema this repo does not own and only reads
from. `select()` against these `Table` objects still gets the type safety and query-
building benefits of SQLAlchemy without that mismatch.

Column names keep the legacy source's own spellings (`SharPoint_ID`, `Time_Recieved`,
...) as-is; they are not this repo's naming choice to fix.
"""

from sqlalchemy import Column, Float, Integer, MetaData, Table
from sqlalchemy.dialects.mssql import BIT, DATETIME2, NVARCHAR, VARCHAR

LegacyMetadata = MetaData()

medical_notes = Table(
    "Medical_Notes",
    LegacyMetadata,
    Column("SharePoint_ID", Integer),
    Column("Notes", NVARCHAR(None)),  # NVARCHAR(MAX)
    Column("TimeStamp", DATETIME2),
    Column("AZURE_UPN", VARCHAR(100)),
    Column("Time_Note", VARCHAR(100)),
    Column("Note_ID", Integer),
)

time_log = Table(
    "Time_Log",
    LegacyMetadata,
    Column("SharPoint_ID", Integer),
    Column("Recording_Time", VARCHAR(8)),
    Column("AZURE_UPN", VARCHAR(100)),
    Column("Notes", VARCHAR(100)),
    Column("Auto_Time", BIT),
    Column("Start_Time", DATETIME2),
    Column("End_Time", DATETIME2),
    Column("Note_ID", Integer),
)

fulfillment_all = Table(
    "Fulfillment_All",
    LegacyMetadata,
    Column("Vendor", VARCHAR(50)),
    Column("Device_ID", VARCHAR(100)),
    Column("Device_Name", VARCHAR(200)),
    Column("Patient_ID", Integer),
    Column("Resupply", BIT),
)

glucose_readings = Table(
    "Glucose_Readings",
    LegacyMetadata,
    Column("SharePoint_ID", Integer),
    Column("Device_Model", VARCHAR(100)),
    Column("Time_Recorded", DATETIME2),
    Column("Time_Recieved", DATETIME2),
    Column("BG_Reading", Float),
    Column("Manual_Reading", BIT),
)

blood_pressure_readings = Table(
    "Blood_Pressure_Readings",
    LegacyMetadata,
    Column("SharePoint_ID", Integer),
    Column("Device_Model", VARCHAR(100)),
    Column("Time_Recorded", DATETIME2),
    Column("Time_Recieved", DATETIME2),
    Column("BP_Reading_Systolic", Float),
    Column("BP_Reading_Diastolic", Float),
    Column("Manual_Reading", BIT),
)
