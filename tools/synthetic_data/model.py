"""Plain data containers shared by the scenario builders and the generator."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Reading:
    kind: str  # "bg" or "bp"
    recorded: datetime
    received: datetime
    values: tuple[float | None, ...]  # (glucose,) or (systolic, diastolic)
    manual: bool | None = False


@dataclass
class Device:
    vendor: str
    hardware_id: str
    name: str
    resupply: bool = False


@dataclass
class Note:
    timestamp: datetime  # Medical_Notes.TimeStamp and Time_Log.End_Time
    upn: str
    body: str
    note_type: str
    seconds: int | None  # None -> no Time_Log row
    auto_time: bool = True
    same_as: "Note | None" = (
        None  # a duplicated source row: same Note_ID, no second Time_Log row
    )


@dataclass
class ExpectedCode:
    """A billing code the demo must end up with, stated by construction."""

    code: str
    applied_at: datetime


@dataclass
class Plan:
    """Everything one scenario contributes to the source data, plus what to expect."""

    scenario: str
    patient_id: int
    in_export: bool = True
    row: dict | None = None
    devices: list[Device] = field(default_factory=list)
    readings: list[Reading] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)
    expected_codes: list[ExpectedCode] = field(default_factory=list)
    rejected_reason: str | None = None
    dx_rows: int = 1
    emergency_contacts: int = 1
    duplicate_readings: int = 0
    duplicate_notes: int = 0
    flags: list[str] = field(default_factory=list)
