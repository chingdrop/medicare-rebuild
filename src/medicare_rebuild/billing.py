"""Billing computation, ported from sql/stored_procedures/batch_medcode_*.sql and
create_billing_report.sql to pandas and the SQLAlchemy ORM (decision 0014). Every
documented rule in docs/billing-rules.md is preserved exactly -- this is a technology
migration, not a rules rewrite. The stored procedures stay in sql/stored_procedures/
for reference; the pipeline no longer calls them.

Each `apply_*` function is a faithful port of one stored procedure (or, for 99453 and
99454, the pair of procedures that run against glucose then blood pressure readings)
and does its own session I/O: query, compute, insert, commit -- matching the
original's one-stored-procedure-per-EXEC-call granularity, including committing after
each one so a later rule's "already has a code" check sees what an earlier one just
inserted. The pandas logic each one depends on is factored into small, pure helpers
(`_since`, `_qualifying_by_minutes`, `_qualifying_by_reading_days`,
`_qualifying_99458`, `_in_report_window`) that take plain DataFrames and return plain
DataFrames, with no session involved, so the boundary cases documented in
docs/billing-rules.md can be tested directly against hand-built data.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session

from medicare_rebuild.models import (
    BloodPressureReading,
    Device,
    GlucoseReading,
    MedicalCode,
    MedicalCodeDevice,
    MedicalCodeType,
    MedicalNecessity,
    NoteType,
    Patient,
    PatientAddress,
    PatientInsurance,
    PatientNote,
)

BILLING_CODES = ["99202", "99453", "99454", "99457", "99458"]

REPORT_COLUMNS = [
    "DateOfService",
    "ID",
    "FirstName",
    "MiddleName",
    "LastName",
    "Suffix",
    "DOB",
    "PhoneNumber",
    "Address",
    "City",
    "State",
    "Zipcode",
    "Gender",
    "MedicareNumber",
    "PrimaryPayer",
    "PrimaryPayerID",
    "SecondaryPayer",
    "SecondaryPayerID",
    "DXCodes",
    *BILLING_CODES,
]


# -- pure pandas helpers: no session, hand-buildable in a test -------------------------


def _since(df: pd.DataFrame, col: str, start: pd.Timestamp) -> pd.DataFrame:
    """Rows on or after `start`. Used for every rolling window (99454's 30 days,
    99457/99458's 1 month) -- a plain `>=`, same as the stored procedures'
    `col >= DATEADD(...)`."""
    return df[df[col] >= start]


def _qualifying_by_minutes(
    notes: pd.DataFrame,
    excluded_patients: set,
    min_minutes: float,
    max_minutes: float | None = None,
    floor_seconds: bool = False,
) -> pd.DataFrame:
    """Shared core of 99202 and 99457: group notes (patient_id, note_datetime,
    call_time_seconds) by patient, sum call time, keep patients whose total falls in
    [min_minutes, max_minutes) -- an open upper bound if given, none otherwise. 99202
    floors the summed seconds before dividing by 60; 99457 does not -- a real
    difference in the original SQL (see docs/billing-rules.md), preserved here via
    `floor_seconds`. Returns patient_id, timestamp_applied (the latest note)."""
    notes = notes[~notes["patient_id"].isin(excluded_patients)]
    if notes.empty:
        return pd.DataFrame(columns=["patient_id", "timestamp_applied"])
    grouped = notes.groupby("patient_id", as_index=False).agg(
        total_seconds=("call_time_seconds", "sum"),
        timestamp_applied=("note_datetime", "max"),
    )
    total = (
        np.floor(grouped["total_seconds"])
        if floor_seconds
        else grouped["total_seconds"]
    )
    minutes = total / 60
    mask = minutes >= min_minutes
    if max_minutes is not None:
        mask &= minutes < max_minutes
    return grouped.loc[mask, ["patient_id", "timestamp_applied"]].reset_index(drop=True)


def _qualifying_by_reading_days(
    readings: pd.DataFrame, exclude: pd.Series | None = None, min_days: int = 16
) -> pd.DataFrame:
    """Shared core of 99453 and 99454: group readings (patient_id, device_id,
    received_datetime) by patient -- combining every device the patient has, same as
    the stored procedures' GROUP BY d.patient_id -- and keep patients with readings on
    at least `min_days` distinct received dates. `exclude`, if given, is a boolean mask
    aligned to `readings` marking rows to drop before grouping (99453 excludes by
    device, 99454 by patient; see the two `apply_*` functions below). Returns
    patient_id, timestamp_applied (the latest reading)."""
    if exclude is not None:
        readings = readings[~exclude]
    if readings.empty:
        return pd.DataFrame(columns=["patient_id", "timestamp_applied"])
    grouped = (
        readings.assign(received_date=readings["received_datetime"].dt.date)
        .groupby("patient_id", as_index=False)
        .agg(
            distinct_days=("received_date", "nunique"),
            timestamp_applied=("received_datetime", "max"),
        )
    )
    return grouped.loc[
        grouped["distinct_days"] >= min_days, ["patient_id", "timestamp_applied"]
    ].reset_index(drop=True)


def _qualifying_99458(
    windowed_notes: pd.DataFrame,
    windowed_codes: pd.DataFrame,
    all_notes: pd.DataFrame,
) -> pd.DataFrame:
    """Pure core of batch_medcode_99458.sql. `windowed_notes`: patient_id,
    call_time_seconds for notes in the last month. `windowed_codes`: patient_id, name
    for medical_code rows stamped in the last month, of any type -- that is what
    "already has a code this month" means here, not specifically a 99457. `all_notes`:
    patient_id, note_datetime for every note, unwindowed -- the new code is stamped at
    the patient's latest note overall, not just the last month's (confirmed from the
    stored procedure's outer, unwindowed join to patient_note; see
    docs/billing-rules.md). Returns one row per code to insert, so a qualifying patient
    can appear more than once, up to 3 times (blocks are capped at 4 and at least one
    existing code is required)."""
    if windowed_notes.empty or windowed_codes.empty:
        return pd.DataFrame(columns=["patient_id", "timestamp_applied"])

    blocks = (
        np.floor(windowed_notes.groupby("patient_id")["call_time_seconds"].sum() / 1200)
        .fillna(0)
        .clip(upper=4)
    )
    code_counts = (
        windowed_codes.assign(is_99458=windowed_codes["name"] == "99458")
        .groupby("patient_id")["is_99458"]
        .sum()
    )
    eligible = blocks.index.intersection(windowed_codes["patient_id"].unique())
    if eligible.empty:
        return pd.DataFrame(columns=["patient_id", "timestamp_applied"])

    latest_note = (
        all_notes[all_notes["patient_id"].isin(eligible)]
        .groupby("patient_id")["note_datetime"]
        .max()
    )

    patient_ids: list[int] = []
    for pid in eligible:
        rpm_blocks = blocks.get(pid, 0)
        existing = code_counts.get(pid, 0)
        if (rpm_blocks - existing) > 1:
            patient_ids.extend([pid] * int(rpm_blocks - existing - 1))
    if not patient_ids:
        return pd.DataFrame(columns=["patient_id", "timestamp_applied"])

    out = pd.DataFrame({"patient_id": patient_ids})
    out["timestamp_applied"] = out["patient_id"].map(latest_note)
    return out


def _in_report_window(
    codes: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    """timestamp_applied >= start and <= end. `start`/`end` must already be
    midnight-normalized: the stored procedure's @start_date/@end_date are SQL `date`
    parameters compared against a `datetime2` column, which SQL Server implicitly casts
    to midnight of that date -- so a code timestamped after midnight on `end` is
    excluded even though its calendar date is the report's last day (see
    docs/billing-rules.md's midnight-truncation note). Callers normalize; this function
    does not, so a test can pin the boundary precisely."""
    return codes[
        (codes["timestamp_applied"] >= start) & (codes["timestamp_applied"] <= end)
    ]


# -- session I/O: query, compute with the helpers above, insert, commit ----------------


def _code_type_id(session: Session, name: str) -> int:
    return session.execute(
        select(MedicalCodeType.med_code_type_id).where(MedicalCodeType.name == name)
    ).scalar_one()


def _notes(session: Session, note_type_name: str | None = None) -> pd.DataFrame:
    stmt = select(
        PatientNote.patient_id, PatientNote.note_datetime, PatientNote.call_time_seconds
    )
    if note_type_name is not None:
        stmt = stmt.join(
            NoteType, PatientNote.note_type_id == NoteType.note_type_id
        ).where(NoteType.name == note_type_name)
    rows = session.execute(stmt).all()
    df = pd.DataFrame(
        rows, columns=["patient_id", "note_datetime", "call_time_seconds"]
    )
    df["note_datetime"] = pd.to_datetime(df["note_datetime"])
    df["call_time_seconds"] = pd.to_numeric(df["call_time_seconds"])
    return df


def _device_readings(
    session: Session, reading_model: type[GlucoseReading] | type[BloodPressureReading]
) -> pd.DataFrame:
    stmt = select(
        Device.patient_id, Device.device_id, reading_model.received_datetime
    ).join(reading_model, Device.device_id == reading_model.device_id)
    rows = session.execute(stmt).all()
    df = pd.DataFrame(rows, columns=["patient_id", "device_id", "received_datetime"])
    df["received_datetime"] = pd.to_datetime(df["received_datetime"])
    return df


def _patients_with_any_code(session: Session, type_names: list[str]) -> set[int]:
    rows = session.execute(
        select(MedicalCode.patient_id)
        .join(
            MedicalCodeType,
            MedicalCode.med_code_type_id == MedicalCodeType.med_code_type_id,
        )
        .where(MedicalCodeType.name.in_(type_names))
    ).all()
    return {pid for (pid,) in rows}


def _excluded_patients(
    session: Session, code_type_id: int, window_start: pd.Timestamp
) -> set[int]:
    rows = session.execute(
        select(MedicalCode.patient_id)
        .where(MedicalCode.med_code_type_id == code_type_id)
        .where(MedicalCode.timestamp_applied >= window_start)
    ).all()
    return {pid for (pid,) in rows}


def _linked_devices(session: Session, code_type_id: int) -> pd.DataFrame:
    rows = session.execute(
        select(MedicalCode.patient_id, MedicalCodeDevice.device_id)
        .join(
            MedicalCodeDevice, MedicalCode.med_code_id == MedicalCodeDevice.med_code_id
        )
        .where(MedicalCode.med_code_type_id == code_type_id)
    ).all()
    return pd.DataFrame(rows, columns=["patient_id", "device_id"])


def _insert_codes(
    session: Session, qualifying: pd.DataFrame, code_type_id: int
) -> list[MedicalCode]:
    if qualifying.empty:
        return []
    codes = [
        MedicalCode(
            patient_id=int(row.patient_id),  # type: ignore[arg-type]
            med_code_type_id=code_type_id,
            timestamp_applied=row.timestamp_applied,
        )
        for row in qualifying.itertuples()
    ]
    session.add_all(codes)
    session.commit()
    return codes


def apply_99202(session: Session) -> None:
    """Port of batch_medcode_99202.sql: 15 to under 30 minutes of Initial Evaluation
    call time, no window, excluded if the patient already has 99202-99205."""
    code_type_id = _code_type_id(session, "99202")
    excluded = _patients_with_any_code(session, ["99202", "99203", "99204", "99205"])
    notes = _notes(session, note_type_name="Initial Evaluation")
    qualifying = _qualifying_by_minutes(
        notes, excluded, min_minutes=15, max_minutes=30, floor_seconds=True
    )
    _insert_codes(session, qualifying, code_type_id)


def _apply_99453_pass(
    session: Session, reading_model: type[GlucoseReading] | type[BloodPressureReading]
) -> None:
    code_type_id = _code_type_id(session, "99453")
    readings = _device_readings(session, reading_model)
    if readings.empty:
        return
    linked = _linked_devices(session, code_type_id)
    exclude = None
    if not linked.empty:
        linked_pairs = set(map(tuple, linked.to_numpy()))
        exclude = (
            readings[["patient_id", "device_id"]]
            .apply(tuple, axis=1)
            .isin(linked_pairs)
        )
    qualifying = _qualifying_by_reading_days(readings, exclude)
    codes = _insert_codes(session, qualifying, code_type_id)
    if not codes:
        return
    # Link every device the patient has to the new code, not just the one whose
    # readings triggered it -- matching the stored procedure's OUTPUT-then-join-by-
    # patient step. This is what lets the blood-pressure pass see the device already
    # linked and skip it (see docs/billing-rules.md).
    codes_df = pd.DataFrame(
        {
            "patient_id": [c.patient_id for c in codes],
            "med_code_id": [c.med_code_id for c in codes],
        }
    )
    devices = pd.DataFrame(
        session.execute(select(Device.patient_id, Device.device_id)).all(),
        columns=["patient_id", "device_id"],
    )
    links = codes_df.merge(devices, on="patient_id")
    session.add_all(
        MedicalCodeDevice(
            med_code_id=int(row.med_code_id),  # type: ignore[arg-type]
            device_id=int(row.device_id),  # type: ignore[arg-type]
        )
        for row in links.itertuples()
    )
    session.commit()


def apply_99453(session: Session) -> None:
    """Port of batch_medcode_99453_bg.sql then batch_medcode_99453_bp.sql: 16+
    distinct reading days, no window, one code per patient linked to every device they
    have. Glucose runs first, so a patient with both device types is already linked by
    the time the blood-pressure pass checks for an existing link."""
    _apply_99453_pass(session, GlucoseReading)
    _apply_99453_pass(session, BloodPressureReading)


def _apply_99454_pass(
    session: Session,
    reading_model: type[GlucoseReading] | type[BloodPressureReading],
    today_date: datetime,
) -> None:
    code_type_id = _code_type_id(session, "99454")
    window_start = pd.Timestamp(today_date).normalize() - pd.Timedelta(days=30)
    readings = _since(
        _device_readings(session, reading_model), "received_datetime", window_start
    )
    if readings.empty:
        return
    excluded = _excluded_patients(session, code_type_id, window_start)
    exclude = readings["patient_id"].isin(excluded) if excluded else None
    qualifying = _qualifying_by_reading_days(readings, exclude)
    _insert_codes(session, qualifying, code_type_id)


def apply_99454(session: Session, today_date: datetime) -> None:
    """Port of batch_medcode_99454_bg.sql then batch_medcode_99454_bp.sql: 16+
    distinct reading days in the last 30 days, excluded if a 99454 is already stamped
    in that window."""
    _apply_99454_pass(session, GlucoseReading, today_date)
    _apply_99454_pass(session, BloodPressureReading, today_date)


def apply_99457(session: Session, today_date: datetime) -> None:
    """Port of batch_medcode_99457.sql: 20+ minutes of call time, of any note type or
    author, in the last month, excluded if a 99457 is already stamped in that
    window."""
    code_type_id = _code_type_id(session, "99457")
    window_start = pd.Timestamp(today_date).normalize() - pd.DateOffset(months=1)
    excluded = _excluded_patients(session, code_type_id, window_start)
    notes = _since(_notes(session), "note_datetime", window_start)
    qualifying = _qualifying_by_minutes(notes, excluded, min_minutes=20)
    _insert_codes(session, qualifying, code_type_id)


def apply_99458(session: Session, today_date: datetime) -> None:
    """Port of batch_medcode_99458.sql: up to 3 additional 20-minute blocks of call
    time in the last month, for a patient who already has any code stamped in that
    window."""
    code_type_id = _code_type_id(session, "99458")
    window_start = pd.Timestamp(today_date).normalize() - pd.DateOffset(months=1)
    all_notes = _notes(session)
    windowed_notes = _since(all_notes, "note_datetime", window_start)[
        ["patient_id", "call_time_seconds"]
    ]
    windowed_codes = pd.DataFrame(
        session.execute(
            select(MedicalCode.patient_id, MedicalCodeType.name)
            .join(
                MedicalCodeType,
                MedicalCode.med_code_type_id == MedicalCodeType.med_code_type_id,
            )
            .where(MedicalCode.timestamp_applied >= window_start)
        ).all(),
        columns=["patient_id", "name"],
    )
    qualifying = _qualifying_99458(
        windowed_notes, windowed_codes, all_notes[["patient_id", "note_datetime"]]
    )
    _insert_codes(session, qualifying, code_type_id)


def clear_medical_codes(session: Session) -> None:
    """Port of reset_medical_code_tables.sql: clears every previously computed code
    before a fresh billing run (children before parents, then each reseeded), called
    once per create_billing_report() run. Separate from models.reset_all_data(), which
    clears the whole GPS database once per pipeline run."""
    session.execute(delete(MedicalCodeDevice))
    session.execute(text("DBCC CHECKIDENT ('medical_code_device', RESEED, 0)"))
    session.execute(delete(MedicalCode))
    session.execute(text("DBCC CHECKIDENT ('medical_code', RESEED, 0)"))
    session.commit()


def run_billing(session: Session, today_date: datetime) -> None:
    """Runs every rule in the fixed order create_billing_report() used to call the
    stored procedures in (see docs/billing-rules.md: How the rules interact)."""
    clear_medical_codes(session)
    apply_99202(session)
    apply_99453(session)
    apply_99454(session, today_date)
    apply_99457(session, today_date)
    apply_99458(session, today_date)


def build_billing_report(
    session: Session, start_date: datetime, end_date: datetime
) -> pd.DataFrame:
    """Port of create_billing_report.sql: one row per patient per date of service,
    with a count per code type, joined to address/insurance/diagnosis data. A patient
    missing an address, insurance, or every diagnosis-code row is dropped from the
    report even if they have codes, matching the original's inner joins."""
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()

    codes = pd.DataFrame(
        session.execute(
            select(
                MedicalCode.patient_id,
                MedicalCode.timestamp_applied,
                MedicalCodeType.name,
            ).join(
                MedicalCodeType,
                MedicalCode.med_code_type_id == MedicalCodeType.med_code_type_id,
            )
        ).all(),
        columns=["patient_id", "timestamp_applied", "name"],
    )
    codes["timestamp_applied"] = pd.to_datetime(codes["timestamp_applied"])
    codes = _in_report_window(codes, start, end)
    if codes.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    daily = (
        codes.assign(date_of_service=codes["timestamp_applied"].dt.date)
        .groupby(["patient_id", "date_of_service", "name"])
        .size()
        .unstack("name", fill_value=0)
        .reindex(columns=BILLING_CODES, fill_value=0)
        .reset_index()
    )

    dx_rows = session.execute(
        select(MedicalNecessity.patient_id, MedicalNecessity.temp_dx_code).order_by(
            MedicalNecessity.medical_necessity_id
        )
    ).all()
    dx_codes = (
        pd.DataFrame(dx_rows, columns=["patient_id", "temp_dx_code"])
        .groupby("patient_id")["temp_dx_code"]
        .apply(", ".join)
    )

    patients = pd.DataFrame(
        session.execute(
            select(
                Patient.patient_id,
                Patient.sharepoint_id,
                Patient.first_name,
                Patient.middle_name,
                Patient.last_name,
                Patient.name_suffix,
                Patient.date_of_birth,
                Patient.phone_number,
                Patient.sex,
            )
        ).all(),
        columns=[
            "patient_id",
            "sharepoint_id",
            "first_name",
            "middle_name",
            "last_name",
            "name_suffix",
            "date_of_birth",
            "phone_number",
            "sex",
        ],
    )
    addresses = pd.DataFrame(
        session.execute(
            select(
                PatientAddress.patient_id,
                PatientAddress.street_address,
                PatientAddress.city,
                PatientAddress.temp_state,
                PatientAddress.zipcode,
            )
        ).all(),
        columns=["patient_id", "street_address", "city", "temp_state", "zipcode"],
    )
    insurance = pd.DataFrame(
        session.execute(
            select(
                PatientInsurance.patient_id,
                PatientInsurance.medicare_beneficiary_id,
                PatientInsurance.primary_payer_name,
                PatientInsurance.primary_payer_id,
                PatientInsurance.secondary_payer_name,
                PatientInsurance.secondary_payer_id,
            )
        ).all(),
        columns=[
            "patient_id",
            "medicare_beneficiary_id",
            "primary_payer_name",
            "primary_payer_id",
            "secondary_payer_name",
            "secondary_payer_id",
        ],
    )

    report = daily.merge(patients, on="patient_id", how="inner")
    report = report.merge(addresses, on="patient_id", how="inner")
    report = report.merge(insurance, on="patient_id", how="inner")
    report["dx_codes"] = report["patient_id"].map(dx_codes)
    # patient_dx_codes is an inner join in the original: a patient with no
    # medical_necessity row at all is dropped, not given an empty DXCodes string.
    report = report[report["dx_codes"].notna()]
    report = report.sort_values(["patient_id", "date_of_service"]).reset_index(
        drop=True
    )

    return pd.DataFrame(
        {
            "DateOfService": report["date_of_service"],
            "ID": report["sharepoint_id"],
            "FirstName": report["first_name"],
            "MiddleName": report["middle_name"],
            "LastName": report["last_name"],
            "Suffix": report["name_suffix"],
            "DOB": report["date_of_birth"],
            "PhoneNumber": report["phone_number"],
            "Address": report["street_address"],
            "City": report["city"],
            "State": report["temp_state"],
            "Zipcode": report["zipcode"],
            "Gender": report["sex"],
            "MedicareNumber": report["medicare_beneficiary_id"],
            "PrimaryPayer": report["primary_payer_name"],
            "PrimaryPayerID": report["primary_payer_id"],
            "SecondaryPayer": report["secondary_payer_name"],
            "SecondaryPayerID": report["secondary_payer_id"],
            "DXCodes": report["dx_codes"],
            "99202": report["99202"],
            "99453": report["99453"],
            "99454": report["99454"],
            "99457": report["99457"],
            "99458": report["99458"],
        }
    )
