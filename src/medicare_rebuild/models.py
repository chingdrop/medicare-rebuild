"""The GPS database schema, as SQLAlchemy declarative models.

This is the schema of record: `sql/schema.sql` is generated from these classes (see
`tools/generate_schema.py`; regenerate with `make schema`), and `make demo` builds its
GPS database from `GpsBase.metadata.create_all()` against the same classes. There is no
other definition of this schema in the repository.

Column names, types and nullability are carried over unchanged from the schema this
repo previously reconstructed for the demo (see decision 0015) — this is a change in how
the schema is defined and enforced, not a change to the schema itself. Types use the
MSSQL dialect's own classes (`DATETIME2`, `BIT`, `NVARCHAR(max)`) rather than generic
SQLAlchemy types, so the generated DDL matches the original reconstruction exactly
instead of silently drifting to the dialect's default mappings (generic `DateTime`
compiles to `DATETIME`, not `DATETIME2`, under mssql). `relationship()` attributes are
added only where they simplify the ORM-based load and billing code; they do not imply
constraints beyond the foreign key columns they wrap.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Float, ForeignKey, Integer, String, delete, text
from sqlalchemy.dialects.mssql import BIT, DATETIME2, NVARCHAR
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


class GpsBase(DeclarativeBase):
    """Declarative base for the GPS target database. Its own MetaData, independent of
    LegacyBase, since the two live in separate databases with separate engines."""


class User(GpsBase):
    __tablename__ = "user"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str | None] = mapped_column(String(100))
    last_name: Mapped[str | None] = mapped_column(String(100))
    display_name: Mapped[str | None] = mapped_column(String(200))
    email: Mapped[str | None] = mapped_column(String(200))
    ms_entra_id: Mapped[str | None] = mapped_column(String(100))


class PatientStatusType(GpsBase):
    __tablename__ = "patient_status_type"

    patient_status_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str | None] = mapped_column(String(50))


class Vendor(GpsBase):
    __tablename__ = "vendor"

    vendor_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str | None] = mapped_column(String(50))


class NoteType(GpsBase):
    __tablename__ = "note_type"

    note_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str | None] = mapped_column(String(100))


class MedicalCodeType(GpsBase):
    __tablename__ = "medical_code_type"

    med_code_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str | None] = mapped_column(String(20))


class Patient(GpsBase):
    __tablename__ = "patient"

    patient_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str | None] = mapped_column(String(100))
    last_name: Mapped[str | None] = mapped_column(String(100))
    middle_name: Mapped[str | None] = mapped_column(String(100))
    name_suffix: Mapped[str | None] = mapped_column(String(20))
    full_name: Mapped[str | None] = mapped_column(String(200))
    nick_name: Mapped[str | None] = mapped_column(String(100))
    date_of_birth: Mapped[datetime | None] = mapped_column(DATETIME2)
    sex: Mapped[str | None] = mapped_column(String(10))
    email: Mapped[str | None] = mapped_column(String(200))
    phone_number: Mapped[str | None] = mapped_column(String(20))
    social_security: Mapped[str | None] = mapped_column(String(20))
    temp_race: Mapped[str | None] = mapped_column(String(50))
    temp_marital_status: Mapped[str | None] = mapped_column(String(50))
    preferred_language: Mapped[str | None] = mapped_column(String(50))
    weight_lbs: Mapped[int | None] = mapped_column(Integer)
    height_in: Mapped[int | None] = mapped_column(Integer)
    sharepoint_id: Mapped[int | None] = mapped_column(Integer)
    temp_user: Mapped[str | None] = mapped_column(String(100))
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user.user_id"))

    notes: Mapped[list[PatientNote]] = relationship(back_populates="patient")
    devices: Mapped[list[Device]] = relationship(back_populates="patient")


class PatientAddress(GpsBase):
    __tablename__ = "patient_address"

    patient_address_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    street_address: Mapped[str | None] = mapped_column(String(200))
    city: Mapped[str | None] = mapped_column(String(100))
    temp_state: Mapped[str | None] = mapped_column(String(10))
    zipcode: Mapped[str | None] = mapped_column(String(10))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))


class PatientInsurance(GpsBase):
    __tablename__ = "patient_insurance"

    patient_insurance_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    medicare_beneficiary_id: Mapped[str | None] = mapped_column(String(20))
    primary_payer_id: Mapped[str | None] = mapped_column(String(50))
    primary_payer_name: Mapped[str | None] = mapped_column(String(100))
    secondary_payer_id: Mapped[str | None] = mapped_column(String(50))
    secondary_payer_name: Mapped[str | None] = mapped_column(String(100))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))


class MedicalNecessity(GpsBase):
    __tablename__ = "medical_necessity"

    medical_necessity_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    evaluation_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    temp_dx_code: Mapped[str | None] = mapped_column(String(20))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))


class PatientStatus(GpsBase):
    __tablename__ = "patient_status"

    patient_status_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    temp_status_type: Mapped[str | None] = mapped_column(String(50))
    modified_date: Mapped[datetime | None] = mapped_column(DATETIME2)
    temp_user: Mapped[str | None] = mapped_column(String(100))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))
    patient_status_type_id: Mapped[int | None] = mapped_column(
        ForeignKey("patient_status_type.patient_status_type_id")
    )


class EmergencyContact(GpsBase):
    __tablename__ = "emergency_contact"

    emergency_contact_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str | None] = mapped_column(String(200))
    phone_number: Mapped[str | None] = mapped_column(String(20))
    # Safe to name this `relationship`, matching the DB column and the DataFrame column
    # create_emcontacts_df produces: a class body is its own namespace, so this does not
    # shadow the `relationship()` function used by other classes in this module.
    relationship: Mapped[str | None] = mapped_column(String(50))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))


class Device(GpsBase):
    __tablename__ = "device"

    device_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    hardware_uuid: Mapped[str | None] = mapped_column(String(100))
    name: Mapped[str | None] = mapped_column(String(200))
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))
    vendor_id: Mapped[int | None] = mapped_column(ForeignKey("vendor.vendor_id"))

    patient: Mapped[Patient | None] = relationship(back_populates="devices")
    glucose_readings: Mapped[list[GlucoseReading]] = relationship(
        back_populates="device"
    )
    blood_pressure_readings: Mapped[list[BloodPressureReading]] = relationship(
        back_populates="device"
    )


class GlucoseReading(GpsBase):
    __tablename__ = "glucose_reading"

    glucose_reading_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    temp_device: Mapped[str | None] = mapped_column(String(100))
    recorded_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    received_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    glucose_reading: Mapped[float | None] = mapped_column(Float)
    is_manual: Mapped[bool | None] = mapped_column(BIT)
    device_id: Mapped[int | None] = mapped_column(ForeignKey("device.device_id"))

    device: Mapped[Device | None] = relationship(back_populates="glucose_readings")


class BloodPressureReading(GpsBase):
    __tablename__ = "blood_pressure_reading"

    blood_pressure_reading_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    temp_device: Mapped[str | None] = mapped_column(String(100))
    recorded_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    received_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    systolic_reading: Mapped[float | None] = mapped_column(Float)
    diastolic_reading: Mapped[float | None] = mapped_column(Float)
    is_manual: Mapped[bool | None] = mapped_column(BIT)
    device_id: Mapped[int | None] = mapped_column(ForeignKey("device.device_id"))

    device: Mapped[Device | None] = relationship(
        back_populates="blood_pressure_readings"
    )


class PatientNote(GpsBase):
    __tablename__ = "patient_note"

    patient_note_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    note_content: Mapped[str | None] = mapped_column(NVARCHAR(None))
    note_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    temp_user: Mapped[str | None] = mapped_column(String(100))
    temp_note_type: Mapped[str | None] = mapped_column(String(100))
    call_time_seconds: Mapped[float | None] = mapped_column(Float)
    is_manual: Mapped[bool | None] = mapped_column(BIT)
    start_call_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    end_call_datetime: Mapped[datetime | None] = mapped_column(DATETIME2)
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))
    note_type_id: Mapped[int | None] = mapped_column(
        ForeignKey("note_type.note_type_id")
    )
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user.user_id"))

    patient: Mapped[Patient | None] = relationship(back_populates="notes")


class MedicalCode(GpsBase):
    __tablename__ = "medical_code"

    med_code_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    patient_id: Mapped[int | None] = mapped_column(ForeignKey("patient.patient_id"))
    med_code_type_id: Mapped[int | None] = mapped_column(
        ForeignKey("medical_code_type.med_code_type_id")
    )
    timestamp_applied: Mapped[datetime | None] = mapped_column(DATETIME2)

    devices: Mapped[list[MedicalCodeDevice]] = relationship(
        back_populates="medical_code"
    )


class MedicalCodeDevice(GpsBase):
    __tablename__ = "medical_code_device"

    medical_code_device_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    med_code_id: Mapped[int | None] = mapped_column(
        ForeignKey("medical_code.med_code_id")
    )
    device_id: Mapped[int | None] = mapped_column(ForeignKey("device.device_id"))

    medical_code: Mapped[MedicalCode | None] = relationship(back_populates="devices")


# Order matters for reset_all_data(): children before the parents they reference, to
# satisfy foreign key constraints during DELETE (mirrors reset_all_billing_tables.sql).
RESET_ORDER: list[type[GpsBase]] = [
    PatientAddress,
    PatientInsurance,
    MedicalNecessity,
    PatientNote,
    BloodPressureReading,
    GlucoseReading,
    MedicalCodeDevice,
    Device,
    MedicalCode,
    PatientStatus,
    EmergencyContact,
    Patient,
    User,
]


def reset_all_data(session: Session) -> None:
    """Delete every row from every GPS table and reseed identity columns to start at 1.

    Python port of reset_all_billing_tables.sql, kept for reference in
    sql/stored_procedures/. Deletes in RESET_ORDER (children before the parents they
    reference, same order the stored procedure documents) and reseeds each table via
    DBCC CHECKIDENT, which has no ORM equivalent and is SQL Server-specific.
    """
    for model in RESET_ORDER:
        session.execute(delete(model))
        session.execute(text(f"DBCC CHECKIDENT ('{model.__tablename__}', RESEED, 0)"))
    session.commit()
