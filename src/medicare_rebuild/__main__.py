import logging
import os
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session

from medicare_rebuild.billing import build_billing_report, run_billing
from medicare_rebuild.helpers import (
    delete_files_in_dir,
    get_files_in_dir,
    get_last_month_billing_cycle,
)
from medicare_rebuild.logger import setup_logger
from medicare_rebuild.models import (
    LOOKUP_SEEDS,
    BloodPressureReading,
    Device,
    EmergencyContact,
    GlucoseReading,
    GpsBase,
    MedicalNecessity,
    NoteType,
    Patient,
    PatientAddress,
    PatientInsurance,
    PatientNote,
    PatientStatus,
    PatientStatusType,
    User,
    Vendor,
    reset_all_data,
)
from medicare_rebuild.queries import (
    get_bg_readings_stmt,
    get_bp_readings_stmt,
    get_fulfillment_stmt,
    get_notes_log_stmt,
    get_time_log_stmt,
)
from medicare_rebuild.utils.api_utils import MSGraphApi
from medicare_rebuild.utils.atomic_io import ensure_dir
from medicare_rebuild.utils.dataframe_utils import (
    BLOOD_PRESSURE_DEVICE,
    GLUCOSE_DEVICE,
    check_patient_db_constraints,
    create_emcontacts_df,
    create_med_necessity_df,
    create_patient_address_df,
    create_patient_df,
    create_patient_insurance_df,
    create_patient_status_df,
    drop_duplicate_sharepoint_ids,
    find_possible_duplicate_patients,
    normalize_bg_readings,
    normalize_bp_readings,
    normalize_devices,
    normalize_patient_notes,
    normalize_patients,
    normalize_users,
    standardize_device_type,
)
from medicare_rebuild.utils.db_utils import DatabaseManager
from medicare_rebuild.utils.tabular_io import write_structured_file


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """df.to_dict("records") for constructing ORM rows, with every missing value (NaN,
    NaT, ...) converted to a real Python None first. `DataFrame.to_sql`, which the
    pipeline used before decision 0015, did this conversion internally; constructing
    ORM rows directly does not, and pyodbc rejects a raw NaN/NaT bound to a VARCHAR or
    DATETIME2 column outright rather than silently writing NULL. `.astype(object)`
    before `.where()` is required: on a typed (e.g. datetime64) column, `.where(...,
    None)` alone still leaves NaT in place, since None gets coerced back to the
    column's own null value for that dtype. DataFrame columns are always strings in
    this pipeline; pandas-stubs types the keys as the more general `Hashable`, which is
    the only reason this needs a cast."""
    clean = df.astype(object).where(df.notna(), None)  # type: ignore[call-overload]
    return clean.to_dict("records")  # type: ignore[return-value]


def _map_id(series: pd.Series, lookup: dict) -> pd.Series:
    """series.map(lookup) for resolving a temp_* column to a foreign-key id, without
    pandas' automatic float64/NaN upcast: plain Series.map() turns any unmatched value
    into numpy NaN and silently turns every *matched* int into a float alongside it,
    and neither round-trips cleanly into an integer column through the ORM. Building
    the result with a plain Python list keeps real ints and real None."""
    return pd.Series([lookup.get(v) for v in series], index=series.index, dtype=object)


def _drop_unresolved(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """Drop rows where any of `cols` failed to resolve (is None) -- what
    `add_id_col`'s inner merge (patient_id, device_id, vendor_id resolution before
    decision 0015) did implicitly: a row whose parent identity can't be found is
    dropped, not inserted with a NULL foreign key. Only used for that kind of
    resolution; a temp_* lookup-table FK (note type, coach, status type) that doesn't
    resolve is meant to stay NULL, matching the deferred UPDATE statements this
    replaced, so those call sites do not use this helper."""
    for col in cols:
        df = df[df[col].notna()]
    return df


def _require_lookup_seeds(session: Session) -> None:
    """Fail fast if the GPS database's lookup tables are missing the rows the load and
    billing steps resolve against (models.LOOKUP_SEEDS, seeded by `make migrate`).
    Without them every device would be dropped as having no vendor -- taking every
    reading with it -- and billing would stop on its first code type lookup, so a
    database that skipped the seed migration fails here, before anything is loaded,
    rather than producing an empty report."""
    missing = []
    for model, names in LOOKUP_SEEDS.items():
        present = set(session.scalars(select(model.name)))  # type: ignore[attr-defined]
        missing += [f"{model.__tablename__}.{n}" for n in names if n not in present]
    if missing:
        raise RuntimeError(
            "GPS lookup tables are missing required rows (run `make migrate`): "
            + ", ".join(missing)
        )


class DataImporter:
    def __init__(self, start_date: str, end_date: str, logger=None):
        """
        Initializes the DataImporter with the given start and end dates.

        Args:
            start_date (str, datetime): The start date for data import.
            end_date (str, datetime): The end date for data import.
            logger (logging.Logger): Logger instance for logging. Defaults to None (optional).
        """
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")

        self.logger = logger or logging.getLogger(__name__)
        self.gps = DatabaseManager(logger=self.logger)
        self.gps.create_engine(
            username=os.environ["GPS_SQL_USERNAME"],
            password=os.environ["GPS_SQL_PASSWORD"],
            host=os.environ["GPS_SQL_HOST"],
            database=os.environ["GPS_SQL_DB"],
        )
        self.session: Session = self.gps.get_session()
        self.snaps_dir = Path.cwd() / "data" / "snaps"

    @staticmethod
    def snap_dataframe(df: pd.DataFrame, path: Path | str) -> None:
        """
        Saves a DataFrame to an Excel file.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            path (Path, str): The path to save the Excel file.
        """
        write_structured_file(df, path, file_type="xlsx", index=False)

    def _lookup(self, model: type, key_attr: str, value_attr: str) -> dict[Any, Any]:
        """A small {key: id} dict from a lookup or already-loaded table, queried fresh
        each call so this works regardless of which DataImporter instance or session
        loaded the rows it depends on (see decision 0015)."""
        rows = self.session.execute(
            select(getattr(model, key_attr), getattr(model, value_attr))
        )
        return {k: v for k, v in rows if k is not None}

    def _unique_lookup(
        self,
        model: type[GpsBase],
        key_attr: str,
        value_attr: str,
        casefold: bool = False,
    ) -> dict[Any, Any]:
        """Like `_lookup`, for a key that is not guaranteed unique (a display name):
        a key shared by more than one row is left out, so whatever refers to it stays
        unresolved (NULL) instead of silently pointing at an arbitrary one of them, and
        each such key is logged. `casefold` lower-cases the keys, for identifiers that
        are matched case-insensitively (an Entra sign-in name); callers lower-case the
        values they look up to match."""
        ids: dict[Any, set[Any]] = {}
        for k, v in self.session.execute(
            select(getattr(model, key_attr), getattr(model, value_attr))
        ):
            if k is not None:
                ids.setdefault(k.lower() if casefold else k, set()).add(v)
        ambiguous = sorted(k for k, v in ids.items() if len(v) > 1)
        if ambiguous:
            # A count only: the values themselves are staff names, and logs carry no
            # row contents (docs/data-handling.md).
            self.logger.warning(
                f"{len(ambiguous)} {model.__tablename__}.{key_attr} value(s) belong to "
                "more than one row and will not be matched"
            )
        return {k: next(iter(v)) for k, v in ids.items() if len(v) == 1}

    @contextmanager
    def _legacy_db(self, database_var: str) -> Iterator[DatabaseManager]:
        """A connection to one legacy source database (named by the environment
        variable `database_var`), closed on exit even if the read fails part-way."""
        db = DatabaseManager(logger=self.logger)
        try:
            db.create_engine(
                username=os.environ["LEGACY_SQL_USERNAME"],
                password=os.environ["LEGACY_SQL_PASSWORD"],
                host=os.environ["LEGACY_SQL_HOST"],
                database=os.environ[database_var],
            )
            yield db
        finally:
            db.close()

    def get_user_data(self, snap: bool = False) -> pd.DataFrame:
        """
        Retrieves user data from Microsoft Graph API and normalizes it.

        Args:
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            pd.DataFrame: The normalized user data.
        """
        msg = MSGraphApi(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"],
            logger=self.logger,
        )
        msg.request_access_token()
        data = msg.get_group_members(os.environ["AZURE_GROUP_ID"])
        # Narrows the type for mypy; the pipeline is not run with `python -O`.
        assert isinstance(data, dict), (  # noqa: S101
            "Expected a JSON object from the members endpoint"
        )
        df = pd.DataFrame(data["value"])
        df = normalize_users(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / "snap_user_df.xlsx")
        return df

    def get_patient_data(
        self, filename: Path | str, snap: bool = False
    ) -> dict[str, pd.DataFrame]:
        """
        Retrieves and normalizes patient data from a CSV file.

        Args:
            filename (Path, str): The path to the CSV file.
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            Dict[str, pd.DataFrame]: A dictionary of normalized patient data DataFrames.
        """
        df = pd.read_csv(
            filename,
            dtype={"Phone Number": "str", "Social Security": "str", "Zip code": "str"},
            parse_dates=["DOB", "On-board Date"],
        )
        self.logger.debug(
            f"Reading patient export from SharePoint (rows: {df.shape[0]}, cols: {df.shape[1]})"
        )
        df = normalize_patients(df)
        df = check_patient_db_constraints(df)
        df, dup_ids = drop_duplicate_sharepoint_ids(df)
        # Counts only: logs carry no row contents (docs/data-handling.md). Run with
        # snap=True to inspect the rows themselves.
        if dup_ids:
            self.logger.warning(
                f"{len(dup_ids)} SharePoint ID(s) appear on more than one patient row; "
                "kept the first row of each"
            )
        possible_dupes = find_possible_duplicate_patients(df)
        if possible_dupes:
            self.logger.warning(
                f"{len(possible_dupes)} possible duplicate patient(s): rows with "
                "different SharePoint IDs share a name and date of birth"
            )
        res = {
            "patient": create_patient_df(df),
            "address": create_patient_address_df(df),
            "insurance": create_patient_insurance_df(df),
            "med_nec": create_med_necessity_df(df),
            "status": create_patient_status_df(df),
            "emcontacts": create_emcontacts_df(df),
        }
        if snap:
            for name, df in res.items():
                self.snap_dataframe(df, self.snaps_dir / f"snap_{name}_df.xlsx")
        return res

    def get_patient_note_data(self, snap: bool = False) -> pd.DataFrame:
        """
        Retrieves and normalizes patient note data from the database.

        Args:
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            pd.DataFrame: The normalized patient note data.
        """
        with self._legacy_db("LEGACY_SQL_SP_NOTES") as notes_db:
            notes_df = notes_db.read_sql(
                get_notes_log_stmt(self.start_date, self.end_date),
                parse_dates=["TimeStamp"],
            )
        with self._legacy_db("LEGACY_SQL_SP_TIME") as time_db:
            time_df = time_db.read_sql(
                get_time_log_stmt(self.start_date, self.end_date),
                parse_dates=["Start_Time", "End_Time"],
            )
        time_df = time_df.rename(
            columns={"SharPoint_ID": "SharePoint_ID", "Notes": "Note_Type"}
        )
        df = pd.merge(
            notes_df, time_df, on=["SharePoint_ID", "Note_ID", "AZURE_UPN"], how="left"
        )
        df["Time_Note"] = df["Time_Note"].fillna(df["Note_Type"])
        df.drop(columns=["Note_ID", "Note_Type"], inplace=True)
        df = normalize_patient_notes(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / "snap_note_df.xlsx")
        return df

    def get_device_data(self, snap: bool = False) -> pd.DataFrame:
        """
        Retrieves and normalizes device data from the database.

        Args:
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            pd.DataFrame: The normalized device data.
        """
        with self._legacy_db("LEGACY_SQL_SP_FULFILLMENT") as fulfillment_db:
            df = fulfillment_db.read_sql(get_fulfillment_stmt())
        df = normalize_devices(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / "snap_device_df.xlsx")
        return df

    def get_gluc_readings(self, snap: bool = False) -> pd.DataFrame:
        """
        Retrieves and normalizes glucose readings from the database.

        Args:
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            pd.DataFrame: The normalized glucose readings.
        """
        with self._legacy_db("LEGACY_SQL_SP_READINGS") as readings_db:
            df = readings_db.read_sql(
                get_bg_readings_stmt(self.start_date, self.end_date),
                parse_dates=["Time_Recorded", "Time_Recieved"],
            )
        if snap:
            self.snap_dataframe(df, self.snaps_dir / "snap_glucose_df.xlsx")
        df = normalize_bg_readings(df)
        return df

    def get_bp_readings(self, snap: bool = False) -> pd.DataFrame:
        """
        Retrieves and normalizes blood pressure readings from the database.

        Args:
            snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).

        Returns:
            pd.DataFrame: The normalized blood pressure readings.
        """
        with self._legacy_db("LEGACY_SQL_SP_READINGS") as readings_db:
            df = readings_db.read_sql(
                get_bp_readings_stmt(self.start_date, self.end_date),
                parse_dates=["Time_Recorded", "Time_Recieved"],
            )
        if snap:
            self.snap_dataframe(df, self.snaps_dir / "snap_blood_pressure_df.xlsx")
        df = normalize_bp_readings(df)
        return df

    def import_user_data(self, df: pd.DataFrame) -> None:
        """
        Imports user data into the database.

        Args:
            df (pd.DataFrame): The user data DataFrame to import.
        """
        self.session.add_all(User(**row) for row in _records(df))
        self.session.commit()

    def import_patient_data(self, patient_data: dict[str, pd.DataFrame]) -> None:
        """
        Imports patient data into the database.

        Resolves temp_user -> user_id and temp_status_type -> patient_status_type_id by
        looking the lookup tables up before insert, then inserts the patient rows and
        uses the identity keys SQLAlchemy assigns them on flush to link every dependent
        row (address, insurance, medical necessity, status, emergency contacts) -- no
        separate SELECT-and-merge step is needed for that part (see decision 0015).

        Args:
            patient_data (Dict[str, pd.DataFrame]): A dictionary of patient data DataFrames to import.
        """
        user_lookup = self._unique_lookup(User, "display_name", "user_id")
        status_lookup = self._lookup(
            PatientStatusType, "name", "patient_status_type_id"
        )

        patient_df = patient_data["patient"].copy()
        patient_df["user_id"] = _map_id(patient_df["temp_user"], user_lookup)
        patients = [Patient(**row) for row in _records(patient_df)]
        self.session.add_all(patients)
        self.session.flush()
        patient_ids = {p.sharepoint_id: p.patient_id for p in patients}

        def with_patient_id(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df["patient_id"] = _map_id(df["sharepoint_id"], patient_ids)
            return df.drop(columns=["sharepoint_id"])

        address_df = with_patient_id(patient_data["address"])
        insurance_df = with_patient_id(patient_data["insurance"])
        med_nec_df = with_patient_id(patient_data["med_nec"])
        status_df = with_patient_id(patient_data["status"])
        status_df["patient_status_type_id"] = _map_id(
            status_df["temp_status_type"], status_lookup
        )
        emcontacts_df = with_patient_id(patient_data["emcontacts"])

        self.session.add_all(PatientAddress(**r) for r in _records(address_df))
        self.session.add_all(PatientInsurance(**r) for r in _records(insurance_df))
        self.session.add_all(MedicalNecessity(**r) for r in _records(med_nec_df))
        self.session.add_all(PatientStatus(**r) for r in _records(status_df))
        self.session.add_all(EmergencyContact(**r) for r in _records(emcontacts_df))
        self.session.commit()

    def import_patient_note_data(self, df: pd.DataFrame) -> None:
        """
        Imports patient note data into the database.

        Args:
            df (pd.DataFrame): The patient note data DataFrame to import.
        """
        patient_ids = self._lookup(Patient, "sharepoint_id", "patient_id")
        note_type_lookup = self._lookup(NoteType, "name", "note_type_id")
        # Notes record their author's Entra sign-in name (AZURE_UPN), not their display
        # name, and Entra matches sign-in names case-insensitively.
        upn_lookup = self._unique_lookup(
            User, "user_principal_name", "user_id", casefold=True
        )

        df = df.copy()
        df["patient_id"] = _map_id(df["sharepoint_id"], patient_ids)
        df = df.drop(columns=["sharepoint_id"])
        df = _drop_unresolved(df, "patient_id")
        df["note_type_id"] = _map_id(df["temp_note_type"], note_type_lookup)
        df["user_id"] = _map_id(df["temp_user"].str.lower(), upn_lookup)

        self.session.add_all(PatientNote(**row) for row in _records(df))
        self.session.commit()

    def import_device_data(self, df: pd.DataFrame) -> None:
        """
        Imports device data into the database.

        Args:
            df (pd.DataFrame): The device data DataFrame to import.
        """
        patient_ids = self._lookup(Patient, "sharepoint_id", "patient_id")
        vendor_ids = self._lookup(Vendor, "name", "vendor_id")

        df = df.copy()
        df["patient_id"] = _map_id(df["sharepoint_id"], patient_ids)
        df["vendor_id"] = _map_id(df["Vendor"], vendor_ids)
        df = df.drop(columns=["sharepoint_id", "Vendor"])
        df = _drop_unresolved(df, "patient_id", "vendor_id")
        unclassified = df["name"].apply(standardize_device_type).isna()
        if unclassified.any():
            self.logger.warning(
                f"{int(unclassified.sum())} device(s) loaded whose name does not say "
                "what type of device they are; no reading will be linked to them"
            )

        self.session.add_all(Device(**row) for row in _records(df))
        self.session.commit()

    def _resolve_device_readings(
        self, df: pd.DataFrame, device_type: str
    ) -> pd.DataFrame:
        """Resolve sharepoint_id -> patient_id, then link each reading to the patient's
        device(s) of `device_type` (standardize_device_type, from the device name):
        legacy readings carry no device identifier, so the device type is what keeps
        a glucose reading off a blood pressure cuff and vice versa. A reading whose
        patient has no device of that type is dropped and counted in a warning. A
        patient with more than one device of the same type still gets the reading
        once per such device -- nothing in the source says which one took it.
        patient_id is a join key only: readings link to a patient through their
        device, not through a patient_id column of their own (see
        docs/reconciliation.md), so it is dropped again once it has done that job."""
        patient_ids = self._lookup(Patient, "sharepoint_id", "patient_id")
        devices = pd.DataFrame(
            self.session.execute(
                select(Device.device_id, Device.patient_id, Device.name)
            ),
            columns=["device_id", "patient_id", "name"],
        )
        devices = devices.loc[
            devices["name"].apply(standardize_device_type) == device_type,
            ["device_id", "patient_id"],
        ]
        df = df.copy()
        df["patient_id"] = _map_id(df["sharepoint_id"], patient_ids)
        df = df.drop(columns=["sharepoint_id"])
        no_device = df["patient_id"].notna() & ~df["patient_id"].isin(
            devices["patient_id"]
        )
        if no_device.any():
            self.logger.warning(
                f"{int(no_device.sum())} reading(s) dropped: the patient has no device "
                "of the reading's type on file"
            )
        return pd.merge(df, devices, on="patient_id").drop(columns=["patient_id"])

    def import_gluc_readings_data(self, df: pd.DataFrame) -> None:
        """
        Imports glucose readings data into the database.

        Args:
            df (pd.DataFrame): The glucose readings data DataFrame to import.
        """
        df = self._resolve_device_readings(df, GLUCOSE_DEVICE)
        self.session.add_all(GlucoseReading(**row) for row in _records(df))
        self.session.commit()

    def import_bp_readings_data(self, df: pd.DataFrame) -> None:
        """
        Imports blood pressure readings data into the database.

        Args:
            df (pd.DataFrame): The blood pressure readings data DataFrame to import.
        """
        df = self._resolve_device_readings(df, BLOOD_PRESSURE_DEVICE)
        self.session.add_all(BloodPressureReading(**row) for row in _records(df))
        self.session.commit()

    def close_db(self) -> None:
        """
        Closes the database connection.
        """
        self.session.close()
        if self.gps:
            self.gps.close()


def import_all_data(
    start_date,
    end_date,
    snap=False,
    logger=logging.getLogger(),  # noqa: B008
):
    """
    Imports all data within the specified date range.

    Args:
        start_date (str, datetime): The start date for data import.
        end_date (str, datetime): The end date for data import.
        snap (bool): Whether to save a snapshot of the DataFrame. Defaults to False (optional).
        logger (logging.Logger): Logger instance for logging. Defaults to logging.getLogger() (optional).
    """
    data_dir = Path.cwd() / "data"
    snaps_dir = data_dir / "snaps"
    ensure_dir(snaps_dir)
    if get_files_in_dir(snaps_dir):
        delete_files_in_dir(snaps_dir)

    dim = DataImporter(start_date, end_date, logger=logger)
    try:
        _require_lookup_seeds(dim.session)
        reset_all_data(dim.session)

        user_df = dim.get_user_data(snap=snap)
        dim.import_user_data(user_df)
        patient_data = dim.get_patient_data(data_dir / "Patient_Export.csv", snap=snap)
        dim.import_patient_data(patient_data)
        note_df = dim.get_patient_note_data(snap=snap)
        dim.import_patient_note_data(note_df)
        device_df = dim.get_device_data(snap=snap)
        dim.import_device_data(device_df)
        gluc_df = dim.get_gluc_readings(snap=snap)
        dim.import_gluc_readings_data(gluc_df)
        bp_df = dim.get_bp_readings(snap=snap)
        dim.import_bp_readings_data(bp_df)
    finally:
        dim.close_db()


def create_billing_report(
    start_date,
    end_date,
    logger=logging.getLogger(),  # noqa: B008
):
    """
    Creates a billing report for the specified date range.

    Args:
        start_date (str, datetime): The start date for the billing report.
        end_date (str, datetime): The end date for the billing report.
        logger (logging.Logger): Logger instance for logging. Defaults to logging.getLogger() (optional).
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
        username=os.environ["GPS_SQL_USERNAME"],
        password=os.environ["GPS_SQL_PASSWORD"],
        host=os.environ["GPS_SQL_HOST"],
        database=os.environ["GPS_SQL_DB"],
    )
    session = gps.get_session()
    try:
        run_billing(session, end_date)
        df = build_billing_report(session, start_date, end_date)
        write_structured_file(
            df, Path.cwd() / "data" / "Billing_Report.xlsx", index=False
        )
    finally:
        session.close()
        gps.close()


def main() -> None:
    warnings.filterwarnings("ignore")
    load_dotenv()
    logger = setup_logger("main", level="debug")

    report_start, report_end = get_last_month_billing_cycle()
    # The billing rules' rolling windows look back up to 30 days (99454) or 1 month
    # (99457/99458) from the report end date, so the import window starts a full
    # calendar month before the report itself -- not just the report's own month -- to
    # make sure every reading/note those windows need has actually been loaded.
    import_start = (report_start - timedelta(days=1)).replace(day=1)
    # Every end bound downstream is midnight at the *start* of the date it is given:
    # the source queries keep rows recorded `<=` it, and the report keeps codes stamped
    # `<=` it (both inherited from the original SQL; see docs/billing-rules.md). Passing
    # the month's last day would drop that whole day, so pass the first day of the
    # next month instead. The rolling windows then look back from that same midnight,
    # covering the full billing month.
    end_bound = report_end + timedelta(days=1)

    import_all_data(
        import_start.strftime("%Y-%m-%d"),
        end_bound.strftime("%Y-%m-%d"),
        logger=logger,
    )
    create_billing_report(
        report_start.strftime("%Y-%m-%d"),
        end_bound.strftime("%Y-%m-%d"),
        logger=logger,
    )


if __name__ == "__main__":
    main()
