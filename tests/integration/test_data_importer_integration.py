import pandas as pd
import pytest

from medicare_rebuild.models import GpsBase
from tests.integration.conftest import (
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    _connect,
)

pytestmark = pytest.mark.integration

JSON_HEADERS = {"Content-Type": "application/json"}

GROUP_ID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def gps_schema(test_database):
    """The real GPS schema (medicare_rebuild.models), not a second, independently
    maintained reconstruction of it -- see decision 0015."""
    db = _connect(test_database)
    try:
        GpsBase.metadata.drop_all(db.engine, checkfirst=True)
        GpsBase.metadata.create_all(db.engine)
    finally:
        db.close()
    return test_database


@pytest.fixture
def data_importer(monkeypatch, gps_schema):
    monkeypatch.setenv("GPS_SQL_USERNAME", DB_USER)
    monkeypatch.setenv("GPS_SQL_PASSWORD", DB_PASSWORD)
    monkeypatch.setenv("GPS_SQL_HOST", f"{DB_HOST},{DB_PORT}")
    monkeypatch.setenv("GPS_SQL_DB", gps_schema)
    monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant-id")
    monkeypatch.setenv("AZURE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("AZURE_GROUP_ID", GROUP_ID)

    from medicare_rebuild.__main__ import DataImporter

    importer = DataImporter(start_date="2026-01-01", end_date="2026-01-31")
    yield importer
    importer.close_db()


@pytest.fixture
def patient_csv(tmp_path):
    df = pd.DataFrame(
        {
            "First Name": ["John", "Jane"],
            "Last Name": ["Doe", "Smith"],
            "Middle Name": ["A", "B"],
            "Nickname": ["Johnny", "Janie"],
            "Phone Number": ["123-456-7890", "234-567-8901"],
            "Gender": ["Male", "Female"],
            "Email": ["john.doe@example.com", "jane.smith@example.com"],
            "Suffix": ["Jr", ""],
            "Social Security": ["123-45-6789", "234-56-7890"],
            "Race": ["White", "Black"],
            "Weight": ["150 lbs", "130 lbs"],
            "Height": ["5'8\"", "5'4\""],
            "Mailing Address": ["123 Main St", "456 Oak Ave"],
            "City": ["Anytown", "Springfield"],
            "State": ["California", "Texas"],
            "Zip code": ["12345", "67890"],
            "EmergencyName": ["Jane Doe", "John Smith"],
            "EmergencyNumber": ["123-456-7890", "234-567-8901"],
            "EmergencyName2": ["", ""],
            "EmergencyNumber2": ["", ""],
            "Medicare ID number": ["1EG4-TE5-MK73", "3CD5-UF6-LM84"],
            "DX_Code": ["E11.9,I10", "I10"],
            "Insurance ID:": ["abc-123-xyz", ""],
            "Insurance Name:": ["Kaiser", ""],
            "InsuranceID2": ["", ""],
            "InsuranceName2": ["", ""],
            "On-board Date": ["2023-01-01", "2023-02-15"],
            "Member_Status": ["Active", "Active"],
            "Health Coach": ["admin", "admin"],
            "Relationship_Status": ["Married", "Single"],
            "Preferred_Language": ["English", "English"],
            "DOB": ["01/01/1960", "02/02/1970"],
            "ID": [101, 102],
        }
    )
    path = tmp_path / "Patient_Export.csv"
    df.to_csv(path, index=False)
    return path


def test_import_user_data(data_importer, requests_mock):
    token_endpoint = (
        "https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token"
    )
    requests_mock.post(
        token_endpoint,
        json={"access_token": "test_token"},
        headers=JSON_HEADERS,
        status_code=200,
    )
    members_endpoint = f"https://graph.microsoft.com/v1.0/groups/{GROUP_ID}/members"
    requests_mock.get(
        members_endpoint,
        json={
            "value": [
                {
                    "givenName": "Alex",
                    "surname": "Coach",
                    "displayName": "Alex Coach",
                    "mail": "alex.coach@example.com",
                    "id": "entra-id-1",
                }
            ]
        },
        headers=JSON_HEADERS,
        status_code=200,
    )

    user_df = data_importer.get_user_data()
    data_importer.import_user_data(user_df)

    landed = data_importer.gps.read_sql(
        "SELECT first_name, last_name, email FROM [user]"
    )
    assert landed.to_dict("records") == [
        {
            "first_name": "Alex",
            "last_name": "Coach",
            "email": "alex.coach@example.com",
        }
    ]


def test_import_patient_data(data_importer, patient_csv):
    patient_data = data_importer.get_patient_data(patient_csv)
    data_importer.import_patient_data(patient_data)

    patients = data_importer.gps.read_sql(
        "SELECT patient_id, first_name, last_name, sharepoint_id FROM patient ORDER BY sharepoint_id"
    )
    assert patients["sharepoint_id"].tolist() == [101, 102]
    assert patients["first_name"].tolist() == ["John", "Jane"]

    john_id, jane_id = patients["patient_id"].tolist()

    addresses = data_importer.gps.read_sql(
        "SELECT patient_id, temp_state, city FROM patient_address ORDER BY patient_id"
    )
    assert set(addresses["patient_id"]) == {john_id, jane_id}
    assert (
        addresses.loc[addresses["patient_id"] == john_id, "temp_state"].item() == "CA"
    )
    assert (
        addresses.loc[addresses["patient_id"] == jane_id, "temp_state"].item() == "TX"
    )

    insurance = data_importer.gps.read_sql(
        "SELECT patient_id, primary_payer_name FROM patient_insurance ORDER BY patient_id"
    )
    assert (
        insurance.loc[insurance["patient_id"] == john_id, "primary_payer_name"].item()
        == "Kaiser"
    )
    # Jane had no insurance name/id on file but does have a Medicare ID, so
    # fill_primary_payer should have auto-resolved her to Medicare Part B.
    assert (
        insurance.loc[insurance["patient_id"] == jane_id, "primary_payer_name"].item()
        == "Medicare Part B"
    )

    med_necessity = data_importer.gps.read_sql(
        "SELECT patient_id, temp_dx_code FROM medical_necessity ORDER BY patient_id"
    )
    # John has 2 Dx codes (E11.9, I10), Jane has 1 (I10) -> 3 rows total.
    assert med_necessity.shape[0] == 3
    assert sorted(
        med_necessity.loc[med_necessity["patient_id"] == john_id, "temp_dx_code"]
    ) == ["E119", "I10"]

    statuses = data_importer.gps.read_sql(
        "SELECT patient_id, temp_status_type FROM patient_status ORDER BY patient_id"
    )
    assert set(statuses["temp_status_type"]) == {"Active"}

    emcontacts = data_importer.gps.read_sql(
        "SELECT patient_id, full_name FROM emergency_contact ORDER BY patient_id"
    )
    # Both patients only had a first emergency contact; the blank second slot
    # is dropped, so exactly one row per patient.
    assert emcontacts.shape[0] == 2
    assert set(emcontacts["patient_id"]) == {john_id, jane_id}
