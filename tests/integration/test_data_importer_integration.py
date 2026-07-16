import pandas as pd
import pytest

from tests.integration.conftest import (
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    execute_ddl,
)

pytestmark = pytest.mark.integration

JSON_HEADERS = {"Content-Type": "application/json"}

GROUP_ID = "4bbe3379-1250-4522-92e6-017f77517470"

SCHEMA = [
    """
    CREATE TABLE [user] (
        user_id INT IDENTITY PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        display_name VARCHAR(200),
        email VARCHAR(200),
        ms_entra_id VARCHAR(100)
    )
    """,
    """
    CREATE TABLE patient (
        patient_id INT IDENTITY PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        middle_name VARCHAR(100),
        name_suffix VARCHAR(20),
        full_name VARCHAR(200),
        nick_name VARCHAR(100),
        date_of_birth DATETIME2,
        sex VARCHAR(10),
        email VARCHAR(200),
        phone_number VARCHAR(20),
        social_security VARCHAR(20),
        temp_race VARCHAR(50),
        temp_marital_status VARCHAR(50),
        preferred_language VARCHAR(50),
        weight_lbs INT,
        height_in INT,
        sharepoint_id INT,
        temp_user VARCHAR(100)
    )
    """,
    """
    CREATE TABLE patient_address (
        patient_address_id INT IDENTITY PRIMARY KEY,
        street_address VARCHAR(200),
        city VARCHAR(100),
        temp_state VARCHAR(10),
        zipcode VARCHAR(10),
        patient_id INT
    )
    """,
    """
    CREATE TABLE patient_insurance (
        patient_insurance_id INT IDENTITY PRIMARY KEY,
        medicare_beneficiary_id VARCHAR(20),
        primary_payer_id VARCHAR(50),
        primary_payer_name VARCHAR(100),
        secondary_payer_id VARCHAR(50),
        secondary_payer_name VARCHAR(100),
        patient_id INT
    )
    """,
    """
    CREATE TABLE medical_necessity (
        medical_necessity_id INT IDENTITY PRIMARY KEY,
        evaluation_datetime DATETIME2,
        temp_dx_code VARCHAR(20),
        patient_id INT
    )
    """,
    """
    CREATE TABLE patient_status (
        patient_status_id INT IDENTITY PRIMARY KEY,
        temp_status_type VARCHAR(50),
        modified_date DATETIME2,
        temp_user VARCHAR(100),
        patient_id INT
    )
    """,
    """
    CREATE TABLE emergency_contact (
        emergency_contact_id INT IDENTITY PRIMARY KEY,
        full_name VARCHAR(200),
        phone_number VARCHAR(20),
        relationship VARCHAR(50),
        patient_id INT
    )
    """,
]


TABLES = [
    "user",
    "patient",
    "patient_address",
    "patient_insurance",
    "medical_necessity",
    "patient_status",
    "emergency_contact",
]


@pytest.fixture
def gps_schema(test_database):
    drop_statements = [f"DROP TABLE IF EXISTS [{table}]" for table in TABLES]
    execute_ddl(test_database, drop_statements + SCHEMA)
    return test_database


@pytest.fixture
def data_importer(monkeypatch, gps_schema):
    monkeypatch.setenv("LCH_SQL_GPS_USERNAME", DB_USER)
    monkeypatch.setenv("LCH_SQL_GPS_PASSWORD", DB_PASSWORD)
    monkeypatch.setenv("LCH_SQL_GPS_HOST", f"{DB_HOST},{DB_PORT}")
    monkeypatch.setenv("LCH_SQL_GPS_DB", gps_schema)
    monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant-id")
    monkeypatch.setenv("AZURE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-client-secret")

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
