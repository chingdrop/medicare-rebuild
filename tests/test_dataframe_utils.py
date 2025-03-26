import re
import pandas as pd
import numpy as np

from utils.dataframe_utils import (
    keyword_search,
    keyword_list_search,
    extract_regex_pattern,
    standardize_name,
    standardize_email,
    standardize_state,
    standardize_mbi,
    standardize_dx_code,
    standardize_insurance_name,
    standardize_insurance_id,
    fill_primary_payer,
    fill_primary_payer_id,
    standardize_call_time,
    standardize_note_types,
    standardize_vendor,
    standardize_emcontact_relationship,
    standardize_race,
    standardize_weight,
    standardize_height,
    create_patient_df,
    create_patient_address_df,
    create_patient_insurance_df,
    create_med_necessity_df,
    create_patient_status_df,
    create_emcontacts_df,
    normalize_users,
    normalize_patients,
    normalize_patient_notes,
    normalize_devices,
    normalize_bp_readings,
    normalize_bg_readings,
    check_patient_db_constraints,
    add_id_col,
)


def test_keyword_search():
    keywords = {"Medicare": "medicare", "Medicaid": "medicaid"}
    assert keyword_search("I have Medicare", keywords) == "Medicare"
    assert keyword_search("I have medicaid", keywords) == "Medicaid"
    assert keyword_search("I have insurance", keywords) is np.nan


def test_keyword_list_search():
    keywords = {"Medicare": [["medicare"]], "Medicaid": [["medicaid"]]}
    assert keyword_list_search("I have Medicare", keywords) == "Medicare"
    assert keyword_list_search("I have medicaid", keywords) == "Medicaid"
    assert keyword_list_search("I have insurance", keywords) is np.nan


def test_extract_regex_pattern():
    pattern = re.compile(r"\d{3}-\d{2}-\d{4}")
    assert extract_regex_pattern("My SSN is 123-45-6789", pattern) == "123-45-6789"
    assert extract_regex_pattern("No SSN here", pattern) is np.nan


def test_standardize_name():
    assert standardize_name(" john doe ", r"[^a-zA-Z\s.-]") == "John Doe"


def test_standardize_email():
    assert standardize_email("  JOHN.DOE@EXAMPLE.COM  ") == "john.doe@example.com"
    assert standardize_email("invalid-email") is np.nan


def test_standardize_state():
    assert standardize_state("california") == "CA"
    assert standardize_state("invalid-state") == "INVALID-STATE"


def test_standardize_mbi():
    assert standardize_mbi(" 1EG4-TE5-MK73 ") == "1EG4TE5MK73"
    assert standardize_mbi("invalid-mbi") == "INVALID-MBI"


def test_standardize_dx_code():
    assert standardize_dx_code("E11.9") == "E119"
    assert standardize_dx_code("I10, E11.9") == "I10,E119"


def test_standardize_insurance_name():
    keywords = {"Medicare": [["medicare"]], "Medicaid": [["medicaid"]]}
    assert standardize_insurance_name("medicare advantage", keywords) == "Medicare"
    assert (
        standardize_insurance_name("unknown insurance", keywords) == "Unknown Insurance"
    )


def test_standardize_insurance_id():
    assert standardize_insurance_id(" abc-123-xyz ") == "ABC123XYZ"
    assert standardize_insurance_id("invalid-id") == "INVALID-ID"


def test_fill_primary_payer():
    row = pd.Series(
        {
            "Insurance Name:": np.nan,
            "Insurance ID:": np.nan,
            "Medicare ID number": "12345",
        }
    )
    assert fill_primary_payer(row) == "Medicare Part B"


def test_fill_primary_payer_id():
    row = pd.Series(
        {
            "Insurance Name:": "Medicare Part B",
            "Insurance ID:": np.nan,
            "Medicare ID number": "12345",
        }
    )
    assert fill_primary_payer_id(row) == "12345"


def test_standardize_call_time():
    assert standardize_call_time("1 days 02:30:00") == 93600
    assert standardize_call_time(None) == 0


def test_standardize_note_types():
    assert (
        standardize_note_types("Initial Evaluation with APRN") == "Initial Evaluation"
    )
    assert standardize_note_types("Follow-up, Check-up") == "Follow-up"


def test_standardize_vendor():
    row = pd.Series({"Vendor": "Omron", "Device_Name": "Omron Blood Pressure Monitor"})
    assert standardize_vendor(row) == "Omron"


def test_standardize_emcontact_relationship():
    keywords = {"Spouse": "spouse", "Child": "child"}
    assert standardize_emcontact_relationship("spouse", keywords) == "Spouse"
    assert standardize_emcontact_relationship("unknown", keywords) is np.nan


def test_standardize_race():
    keywords = {"White": [["white"]], "Black": [["black"]]}
    assert standardize_race("white", keywords) == "White"
    assert standardize_race("unknown", keywords) == "Unknown"


def test_standardize_weight():
    assert standardize_weight("150 lbs") == 150
    assert standardize_weight("invalid-weight") is np.nan


def test_standardize_height():
    assert standardize_height("5'8\"") == 68
    assert standardize_height("invalid-height") is np.nan


def test_create_patient_df():
    df = pd.DataFrame(
        {
            "first_name": ["John"],
            "last_name": ["Doe"],
            "middle_name": ["A"],
            "name_suffix": ["Jr"],
            "full_name": ["John A Doe Jr"],
            "nick_name": ["Johnny"],
            "date_of_birth": ["01/01/2000"],
            "sex": ["M"],
            "email": ["john.doe@example.com"],
            "phone_number": ["1234567890"],
            "social_security": ["123-45-6789"],
            "temp_race": ["White"],
            "temp_marital_status": ["Single"],
            "preferred_language": ["English"],
            "weight_lbs": [150],
            "height_in": [68],
            "sharepoint_id": [1],
            "temp_user": ["admin"],
        }
    )
    result = create_patient_df(df)
    assert result.shape == (1, 18)


def test_create_patient_address_df():
    df = pd.DataFrame(
        {
            "street_address": ["123 Main St"],
            "city": ["Anytown"],
            "temp_state": ["CA"],
            "zipcode": ["12345"],
            "sharepoint_id": [1],
        }
    )
    result = create_patient_address_df(df)
    assert result.shape == (1, 5)


def test_create_patient_insurance_df():
    df = pd.DataFrame(
        {
            "medicare_beneficiary_id": ["12345"],
            "primary_payer_id": ["67890"],
            "primary_payer_name": ["Medicare"],
            "secondary_payer_id": ["54321"],
            "secondary_payer_name": ["Medicaid"],
            "sharepoint_id": [1],
        }
    )
    result = create_patient_insurance_df(df)
    assert result.shape == (1, 6)


def test_create_med_necessity_df():
    df = pd.DataFrame(
        {
            "evaluation_datetime": ["2023-01-01"],
            "temp_dx_code": ["E11.9,I10"],
            "sharepoint_id": [1],
        }
    )
    result = create_med_necessity_df(df)
    assert result.shape == (2, 3)


def test_create_patient_status_df():
    df = pd.DataFrame({"temp_status_type": ["Active"], "sharepoint_id": [1]})
    result = create_patient_status_df(df)
    assert result.shape == (1, 4)


def test_create_emcontacts_df():
    df = pd.DataFrame(
        {
            "emergency_full_name": ["Jane Doe"],
            "emergency_phone_number": ["1234567890"],
            "emergency_relationship": ["Spouse"],
            "sharepoint_id": [1],
            "emergency_full_name2": ["John Smith"],
            "emergency_phone_number2": ["0987654321"],
            "emergency_relationship2": ["Friend"],
        }
    )
    result = create_emcontacts_df(df)
    assert result.shape == (2, 4)


def test_normalize_users():
    df = pd.DataFrame(
        {
            "givenName": ["John"],
            "surname": ["Doe"],
            "displayName": ["John Doe"],
            "mail": ["john.doe@example.com"],
            "id": ["12345"],
        }
    )
    result = normalize_users(df)
    assert result.shape == (1, 5)


def test_normalize_patients():
    df = pd.DataFrame(
        {
            "First Name": ["John"],
            "Last Name": ["Doe"],
            "Middle Name": ["A"],
            "Nickname": ["Johnny"],
            "Phone Number": ["123-456-7890"],
            "Gender": ["Male"],
            "Email": ["JOHN.DOE@EXAMPLE.COM"],
            "Suffix": ["Jr"],
            "Social Security": ["123-45-6789"],
            "Race": ["White"],
            "Weight": ["150 lbs"],
            "Height": ["5'8\""],
            "Mailing Address": ["123 Main St"],
            "City": ["Anytown"],
            "State": ["California"],
            "Zip code": ["12345"],
            "EmergencyName": ["Jane Doe"],
            "EmergencyNumber": ["123-456-7890"],
            "EmergencyRelationship": ["Spouse"],
            "EmergencyName2": ["John Smith"],
            "EmergencyNumber2": ["098-765-4321"],
            "EmergencyRelationship2": ["Friend"],
            "Medicare ID number": ["1EG4-TE5-MK73"],
            "DX_Code": ["E11.9,I10"],
            "Insurance ID:": ["abc-123-xyz"],
            "InsuranceName2": ["Medicaid"],
            "On-board Date": ["2023-01-01"],
            "Member_Status": ["Active"],
            "Health Coach": ["admin"],
            "ID": [1],
        }
    )
    result = normalize_patients(df)
    assert result.shape == (1, 40)


def test_normalize_patient_notes():
    df = pd.DataFrame(
        {
            "Recording_Time": ["1 days 02:30:00"],
            "Notes": ["<p>Note content</p>"],
            "Time_Note": ["Initial Evaluation with APRN"],
            "LCH_UPN": ["Joycelynn Harris"],
            "SharePoint_ID": ["1"],
            "Auto_Time": [True],
            "Start_Time": ["2023-01-01 00:00:00"],
            "End_Time": ["2023-01-01 00:15:00"],
        }
    )
    result = normalize_patient_notes(df)
    assert result.shape == (1, 10)


def test_normalize_devices():
    df = pd.DataFrame(
        {
            "Patient_ID": ["1"],
            "Device_ID": ["abc-123-xyz"],
            "Device_Name": ["Omron Blood Pressure Monitor"],
            "Vendor": ["Omron"],
        }
    )
    result = normalize_devices(df)
    assert result.shape == (1, 3)


def test_normalize_bp_readings():
    df = pd.DataFrame(
        {
            "SharePoint_ID": ["1"],
            "Device_Model": ["Omron"],
            "Time_Recorded": ["2023-01-01 00:00:00"],
            "Time_Recieved": ["2023-01-01 00:15:00"],
            "BP_Reading_Systolic": ["120"],
            "BP_Reading_Diastolic": ["80"],
            "Manual_Reading": [True],
        }
    )
    result = normalize_bp_readings(df)
    assert result.shape == (1, 7)


def test_normalize_bg_readings():
    df = pd.DataFrame(
        {
            "SharePoint_ID": ["1"],
            "Device_Model": ["Omron"],
            "Time_Recorded": ["2023-01-01 00:00:00"],
            "Time_Recieved": ["2023-01-01 00:15:00"],
            "BG_Reading": ["100"],
            "Manual_Reading": [True],
        }
    )
    result = normalize_bg_readings(df)
    assert result.shape == (1, 7)


def test_check_patient_db_constraints():
    df = pd.DataFrame(
        {
            "phone_number": ["1234567890"],
            "social_security": ["123456789"],
            "temp_state": ["CA"],
            "zipcode": ["12345"],
            "emergency_phone_number": ["1234567890"],
            "emergency_phone_number2": ["0987654321"],
            "medicare_beneficiary_id": ["1EG4TE5MK73"],
            "primary_payer_id": ["67890"],
            "secondary_payer_id": ["54321"],
        }
    )
    result = check_patient_db_constraints(df)
    assert result.shape == (1, 9)


def test_add_id_col():
    df = pd.DataFrame({"name": ["John Doe"], "age": [30]})
    id_df = pd.DataFrame({"name": ["John Doe"], "id": [1]})
    result = add_id_col(df, id_df, "name")
    assert result.shape == (1, 3)
    assert "name" not in result.columns
