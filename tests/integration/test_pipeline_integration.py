import numpy as np
import pandas as pd
import pytest

from medicare_rebuild.utils.dataframe_utils import (
    normalize_patients,
    check_patient_db_constraints,
    create_patient_df,
    create_patient_address_df,
    create_patient_insurance_df,
    create_med_necessity_df,
    create_patient_status_df,
    create_emcontacts_df,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def raw_patient_export() -> pd.DataFrame:
    """A small but realistic raw SharePoint patient export, covering the normal
    case, a patient with only one emergency contact, and multiple Dx codes.
    """
    return pd.DataFrame(
        {
            "First Name": ["John", "Jane", "Robert"],
            "Last Name": ["Doe", "Smith", "Jones"],
            "Middle Name": ["A", np.nan, "Lee"],
            "Nickname": ["Johnny", np.nan, "Bob"],
            "Phone Number": ["123-456-7890", "234-567-8901", "345-678-9012"],
            "Gender": ["Male", "Female", "Male"],
            "Email": [
                "JOHN.DOE@EXAMPLE.COM",
                "jane.smith@example.com",
                "invalid-email",
            ],
            "Suffix": ["Jr", np.nan, np.nan],
            "Social Security": ["123-45-6789", "234-56-7890", "345-67-8901"],
            "Race": ["White", "Black", "Unknown"],
            "Weight": ["150 lbs", "130 lbs", "invalid-weight"],
            "Height": ["5'8\"", "5'4\"", "invalid-height"],
            "Mailing Address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
            "City": ["Anytown", "Springfield", "Rivertown"],
            "State": ["California", "Texas", "invalid-state"],
            "Zip code": ["12345", "67890-1234", "54321"],
            "EmergencyName": ["Jane Doe", "John Smith", np.nan],
            "EmergencyNumber": ["123-456-7890", "234-567-8901", np.nan],
            "EmergencyName2": [np.nan, np.nan, "Alice Jones"],
            "EmergencyNumber2": [np.nan, np.nan, "456-789-0123"],
            "Medicare ID number": ["1EG4-TE5-MK73", "3CD5-UF6-LM84", "2AB4-CD5-EF67"],
            "DX_Code": ["E11.9,I10", "I10", "E11.9,R05,I10"],
            "Insurance ID:": ["abc-123-xyz", np.nan, "def-456-uvw"],
            "InsuranceName2": ["Medicaid", np.nan, np.nan],
            "On-board Date": ["2023-01-01", "2023-02-15", "2023-03-20"],
            "Member_Status": ["Active", "On-Board", "In-Active"],
            "Health Coach": ["admin", "admin2", "admin"],
            "Relationship_Status": ["Married", "Single", "Divorced"],
            "Preferred_Language": ["English", "Spanish", "English"],
            "DOB": ["01/01/1960", "02/02/1970", "03/03/1980"],
            "Insurance Name:": [np.nan, np.nan, "Kaiser"],
            "InsuranceID2": [np.nan, np.nan, "ghi-789-rst"],
            "ID": [1, 2, 3],
        }
    )


def test_pipeline_produces_internally_consistent_dataframes(raw_patient_export):
    normalized = normalize_patients(raw_patient_export)

    # Invalid weight/height/email produced nulls at normalize time rather than raising.
    robert = normalized.loc[normalized["sharepoint_id"] == 3].iloc[0]
    assert pd.isna(robert["weight_lbs"])
    assert pd.isna(robert["height_in"])
    assert pd.isna(robert["email"])

    constrained = check_patient_db_constraints(normalized)

    # Robert's unrecognized state ("invalid-state") doesn't resolve to a 2-letter
    # abbreviation, so temp_state fails the length constraint and his row is
    # dropped entirely -- only John and Jane make it through.
    assert sorted(constrained["sharepoint_id"]) == [1, 2]

    patient_df = create_patient_df(constrained)
    address_df = create_patient_address_df(constrained)
    insurance_df = create_patient_insurance_df(constrained)
    med_necessity_df = create_med_necessity_df(constrained)
    status_df = create_patient_status_df(constrained)
    emcontacts_df = create_emcontacts_df(constrained)

    # Every downstream frame's sharepoint_id set is a subset of the patient frame's.
    patient_ids = set(patient_df["sharepoint_id"])
    assert patient_ids == {1, 2}
    for df in (address_df, insurance_df, status_df, med_necessity_df, emcontacts_df):
        assert set(df["sharepoint_id"]) <= patient_ids

    # Dx codes explode one row per code: John has 2 (E11.9, I10), Jane has 1 (I10).
    assert med_necessity_df.shape[0] == 3
    assert sorted(
        med_necessity_df.loc[med_necessity_df["sharepoint_id"] == 1, "temp_dx_code"]
    ) == ["E119", "I10"]

    # Both John and Jane only have a first emergency contact on file; the blank
    # second slot is dropped by create_emcontacts_df's dropna.
    assert emcontacts_df.shape[0] == 2

    # standardize_state resolved real state names to abbreviations.
    assert address_df.loc[address_df["sharepoint_id"] == 1, "temp_state"].item() == "CA"
    assert address_df.loc[address_df["sharepoint_id"] == 2, "temp_state"].item() == "TX"

    # fill_primary_payer resolved Jane's missing insurance to Medicare Part B
    # since she has a Medicare ID and no other insurance on file.
    jane_insurance = insurance_df.loc[insurance_df["sharepoint_id"] == 2].iloc[0]
    assert jane_insurance["primary_payer_name"] == "Medicare Part B"

    # previous_patient_statuses remapping applied before status_df was carved out.
    assert (
        status_df.loc[status_df["sharepoint_id"] == 2, "temp_status_type"].item()
        == "Onboard"
    )


def test_pipeline_blank_fields_become_real_nulls_not_the_string_nan(raw_patient_export):
    normalized = normalize_patients(raw_patient_export)
    constrained = check_patient_db_constraints(normalized)
    patient_df = create_patient_df(constrained)

    # Jane's Middle Name/Nickname were np.nan in the raw export. standardize_name
    # runs str(name) on its input, which would turn a bare NaN into the literal
    # text "Nan" -- normalize_patients' final regex replace is what's supposed to
    # convert that back into a real null. Confirm it actually is null, not the
    # string "nan"/"Nan" masquerading as one.
    jane = patient_df.loc[patient_df["sharepoint_id"] == 2].iloc[0]
    assert pd.isna(jane["middle_name"])
    assert pd.isna(jane["nick_name"])
    assert jane["middle_name"] != "nan"
    assert jane["nick_name"] != "nan"
