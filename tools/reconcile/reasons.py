"""Reason codes for patients the pipeline rejects, derived without changing it.

The pipeline drops patient rows in `check_patient_db_constraints` without recording
why. This module re-runs the pipeline's own `normalize_patients` on the source file
and assigns a reason code per violated length limit. The limits below are declared
once here and cross-checked against the pipeline's function on every run
(`agrees_with_pipeline`), so drift is reported instead of silently trusted.
"""

import warnings
from pathlib import Path

import pandas as pd

from medicare_rebuild.utils.dataframe_utils import (
    check_patient_db_constraints,
    create_emcontacts_df,
    create_med_necessity_df,
    normalize_patients,
)

# (normalized column, maximum length, reason code)
LIMITS = [
    ("phone_number", 11, "PHONE_LENGTH"),
    ("social_security", 9, "SSN_LENGTH"),
    ("temp_state", 2, "STATE_LENGTH"),
    ("zipcode", 5, "ZIP_LENGTH"),
    ("emergency_phone_number", 11, "EMERGENCY_PHONE_1_LENGTH"),
    ("emergency_phone_number2", 11, "EMERGENCY_PHONE_2_LENGTH"),
    ("medicare_beneficiary_id", 11, "MBI_LENGTH"),
    ("primary_payer_id", 30, "PRIMARY_PAYER_ID_LENGTH"),
    ("secondary_payer_id", 30, "SECONDARY_PAYER_ID_LENGTH"),
]


def read_export(path: Path | str) -> pd.DataFrame:
    """Read the patient export exactly as `DataImporter.get_patient_data` does."""
    return pd.read_csv(
        path,
        dtype={"Phone Number": "str", "Social Security": "str", "Zip code": "str"},
        parse_dates=["DOB", "On-board Date"],
    )


def normalize(raw: pd.DataFrame) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return normalize_patients(raw.copy())


def rejection_reasons(normalized: pd.DataFrame) -> dict[int, list[str]]:
    """Map row index -> reason codes, for rows that break at least one limit."""
    reasons: dict[int, list[str]] = {}
    for column, limit, code in LIMITS:
        too_long = normalized[column].apply(lambda v, n=limit: len(str(v)) > n)
        for idx in normalized.index[too_long]:
            reasons.setdefault(int(idx), []).append(code)
    return reasons


def reasons_by_patient_id(
    normalized: pd.DataFrame, by_index: dict[int, list[str]]
) -> dict[int, list[str]]:
    return {
        int(normalized.loc[idx, "sharepoint_id"]): sorted(codes)
        for idx, codes in by_index.items()
    }


def agrees_with_pipeline(
    normalized: pd.DataFrame, by_index: dict[int, list[str]]
) -> bool:
    """True when the rows kept by the pipeline's own check are exactly the rows
    with no reason code here."""
    kept = set(check_patient_db_constraints(normalized.copy()).index)
    return kept == set(normalized.index) - set(by_index)


def kept_rows(normalized: pd.DataFrame, by_index: dict[int, list[str]]) -> pd.DataFrame:
    return normalized.drop(index=list(by_index))


def expected_derived_rows(kept: pd.DataFrame) -> dict[str, int]:
    """Rows the patient-derived tables should hold, predicted with the pipeline's
    own create_* functions on the rows that survived the constraint check."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return {
            "patient_address": len(kept),
            "patient_insurance": len(kept),
            "patient_status": len(kept),
            "medical_necessity": len(create_med_necessity_df(kept)),
            "emergency_contact": len(create_emcontacts_df(kept)),
        }
