import pandas as pd
from pathlib import Path

from dataframe_utils import standardize_patients, patient_check_db_constraints, \
    create_patient_df, create_patient_address_df, create_patient_insurance_df, \
    create_med_necessity_df, create_patient_status_df, create_emcontacts_df


def snap_patient_data(filename) -> None:
    export_df = pd.read_csv(
        filename,
        dtype={
            'Phone Number': 'str',
            'Social Security': 'str',
            'Zip code': 'str'
        },
        parse_dates=['DOB', 'On-board Date']
    )
    export_df = standardize_patients(export_df)
    export_df = patient_check_db_constraints(export_df)
    patient_df = create_patient_df(export_df)
    address_df = create_patient_address_df(export_df)
    insurance_df = create_patient_insurance_df(export_df)
    med_nec_df = create_med_necessity_df(export_df)
    patient_status_df = create_patient_status_df(export_df)
    emcontacts_df = create_emcontacts_df(export_df)

    data_dir = Path.cwd() / 'data'
    patient_df.to_excel(data_dir / 'snap_patient_df.xlsx', index=False, engine='openpyxl')
    address_df.to_excel(data_dir / 'snap_patient_address_df.xlsx', index=False, engine='openpyxl')
    insurance_df.to_excel(data_dir / 'snap_patient_insurance_df.xlsx', index=False, engine='openpyxl')
    med_nec_df.to_excel(data_dir / 'snap_med_necessity_df.xlsx', index=False, engine='openpyxl')
    patient_status_df.to_excel(data_dir / 'snap_patient_status_df.xlsx', index=False, engine='openpyxl')
    emcontacts_df.to_excel(data_dir / 'snap_emcontacts_df.xlsx', index=False, engine='openpyxl')


snap_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv')