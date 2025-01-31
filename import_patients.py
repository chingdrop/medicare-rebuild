import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from dataframe_utils import standardize_patients, check_database_constraints
from sql_connect import create_alchemy_engine

load_dotenv()
engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)

data_dir = Path.cwd() / 'data'
# Patient data MUST be exported from SharePoint first.
export_df = pd.read_csv(
    data_dir / 'Patient_Export.csv',
    dtype={
        'Phone Number': 'str',
        'Social Security': 'str',
        'Zip code': 'str'
    },
    parse_dates=['DOB', 'On-board Date']
)

export_df = standardize_patients(export_df)

# export_df, failed_df = check_database_constraints(export_df)
# failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)

patient_df = export_df[[
    'first_name',
    'last_name',
    'middle_name',
    'name_suffix',
    'full_name',
    'nick_name',
    'date_of_birth',
    'sex',
    'email',
    'phone_number',
    'social_security',
    'sharepoint_id'
]]
address_df = export_df[['street_address', 'city', 'temp_state', 'zipcode', 'sharepoint_id']]
insurance_df = export_df[[
    'medicare_beneficiary_id',
    'primary_payer_id',
    'primary_payer_name',
    'secondary_payer_id',
    'secondary_payer_name',
    'sharepoint_id'
]]
med_nec_df = export_df[['evaluation_datetime', 'temp_dx_code', 'sharepoint_id']]
med_nec_df.loc[:, 'temp_dx_code'] = med_nec_df['temp_dx_code'].str.split(',')
med_nec_df = med_nec_df.explode('temp_dx_code', ignore_index=True)
patient_status_df = export_df[['temp_status_type', 'sharepoint_id']]
patient_status_df['modified_date'] = pd.Timestamp.now()
patient_status_df['temp_user'] = 'ITHelp'

with engine.begin() as conn:
    # Patient data is imported first to get the patient_id.
    patient_df.to_sql('patient', conn, if_exists='append', index=False)
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)

    address_df = pd.merge(address_df, patient_id_df, on='sharepoint_id')
    address_df.drop(columns=['sharepoint_id'], inplace=True)
    insurance_df = pd.merge(insurance_df, patient_id_df, on='sharepoint_id')
    insurance_df.drop(columns=['sharepoint_id'], inplace=True)
    med_nec_df = pd.merge(med_nec_df, patient_id_df, on='sharepoint_id')
    med_nec_df.drop(columns=['sharepoint_id'], inplace=True)
    patient_status_df = pd.merge(patient_status_df, patient_id_df, on='sharepoint_id')
    patient_status_df.drop(columns=['sharepoint_id'], inplace=True)

    address_df.to_sql('patient_address', conn, if_exists='append', index=False)
    insurance_df.to_sql('patient_insurance', conn, if_exists='append', index=False)
    med_nec_df.to_sql('medical_necessity', conn, if_exists='append', index=False)
    patient_status_df.to_sql('patient_status', conn, if_exists='append', index=False)