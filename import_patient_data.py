import pandas as pd
from pathlib import Path

from sql_connect import engine


data_dir = Path.cwd() / 'data'

dtypes = {
    'Phone Number': 'str',
    'Social Security': 'str',
    'Zip code': 'str'
}
export_df = pd.read_csv(
    data_dir / 'final_patient_export.csv',
    dtype=dtypes,
    parse_dates=['DOB', 'On-board Date']
)

patient_df = export_df[['First Name', 'Last Name', 'Middle Name', 'Suffix', 'Full Name', 'Nickname', 'DOB', 'Gender', 'Email', 'Phone Number', 'Social Security', 'ID']]
patient_df = patient_df.rename(
    columns={
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'Middle Name': 'middle_name',
        'Suffix': 'name_suffix',
        'Full Name': 'full_name',
        'Nickname': 'nick_name',
        'DOB': 'date_of_birth',
        'Gender': 'sex',
        'Email': 'email', 
        'Phone Number': 'phone_number',
        'Social Security': 'social_security',
        'ID': 'sharepoint_id'
    }
)
address_df = export_df[['Mailing Address', 'City', 'State', 'Zip code', 'ID']]
address_df = address_df.rename(
    columns= {
        'Mailing Address': 'street_address',
        'City': 'city',
        'State': 'temp_state',
        'Zip code': 'zipcode',
        'ID': 'sharepoint_id'
    }
)
insurance_df = export_df[['Medicare ID number', 'Insurance ID:', 'Insurance Name:', 'InsuranceID2', 'InsuranceName2', 'ID']]
insurance_df = insurance_df.rename(
    columns={
        'Medicare ID number': 'medicare_beneficiary_id',
        'Insurance ID:': 'primary_payer_id',
        'Insurance Name:': 'primary_payer_name',
        'InsuranceID2': 'secondary_payer_id',
        'InsuranceName2': 'secondary_payer_name',
        'ID': 'sharepoint_id'
    }
)
med_nec_df = export_df[['On-board Date', 'DX_Code', 'ID']]
med_nec_df = med_nec_df.rename(
    columns={
        'On-board Date': 'evaluation_datetime',
        'DX_Code': 'temp_dx_code',
        'ID': 'sharepoint_id'
    }
)

# Using engine.begin for simpler connection and commit handling. 
with engine.begin() as conn:
    patient_df.to_sql('patient', conn, if_exists='append', index=False)
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)

    address_df = pd.merge(address_df, patient_id_df, on='sharepoint_id')
    address_df.drop(columns=['sharepoint_id'], inplace=True)
    insurance_df = pd.merge(insurance_df, patient_id_df, on='sharepoint_id')
    insurance_df.drop(columns=['sharepoint_id'], inplace=True)
    med_nec_df = pd.merge(med_nec_df, patient_id_df, on='sharepoint_id')
    med_nec_df.drop(columns=['sharepoint_id'], inplace=True)

    med_nec_df['temp_dx_code'] = med_nec_df['temp_dx_code'].str.split(',')
    med_nec_df = med_nec_df.explode('temp_dx_code', ignore_index=True)

    address_df.to_sql('patient_address', conn, if_exists='append', index=False)
    insurance_df.to_sql('patient_insurance', conn, if_exists='append', index=False)
    med_nec_df.to_sql('medical_necessity', conn, if_exists='append', index=False)