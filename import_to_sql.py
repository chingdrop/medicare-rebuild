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
address_df = export_df[['Mailing Address', 'City', 'State', 'Zip code']]
address_df = address_df.rename(
    columns= {
        'Mailing Address': 'street_address',
        'City': 'city',
        'State': 'temp_state',
        'Zip code': 'zipcode'
    }
)
insurance_df = export_df[['Medicare ID number', 'Insurance ID:', 'Insurance Name:', 'InsuranceID2', 'InsuranceName2']]
insurance_df = insurance_df.rename(
    columns={
        'Medicare ID number': 'medicare_beneficiary_id',
        'Insurance ID:': 'primary_payer_id',
        'Insurance Name:': 'primary_payer_name',
        'InsuranceID2': 'secondary_payer_id',
        'InsuranceName2': 'secondary_payer_name'
    }
)
med_nec_df = export_df[['On-board Date', 'DX_Code']]
med_nec_df = med_nec_df.rename(
    columns={
        'On-board Date': 'evaluation_datetime',
        'DX_Code': 'temp_dx_code'
    }
)

with engine.connect() as conn:
    

    patient_df.to_sql('patient', conn, if_exists="append", index=False)

