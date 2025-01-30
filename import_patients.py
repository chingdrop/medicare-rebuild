import os
import re
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from enums import state_abbreviations, insurance_keywords
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

def standardize_state(state):
    state = str(state).strip().title()
    return state_abbreviations.get(state, state).upper()

def standardize_dx_code(dx_code):
    dx_code = str(dx_code).strip()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

def standardize_insurance_name(name):
    for standard_name, keyword_sets in insurance_keywords.items():
        for keyword_set in keyword_sets:
            if all(re.search(r'\b' + re.escape(keyword) + r'\b', str(name).lower()) for keyword in keyword_set):
                return standard_name
    return None

def fill_primary_payer(row):
    if pd.isnull(row['Insurance Name:']) and pd.isnull(row['Insurance ID:']) and not pd.isnull(row['Medicare ID number']):
        return 'Medicare Part B'
    return row['Insurance Name:']

def fill_primary_payer_id(row):
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']

export_df['First Name'] = export_df['First Name'].str.replace(r'\s+', ' ', regex=True)
export_df['First Name'] = export_df['First Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
export_df['First Name'] = export_df['First Name'].str.strip().str.title()
export_df['Last Name'] = export_df['Last Name'].str.replace(r'\s+', ' ', regex=True)
export_df['Last Name'] = export_df['Last Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
export_df['Last Name'] = export_df['Last Name'].str.strip().str.title()
export_df['Full Name'] = export_df['First Name'] + ' ' + export_df['Last Name']
export_df['Middle Name'] = export_df['Middle Name'].str.replace(r'[^a-zA-Z-\s]', '', regex=True)
export_df['Middle Name'] = export_df['Middle Name'].str.strip().str.title()
export_df['Nickname'] = export_df['Nickname'].str.strip().str.title()
export_df['Phone Number'] = export_df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
export_df['Gender'] = export_df['Gender'].replace({'Male': 'M', 'Female': 'F'})
# Regex pattern must have a capture group for extraction. i.e., ()
email_pattern = r'(^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$)'
export_df['Email'] = export_df['Email'].str.extract(email_pattern)
export_df['Suffix'] = export_df['Suffix'].str.strip().str.title()
export_df['Social Security'] = export_df['Social Security'].astype(str).str.replace(r'\D', '', regex=True)

export_df['Mailing Address'] = export_df['Mailing Address'].str.replace(r'\s+', ' ', regex=True)
export_df['Mailing Address'] = export_df['Mailing Address'].str.replace(r'[^a-zA-Z0-9\s#.-/]', '', regex=True)
export_df['Mailing Address'] = export_df['Mailing Address'].str.strip().str.title()
export_df['City'] = export_df['City'].str.replace(r'\s+', ' ', regex=True)
export_df['City'] = export_df['City'].str.replace(r'[^a-zA-Z-]', '', regex=True)
export_df['City'] = export_df['City'].str.strip().str.title()
export_df['State'] = export_df['State'].apply(standardize_state)
export_df['State'] = export_df['State'].replace('NAN', None)
export_df['Zip code'] = export_df['Zip code'].astype(str).str.split('-', n=1).str[0]

# Regex pattern must have a capture group for extraction. i.e., ()
# Only matching capital letters since I use upper() on value.
mbi_pattern = r'([A-Z0-9]{11})'
# Medicare formatting/extraction MUST be done first since the MBI is used in Null fills.
export_df['Medicare ID number'] = export_df['Medicare ID number'].str.strip().str.upper()
export_df['Medicare ID number'] = export_df['Medicare ID number'].str.extract(mbi_pattern)[0]

export_df['DX_Code'] = export_df['DX_Code'].apply(standardize_dx_code)

# Regex pattern must have a capture group for extraction. i.e., ()
# Only matching capital letters since I use upper() on value.
insurance_id_pattern = r'([A-Z]*\d+[A-Z]*\d*)'
export_df['Insurance ID:'] = export_df['Insurance ID:'].str.strip().str.upper()
export_df['Insurance ID:'] = export_df['Insurance ID:'].str.extract(insurance_id_pattern)[0]
# Sometimes Insurance ID is placed in Insurance Name.
export_df['Insurance ID:'] = export_df['Insurance ID:'].fillna(export_df['Insurance Name:'].str.extract(insurance_id_pattern)[0])
export_df['InsuranceID2'] = export_df['InsuranceID2'].str.strip().str.upper()
export_df['InsuranceID2'] = export_df['InsuranceID2'].str.extract(insurance_id_pattern)[0]
# Sometimes Insurance ID is placed in Insurance Name.
export_df['InsuranceID2'] = export_df['InsuranceID2'].fillna(export_df['InsuranceName2'].str.extract(insurance_id_pattern)[0])
# Insurane names are standardized after Insurance ID extractions to preserve original values.
export_df['Insurance Name:'] = export_df['Insurance Name:'].apply(standardize_insurance_name)
export_df['InsuranceName2'] = export_df['InsuranceName2'].apply(standardize_insurance_name)
# Null fills are done after regex extraction to preserve original values.
export_df['Insurance Name:'] = export_df.apply(fill_primary_payer, axis=1)
export_df['Insurance ID:'] = export_df.apply(fill_primary_payer_id, axis=1)

previous_patient_statuses = {
    'DO NOT CALL': 'Do Not Call' ,
    'In-Active': 'Inactive',
    'On-Board': 'Onboard'
}
export_df['Member_Status'] = export_df['Member_Status'].replace(previous_patient_statuses)

# Check for database constraints and replace with Null if failed condition.
export_df['Phone Number'] = export_df['Phone Number'].apply(lambda x: x if len(str(x)) == 10 else None)
export_df['Social Security'] = export_df['Social Security'].apply(lambda x: x if len(str(x)) == 9 else None)
export_df['Zip code'] = export_df['Zip code'].apply(lambda x: x if len(str(x)) == 5 else None)
export_df['Medicare ID number'] = export_df['Medicare ID number'].apply(lambda x: x if len(str(x)) == 11 else None)
failed_df = export_df[export_df[['First Name', 'Last Name', 'ID', 'DOB', 'Phone Number', 'Gender', 'Mailing Address', 'City', 'State', 'Zip code', 'DX_Code']].isnull().any(axis=1)]
failed_df = export_df[export_df[['Insurance ID:', 'Insurance Name:', 'Medicare ID number']].isnull().all(axis=1)]

failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)

export_df = export_df.rename(
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
        'ID': 'sharepoint_id',
        'Mailing Address': 'street_address',
        'City': 'city',
        'State': 'temp_state',
        'Zip code': 'zipcode',
        'Medicare ID number': 'medicare_beneficiary_id',
        'Insurance ID:': 'primary_payer_id',
        'Insurance Name:': 'primary_payer_name',
        'InsuranceID2': 'secondary_payer_id',
        'InsuranceName2': 'secondary_payer_name',
        'On-board Date': 'evaluation_datetime',
        'DX_Code': 'temp_dx_code',
        'Member_Status': 'temp_status_type'
    }
)
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