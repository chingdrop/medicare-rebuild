import re
import pandas as pd
from pathlib import Path

from enums import state_abbreviations, insurance_keywords


data_dir = Path.cwd() / 'data'
export_df = pd.read_csv(data_dir / 'Patient_Export.csv')

# Data Standardization
def standardize_state(state):
    state = str(state).strip().title()
    return state_abbreviations.get(state, state).upper()

def standardize_dx_code(dx_code):
    dx_code = str(dx_code).strip()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

def fill_primary_payer(row):
    if pd.isnull(row['Insurance Name:']) and pd.isnull(row['Insurance ID:']) and not pd.isnull(row['Medicare ID number']):
        return 'Medicare Part B'
    return row['Insurance Name:']

def fill_primary_payer_id(row):
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']

def standardize_insurance_name(name):
    for standard_name, keyword_sets in insurance_keywords.items():
        for keyword_set in keyword_sets:
            if all(re.search(r'\b' + re.escape(keyword) + r'\b', str(name).lower()) for keyword in keyword_set):
                return standard_name
    return None

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
# Regex pattern must have a capture group. i.e., ()
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

# Regex pattern must have a capture group. i.e., ()
# Only matching capital letters since I use upper() on value.
mbi_pattern = r'([A-Z0-9]{11})'
export_df['Medicare ID number'] = export_df['Medicare ID number'].str.strip().str.upper()
export_df['Medicare ID number'] = export_df['Medicare ID number'].str.extract(mbi_pattern)[0]

# Regex pattern must have a capture group. i.e., ()
# Only matching capital letters since I use upper() on value.
insurance_id_pattern = r'([A-Z]*\d+[A-Z]*\d*)'
export_df['Insurance ID:'] = export_df['Insurance ID:'].str.strip().str.upper()
export_df['Insurance ID:'] = export_df['Insurance ID:'].str.extract(insurance_id_pattern)[0]
export_df['Insurance ID:'] = export_df['Insurance ID:'].fillna(export_df['Insurance Name:'].str.extract(insurance_id_pattern)[0])
export_df['InsuranceID2'] = export_df['InsuranceID2'].str.strip().str.upper()
export_df['InsuranceID2'] = export_df['InsuranceID2'].str.extract(insurance_id_pattern)[0]
export_df['InsuranceID2'] = export_df['InsuranceID2'].fillna(export_df['InsuranceName2'].str.extract(insurance_id_pattern)[0])
export_df['Insurance Name:'] = export_df['Insurance Name:'].apply(standardize_insurance_name)
export_df['InsuranceName2'] = export_df['InsuranceName2'].apply(standardize_insurance_name)
export_df['Insurance Name:'] = export_df.apply(fill_primary_payer, axis=1)
export_df['Insurance ID:'] = export_df.apply(fill_primary_payer_id, axis=1)

export_df['DX_Code'] = export_df['DX_Code'].apply(standardize_dx_code)

export_df['Phone Number'] = export_df['Phone Number'].apply(lambda x: x if len(str(x)) == 10 else None)
export_df['Social Security'] = export_df['Social Security'].apply(lambda x: x if len(str(x)) == 9 else None)
export_df['Zip code'] = export_df['Zip code'].apply(lambda x: x if len(str(x)) == 5 else None)
export_df['Medicare ID number'] = export_df['Medicare ID number'].apply(lambda x: x if len(str(x)) == 11 else None)
failed_df = export_df[export_df[['First Name', 'Last Name', 'ID', 'DOB', 'Phone Number', 'Gender', 'Mailing Address', 'City', 'State', 'Zip code', 'DX_Code']].isnull().any(axis=1)]
failed_df = export_df[export_df[['Insurance ID:', 'Insurance Name:', 'Medicare ID number']].isnull().all(axis=1)]

export_df.to_csv(data_dir / 'final_patient_export.csv', index=False)
failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)
