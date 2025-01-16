import re
import pandas as pd
from pathlib import Path

from enums import state_abbreviations


data_dir = Path.cwd() / 'data'
temp_df = pd.read_csv(data_dir / 'Patient_Export.csv')

# Data Standardization
def standardize_state(state):
    state = str(state).strip().title()
    return state_abbreviations.get(state, state).upper()

def standardize_dx_code(dx_code):
    dx_code = str(dx_code).strip()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

temp_df['First Name'] = temp_df['First Name'].str.replace(r'\s+', ' ', regex=True)
temp_df['First Name'] = temp_df['First Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
temp_df['First Name'] = temp_df['First Name'].str.strip().str.title()
temp_df['Last Name'] = temp_df['Last Name'].str.replace(r'\s+', ' ', regex=True)
temp_df['Last Name'] = temp_df['Last Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
temp_df['Last Name'] = temp_df['Last Name'].str.strip().str.title()
temp_df['Full Name'] = temp_df['First Name'] + ' ' + temp_df['Last Name']
temp_df['Middle Name'] = temp_df['Middle Name'].str.replace(r'[^a-zA-Z-\s]', '', regex=True)
temp_df['Middle Name'] = temp_df['Middle Name'].str.strip().str.title()
temp_df['Nickname'] = temp_df['Nickname'].str.strip().str.title()
temp_df['Phone Number'] = temp_df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
temp_df['Phone Number'] = temp_df['Phone Number'].apply(lambda x: x if len(str(x)) == 10 else None)
temp_df['Gender'] = temp_df['Gender'].replace({'Male': 'M', 'Female': 'F'})
email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
temp_df['Email'] = temp_df['Email'].apply(lambda x: x if re.match(email_pattern, str(x)) else None)
temp_df['Suffix'] = temp_df['Suffix'].str.strip()
temp_df['Social Security'] = temp_df['Social Security'].astype(str).str.replace(r'\D', '', regex=True)
temp_df['Social Security'] = temp_df['Social Security'].apply(lambda x: x if len(str(x)) == 9 else None)
patient_df = temp_df[['First Name', 'Last Name', 'Middle Name', 'Suffix', 'Full Name', 'Nickname', 'DOB', 'Gender', 'Email', 'Phone Number', 'Social Security', 'ID']]

temp_df['Mailing Address'] = temp_df['Mailing Address'].str.replace(r'\s+', ' ', regex=True)
temp_df['Mailing Address'] = temp_df['Mailing Address'].str.replace(r'[^a-zA-Z0-9\s#.-/]', '', regex=True)
temp_df['Mailing Address'] = temp_df['Mailing Address'].str.strip().str.title()
temp_df['City'] = temp_df['City'].str.replace(r'\s+', ' ', regex=True)
temp_df['City'] = temp_df['City'].str.replace(r'[^a-zA-Z-]', '', regex=True)
temp_df['City'] = temp_df['City'].str.strip().str.title()
temp_df['State'] = temp_df['State'].apply(standardize_state)
temp_df['State'] = temp_df['State'].replace('NAN', None)
temp_df['Zip code'] = temp_df['Zip code'].astype(str).str.split('-', n=1).str[0]
temp_df['Zip code'] = temp_df['Zip code'].apply(lambda x: x if len(str(x)) == 5 else None)
address_df = temp_df[['Mailing Address', 'City', 'State', 'Zip code']]

temp_df['Insurance ID:'] = temp_df['Insurance ID:'].str.strip().str.upper()
temp_df['Insurance Name:'] = temp_df['Insurance Name:'].str.strip().str.title()
temp_df['InsuranceID2'] = temp_df['InsuranceID2'].str.strip().str.upper()
temp_df['InsuranceName2'] = temp_df['InsuranceName2'].str.strip().str.title()
temp_df['Medicare ID number'] = temp_df['Medicare ID number'].str.strip().str.upper()
temp_df['Medicare ID number'] = temp_df['Medicare ID number'].apply(lambda x: x if len(str(x)) == 11 else None)
insurance_df = temp_df[['Medicare ID number', 'Insurance ID:', 'Insurance Name:', 'InsuranceID2', 'InsuranceName2']]

temp_df['DX_Code'] = temp_df['DX_Code'].apply(standardize_dx_code)
med_nec_df = temp_df[['On-board Date', 'DX_Code']]

temp_df.to_csv(data_dir / 'final_patient_export.csv')
