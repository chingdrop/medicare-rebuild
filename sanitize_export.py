import re
import pandas as pd
from pathlib import Path

from enums import state_abbreviations


data_dir = Path.cwd() / 'data'
patient_df = pd.read_csv(data_dir / 'Patient_Export.csv')

# Data Standardization
def standardize_state(state):
    state = str(state).strip().title()
    return state_abbreviations.get(state, state).upper()

def standardize_dx_code(dx_code):
    dx_code = str(dx_code).strip()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

patient_df['First Name'] = patient_df['First Name'].str.replace(r'\s+', ' ', regex=True)
patient_df['First Name'] = patient_df['First Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
patient_df['First Name'] = patient_df['First Name'].str.strip().str.title()
patient_df['Last Name'] = patient_df['Last Name'].str.replace(r'\s+', ' ', regex=True)
patient_df['Last Name'] = patient_df['Last Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
patient_df['Last Name'] = patient_df['Last Name'].str.strip().str.title()
patient_df['Full Name'] = patient_df['First Name'] + ' ' + patient_df['Last Name']
patient_df['Middle Name'] = patient_df['Middle Name'].str.replace(r'[^a-zA-Z-\s]', '', regex=True)
patient_df['Middle Name'] = patient_df['Middle Name'].str.strip().str.title()
patient_df['Nickname'] = patient_df['Nickname'].str.strip().str.title()
patient_df['Phone Number'] = patient_df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
patient_df['Phone Number'] = patient_df['Phone Number'].apply(lambda x: x if len(str(x)) == 10 else None)
patient_df['Gender'] = patient_df['Gender'].replace({'Male': 'M', 'Female': 'F'})
email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
patient_df['Email'] = patient_df['Email'].apply(lambda x: x if re.match(email_pattern, str(x)) else None)
patient_df['Suffix'] = patient_df['Suffix'].str.strip()
patient_df['Social Security'] = patient_df['Social Security'].astype(str).str.replace(r'\D', '', regex=True)
patient_df['Social Security'] = patient_df['Social Security'].apply(lambda x: x if len(str(x)) == 9 else None)

patient_df['Mailing Address'] = patient_df['Mailing Address'].str.replace(r'\s+', ' ', regex=True)
patient_df['Mailing Address'] = patient_df['Mailing Address'].str.replace(r'[^a-zA-Z0-9\s#.-/]', '', regex=True)
patient_df['Mailing Address'] = patient_df['Mailing Address'].str.strip().str.title()
patient_df['City'] = patient_df['City'].str.replace(r'\s+', ' ', regex=True)
patient_df['City'] = patient_df['City'].str.replace(r'[^a-zA-Z-]', '', regex=True)
patient_df['City'] = patient_df['City'].str.strip().str.title()
patient_df['State'] = patient_df['State'].apply(standardize_state)
patient_df['State'] = patient_df['State'].replace('NAN', None)
patient_df['Zip code'] = patient_df['Zip code'].astype(str).str.split('-', n=1).str[0]
patient_df['Zip code'] = patient_df['Zip code'].apply(lambda x: x if len(str(x)) == 5 else None)

patient_df['Insurance ID:'] = patient_df['Insurance ID:'].str.strip().str.upper()
patient_df['Insurance Name:'] = patient_df['Insurance Name:'].str.strip().str.title()
patient_df['InsuranceID2'] = patient_df['InsuranceID2'].str.strip().str.upper()
patient_df['InsuranceName2'] = patient_df['InsuranceName2'].str.strip().str.title()
patient_df['Medicare ID number'] = patient_df['Medicare ID number'].str.strip().str.upper()
patient_df['Medicare ID number'] = patient_df['Medicare ID number'].apply(lambda x: x if len(str(x)) == 11 else None)
patient_df['DX_Code'] = patient_df['DX_Code'].apply(standardize_dx_code)

patient_df.to_csv(data_dir / 'final_patient_export.csv')
