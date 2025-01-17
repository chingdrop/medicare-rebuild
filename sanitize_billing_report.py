import re
import pandas as pd
from pathlib import Path

from enums import state_abbreviations


data_dir = Path.cwd() / 'data'
report_df = pd.read_excel(data_dir / 'BILLING_SHEET_123124.xlsx')

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
    if pd.isnull(row['Primary Payer']) and pd.isnull(row['Primary Payer ID']) and not pd.isnull(row['MedicareNumber']):
        return 'Medicare Part B'
    return row['Primary Payer']

def fill_primary_payer_id(row):
    if row['Primary Payer'] == 'Medicare Part B' and pd.isnull(row['Primary Payer ID']) :
        return row['MedicareNumber']
    return row['Primary Payer ID']

report_df['First Name'] = report_df['First Name'].str.replace(r'\s+', ' ', regex=True)
report_df['First Name'] = report_df['First Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
report_df['First Name'] = report_df['First Name'].str.strip().str.title()
report_df['Last Name'] = report_df['Last Name'].str.replace(r'\s+', ' ', regex=True)
report_df['Last Name'] = report_df['Last Name'].str.replace(r'[^a-zA-Z\s.-]', '', regex=True)
report_df['Last Name'] = report_df['Last Name'].str.strip().str.title()
report_df['Middle Name'] = report_df['Middle Name'].str.replace(r'[^a-zA-Z-\s]', '', regex=True)
report_df['Middle Name'] = report_df['Middle Name'].str.strip().str.title()
report_df['Phone Number'] = report_df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
report_df['Suffix'] = report_df['Suffix'].str.strip().str.title()

report_df['Address'] = report_df['Address'].str.replace(r'\s+', ' ', regex=True)
report_df['Address'] = report_df['Address'].str.replace(r'[^a-zA-Z0-9\s#.-/]', '', regex=True)
report_df['Address'] = report_df['Address'].str.strip().str.title()
report_df['City'] = report_df['City'].str.replace(r'\s+', ' ', regex=True)
report_df['City'] = report_df['City'].str.replace(r'[^a-zA-Z-]', '', regex=True)
report_df['City'] = report_df['City'].str.strip().str.title()
report_df['State'] = report_df['State'].apply(standardize_state)
report_df['State'] = report_df['State'].replace('NAN', None)
report_df['Zip code'] = report_df['Zip code'].astype(str).str.split('-', n=1).str[0]

# Regex pattern must have a capture group. i.e., ()
mbi_pattern = r'([A-Z0-9]{11})'
report_df['MedicareNumber'] = report_df['MedicareNumber'].str.strip().str.upper()
report_df['MedicareNumber'] = report_df['MedicareNumber'].str.extract(mbi_pattern)
report_df['DX_Code'] = report_df['DX_Code'].apply(standardize_dx_code)

# Regex pattern must have a capture group. i.e., ()
insurance_id_pattern = r'([A-Za-z]*\d+[A-Za-z]*\d*)'
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].str.extract(insurance_id_pattern)
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].fillna(report_df['Primary Payer'].str.extract(insurance_id_pattern))
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].str.strip().str.upper()
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].str.extract(insurance_id_pattern)
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].fillna(report_df['Secondary Payer'].str.extract(insurance_id_pattern))
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].str.strip().str.upper()
report_df['Primary Payer'] = report_df.apply(fill_primary_payer, axis=1)
report_df['Primary Payer ID'] = report_df.apply(fill_primary_payer_id, axis=1)

# failed_df = report_df[report_df['Phone Number'].apply(lambda x: len(str(x)) != 10)]
# failed_df = report_df[report_df['Zip code'].apply(lambda x: len(str(x)) != 5)]
# failed_df = report_df[report_df['MedicareNumber'].apply(lambda x: len(str(x)) != 11)]

report_df.to_csv(data_dir / 'final_billing_report.csv', index=False)
# failed_df.to_csv(data_dir / 'failed_billing_report.csv', index=False)