import re
import pandas as pd
from pathlib import Path

from enums import state_abbreviations, insurance_keywords


data_dir = Path.cwd() / 'data'
report_df = pd.read_excel(data_dir / 'BILLING_SHEET_123124.xlsx')

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
    return name

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

# Regex pattern must have a capture group for extraction. i.e., ()
# Only matching capital letters since I use upper() on value.
mbi_pattern = r'([A-Z0-9]{11})'
# Medicare formatting/extraction MUST be done first since the MBI is used in Null fills.
report_df['MedicareNumber'] = report_df['MedicareNumber'].str.strip().str.upper()
report_df['MedicareNumber'] = report_df['MedicareNumber'].str.extract(mbi_pattern)[0]

report_df['DX_Code'] = report_df['DX_Code'].apply(standardize_dx_code)

# Regex pattern must have a capture group for extraction. i.e., ()
# Only matching capital letters since I use upper() on value.
insurance_id_pattern = r'([A-Z]*\d+[A-Z]*\d*)'
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].str.strip().str.upper()
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].str.extract(insurance_id_pattern)[0]
# Sometimes Insurance ID is placed in Insurance Name.
report_df['Primary Payer ID'] = report_df['Primary Payer ID'].fillna(report_df['Primary Payer'].str.extract(insurance_id_pattern)[0])
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].str.strip().str.upper()
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].str.extract(insurance_id_pattern)[0]
# Sometimes Insurance ID is placed in Insurance Name.
report_df['Secondary Payer ID'] = report_df['Secondary Payer ID'].fillna(report_df['Secondary Payer'].str.extract(insurance_id_pattern)[0])
# Insurane names are standardized after Insurance ID extractions to preserve original values.
report_df['Primary Payer'] = report_df['Primary Payer'].apply(standardize_insurance_name)
report_df['Secondary Payer'] = report_df['Secondary Payer'].apply(standardize_insurance_name)
# Null fills are done after regex extraction to preserve original values.
report_df['Primary Payer'] = report_df.apply(fill_primary_payer, axis=1)
report_df['Primary Payer ID'] = report_df.apply(fill_primary_payer_id, axis=1)

report_df.to_csv(data_dir / 'final_billing_report.csv', index=False)