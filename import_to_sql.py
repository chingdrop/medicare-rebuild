import pandas as pd
from pathlib import Path


data_dir = Path.cwd() / 'data'
export_df = pd.read_csv(data_dir / 'final_patient_export.csv')

patient_df = export_df[['First Name', 'Last Name', 'Middle Name', 'Suffix', 'Full Name', 'Nickname', 'DOB', 'Gender', 'Email', 'Phone Number', 'Social Security', 'ID']]
address_df = export_df[['Mailing Address', 'City', 'State', 'Zip code']]
insurance_df = export_df[['Medicare ID number', 'Insurance ID:', 'Insurance Name:', 'InsuranceID2', 'InsuranceName2']]
med_nec_df = export_df[['On-board Date', 'DX_Code']]
