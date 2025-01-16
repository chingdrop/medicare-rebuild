import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pathlib import Path


load_dotenv()
connection_url = URL.create(
    "mssql+pyodbc",
    username=os.getenv('SQL_USERNAME'),
    password=os.getenv('SQL_PASSWORD'),
    host=os.getenv('SQL_HOST'),
    port=1433,
    database=os.getenv('SQL_DB'),
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate": "yes",
    },
)
engine = create_engine(connection_url)

data_dir = Path.cwd() / 'data'
export_df = pd.read_csv(data_dir / 'final_patient_export.csv')

patient_df = export_df[['First Name', 'Last Name', 'Middle Name', 'Suffix', 'Full Name', 'Nickname', 'DOB', 'Gender', 'Email', 'Phone Number', 'Social Security', 'ID']]
address_df = export_df[['Mailing Address', 'City', 'State', 'Zip code']]
insurance_df = export_df[['Medicare ID number', 'Insurance ID:', 'Insurance Name:', 'InsuranceID2', 'InsuranceName2']]
med_nec_df = export_df[['On-board Date', 'DX_Code']]

# try:
#     with engine.connect() as connection:
#         result = connection.execute(text("SELECT 1"))
#         print("Connection successful. Test query result:", result.scalar())
# except Exception as e:
#     print("Connection failed:", e)
