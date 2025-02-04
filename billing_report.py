import os
from dotenv import load_dotenv
from pathlib import Path

from db_utils import DatabaseManager
from helpers import read_sql_file


load_dotenv()
dbm = DatabaseManager()
dbm.create_engine(
    'gps',
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)

queries_dir = Path.cwd() / 'queries'
update_patient_note_stmt = read_sql_file(queries_dir / 'updates' / 'update_patient_note.sql', encoding="utf-8-sig")
with dbm.begin('gps') as conn:
    dbm.execute(update_patient_note_stmt, conn)
    dbm.execute("EXEC batch_medcode_99202", conn)
    dbm.execute("EXEC batch_medcode_99453", conn)
    dbm.execute("EXEC batch_medcode_99454", conn)
    dbm.execute("EXEC batch_medcode_99457", conn)
    dbm.execute("EXEC batch_medcode_99458", conn)
    
df = dbm.read_sql("EXEC create_billing_report", 'gps')
df.to_excel(Path.cwd() / 'data' / 'test_billing_report.xlsx', index=False, engine='openpyxl')