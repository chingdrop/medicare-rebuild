import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from db_utils import DatabaseManager
from helpers import read_file


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
update_patient_note_stmt = read_file(queries_dir / 'updates' / 'update_patient_note.sql', encoding="utf-8-sig")
sproc_params = {'today_date': datetime.strptime('2025-01-31', '%Y-%m-%d')}
with dbm.begin('gps') as conn:
    dbm.execute(update_patient_note_stmt, conn=conn)
    dbm.execute("EXEC batch_medcode_99202", conn=conn)
    dbm.execute("EXEC batch_medcode_99453", conn=conn)
    dbm.execute("EXEC batch_medcode_99454 :today_date", sproc_params, conn=conn)
    dbm.execute("EXEC batch_medcode_99457 :today_date", sproc_params, conn=conn)
    dbm.execute("EXEC batch_medcode_99458 :today_date", sproc_params, conn=conn)
    
df = dbm.read_sql("EXEC create_billing_report", 'gps')
df.to_excel(Path.cwd() / 'data' / 'test_billing_report.xlsx', index=False, engine='openpyxl')