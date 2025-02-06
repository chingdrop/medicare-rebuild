import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from db_utils import DatabaseManager
from helpers import get_last_month_billing_cycle
from queries import update_patient_note_stmt, update_patient_status_stmt


load_dotenv()
dbm = DatabaseManager()
dbm.create_engine(
    'gps',
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)

updates_dir = Path.cwd() / 'queries' / 'updates'
medcode_params = {'today_date': datetime.strptime('2025-01-31', '%Y-%m-%d')}
with dbm.begin('gps') as conn:
    dbm.execute("EXEC reset_medical_code_tables", conn=conn)
    dbm.execute(update_patient_note_stmt, conn=conn)
    dbm.execute(update_patient_status_stmt, conn=conn)
    dbm.execute("EXEC batch_medcode_99202", conn=conn)
    dbm.execute("EXEC batch_medcode_99453_bg", conn=conn)
    dbm.execute("EXEC batch_medcode_99453_bp", conn=conn)
    dbm.execute("EXEC batch_medcode_99454_bg :today_date", medcode_params, conn=conn)
    dbm.execute("EXEC batch_medcode_99454_bp :today_date", medcode_params, conn=conn)
    dbm.execute("EXEC batch_medcode_99457 :today_date", medcode_params, conn=conn)
    dbm.execute("EXEC batch_medcode_99458 :today_date", medcode_params, conn=conn)
    
start_date, end_date = get_last_month_billing_cycle()
df = dbm.read_sql("EXEC create_billing_report @start_date = ?, @end_date = ?", 'gps', params=(start_date, end_date))
df.to_excel(Path.cwd() / 'data' / 'LCH_Billing_Report.xlsx', index=False, engine='openpyxl')