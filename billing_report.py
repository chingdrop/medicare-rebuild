import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from db_utils import DatabaseManager
from helpers import read_file, get_last_month_billing_cycle


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
update_patient_note_stmt = read_file(updates_dir / 'update_patient_note.sql', encoding='utf-8-sig')
update_patient_status_stmt = read_file(updates_dir / 'update_patient_status.sql', encoding='utf-8-sig')
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
billing_report_params = {'start_date': start_date, 'end_date': end_date}
df = dbm.read_sql("EXEC create_billing_report :start_date, :end_date", 'gps', params=billing_report_params)
df.to_excel(Path.cwd() / 'data' / 'LCH_Billing_Report.xlsx', index=False, engine='openpyxl')