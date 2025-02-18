import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from db_utils import DatabaseManager
from helpers import get_last_month_billing_cycle
from queries import update_patient_note_stmt, update_patient_status_stmt


load_dotenv()
gps = DatabaseManager()
gps.create_engine(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)

updates_dir = Path.cwd() / 'queries' / 'updates'
medcode_params = {'today_date': datetime.strptime('2025-01-31', '%Y-%m-%d')}
gps.execute_query("EXEC reset_medical_code_tables")
gps.execute_query(update_patient_note_stmt)
gps.execute_query(update_patient_status_stmt)
gps.execute_query("EXEC batch_medcode_99202")
gps.execute_query("EXEC batch_medcode_99453_bg")
gps.execute_query("EXEC batch_medcode_99453_bp")
gps.execute_query("EXEC batch_medcode_99454_bg :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99454_bp :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99457 :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99458 :today_date", medcode_params)
    
start_date, end_date = get_last_month_billing_cycle()
df = gps.read_sql("EXEC create_billing_report @start_date = ?, @end_date = ?", params=(start_date, end_date))
df.to_excel(Path.cwd() / 'data' / 'LCH_Billing_Report.xlsx', index=False, engine='openpyxl')
gps.close()