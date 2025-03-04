import os
import warnings
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from db_utils import DatabaseManager
from helpers import get_last_month_billing_cycle
from logger import setup_logger


warnings.filterwarnings("ignore")
load_dotenv()
logger = setup_logger('billing_report', level='debug')

gps = DatabaseManager(logger=logger)
gps.create_engine(
    username=os.getenv('GPS_SQL_USERNAME'),
    password=os.getenv('GPS_SQL_PASSWORD'),
    host=os.getenv('GPS_SQL_HOST'),
    database=os.getenv('GPS_SQL_DB')
)

updates_dir = Path.cwd() / 'queries' / 'updates'
medcode_params = {'today_date': datetime.strptime('2025-01-31', '%Y-%m-%d')}
gps.execute_query("EXEC reset_medical_code_tables")
gps.execute_query("EXEC batch_medcode_99202")
gps.execute_query("EXEC batch_medcode_99453_bg")
gps.execute_query("EXEC batch_medcode_99453_bp")
gps.execute_query("EXEC batch_medcode_99454_bg :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99454_bp :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99457 :today_date", medcode_params)
gps.execute_query("EXEC batch_medcode_99458 :today_date", medcode_params)
    
start_date, end_date = get_last_month_billing_cycle()
df = gps.read_sql("EXEC create_billing_report @start_date = ?, @end_date = ?",
                  params=(start_date, end_date))
df.to_excel(Path.cwd() / 'data' / 'Billing_Report.xlsx', index=False, engine='openpyxl')
gps.close()