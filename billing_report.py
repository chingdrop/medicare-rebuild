import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text

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
with dbm.begin('gps') as conn:
    update_patient_note_stmt = read_sql_file(queries_dir / 'updates' / 'update_patient_note.sql', encoding="utf-8-sig")
    conn.execute(text(update_patient_note_stmt))
    conn.execute(text("EXEC batch_medcode_99202"))
    conn.execute(text("EXEC batch_medcode_99453"))
    conn.execute(text("EXEC batch_medcode_99454"))
    conn.execute(text("EXEC batch_medcode_99457"))
    conn.execute(text("EXEC batch_medcode_99458"))
    df = dbm.read_sql_query("EXEC create_billing_report", conn)
    
df.to_excel(Path.cwd() / 'data' / 'test_billing_report.xlsx', index=False, engine='openpyxl')