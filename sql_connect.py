import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL


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
my_engine = create_engine(connection_url)

@event.listens_for(my_engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True