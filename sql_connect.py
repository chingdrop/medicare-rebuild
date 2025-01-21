from sqlalchemy import create_engine
from sqlalchemy.engine import URL, default


def my_creator(*args, **kwargs):
    connection = default.DefaultConnection(*args, **kwargs)
    if hasattr(connection.connection, 'fast_executemany'):
        connection.connection.fast_executemany = True
    return connection

def create_alchemy_engine(username, password, host, database):
    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=host,
        port=1433,
        database=database,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
        },
    )
    return create_engine(connection_url, creator=my_creator)