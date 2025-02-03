import logging
import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL


class DatabaseManager:
    def __init__(self, username, password, host, database, logger=None):
        self.connection_url = URL.create(
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
        self.engine = None
        self.connection = None
        self.logger = logger or logging.getLogger(DatabaseManager.__name__)

    def create_engine(self):
        self.engine = create_engine(self.connection_url)
        event.listen(self.engine, 'before_cursor_execute', self.__receive_before_cursor_execute)

    def begin(self,):
        if not self.engine:
            self.create_engine()
        return self.engine.begin()

    def connect(self,):
        if not self.engine:
            self.create_engine()
        return self.engine.connect()

    def read_sql(self, query: str, parse_dates=None) -> pd.DataFrame:
        """Executes a SQL query and returns the result as a DataFrame.
        
        Args:
            - query_string (str): The SQL query to execute.
            - parse_dates (list or dict, optional): List of column names to parse as datetime or 
            a dictionary specifying column names and their respective date formats.
        
        Returns:
            - pandas.DataFrame: The query results as a DataFrame.
        """
        df = pd.read_sql(query, self.engine, parse_dates=parse_dates)
        self.logger.debug(f'Query: {query.replace('\n', ' ')}')
        self.logger.debug(f'Reading (rows: {df.shape[0]}, cols: {df.shape[1]})...')
        return df
    
    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists='fail', index=False) -> None:
        """Save a Pandas DataFrame to a SQL table.

        Args:
            - df (pandas.DataFrame): The DataFrame to be written to the SQL table.
            table_name (string): The name of the target SQL table where the DataFrame will be saved.
            - if_exists (string, optional): Specifies what to do if the table already exists. 
            Options are:
                - 'fail' (default): Raise a ValueError.
                - 'replace': Drop the table and recreate it.
                - 'append': Append the DataFrame to the existing table.
            - index (bool, optional): Whether to write the DataFrame's index as a column in the table.
            Default is `False`, which means the index will not be written.
        
        Returns:
            - None: This method performs the database operation and does not return anything.
        """
        self.logger.debug(f'Writing (rows: {df.shape[0]}, cols: {df.shape[1]}) to {table_name}...')
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=index)

    def dispose(self,):
        self.engine.dispose()
        self.engine = None

    @staticmethod
    def __receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
