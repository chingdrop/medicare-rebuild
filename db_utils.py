import logging
import pandas as pd
from typing import List
from sqlalchemy import create_engine, event, text, Connection, Result, Engine
from sqlalchemy.engine import URL


class DatabaseManager:
    def __init__(self, logger=None):
        
        self.engines = {}
        self.logger = logger or logging.getLogger(DatabaseManager.__name__)

    def create_engine(
            self,
            name: str,
            username: str,
            password: str,
            host: str,
            database: str
    ):
        """Creates SQLAlchemy engine object with credentials. Creates an event listener on cursor's receive_many flag, enables fast execute_many.
        
        Args:
            - name (str): The name of the engine object.
            - username (str): The username of your credentials.
            - password (str): The password of your credentials.
            - host (str): The hostname of the SQL Server.
            - database (str): The name of your database.
        
        Returns:
            - None
        """
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
        engine = create_engine(connection_url)
        event.listen(engine, 'before_cursor_execute', self.__receive_before_cursor_execute)
        self.engines[name] = engine

    def get_engine(self, name: str) -> Engine:
        return self.engines[name]

    def begin(self, name: str) -> Connection:
        return self.engines[name].begin()

    def connect(self, name: str) -> Connection:
        return self.engines[name].connect()

    def execute(self, query: str, params:dict=None, conn:Connection=None) -> Result:
        """Executes a SQL query and returns the result object if any.
        
        Args:
            - query (str): The SQL query to execute.
            - conn (sqlalchemy.Connection): SQLAlchemy Connection object.
        
        Returns:
            - List[tuple()]: SQLAlchemy result rows.
        """
        self.logger.debug(f'Query: {query.replace('\n', ' ')}')
        if isinstance(query, str):
            query = text(query)
        res = conn.execute(query, params)
        if res.returns_rows:
            return res.fetchall()

    def read_sql(self, query: str, eng: str='', params:dict=None, parse_dates: List[str]=None) -> pd.DataFrame:
        """Reads SQL table and returns the result as a DataFrame.
        
        Args:
            - query (str): The SQL query to execute.
            - eng (str): Name of the SQLAlchemy engine obj.
            - parse_dates (list or dict, optional): List of column names to parse as datetime or 
            a dictionary specifying column names and their respective date formats.
        
        Returns:
            - pandas.DataFrame: The query results as a DataFrame.
        """
        engine = self.get_engine(eng)
        df = pd.read_sql(query, engine, params=params, parse_dates=parse_dates)
        self.logger.debug(f'Query: {query.replace('\n', ' ')}')
        self.logger.debug(f'Reading (rows: {df.shape[0]}, cols: {df.shape[1]})...')
        return df

    def to_sql(
            self,
            df: pd.DataFrame,
            table: str,
            eng: str,
            if_exists: str='fail',
            index: bool=False
    ) -> None:
        """Save Pandas DataFrame to a SQL table.

        Args:
            - df (pandas.DataFrame): The DataFrame to be written to the SQL table.
            table (string): The name of the target SQL table where the DataFrame will be saved.
            - eng (str): Name of the SQLAlchemy engine obj.
            - if_exists (string, optional): Specifies what to do if the table already exists. 
            Options are:
                - 'fail' (default): Raise a ValueError.
                - 'replace': Drop the table and recreate it.
                - 'append': Append the DataFrame to the existing table.
            - index (bool, optional): Whether to write the DataFrame's index as a column in the table.
            Default is `False`, which means the index will not be written.
        
        Returns:
            - None
        """
        engine = self.get_engine(eng)
        self.logger.debug(f'Writing (rows: {df.shape[0]}, cols: {df.shape[1]}) to {table}...')
        df.to_sql(table, engine, if_exists=if_exists, index=index)

    def dispose(self,):
        for eng, engine in list(self.engines.items()):
            engine.dispose()
            del self.engines[eng]

    @staticmethod
    def __receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
