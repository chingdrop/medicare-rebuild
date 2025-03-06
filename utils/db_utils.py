import logging
import pandas as pd
from typing import List
from sqlalchemy import create_engine, event, text, Row
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, Session


class DatabaseManager:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.engine = None
        self.session = None

    def create_engine(
            self,
            username: str,
            password: str,
            host: str,
            database: str
    ) -> None:
        """Creates SQLAlchemy engine object with credentials.
        Creates an event listener on cursor's receive_many flag, enables fast execute_many.
        
        Args:
            username (str): The username of your credentials.
            password (str): The password of your credentials.
            host (str): The hostname of the SQL Server.
            database (str): The name of your database.
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
        self.engine = engine
        self.session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self,) -> Session:
        if not self.session:
            raise Exception("Database connection is not established. Call connect() first.")
        return self.session()

    def execute_query(self, query: str, params: dict=None) -> List[Row]:
        """Executes a SQL query and returns the result object if any.
        
        Args:
            query (str) : The SQL query to execute.
            params (dict) : Query parameters used in execution.
        
        Returns:
            List[Row]: SQLAlchemy result rows.
        """
        session = self.get_session()
        try:
            self.logger.debug(f'Query: {query.replace('\n', ' ')}')
            res = session.execute(text(query), params)
            session.commit()
            if res.returns_rows:
                return res.fetchall()
        except Exception as e:
            session.rollback()
            self.logger.error(f'Error executing query: {e}')
        finally:
            session.close()

    def read_sql(self, query: str, params: tuple=None, parse_dates: List[str]=None) -> pd.DataFrame:
        """Reads SQL table and returns the result as a DataFrame.
        
        Args:
            query (str): The SQL query to execute.
            eng (str): Name of the SQLAlchemy engine obj.
            parse_dates (list or dict, optional): List of column names to parse as datetime or a dictionary specifying column names and their respective date formats.
        
        Returns:
            pandas.DataFrame: The query results as a DataFrame.
        """
        df = pd.read_sql(query, self.engine, params=params, parse_dates=parse_dates)
        self.logger.debug(f'Query: {query.replace('\n', ' ')}')
        self.logger.debug(f'Reading (rows: {df.shape[0]}, cols: {df.shape[1]})...')
        return df

    def to_sql(
            self,
            df: pd.DataFrame,
            table: str,
            if_exists: str='fail',
            index: bool=False
    ) -> None:
        """Save Pandas DataFrame to a SQL table.

        Args:
            df (pandas.DataFrame): The DataFrame to be written to the SQL table.
            table (str): The name of the target SQL table where the DataFrame will be saved.
            if_exists (str, optional): Specifies what to do if the table already exists. Default is `fail`, which means the operation will fail if a table already exists.
            index (bool, optional): Whether to write the DataFrame's index as a column in the table. Default is `False`, which means the index will not be written.
        """
        self.logger.debug(f'Writing (rows: {df.shape[0]}, cols: {df.shape[1]}) to {table}...')
        df.to_sql(table, self.engine, if_exists=if_exists, index=index)

    def close(self,):
        if self.engine:
            self.engine.dispose()

    @staticmethod
    def __receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
