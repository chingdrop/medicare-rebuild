import logging
from typing import Literal

import pandas as pd
from sqlalchemy import Row, create_engine, event, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import Selectable


def build_mssql_url(username: str, password: str, host: str, database: str) -> URL:
    """The `mssql+pyodbc` connection URL `DatabaseManager.create_engine` connects with,
    factored out so `alembic/env.py` can build the same URL from the same `GPS_SQL_*`
    environment variables without either duplicating this or instantiating a full
    `DatabaseManager` (see decision 0016)."""
    return URL.create(
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


class DatabaseManager:
    def __init__(self, logger=None):
        """
        Initializes the DatabaseManager with an optional logger.

        Args:
            logger (logging.Logger, optional): Logger instance for logging. Defaults to None.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.engine = None
        self.session = None

    @staticmethod
    def __receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
    ):
        """
        Event listener to enable fast execution of multiple statements.

        Args:
            conn: Connection object.
            cursor: Cursor object.
            statement: SQL statement.
            params: Parameters for the SQL statement.
            context: Execution context.
            executemany: Boolean indicating if multiple statements are being executed.
        """
        if executemany:
            cursor.fast_executemany = True

    def create_engine(
        self, username: str, password: str, host: str, database: str
    ) -> None:
        """
        Creates a SQLAlchemy engine object with the provided credentials and sets up the session.

        Args:
            username (str): The username for the database.
            password (str): The password for the database.
            host (str): The hostname of the SQL Server.
            database (str): The name of the database.
        """
        connection_url = build_mssql_url(username, password, host, database)
        # hide_parameters keeps bound values (patient row data) out of the text of
        # SQLAlchemy errors, which are logged and can reach tracebacks.
        engine = create_engine(connection_url, hide_parameters=True)
        event.listen(
            engine, "before_cursor_execute", self.__receive_before_cursor_execute
        )
        self.engine = engine
        self.session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(
        self,
    ) -> Session:
        """
        Retrieves the current database session.

        Returns:
            Session: The current SQLAlchemy session.

        Raises:
            Exception: If the database connection is not established.
        """
        if not self.session:
            raise Exception(
                "Database connection is not established. Call create_engine() first."
            )
        return self.session()

    def execute_query(self, query: str, params: dict | None = None) -> list[Row] | None:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to execute.
            params (dict): Query parameters used in execution. Defaults to None (optional).

        Returns:
            List[Row]: SQLAlchemy result rows.

        Raises:
            Exception: If there is an error executing the query.
        """
        session = self.get_session()
        try:
            single_line_query = query.replace("\n", " ")
            self.logger.debug(f"Query: {single_line_query}")
            res = session.execute(text(query), params)
            rows = list(res.fetchall()) if res.returns_rows else None  # type: ignore[attr-defined]
            session.commit()
            return rows
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error executing query: {e}")
        finally:
            session.close()
        return None

    def read_sql(
        self,
        query: str | Selectable,
        params: tuple | None = None,
        parse_dates: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Reads a SQL query and returns the result as a DataFrame.

        Args:
            query (str | Selectable): The SQL query to execute, either a raw string
                (e.g. an ``EXEC`` call) or a SQLAlchemy Core/ORM select construct with
                any filtering already bound into it (in which case `params` is unused).
            params (tuple): Positional parameters for a string query with `?`
                placeholders. Defaults to None (optional).
            parse_dates (List[str]): List of column names to parse as datetime. Defaults to None (optional).

        Returns:
            pd.DataFrame: The query results as a DataFrame.
        """
        df = pd.read_sql(query, self.engine, params=params, parse_dates=parse_dates)
        single_line_query = str(query).replace("\n", " ")
        self.logger.debug(f"Query: {single_line_query}")
        self.logger.debug(f"Reading (rows: {df.shape[0]}, cols: {df.shape[1]})...")
        return df

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "fail",
        index: bool = False,
    ) -> None:
        """
        Saves a Pandas DataFrame to a SQL table.

        Args:
            df (pd.DataFrame): The DataFrame to be written to the SQL table.
            table (str): The name of the target SQL table.
            if_exists (str): Specifies what to do if the table already exists. Defaults to 'fail' (optional).
            index (bool): Whether to write the DataFrame's index as a column. Defaults to False (optional).
        """
        self.logger.debug(
            f"Writing (rows: {df.shape[0]}, cols: {df.shape[1]}) to {table}..."
        )
        df.to_sql(table, self.engine, if_exists=if_exists, index=index)

    def close(
        self,
    ) -> None:
        """
        Closes the database connection.
        """
        if self.engine:
            self.engine.dispose()
