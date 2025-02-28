"""Data Transfer utils to connect to DB and transform data"""

from dataclasses import dataclass
from typing import List, Tuple, Set, Dict
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection


@dataclass
class ConnectionDetails:
    """
    Database Connection Builder
    """

    dbtype: str
    dbname: str
    username: str
    password: str
    host: str
    port: str


class DataTransfer:
    """
    A Class designed to transform data and manipulate DB
    """
    def __init__(self, target: ConnectionDetails):
        """
        :param target: connection details for the target
        """
        self.target_connection = None
        self.data = None

        self.target_connection = create_engine(
            f"{target.dbtype}+psycopg2://{target.username}:{target.password}@{target.host}:{target.port}/{target.dbname}",
            future=True
        ).connect()

    def get_connection(self) -> Connection:
        """
        :return: SQLAlchemy Connection
        """
        return self.target_connection

    def read_data(self, data: pd.DataFrame) -> None:
        """
        :param data: dataframe
        """

        self.data = data

    def run_query(self, sql_query: str, parameters: Dict[str, str] = None) -> None:
        """
        :param sql_query: SQL query
        :param parameters: query parameters to parse
        """

        self.target_connection.execute(text(sql_query), parameters)
        self.target_connection.commit()

    def fetch_data(self, sql_query: str, parameters: Dict[str, str] = None) -> pd.DataFrame:
        """
        :param sql_query: SQL query
        :param parameters: query parameters to parse
        :return: dataframe of the query results
        """

        data = pd.read_sql(sql_query, self.target_connection, params=parameters)
        self.target_connection.commit()
        return data

    def transfer_data(self, schema_name: str, table_name: str) -> None:
        """
        :param schema_name: name of the schema in DB
        :param table_name: name of the table in DB
        """
        self.data.to_sql(
            table_name,
            self.target_connection,
            if_exists='append',
            schema=schema_name,
            index=False,
            method='multi'
        )
        self.target_connection.commit()


def filter_matching_rows(df: pd.DataFrame, target_columns: List[str], result: Set[Tuple]) -> pd.DataFrame:
    """
    :param df: input dataframe
    :param target_columns: list of columns to target
    :param result: result
    :return: dataframe with only rows matching the target columns
    """

    return df[df.apply(lambda row: tuple(row[column] for column in target_columns) in result, axis=1)]
