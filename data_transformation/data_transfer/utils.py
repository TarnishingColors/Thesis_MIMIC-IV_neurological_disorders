from typing import List, Tuple, Set
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import create_engine


@dataclass
class Connection:
    dbtype: str
    dbname: str
    username: str
    password: str
    host: str
    port: str


class DataTransfer:
    def __init__(self, target: Connection):
        self.target_connection = None
        self.data = None

        self.target_connection = create_engine(
            f"{target.dbtype}+psycopg2://{target.username}:{target.password}@{target.host}:{target.port}/{target.dbname}"
        )

    def read_data(self, data: pd.DataFrame):
        self.data = data

    def transfer_data(self, schema_name: str, table_name: str):
        self.data.to_sql(
            table_name,
            self.target_connection,
            if_exists='append',
            schema=schema_name,
            index=False,
            method='multi'
        )


def filter_matching_rows(df: pd.DataFrame, target_columns: List[str], result: Set[Tuple]):
    return df[df.apply(lambda row: tuple(row[column] for column in target_columns) in result, axis=1)]
