from typing import Any

import numpy as np
import pandas as pd

from airsql.table import Table

CONN_ID = 'postgres_conn'
BIGQUERY_CONN_ID = 'bigquery_conn'

SQL_SIMPLE = "SELECT 1 AS id, 'test' AS value"
SQL_MULTI_ROW = (
    "SELECT generate_series(1, 10) AS id, 'value_' || generate_series(1, 10) AS name"
)


def make_table(conn_id: str = CONN_ID, table_name: str = 'public.test_table') -> Table:
    return Table(conn_id=conn_id, table_name=table_name)


def make_temp_table(
    conn_id: str = CONN_ID, table_name: str = 'public.temp_test'
) -> Table:
    return Table(conn_id=conn_id, table_name=table_name, temporary=True)


def make_dataframe(rows: int = 10, with_timestamp: bool = False) -> pd.DataFrame:
    data: dict[str, Any] = {
        'id': list(range(1, rows + 1)),
        'value': [f'value_{i}' for i in range(1, rows + 1)],
    }
    if with_timestamp:
        data['updated_at'] = pd.Timestamp.now().as_unit('us').value * np.ones(
            rows, dtype='datetime64[us]'
        )
    return pd.DataFrame(data)


def make_dataframe_for_merge() -> pd.DataFrame:
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['alice', 'bob', 'charlie'],
        'value': [100, 200, 300],
    })


TABLE_SIMPLE = make_table()
TABLE_TEMP = make_temp_table()
DF_SIMPLE = make_dataframe(rows=5)
DF_MERGE = make_dataframe_for_merge()
