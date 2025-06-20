"""
Airflow operators for the airsql framework.
"""

from typing import Any, List, Optional

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator as BaseSQLCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.context import Context

from airsql.hooks import SQLHookManager
from airsql.table import Table


class BaseSQLOperator(BaseOperator):
    """Base class for SQL operators."""

    def __init__(self, sql: str, source_conn: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.source_conn = source_conn
        self.hook_manager = SQLHookManager()


class SQLQueryOperator(BaseSQLOperator):
    """Operator for SQL queries that write to a destination table."""

    def __init__(
        self, sql: str, output_table: Table, source_conn: Optional[str] = None, **kwargs
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table

    def execute(self, context: Context) -> str:
        """Execute the SQL query and write to the output table."""
        self.log.info(f'Executing SQL query to write to {self.output_table}')
        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                df = hook.get_pandas_df(self.sql)
            self.hook_manager.write_dataframe_to_table(df, self.output_table)
        else:
            raise NotImplementedError('Cross-database queries not yet implemented')

        return str(self.output_table)


class SQLDataFrameOperator(BaseSQLOperator):
    """Operator for SQL queries that return a pandas DataFrame."""

    def execute(self, context: Context) -> pd.DataFrame:
        """Execute the SQL query and return a DataFrame."""
        self.log.info('Executing SQL query to return DataFrame')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                return hook.get_pandas_df(self.sql, dialect='standard')
            return hook.get_pandas_df(self.sql)
        else:
            raise NotImplementedError('Cross-database queries not yet implemented')


class SQLReplaceOperator(BaseSQLOperator):
    """Operator for SQL queries that replace the destination table content."""

    def __init__(
        self, sql: str, output_table: Table, source_conn: Optional[str] = None, **kwargs
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table

    def execute(self, context: Context) -> str:
        """Execute the SQL query and replace the output table."""
        self.log.info(f'Executing SQL query to replace {self.output_table}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                df = hook.get_pandas_df(self.sql)
            self.hook_manager.replace_table_content(df, self.output_table)
        else:
            raise NotImplementedError('Cross-database queries not yet implemented')

        return str(self.output_table)


class SQLMergeOperator(BaseSQLOperator):
    """Operator for SQL queries that merge/upsert into the destination table."""

    def __init__(
        self,
        sql: str,
        output_table: Table,
        conflict_columns: List[str],
        source_conn: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.conflict_columns = conflict_columns

    def execute(self, context: Context) -> Any:
        """Execute the SQL query and merge into the output table."""
        self.log.info(f'Executing SQL query to merge into {self.output_table}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                df = hook.get_pandas_df(self.sql)
            self.hook_manager.merge_dataframe_to_table(
                df, self.output_table, self.conflict_columns
            )
        else:
            raise NotImplementedError('Cross-database queries not yet implemented')

        return str(self.output_table)


class DataFrameLoadOperator(BaseOperator):
    """Operator for loading DataFrame data into a table."""

    def __init__(
        self,
        dataframe: pd.DataFrame,
        output_table: Table,
        timestamp_column: Optional[str] = None,
        if_exists: str = 'append',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.timestamp_column = timestamp_column
        self.if_exists = if_exists
        self.hook_manager = SQLHookManager()

    def execute(self, context: Context) -> None:
        """Execute the DataFrame load operation."""
        self.log.info(f'Loading DataFrame to {self.output_table}')

        self.hook_manager.write_dataframe_to_table(
            df=self.dataframe,
            table=self.output_table,
            if_exists=self.if_exists,
            add_timestamps=True,
        )


class DataFrameMergeOperator(BaseOperator):
    """Operator for merging DataFrame data into a table."""

    def __init__(
        self,
        dataframe: pd.DataFrame,
        output_table: Table,
        conflict_columns: List[str],
        timestamp_column: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.conflict_columns = conflict_columns
        self.timestamp_column = timestamp_column
        self.hook_manager = SQLHookManager()

    def execute(self, context: Context) -> None:
        """Execute the DataFrame merge operation."""
        self.log.info(f'Merging DataFrame into {self.output_table}')

        self.hook_manager.merge_dataframe_to_table(
            df=self.dataframe,
            table=self.output_table,
            merge_keys=self.conflict_columns,
            add_timestamps=True,
        )


class SQLCheckOperator(BaseSQLCheckOperator):
    """
    Wrapper around Airflow's native SQLCheckOperator that follows airsql standards.

    This operator performs data quality checks using SQL. The SQL should return a single row
    where each value is evaluated using Python bool casting. If any value is False, the check fails.

    For dbt tests, the SQL should return:
    - 0 (or empty result) = test passes
    - Any other value = test fails
    """

    def __init__(
        self,
        sql: str,
        source_conn: Optional[str] = None,
        retries: int = 1,
        **kwargs,
    ):
        if source_conn and 'conn_id' not in kwargs:
            kwargs['conn_id'] = source_conn

        if 'retries' not in kwargs:
            kwargs['retries'] = retries

        super().__init__(sql=sql, **kwargs)
