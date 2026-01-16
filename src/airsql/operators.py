"""
Airflow operators for the airsql framework.
"""

import time
from typing import Any, List, Optional

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator as BaseSQLCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Context

from airsql.hooks import SQLHookManager
from airsql.table import Table
from airsql.utils import OperationSummary


class BaseSQLOperator(BaseOperator):
    """Base class for SQL operators.

    Supports dynamic task mapping through the dynamic_params parameter.
    Parameters in dynamic_params are stored as instance attributes for use in map_index_template.
    """

    def __init__(
        self,
        sql: str,
        source_conn: Optional[str] = None,
        dynamic_params: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.source_conn = source_conn
        self.hook_manager = SQLHookManager()

        # Store dynamic parameters as instance attributes for map_index_template
        # This enables use with partial().expand_kwargs() for dynamic task naming
        if dynamic_params:
            for key, value in dynamic_params.items():
                setattr(self, key, value)


class SQLQueryOperator(BaseSQLOperator):
    """Operator for SQL queries that write to a destination table.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        sql: str,
        output_table: Table,
        source_conn: Optional[str] = None,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.dry_run = dry_run

    def execute(self, context: Context) -> str:
        """Execute the SQL query and write to the output table."""
        start_time = time.time()
        self.log.info(f'Executing SQL query to write to {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')
        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='query',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.dry_run,
            )

            if not self.dry_run:
                self.log.info(f'Query returned {len(df)} rows')
                self.hook_manager.write_dataframe_to_table(df, self.output_table)
                self.log.info(f'Successfully wrote {len(df)} rows')
            else:
                self.log.info(
                    f'[DRY RUN] Would write {len(df)} rows to {self.output_table}'
                )

            self.log.info(summary.to_log_summary())
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class SQLAppendOperator(BaseSQLOperator):
    """Operator for SQL queries that append data to a destination table.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        sql: str,
        output_table: Table,
        source_conn: Optional[str] = None,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.dry_run = dry_run

    def execute(self, context: Context) -> str:
        """Execute the SQL query and append to the output table."""
        start_time = time.time()
        self.log.info(f'Executing SQL query to append to {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='append',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.dry_run,
            )

            if not self.dry_run:
                self.hook_manager.write_dataframe_to_table(
                    df, self.output_table, if_exists='append'
                )
                self.log.info(f'Successfully appended {len(df)} rows')
            else:
                self.log.info(
                    f'[DRY RUN] Would append {len(df)} rows to {self.output_table}'
                )

            self.log.info(summary.to_log_summary())
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class SQLDataFrameOperator(BaseSQLOperator):
    """Operator for SQL queries that return a pandas DataFrame."""

    def execute(self, context: Context) -> pd.DataFrame:
        """Execute the SQL query and return a DataFrame."""
        start_time = time.time()
        self.log.info('Executing SQL query to return DataFrame')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='dataframe',
                rows_extracted=len(df),
                duration_seconds=duration,
                format_used='parquet',
            )

            self.log.info(
                f'Query returned DataFrame with {len(df)} rows and {len(df.columns)} columns'
            )
            self.log.info(summary.to_log_summary())
            return df
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )


class SQLReplaceOperator(BaseSQLOperator):
    """Operator for SQL queries that replace the destination table content.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        sql: str,
        output_table: Table,
        source_conn: Optional[str] = None,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.dry_run = dry_run

    def execute(self, context: Context) -> str:
        """Execute the SQL query and replace the output table."""
        start_time = time.time()
        self.log.info(f'Executing SQL query to replace {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='replace',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.dry_run,
            )

            if not self.dry_run:
                self.hook_manager.replace_table_content(df, self.output_table)
                self.log.info(f'Successfully replaced table with {len(df)} rows')
            else:
                self.log.info(
                    f'[DRY RUN] Would replace {self.output_table} with {len(df)} rows'
                )

            self.log.info(summary.to_log_summary())
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class SQLTruncateOperator(BaseSQLOperator):
    """Operator for SQL queries that truncate the destination table and insert new data, preserving structure.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        sql: str,
        output_table: Table,
        source_conn: Optional[str] = None,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.dry_run = dry_run

    def execute(self, context: Context) -> str:
        """Execute the SQL query and truncate/reload the output table."""
        start_time = time.time()
        self.log.info(f'Executing SQL query to truncate and reload {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='truncate',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.dry_run,
            )

            if not self.dry_run:
                self.hook_manager.truncate_table_content(df, self.output_table)
                self.log.info(
                    f'Successfully truncated and reloaded table with {len(df)} rows'
                )
            else:
                self.log.info(
                    f'[DRY RUN] Would truncate and reload {self.output_table} with {len(df)} rows'
                )

            self.log.info(summary.to_log_summary())
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class SQLMergeOperator(BaseSQLOperator):
    """Operator for SQL queries that merge/upsert into the destination table.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        sql: str,
        output_table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        source_conn: Optional[str] = None,
        pre_truncate: bool = False,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns
        self.pre_truncate = pre_truncate
        self.dry_run = dry_run

    def execute(self, context: Context) -> Any:
        """Execute the SQL query and merge into the output table."""
        start_time = time.time()
        self.log.info(f'Executing SQL query to merge into {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')
        self.log.debug(f'Conflict columns: {self.conflict_columns}')
        self.log.debug(f'Update columns: {self.update_columns or "all columns"}')
        self.log.debug(f'Pre-truncate: {self.pre_truncate}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            if isinstance(hook, BigQueryHook):
                df = hook.get_pandas_df(self.sql, dialect='standard')
            else:
                # Use raw DBAPI connection for pandas with PyArrow for optimization
                conn = hook.get_conn()
                try:
                    df = pd.read_sql(self.sql, conn, dtype_backend='pyarrow')
                finally:
                    conn.close()

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='merge',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.dry_run,
            )

            if not self.dry_run:
                if self.pre_truncate:
                    self.log.info(
                        f'Pre-truncating table {self.output_table} before merge'
                    )
                    # Create an empty DataFrame with the same structure for truncation
                    empty_df = df.iloc[0:0].copy()
                    self.hook_manager.truncate_table_content(
                        empty_df, self.output_table
                    )

                self.log.info(f'Query returned {len(df)} rows, merging into table')
                self.hook_manager.merge_dataframe_to_table(
                    df,
                    self.output_table,
                    self.conflict_columns,
                    update_columns=self.update_columns,
                )
                self.log.info(f'Successfully merged {len(df)} rows')
            else:
                self.log.info(
                    f'[DRY RUN] Would merge {len(df)} rows into {self.output_table}'
                )

            self.log.info(summary.to_log_summary())
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class DataFrameLoadOperator(BaseOperator):
    """Operator for loading DataFrame data into a table.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        output_table: Table,
        timestamp_column: Optional[str] = None,
        if_exists: str = 'append',
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.timestamp_column = timestamp_column
        self.if_exists = if_exists
        self.dry_run = dry_run
        self.hook_manager = SQLHookManager()

        # Store extra kwargs as instance attributes for task mapping
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    def execute(self, context: Context) -> None:
        """Execute the DataFrame load operation."""
        start_time = time.time()
        self.log.info(
            f'Loading DataFrame with {len(self.dataframe)} rows to {self.output_table}'
        )
        self.log.debug(f'DataFrame columns: {list(self.dataframe.columns)}')
        self.log.debug(f'If exists strategy: {self.if_exists}')

        duration = time.time() - start_time
        summary = OperationSummary(
            operation_type='load',
            rows_extracted=len(self.dataframe),
            rows_loaded=len(self.dataframe) if not self.dry_run else 0,
            duration_seconds=duration,
            format_used='parquet',
            dry_run=self.dry_run,
        )

        if not self.dry_run:
            self.hook_manager.write_dataframe_to_table(
                df=self.dataframe,
                table=self.output_table,
                if_exists=self.if_exists,
                timestamp_column=self.timestamp_column,
            )
            self.log.info(f'Successfully loaded {len(self.dataframe)} rows')
        else:
            self.log.info(
                f'[DRY RUN] Would load {len(self.dataframe)} rows to {self.output_table}'
            )

        self.log.info(summary.to_log_summary())


class DataFrameMergeOperator(BaseOperator):
    """Operator for merging DataFrame data into a table.

    Supports extra kwargs for dynamic task naming and mapping.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        output_table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        pre_truncate: bool = False,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns
        self.timestamp_column = timestamp_column
        self.pre_truncate = pre_truncate
        self.dry_run = dry_run
        self.hook_manager = SQLHookManager()

        # Store extra kwargs as instance attributes for task mapping
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    def execute(self, context: Context) -> None:
        """Execute the DataFrame merge operation."""
        start_time = time.time()
        self.log.info(
            f'Merging DataFrame with {len(self.dataframe)} rows into {self.output_table}'
        )
        self.log.debug(f'Conflict columns: {self.conflict_columns}')
        self.log.debug(f'Update columns: {self.update_columns or "all columns"}')
        self.log.debug(f'DataFrame columns: {list(self.dataframe.columns)}')
        self.log.debug(f'Pre-truncate: {self.pre_truncate}')

        duration = time.time() - start_time
        summary = OperationSummary(
            operation_type='merge',
            rows_extracted=len(self.dataframe),
            rows_loaded=len(self.dataframe) if not self.dry_run else 0,
            duration_seconds=duration,
            format_used='parquet',
            dry_run=self.dry_run,
        )

        if not self.dry_run:
            if self.pre_truncate:
                self.log.info(f'Pre-truncating table {self.output_table} before merge')
                # Create an empty DataFrame with the same structure for truncation
                empty_df = self.dataframe.iloc[0:0].copy()
                self.hook_manager.truncate_table_content(empty_df, self.output_table)

            self.hook_manager.merge_dataframe_to_table(
                df=self.dataframe,
                table=self.output_table,
                conflict_columns=self.conflict_columns,
                update_columns=self.update_columns,
                timestamp_column=self.timestamp_column,
            )
            self.log.info(f'Successfully merged {len(self.dataframe)} rows')
        else:
            self.log.info(
                f'[DRY RUN] Would merge {len(self.dataframe)} rows into {self.output_table}'
            )

        self.log.info(summary.to_log_summary())


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

    def execute(self, context: Context) -> None:
        """Execute the SQL check with debug logging."""
        self.log.info('Executing SQL data quality check')
        self.log.debug(f'SQL Query: {self.sql}')

        # Call the parent's execute method
        super().execute(context)

        self.log.info('SQL check completed successfully')
