"""
Airflow operators for the airsql framework.
"""

import time
from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator as BaseSQLCheckOperator,
)
from airflow.sdk import Context

from airsql.hooks import SQLHookManager
from airsql.table import Table
from airsql.utils import OperationSummary


def _is_bigquery_hook(hook: Any) -> bool:
    """Check if a hook is a BigQueryHook without importing the class.

    Uses class name check to avoid expensive google.cloud.bigquery import during DAG parsing.
    """
    return hook.__class__.__name__ == 'BigQueryHook'


def _read_dataframe_from_hook(hook: Any, sql: str):
    def _convert_to_numpy_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            dtype_name = str(df[col].dtype)
            if dtype_name in {'dbdate', 'dbtime'}:
                df[col] = df[col].astype('object')
            elif isinstance(df[col].dtype, pd.ArrowDtype):
                arrow_type = df[col].dtype.pyarrow_dtype
                if pa.types.is_date(arrow_type):
                    df[col] = df[col].astype('datetime64[ns]').dt.date.astype('object')
                elif pa.types.is_timestamp(arrow_type):
                    df[col] = df[col].astype('datetime64[ns]')
                elif pa.types.is_integer(arrow_type):
                    df[col] = df[col].astype('Int64')
                elif pa.types.is_floating(arrow_type):
                    df[col] = df[col].astype('Float64')
                elif pa.types.is_boolean(arrow_type):
                    df[col] = df[col].astype('boolean')
                elif pa.types.is_string(arrow_type):
                    df[col] = df[col].astype('string')
                else:
                    df[col] = df[col].astype(df[col].dtype.numpy_dtype)
        return df

    if _is_bigquery_hook(hook):
        df = hook.get_pandas_df(sql, dialect='standard')
        return _convert_to_numpy_dtypes(df)

    try:
        engine = hook.get_sqlalchemy_engine()
    except (AttributeError, NotImplementedError):
        conn = hook.get_conn()
        try:
            df = pd.read_sql(sql, conn, dtype_backend='pyarrow')
            return _convert_to_numpy_dtypes(df)
        finally:
            conn.close()

    df = pd.read_sql(sql, engine, dtype_backend='pyarrow')
    return _convert_to_numpy_dtypes(df)


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
        """Initialize the base SQL operator.

        Args:
            sql: SQL template or rendered SQL string associated with the operator.
            source_conn: Optional connection id of the source database.
            dynamic_params: Optional mapping used for dynamic task naming and mapping.
            **kwargs: Additional keyword arguments passed to BaseOperator.

        Notes:
            Child operators commonly accept additional flags such as
            ``dry_run_flag`` or ``pre_truncate``. This constructor stores
            parameters on the instance for use by operators and templates.
        """
        # Backwards-compatibility: some DAGs / decorators may pass a legacy
        # `dry_run` kwarg. Airflow's BaseOperator rejects unknown kwargs when
        # apply_defaults runs, so remove it here and store the value as an
        # instance attribute. Child operators also accept `dry_run_flag`
        # explicitly; if they do, they'll overwrite `self.is_dry_run`.
        if 'dry_run' in kwargs:
            try:
                self.is_dry_run = bool(kwargs.pop('dry_run'))
            except Exception:
                self.is_dry_run = False

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
        dry_run_flag: bool = False,
        pre_truncate: bool = False,
        **kwargs,
    ):
        """Create an operator that executes a SQL query and writes results.

        Args:
            sql: SQL string to execute.
            output_table: Target table reference where results will be written.
            source_conn: Connection ID to execute the SQL against.
            dry_run_flag: If True, simulate the operation without writing data.
            pre_truncate: If True, truncate the destination before writing.
            **kwargs: Extra keyword args forwarded to BaseSQLOperator.
        """
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.is_dry_run = dry_run_flag
        self.pre_truncate = pre_truncate

    def execute(self, context: Context) -> str:
        """Execute the SQL query and write to the output table.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            The string representation of the output table.
        """
        start_time = time.time()
        self.log.info(f'Executing SQL query to write to {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')
        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='query',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.is_dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.is_dry_run,
            )

            if not self.is_dry_run:
                self.log.info(f'Query returned {len(df)} rows')
                if_exists = 'truncate' if self.pre_truncate else 'append'
                self.hook_manager.write_dataframe_to_table(
                    df, self.output_table, if_exists=if_exists
                )
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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        """Create an operator that executes a SQL query and appends results.

        Args:
            sql: SQL string to execute.
            output_table: Target table reference where results will be appended.
            source_conn: Connection ID to execute the SQL against.
            dry_run_flag: If True, simulate without writing data.
            **kwargs: Extra keyword args forwarded to BaseSQLOperator.
        """
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.is_dry_run = dry_run_flag

    def execute(self, context: Context) -> str:
        """Execute the SQL query and append to the output table.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            The string representation of the output table.
        """
        start_time = time.time()
        self.log.info(f'Executing SQL query to append to {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='append',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.is_dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.is_dry_run,
            )

            if not self.is_dry_run:
                self.hook_manager.write_dataframe_to_table(
                    df, self.output_table, if_exists='append'
                )
                self.log.info(f'Successfully appended {len(df)} rows')
            else:
                self.log.info(
                    f'[DRY RUN] Would append {len(df)} rows to {self.output_table}'
                )

            self.log.info(summary.to_log_summary())

            # Drop temporary table after append if marked as temporary
            if self.output_table.temporary:
                self.log.info(f'Dropping temporary table: {self.output_table}')
                SQLHookManager.drop_table(self.output_table)
        else:
            raise ValueError(
                f'source_conn is required for {self.__class__.__name__}. '
                'Please provide a connection ID to query from.'
            )

        return str(self.output_table)


class SQLDataFrameOperator(BaseSQLOperator):
    """Operator for SQL queries that return a pandas DataFrame."""

    def execute(self, context: Context) -> pd.DataFrame:
        """Execute the SQL query and return a pandas DataFrame.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            A pandas DataFrame with the query results.
        """
        start_time = time.time()
        self.log.info('Executing SQL query to return DataFrame')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.is_dry_run = dry_run_flag

    def execute(self, context: Context) -> str:
        """Execute the SQL query and replace the output table.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            The string representation of the output table.
        """
        start_time = time.time()
        self.log.info(f'Executing SQL query to replace {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='replace',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.is_dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.is_dry_run,
            )

            if not self.is_dry_run:
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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.is_dry_run = dry_run_flag

    def execute(self, context: Context) -> str:
        """Execute the SQL query and truncate/reload the output table.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            The string representation of the output table.
        """
        start_time = time.time()
        self.log.info(f'Executing SQL query to truncate and reload {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='truncate',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.is_dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.is_dry_run,
            )

            if not self.is_dry_run:
                self.hook_manager.truncate_table_content(df, self.output_table)
                self.log.info(
                    f'Successfully truncated and reloaded table with {len(df)} rows'
                )
            else:
                self.log.info(
                    f'[DRY RUN] Would truncate and reload {self.output_table} with {len(df)} rows'
                )

            self.log.info(summary.to_log_summary())

            # Drop temporary table after truncate/reload if marked as temporary
            if self.output_table.temporary:
                self.log.info(f'Dropping temporary table: {self.output_table}')
                SQLHookManager.drop_table(self.output_table)
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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        """Create an operator that runs a SQL query and merges results.

        Args:
            sql: SQL string to execute.
            output_table: Target table reference for the merge.
            conflict_columns: Columns used to identify conflicts (ON clause).
            update_columns: Columns to update on conflict (optional).
            source_conn: Connection ID for executing the SQL.
            pre_truncate: If True, truncate destination before merge.
            dry_run_flag: If True, simulate without applying changes.
            **kwargs: Extra keyword args forwarded to BaseSQLOperator.
        """
        super().__init__(sql=sql, source_conn=source_conn, **kwargs)
        self.output_table = output_table
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns
        self.pre_truncate = pre_truncate
        self.is_dry_run = dry_run_flag

    def execute(self, context: Context) -> Any:
        """Execute the SQL query and merge into the output table.

        Args:
            context: Airflow execution context passed by the scheduler.

        Returns:
            The string representation of the output table.
        """
        start_time = time.time()
        self.log.info(f'Executing SQL query to merge into {self.output_table}')
        self.log.debug(f'SQL Query: {self.sql}')
        self.log.debug(f'Conflict columns: {self.conflict_columns}')
        self.log.debug(f'Update columns: {self.update_columns or "all columns"}')
        self.log.debug(f'Pre-truncate: {self.pre_truncate}')

        if self.source_conn:
            hook = self.hook_manager.get_hook(self.source_conn)
            df = _read_dataframe_from_hook(hook, self.sql)

            duration = time.time() - start_time
            summary = OperationSummary(
                operation_type='merge',
                rows_extracted=len(df),
                rows_loaded=len(df) if not self.is_dry_run else 0,
                duration_seconds=duration,
                format_used='parquet',
                dry_run=self.is_dry_run,
            )

            if not self.is_dry_run:
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

            # Drop temporary table after merge if marked as temporary
            if self.output_table.temporary:
                self.log.info(f'Dropping temporary table: {self.output_table}')
                SQLHookManager.drop_table(self.output_table)
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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        """Create an operator that loads a pandas DataFrame into a table.

        Args:
            dataframe: The pandas DataFrame to load.
            output_table: Target table reference where data will be written.
            timestamp_column: Optional column to populate with current timestamp.
            if_exists: Behavior when table exists ('append', 'replace', 'fail', 'truncate').
            dry_run_flag: If True, simulate without writing data.
            **kwargs: Extra keyword args forwarded to BaseOperator (used for task mapping).
        """
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.timestamp_column = timestamp_column
        self.if_exists = if_exists
        self.is_dry_run = dry_run_flag
        self.hook_manager = SQLHookManager()

        # Store extra kwargs as instance attributes for task mapping
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    def execute(self, context: Context) -> None:
        """Execute the DataFrame load operation.

        Args:
            context: Airflow execution context passed by the scheduler.
        """
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
            rows_loaded=len(self.dataframe) if not self.is_dry_run else 0,
            duration_seconds=duration,
            format_used='parquet',
            dry_run=self.is_dry_run,
        )

        if not self.is_dry_run:
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

        # Drop temporary table after load if marked as temporary
        if self.output_table.temporary:
            self.log.info(f'Dropping temporary table: {self.output_table}')
            SQLHookManager.drop_table(self.output_table)


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
        dry_run_flag: bool = False,
        **kwargs,
    ):
        """Create an operator that merges a pandas DataFrame into a table.

        Args:
            dataframe: The pandas DataFrame to merge.
            output_table: Target table reference for the merge.
            conflict_columns: Columns used to identify conflicting rows.
            update_columns: Columns to update on conflict (optional).
            timestamp_column: Optional name of the timestamp column to populate.
            pre_truncate: If True, truncate the destination before merging.
            dry_run_flag: If True, simulate without applying changes.
            **kwargs: Extra keyword args forwarded to BaseOperator (used for task mapping).
        """
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.output_table = output_table
        self.conflict_columns = conflict_columns
        self.update_columns = update_columns
        self.timestamp_column = timestamp_column
        self.pre_truncate = pre_truncate
        self.is_dry_run = dry_run_flag
        self.hook_manager = SQLHookManager()

        # Store extra kwargs as instance attributes for task mapping
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    def execute(self, context: Context) -> None:
        """Execute the DataFrame merge operation.

        Args:
            context: Airflow execution context passed by the scheduler.
        """
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
            rows_loaded=len(self.dataframe) if not self.is_dry_run else 0,
            duration_seconds=duration,
            format_used='parquet',
            dry_run=self.is_dry_run,
        )

        if not self.is_dry_run:
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

        # Drop temporary table after merge if marked as temporary
        if self.output_table.temporary:
            self.log.info(f'Dropping temporary table: {self.output_table}')
            SQLHookManager.drop_table(self.output_table)


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
