"""
SQL decorators for airsql framework with support for SQL files and Jinja templating.
"""

import inspect
import os
from functools import wraps
from pathlib import Path
from typing import Any, Callable, List, Optional

import pandas as pd
from airflow.decorators import task
from airflow.sdk import get_current_context
from jinja2 import Environment, FileSystemLoader, select_autoescape

from airsql.file import File
from airsql.operators import (
    DataFrameLoadOperator,
    DataFrameMergeOperator,
    SQLCheckOperator,
    SQLDataFrameOperator,
    SQLMergeOperator,
    SQLQueryOperator,
    SQLReplaceOperator,
)
from airsql.table import Table


class SQLDecorators:
    """Collection of SQL operation decorators."""

    def __init__(self, sql_files_path: Optional[str] = None):
        """Initialize SQL decorators with optional SQL files path."""
        self.sql_files_path = sql_files_path or os.path.join(
            os.getcwd(), 'dags', 'git_sql'
        )

        if os.path.exists(self.sql_files_path):
            self.jinja_env = Environment(
                loader=FileSystemLoader(self.sql_files_path),
                autoescape=select_autoescape([
                    'html',
                    'xml',
                    'sql',
                ]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self.jinja_env = None
        self.string_jinja_env = Environment(
            loader=None, autoescape=select_autoescape(['sql'])
        )

    def _load_sql_from_file(self, sql_file: str, **template_vars) -> str:
        """
        Load and render SQL from a file with Jinja templating.

        First tries to find the file relative to the calling DAG's directory,
        then falls back to the configured sql_files_path.
        """
        if not sql_file.endswith('.sql'):
            sql_file += '.sql'

        if os.path.isabs(sql_file):
            sql_path = Path(sql_file)
            if sql_path.exists():
                file_obj = File(str(sql_path), variables=template_vars)
                return file_obj.render()
            else:
                raise FileNotFoundError(f'SQL file not found: {sql_file}')

        # First, try to find the file relative to the calling file's directory
        frame = inspect.currentframe()
        try:
            # Walk up the call stack to find the caller outside of this decorator class
            caller_frame = frame
            while caller_frame:
                caller_frame = caller_frame.f_back
                if caller_frame and caller_frame.f_code.co_filename != __file__:
                    caller_dir = os.path.dirname(
                        os.path.abspath(caller_frame.f_code.co_filename)
                    )
                    relative_sql_path = os.path.join(caller_dir, sql_file)
                    if os.path.exists(relative_sql_path):
                        file_obj = File(relative_sql_path, variables=template_vars)
                        return file_obj.render()
                    break
        finally:
            del frame

        # Fall back to the configured sql_files_path
        if not self.jinja_env:
            raise ValueError(f'SQL files directory not found: {self.sql_files_path}')

        template = self.jinja_env.get_template(sql_file)
        return template.render(**template_vars)

    def _process_sql_input(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        sql_file_template_path: Optional[str] = None,
        **decorator_template_vars,
    ) -> str:
        """
        Process SQL input.
        If sql_file_template_path is provided, it's loaded and rendered.
        Otherwise, the decorated function is called;
        its string return is treated as a template,
        or a File object's render method is used.
        All runtime arguments to the decorated function are made available to the
        Jinja template.
        """
        final_template_vars = decorator_template_vars.copy()
        sig = inspect.signature(func)
        bound_args = sig.bind_partial(*args, **kwargs)
        bound_args.apply_defaults()
        final_template_vars.update(bound_args.arguments)

        if sql_file_template_path:
            if not self.jinja_env:
                raise ValueError(
                    f"SQL files directory '{self.sql_files_path}'"
                    ' not found or Jinja environment '
                    "for files not initialized, but 'sql_file' was specified."
                )
            return self._load_sql_from_file(
                sql_file_template_path, **final_template_vars
            )
        else:
            result = func(*args, **kwargs)

            if isinstance(result, str):
                try:
                    sql_template = self.string_jinja_env.from_string(result)
                    return sql_template.render(**final_template_vars)
                except Exception as e:
                    raise ValueError(
                        f'Error rendering SQL template from function {func.__name__}:\n'
                        f'Error: {e}\n'
                        f"Template: '''{result}'''\n"
                        f'Variables: {final_template_vars}'
                    ) from e
            elif isinstance(result, File):
                return result.render(context=final_template_vars)
            else:
                raise ValueError(
                    f'Decorated function {func.__name__} '
                    'must return a SQL string or a airsql.File object '
                    "when 'sql_file' is not specified in the decorator."
                )

    def query(
        self,
        output_table: Optional[Table] = None,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for SQL queries.

        Args:
            output_table: Table to write results to (optional)
            source_conn: Connection ID for simple queries without table parameters
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {}
                if output_table:
                    op_kwargs['outlets'] = [output_table.as_asset()]

                operator = SQLQueryOperator(
                    task_id=func.__name__,
                    sql=sql_query,
                    output_table=output_table,
                    source_conn=source_conn,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def dataframe(
        self,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for SQL queries that return a pandas DataFrame.

        Now TaskFlow-compatible: creates a proper task that can be used
        in dependencies and data passing.

        Args:
            source_conn: Connection ID for simple queries
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template
        """

        def decorator(func: Callable) -> Callable:
            @task(task_id=func.__name__)
            @wraps(func)
            def wrapper(*args, **kwargs) -> pd.DataFrame:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                operator = SQLDataFrameOperator(
                    task_id=f'{func.__name__}_internal',
                    sql=sql_query,
                    source_conn=source_conn,
                )

                context = get_current_context()
                return operator.execute(context)

            return wrapper

        return decorator

    def replace(
        self,
        output_table: Table,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for SQL operations that replace table content.

        Args:
            output_table: Table to replace content in
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> None:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {'outlets': [output_table.as_asset()]}

                operator = SQLReplaceOperator(
                    task_id=func.__name__,
                    sql=sql_query,
                    output_table=output_table,
                    source_conn=source_conn,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def merge(
        self,
        output_table: Table,
        conflict_columns: List[str],
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for SQL operations that merge/upsert into tables.

        Args:
            output_table: Table to merge data into
            conflict_columns: Columns to use for conflict resolution
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> None:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {'outlets': [output_table.as_asset()]}

                operator = SQLMergeOperator(
                    task_id=func.__name__,
                    sql=sql_query,
                    output_table=output_table,
                    conflict_columns=conflict_columns,
                    source_conn=source_conn,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    @staticmethod
    def load_dataframe(
        output_table: Table,
        timestamp_column: Optional[str] = None,
        if_exists: str = 'append',
        dataframe: Optional[pd.DataFrame] = None,
    ) -> Callable:
        """
        Decorator for functions that return a DataFrame to be loaded into a table,
        or for directly loading a provided DataFrame.

        Args:
            output_table: Table to write DataFrame to
            timestamp_column: Custom timestamp column name (optional)
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            dataframe: Pre-existing DataFrame to load (optional)

        Example 1 - Function that returns DataFrame:
            @sql.load_dataframe(
                output_table=Table(
                    conn_id="postgres_conn",
                    table_name="analytics.users"
                ),
                if_exists='replace'
            )
            def create_user_summary():
                # Your DataFrame creation logic
                return pd.DataFrame({
                    'user_id': [1, 2, 3],
                    'name': ['Alice', 'Bob', 'Charlie']
                })

        Example 2 - Direct DataFrame loading:
            @sql.load_dataframe(
                output_table=Table(
                    conn_id="postgres_conn",
                    table_name="analytics.users"
                ),
                if_exists='replace',
                dataframe=my_existing_df
            )
            def load_existing_data():
                pass  # Function body can be empty when dataframe is provided
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                # Use provided dataframe or get it from function
                if dataframe is not None:
                    df = dataframe
                else:
                    df = func(*args, **kwargs)

                if not isinstance(df, pd.DataFrame):
                    raise ValueError(
                        f'Function {func.__name__} must return a pandas DataFrame '
                        'or a DataFrame must be provided to the decorator'
                    )

                op_kwargs = {'outlets': [output_table.as_asset()]}

                operator = DataFrameLoadOperator(
                    task_id=func.__name__,
                    dataframe=df,
                    output_table=output_table,
                    timestamp_column=timestamp_column,
                    if_exists=if_exists,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    @staticmethod
    def merge_dataframe(
        output_table: Table,
        conflict_columns: List[str],
        timestamp_column: Optional[str] = None,
        dataframe: Optional[pd.DataFrame] = None,
    ) -> Callable:
        """
        Decorator for functions that return a DataFrame to be merged/upserted
        into a table, or for directly merging a provided DataFrame.

        Args:
            output_table: Table to merge DataFrame into
            conflict_columns: Columns to use for conflict resolution
            timestamp_column: Custom timestamp column name (optional)
            dataframe: Pre-existing DataFrame to merge (optional)

        Example 1 - Function that returns DataFrame:
            @sql.merge_dataframe(
                output_table=Table(
                    conn_id="bigquery_conn",
                    table_name="analytics.user_events"
                ),
                conflict_columns=['user_id', 'event_date']
            )
            def update_user_events():
                # Your DataFrame creation logic
                return pd.DataFrame({
                    'user_id': [1, 2],
                    'event_date': ['2025-05-29', '2025-05-29'],
                    'event_count': [10, 15]
                })

        Example 2 - Direct DataFrame merging:
            @sql.merge_dataframe(
                output_table=Table(
                    conn_id="bigquery_conn",
                    table_name="analytics.user_events"
                ),
                conflict_columns=['user_id', 'event_date'],
                dataframe=my_existing_df
            )
            def merge_existing_data():
                pass  # Function body can be empty when dataframe is provided
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                # Use provided dataframe or get it from function
                if dataframe is not None:
                    df = dataframe
                else:
                    df = func(*args, **kwargs)

                if not isinstance(df, pd.DataFrame):
                    raise ValueError(
                        f'Function {func.__name__} must return a pandas DataFrame '
                        'or a DataFrame must be provided to the decorator'
                    )

                op_kwargs = {'outlets': [output_table.as_asset()]}

                operator = DataFrameMergeOperator(
                    task_id=func.__name__,
                    dataframe=df,
                    output_table=output_table,
                    conflict_columns=conflict_columns,
                    timestamp_column=timestamp_column,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def check(
        self,
        conn_id: Optional[str] = None,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for SQL data quality checks (for dbt tests).

        Uses Airflow's native SQLCheckOperator which expects SQL that returns a single row.
        Each value is evaluated using Python bool casting - if any value is False, the check fails.

        For dbt tests compatibility:
        - SQL returning 0 (or empty) = test passes
        - SQL returning any other value = test fails

        Args:
            conn_id: Connection ID for the database (preferred)
            source_conn: Alternative connection parameter for compatibility
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template

        Example:
            @sql.check(conn_id="bigquery_conn")
            def test_no_nulls(table):
                return "SELECT COUNT(*) FROM {{ table }} WHERE id IS NULL"
        """
        connection_id = conn_id or source_conn

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                operator = SQLCheckOperator(
                    task_id=func.__name__,
                    sql=sql_query,
                    source_conn=connection_id,
                )

                return operator

            return wrapper

        return decorator

    def ddl(
        self,
        output_table: Optional[Table] = None,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars,
    ) -> Callable:
        """
        Decorator for DDL operations like CREATE VIEW, CREATE TABLE AS.

        Args:
            output_table: Optional table reference for lineage tracking
            source_conn: Connection ID for the database
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template

        Example:
            @sql.ddl(source_conn="bigquery_conn")
            def create_view(source_table):
                return "CREATE OR REPLACE VIEW my_view AS SELECT * FROM {{ source_table }}"
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {}
                if output_table:
                    op_kwargs['outlets'] = [output_table.as_asset()]

                operator = SQLQueryOperator(
                    task_id=func.__name__,
                    sql=sql_query,
                    output_table=None,
                    source_conn=source_conn,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator


sql = SQLDecorators()
