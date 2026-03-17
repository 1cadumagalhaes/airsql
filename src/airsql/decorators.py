"""
SQL decorators for airsql framework with support for SQL files and Jinja templating.
"""

import inspect
import logging
import os
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, List, Optional

from airflow.models import BaseOperator
from airflow.sdk import get_current_context, task
from jinja2 import Environment, Undefined, select_autoescape

from airsql.file import File
from airsql.table import Table

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _get_func_name(func: Any) -> str:
    """Get function name safely for typing."""
    return getattr(func, '__name__', 'unknown')


def _render_airflow_templates(
    value: Any, context: dict, airflow_jinja_env: Environment
) -> Any:
    """Render Airflow Jinja templates in values from context params.

    Handles nested dicts, lists, and strings that may contain {{ params.x }}.
    Returns the original type with templates rendered.
    Converts 'None' string and empty strings to None for proper airsql conditional handling.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return {
            k: _render_airflow_templates(v, context, airflow_jinja_env)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            _render_airflow_templates(item, context, airflow_jinja_env)
            for item in value
        ]
    if isinstance(value, str):
        if '{{' in value or '{%' in value:
            try:
                rendered = airflow_jinja_env.from_string(value).render(context)
                if rendered in {'None', ''}:
                    return None
                return rendered or None
            except Exception:
                return value
        return value
    return value


def _sanitize_airflow_params(params: dict) -> dict:
    """Recursively sanitize Airflow params, converting string 'None' to Python None.

    Airflow UI often serializes None as the string 'None' when triggering DAGs.
    """
    if not isinstance(params, dict):
        return params

    sanitized = {}
    for key, value in params.items():
        if isinstance(value, dict):
            sanitized[key] = _sanitize_airflow_params(value)
        elif isinstance(value, list):
            sanitized[key] = [
                _sanitize_airflow_params(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif value in {'None', 'null'}:
            sanitized[key] = None
        else:
            sanitized[key] = value
    return sanitized


def _get_airflow_context() -> dict:
    """Get Airflow context for Jinja rendering, returns empty dict if not available."""
    try:
        context = get_current_context()
        if context:
            params = context.get('params', {})
            return {
                'params': _sanitize_airflow_params(params) if params else {},
                'ds': context.get('ds', ''),
                'ds_nodash': context.get('ds_nodash', ''),
                'ts': context.get('ts', ''),
                'ts_nodash': context.get('ts_nodash', ''),
                'logical_date': context.get('logical_date'),
                'data_interval_start': context.get('data_interval_start'),
                'data_interval_end': context.get('data_interval_end'),
                'dag': context.get('dag'),
                'dag_run': context.get('dag_run'),
                'run_id': context.get('run_id'),
                'task': context.get('task'),
                'task_instance': context.get('task_instance'),
            }
    except Exception as e:
        logger.debug('Could not get Airflow context: %s', e)
    return {}


class SQLDecorators:
    """Collection of SQL operation decorators."""

    def __init__(self, sql_files_path: Optional[str] = None):
        """Initialize SQL decorators with optional SQL files path."""
        self.sql_files_path = sql_files_path or os.path.join(
            os.getcwd(), 'dags', 'git_sql'
        )
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

        # Fall back to common SQL roots and configured sql_files_path
        search_roots = [
            Path.cwd(),
            Path.cwd() / 'dags',
            Path.cwd() / 'dags' / 'git_sql',
            Path.cwd() / 'sql',
            Path(self.sql_files_path),
        ]

        for root in search_roots:
            candidate = (root / sql_file).resolve()
            if candidate.exists():
                file_obj = File(str(candidate), variables=template_vars)
                return file_obj.render()

        searched = ', '.join(str(p) for p in search_roots)
        raise FileNotFoundError(
            f"SQL file '{sql_file}' not found. Searched relative to caller and in: {searched}"
        )

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
        Jinja template. Airflow context (params, ds, ts, etc.) is also available
        for direct use in templates like {% if params.username %}.
        """
        final_template_vars = decorator_template_vars.copy()
        sig = inspect.signature(func)
        bound_args = sig.bind_partial(*args, **kwargs)
        bound_args.apply_defaults()

        airflow_context = _get_airflow_context()
        airflow_jinja_env = Environment(autoescape=False, undefined=Undefined)  # noqa: S701 Airflow templates are already values, not HTML

        for key, value in bound_args.arguments.items():
            final_template_vars[key] = _render_airflow_templates(
                value, airflow_context, airflow_jinja_env
            )

        final_template_vars.update(airflow_context)

        if sql_file_template_path:
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
                        f'Error rendering SQL template from function {_get_func_name(func)}:\n'
                        f'Error: {e}\n'
                        f"Template: '''{result}'''\n"
                        f'Variables: {final_template_vars}'
                    ) from e
            elif isinstance(result, File):
                return result.render(context=final_template_vars)
            else:
                raise ValueError(
                    f'Decorated function {_get_func_name(func)} '
                    'must return a SQL string or a airsql.File object '
                    "when 'sql_file' is not specified in the decorator."
                )

    def query(
        self,
        output_table: Optional[Table] = None,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        dry_run: bool = False,
        pre_truncate: bool = False,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """Decorator for SQL queries.

        Args:
            output_table: Table to write results to (optional)
            source_conn: Connection ID for simple queries without table parameters
            sql_file: Path to SQL file (relative to sql_files_path)
            dry_run: If True, simulate the operation without writing data
            pre_truncate: If True, truncate table before writing
            **template_vars: Variables to pass to Jinja template. Non-Jinja
                            variables are passed as kwargs to the operator for
                            dynamic task naming.
        """

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLQueryOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs: dict[str, Any] = {}
                if output_table:
                    op_kwargs['outlets'] = [output_table.as_asset()]

                # Pass through extra template vars to operator for dynamic task naming
                # Map legacy `dry_run` into operator-friendly `dry_run_flag`
                if 'dry_run' in template_vars:
                    op_kwargs['dry_run_flag'] = template_vars['dry_run']
                # Avoid passing unsupported kwargs (like `dry_run`) directly
                template_filtered = {
                    k: v for k, v in template_vars.items() if k != 'dry_run'
                }
                op_kwargs.update(template_filtered)

                # Validate output_table is provided
                if output_table is None:
                    raise ValueError(
                        'output_table is required for @sql.query decorator'
                    )

                operator = SQLQueryOperator(
                    task_id=_get_func_name(func),
                    sql=sql_query,
                    output_table=output_table,
                    source_conn=source_conn,
                    dry_run_flag=dry_run,
                    pre_truncate=pre_truncate,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def append(
        self,
        output_table: Table,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """Decorator for SQL queries that append data to a destination table.

        Args:
            output_table: Table to append data to
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template. Non-Jinja
                            variables are passed as kwargs to the operator for
                            dynamic task naming.
        """

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLAppendOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs: dict[str, Any] = {'outlets': [output_table.as_asset()]}
                # Pass through extra template vars for dynamic task naming
                if 'dry_run' in template_vars:
                    op_kwargs['dry_run_flag'] = template_vars['dry_run']
                template_filtered = {
                    k: v for k, v in template_vars.items() if k != 'dry_run'
                }
                op_kwargs.update(template_filtered)

                operator = SQLAppendOperator(
                    task_id=_get_func_name(func),
                    sql=sql_query,
                    output_table=output_table,
                    source_conn=source_conn,
                    dry_run_flag=dry_run,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def dataframe(
        self,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator for SQL queries that return a pandas DataFrame.

        Now TaskFlow-compatible: creates a proper task that can be used
        in dependencies and data passing.

        Args:
            source_conn: Connection ID for simple queries
            sql_file: Path to SQL file (relative to sql_files_path)
            **template_vars: Variables to pass to Jinja template
        """

        def decorator(func: Any) -> Any:
            @task(task_id=_get_func_name(func))
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any):
                from airsql.operators import SQLDataFrameOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                operator = SQLDataFrameOperator(
                    task_id=f'{_get_func_name(func)}_internal',
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
        method: str = 'replace',
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """Decorator for SQL operations that replace table content.

        Args:
            output_table: Table to replace content in
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            method: Replace method - 'replace' (default) or 'truncate'
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template. Non-Jinja
                            variables are passed as kwargs to the operator for
                            dynamic task naming.
        """

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                from airsql.operators import (  # noqa: PLC0415
                    SQLReplaceOperator,
                    SQLTruncateOperator,
                )

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {'outlets': [output_table.as_asset()]}
                # Pass through extra template vars for dynamic task naming
                if 'dry_run' in template_vars:
                    op_kwargs['dry_run_flag'] = template_vars['dry_run']
                template_filtered = {
                    k: v for k, v in template_vars.items() if k != 'dry_run'
                }
                op_kwargs.update(template_filtered)

                if method == 'truncate':
                    operator = SQLTruncateOperator(
                        task_id=_get_func_name(func),
                        sql=sql_query,
                        output_table=output_table,
                        source_conn=source_conn,
                        dry_run_flag=dry_run,
                        **op_kwargs,
                    )
                else:
                    operator = SQLReplaceOperator(
                        task_id=_get_func_name(func),
                        sql=sql_query,
                        output_table=output_table,
                        source_conn=source_conn,
                        dry_run_flag=dry_run,
                        **op_kwargs,
                    )

                return operator

            return wrapper

        return decorator

    def truncate(
        self,
        output_table: Table,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """Decorator for SQL operations that truncate table content and reload.

        Truncates the table while preserving table structure and resetting
        sequences.

        Args:
            output_table: Table to truncate and reload
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template. Non-Jinja
                            variables are passed as kwargs to the operator for
                            dynamic task naming.
        """

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLTruncateOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {'outlets': [output_table.as_asset()]}
                # Pass through extra template vars for dynamic task naming
                if 'dry_run' in template_vars:
                    op_kwargs['dry_run_flag'] = template_vars['dry_run']
                template_filtered = {
                    k: v for k, v in template_vars.items() if k != 'dry_run'
                }
                op_kwargs.update(template_filtered)

                operator = SQLTruncateOperator(
                    task_id=_get_func_name(func),
                    sql=sql_query,
                    output_table=output_table,
                    source_conn=source_conn,
                    dry_run_flag=dry_run,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def merge(
        self,
        output_table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        source_conn: Optional[str] = None,
        sql_file: Optional[str] = None,
        pre_truncate: bool = False,
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """Decorator for SQL operations that merge/upsert into tables.

        Args:
            output_table: Table to merge data into
            conflict_columns: Columns to use for conflict resolution (ON clause)
            update_columns: Columns to update on conflict (optional, defaults to
                           all non-conflict columns)
            source_conn: Connection ID for the source database
            sql_file: Path to SQL file (relative to sql_files_path)
            pre_truncate: If True, truncate the table before performing merge
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template. Non-Jinja
                            variables are passed as kwargs to the operator for
                            dynamic task naming.
        """

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLMergeOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {'outlets': [output_table.as_asset()]}
                # Pass through extra template vars for dynamic task naming
                if 'dry_run' in template_vars:
                    op_kwargs['dry_run_flag'] = template_vars['dry_run']
                template_filtered = {
                    k: v for k, v in template_vars.items() if k != 'dry_run'
                }
                op_kwargs.update(template_filtered)

                operator = SQLMergeOperator(
                    task_id=_get_func_name(func),
                    sql=sql_query,
                    output_table=output_table,
                    conflict_columns=conflict_columns,
                    update_columns=update_columns,
                    source_conn=source_conn,
                    pre_truncate=pre_truncate,
                    dry_run_flag=dry_run,
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
        dataframe=None,
        dry_run: bool = False,
        **extra_kwargs,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """
        Decorator for functions that return a DataFrame to be loaded into a table,
        or for directly loading a provided DataFrame.

        Args:
            output_table: Table to write DataFrame to
            timestamp_column: Custom timestamp column name (optional)
            if_exists: How to behave if table exists ('append', 'replace',
                      'truncate', 'fail')
            dataframe: Pre-existing DataFrame to load (optional)
            dry_run: If True, simulate the operation without writing data
            **extra_kwargs: Extra keyword arguments for dynamic task naming and mapping

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

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                import pandas as pd  # noqa: PLC0415

                from airsql.operators import DataFrameLoadOperator  # noqa: PLC0415

                # Use provided dataframe or get it from function
                if dataframe is not None:
                    df = dataframe
                else:
                    df = func(*args, **kwargs)

                if not isinstance(df, pd.DataFrame):
                    raise ValueError(
                        f'Function {_get_func_name(func)} must return a pandas DataFrame '
                        'or a DataFrame must be provided to the decorator'
                    )

                op_kwargs = {'outlets': [output_table.as_asset()]}
                # Map legacy `dry_run` into `dry_run_flag` and avoid passing
                # unsupported kwargs (like `dry_run`) directly to operators.
                if 'dry_run' in extra_kwargs:
                    op_kwargs['dry_run_flag'] = extra_kwargs['dry_run']
                extra_filtered = {
                    k: v for k, v in extra_kwargs.items() if k != 'dry_run'
                }
                op_kwargs.update(extra_filtered)

                operator = DataFrameLoadOperator(
                    task_id=_get_func_name(func),
                    dataframe=df,
                    output_table=output_table,
                    timestamp_column=timestamp_column,
                    if_exists=if_exists,
                    dry_run_flag=dry_run,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    @staticmethod
    def merge_dataframe(
        output_table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        dataframe=None,
        dry_run: bool = False,
        **extra_kwargs,
    ) -> Callable[[Callable[..., Any]], Callable[..., BaseOperator]]:
        """
        Decorator for functions that return a DataFrame to be merged/upserted
        into a table, or for directly merging a provided DataFrame.

        Args:
            output_table: Table to merge DataFrame into
            conflict_columns: Columns to use for conflict resolution (ON clause)
            update_columns: Columns to update on conflict (optional, defaults to
                           all non-conflict columns)
            timestamp_column: Custom timestamp column name (optional)
            dataframe: Pre-existing DataFrame to merge (optional)
            dry_run: If True, simulate the operation without writing data
            **extra_kwargs: Extra keyword arguments for dynamic task naming and mapping

        Example 1 - Function that returns DataFrame:
            @sql.merge_dataframe(
                output_table=Table(
                    conn_id="bigquery_conn",
                    table_name="analytics.user_events"
                ),
                conflict_columns=['user_id', 'event_date'],
                update_columns=['event_count', 'last_updated']  # Only update these columns
            )
            def update_user_events():
                # Your DataFrame creation logic
                return pd.DataFrame({
                    'user_id': [1, 2],
                    'event_date': ['2025-05-29', '2025-05-29'],
                    'event_count': [10, 15],
                    'last_updated': [datetime.now(), datetime.now()]
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

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                import pandas as pd  # noqa: PLC0415

                from airsql.operators import DataFrameMergeOperator  # noqa: PLC0415

                # Use provided dataframe or get it from function
                if dataframe is not None:
                    df = dataframe
                else:
                    df = func(*args, **kwargs)

                if not isinstance(df, pd.DataFrame):
                    raise ValueError(
                        f'Function {_get_func_name(func)} must return a pandas DataFrame '
                        'or a DataFrame must be provided to the decorator'
                    )

                op_kwargs = {'outlets': [output_table.as_asset()]}
                # Map legacy `dry_run` into `dry_run_flag` and avoid passing
                # unsupported kwargs (like `dry_run`) directly to operators.
                if 'dry_run' in extra_kwargs:
                    op_kwargs['dry_run_flag'] = extra_kwargs['dry_run']
                extra_filtered = {
                    k: v for k, v in extra_kwargs.items() if k != 'dry_run'
                }
                op_kwargs.update(extra_filtered)

                operator = DataFrameMergeOperator(
                    task_id=_get_func_name(func),
                    dataframe=df,
                    output_table=output_table,
                    conflict_columns=conflict_columns,
                    update_columns=update_columns,
                    timestamp_column=timestamp_column,
                    dry_run_flag=dry_run,
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
        **template_vars: Any,
    ) -> Callable[[Any], BaseOperator]:
        """Decorator for SQL data quality checks (for dbt tests).

        Uses Airflow's native SQLCheckOperator which expects SQL that returns a
        single row. Each value is evaluated using Python bool casting - if any
        value is False, the check fails.

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

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLCheckOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                operator = SQLCheckOperator(
                    task_id=_get_func_name(func),
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
        **template_vars: Any,
    ) -> Callable[[Any], BaseOperator]:
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

        def decorator(func: Any) -> Any:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> BaseOperator:
                from airsql.operators import SQLQueryOperator  # noqa: PLC0415

                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                op_kwargs = {}
                if output_table:
                    op_kwargs['outlets'] = [output_table.as_asset()]

                operator = SQLQueryOperator(
                    task_id=_get_func_name(func),
                    sql=sql_query,
                    output_table=None,
                    source_conn=source_conn,
                    **op_kwargs,
                )

                return operator

            return wrapper

        return decorator

    def extract_and_merge(
        self,
        output_table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        source_conn: Optional[str] = None,
        timestamp_column: Optional[str] = None,
        sql_file: Optional[str] = None,
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable:
        """TaskFlow-compatible decorator that extracts data via SQL and merges.

        Combines SQL extraction and DataFrame merge operations in a single
        task. The decorated function should return SQL that extracts data to
        be merged.

        Args:
            output_table: Table to merge extracted data into
            conflict_columns: Columns to use for conflict resolution
            update_columns: Columns to update on conflict (optional, defaults to
                           all non-conflict columns)
            source_conn: Connection ID for the source database
            timestamp_column: Custom timestamp column name (optional)
            sql_file: Path to SQL file (relative to sql_files_path)
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template

        Example:
            @sql.extract_and_merge(
                output_table=Table(
                    conn_id="bigquery_conn",
                    table_name="analytics.user_events"
                ),
                conflict_columns=['user_id', 'event_date'],
                update_columns=['event_count', 'last_updated'],  # Only update these columns
                source_conn="postgres_conn"
            )
            def extract_user_events():
                return '''
                    SELECT user_id, event_date, COUNT(*) as event_count, NOW() as last_updated
                    FROM raw_events
                    WHERE event_date = '{{ ds }}'
                    GROUP BY user_id, event_date
                '''
        """

        def decorator(func: Any) -> Any:
            @task(task_id=_get_func_name(func))
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> str:
                from airsql.operators import (  # noqa: PLC0415
                    DataFrameMergeOperator,
                    SQLDataFrameOperator,
                )

                # First, extract the data using SQL
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                # Get DataFrame from SQL query
                dataframe_operator = SQLDataFrameOperator(
                    task_id=f'{_get_func_name(func)}_extract',
                    sql=sql_query,
                    source_conn=source_conn,
                )

                context = get_current_context()
                df = dataframe_operator.execute(context)

                # Merge the DataFrame into the target table
                merge_operator = DataFrameMergeOperator(
                    task_id=f'{_get_func_name(func)}_merge',
                    dataframe=df,
                    output_table=output_table,
                    conflict_columns=conflict_columns,
                    update_columns=update_columns,
                    timestamp_column=timestamp_column,
                    dry_run_flag=dry_run,
                    outlets=[output_table.as_asset()],
                )

                merge_operator.execute(context)
                return f'Extracted and merged {len(df)} rows into {output_table.table_name}'

            return wrapper

        return decorator

    def extract_and_load(
        self,
        output_table: Table,
        source_conn: Optional[str] = None,
        timestamp_column: Optional[str] = None,
        if_exists: str = 'append',
        sql_file: Optional[str] = None,
        dry_run: bool = False,
        **template_vars: Any,
    ) -> Callable:
        """TaskFlow-compatible decorator that extracts data via SQL and loads.

        Combines SQL extraction and DataFrame load operations in a single task.
        The decorated function should return SQL that extracts data to be
        loaded.

        Args:
            output_table: Table to load extracted data into
            source_conn: Connection ID for the source database
            timestamp_column: Custom timestamp column name (optional)
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            sql_file: Path to SQL file (relative to sql_files_path)
            dry_run: If True, simulate the operation without writing data
            **template_vars: Variables to pass to Jinja template

        Example:
            @sql.extract_and_load(
                output_table=Table(
                    conn_id="postgres_conn",
                    table_name="analytics.daily_summary"
                ),
                source_conn="bigquery_conn",
                if_exists='replace'
            )
            def extract_daily_summary():
                return '''
                    SELECT
                        DATE(created_at) as summary_date,
                        COUNT(*) as total_records,
                        AVG(amount) as avg_amount
                    FROM transactions
                    WHERE DATE(created_at) = '{{ ds }}'
                    GROUP BY DATE(created_at)
                '''
        """

        def decorator(func: Any) -> Any:
            @task(task_id=_get_func_name(func))
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> str:
                from airsql.operators import (  # noqa: PLC0415
                    DataFrameLoadOperator,
                    SQLDataFrameOperator,
                )

                # First, extract the data using SQL
                sql_query = self._process_sql_input(
                    func, args, kwargs, sql_file, **template_vars
                )

                # Get DataFrame from SQL query
                dataframe_operator = SQLDataFrameOperator(
                    task_id=f'{_get_func_name(func)}_extract',
                    sql=sql_query,
                    source_conn=source_conn,
                )

                context = get_current_context()
                df = dataframe_operator.execute(context)

                # Load the DataFrame into the target table
                load_operator = DataFrameLoadOperator(
                    task_id=f'{_get_func_name(func)}_load',
                    dataframe=df,
                    output_table=output_table,
                    timestamp_column=timestamp_column,
                    if_exists=if_exists,
                    dry_run_flag=dry_run,
                    outlets=[output_table.as_asset()],
                )

                load_operator.execute(context)
                return f'Extracted and loaded {len(df)} rows into {output_table.table_name}'

            return wrapper

        return decorator


sql = SQLDecorators()
