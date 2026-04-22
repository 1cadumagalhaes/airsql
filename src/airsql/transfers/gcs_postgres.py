import fnmatch
import hashlib
import importlib
import json
import time
from io import BytesIO, StringIO
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airsql.utils import DataValidator, OperationSummary


def _get_sql_module(conn):
    if conn is None:
        from psycopg import sql

        return sql
    conn_module = getattr(conn.__class__, '__module__', '')
    if conn_module.startswith('psycopg2'):
        from psycopg2 import sql

        return sql
    else:
        from psycopg import sql

        return sql


def _is_psycopg2_connection(conn):
    if conn is None:
        return False
    conn_module = getattr(conn.__class__, '__module__', '')
    return conn_module.startswith('psycopg2')


class GCSToPostgresOperator(BaseOperator):
    """Operator to load data from Google Cloud Storage to PostgreSQL.

    Downloads files from GCS and loads them into PostgreSQL tables. Supports
    CSV, JSONL, Parquet, and Avro formats with automatic type coercion.

    Args:
        target_table_name: PostgreSQL table name (schema.table or just table).
        bucket_name: GCS bucket name.
        object_name: GCS object path/filename.
        postgres_conn_id: PostgreSQL connection ID.
        gcp_conn_id: GCP connection ID. Defaults to 'google_cloud_default'.
        conflict_columns: Columns for upsert conflict resolution. Optional.
        replace: If True, replace table content. If False, append. Defaults to False.
        grant_table_privileges: If True, grant ALL privileges to PUBLIC.
            Defaults to True.
        create_if_empty: If True, create table when source is empty.
            Defaults to False.
        create_if_missing: If True, create table if it doesn't exist.
            Defaults to False.
        partition_column: Column name for partitioning. When set, creates one
            partition per unique value in the column. Requires replace=False.
            Defaults to None.
        source_schema: Source column types for type coercion. Optional.
        audit_cols_to_exclude: Columns to exclude from updates during upsert.
        dry_run: If True, simulate the operation without writing data.
    """

    JSON_COLUMN_TYPES = {'JSON', 'JSONB'}
    BQ_TO_PG_TYPE_MAP = {
        'INTEGER': 'INTEGER',
        'INT64': 'INTEGER',
        'FLOAT': 'DOUBLE PRECISION',
        'FLOAT64': 'DOUBLE PRECISION',
        'NUMERIC': 'NUMERIC',
        'BIGNUMERIC': 'NUMERIC',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'STRING': 'TEXT',
        'BYTES': 'BYTEA',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP',
        'TIME': 'TIME',
        'TIMESTAMP': 'TIMESTAMPTZ',
        'JSON': 'JSONB',
    }
    POSTGRES_IDENTIFIER_MAX_LENGTH = 63

    def __init__(
        self,
        target_table_name: str,
        bucket_name: str,
        object_name: str,
        postgres_conn_id: str,
        gcp_conn_id: str,
        conflict_columns=None,
        replace=False,
        grant_table_privileges: bool = True,
        create_if_empty: bool = False,
        create_if_missing: bool = False,
        partition_column: Optional[str] = None,
        source_schema: Optional[dict] = None,
        audit_cols_to_exclude=None,
        dry_run: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.postgres_conn_id = postgres_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.target_table_name = target_table_name
        self.conflict_columns = conflict_columns
        self.replace = replace
        self.grant_table_privileges = grant_table_privileges
        self.create_if_empty = create_if_empty
        self.create_if_missing = create_if_missing
        self.partition_column = partition_column
        self.source_schema = source_schema
        self._skip_execution = dry_run
        self.audit_cols_to_exclude = audit_cols_to_exclude or {
            'criado_em',
            'atualizado_em',
            'created_at',
            'updated_at',
        }

    @staticmethod
    def _dataframe_to_tuples(df):
        """Convert DataFrame to tuples with proper type conversion for PostgreSQL.

        Handles both PyArrow-backed and standard pandas DataFrames efficiently.
        Converts dict/list values to JSON strings for PostgreSQL JSON columns.
        """
        import numpy as np  # noqa: PLC0415
        import pandas as pd  # noqa: PLC0415

        if hasattr(df, '__arrow_c_stream__'):
            # PyArrow-backed DataFrames: convert to object dtype while preserving None
            df_clean = df.astype(object).where(pd.notna(df), None)
        else:
            df_clean = df.convert_dtypes().replace({pd.NA: None, np.nan: None})

        def convert_value(val):
            if val is None:
                return None
            if pd.isna(val):
                return None
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            return val

        return [
            tuple(convert_value(v) for v in row) for row in df_clean.values.tolist()
        ]

    @staticmethod
    def _convert_json_columns_to_strings(df, json_columns=None):
        """Convert dict/list columns to JSON strings for PostgreSQL.

        Args:
            df: DataFrame to convert
            json_columns: Optional set of column names to convert. If None, auto-detects.

        Returns:
            DataFrame with dict/list columns converted to JSON strings
        """

        df_copy = df.copy()

        if json_columns is None:
            json_columns = [
                col
                for col in df_copy.columns
                if df_copy[col].apply(lambda x: isinstance(x, (dict, list))).any()
            ]

        for col in json_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )

        return df_copy

    def _coerce_column_types(self, df, column_types):
        """Coerce DataFrame column types to match PostgreSQL table schema.

        BigQuery exports to Parquet may produce string values (e.g., "0.0", "100.5")
        even for numeric columns. This method coerces DataFrame columns to match
        the target PostgreSQL types.

        Args:
            df: DataFrame to coerce
            column_types: Dict mapping column names to PostgreSQL data types

        Returns:
            DataFrame with coerced column types
        """
        import pandas as pd  # noqa: PLC0415

        INTEGER_TYPES = {
            'integer',
            'bigint',
            'smallint',
            'int',
            'int2',
            'int4',
            'int8',
            'serial',
            'bigserial',
            'smallserial',
        }

        NUMERIC_TYPES = {
            'numeric',
            'decimal',
            'double precision',
            'float',
            'float4',
            'float8',
            'real',
        }

        BOOLEAN_TYPES = {'boolean', 'bool'}

        BQ_INTEGER_TYPES = {'INTEGER', 'INT64'}
        BQ_NUMERIC_TYPES = {'FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'}
        BQ_BOOLEAN_TYPES = {'BOOLEAN', 'BOOL'}

        df_copy = df.copy()

        for col, pg_type in column_types.items():
            if col not in df_copy.columns:
                continue

            pg_type_lower = pg_type.lower()
            col_dtype = str(df_copy[col].dtype)
            bq_type = (
                self.source_schema.get(col, '').upper() if self.source_schema else None
            )

            is_integer_target = pg_type_lower in INTEGER_TYPES
            is_integer_source = bq_type in BQ_INTEGER_TYPES

            is_numeric_target = pg_type_lower in NUMERIC_TYPES
            is_numeric_source = bq_type in BQ_NUMERIC_TYPES

            is_boolean_target = pg_type_lower in BOOLEAN_TYPES
            is_boolean_source = bq_type in BQ_BOOLEAN_TYPES

            # Coerce to integer
            if is_integer_target or is_integer_source:
                # Skip if already an integer type
                if (
                    col_dtype.startswith('int')
                    or col_dtype.startswith('Int')
                    or col_dtype.startswith('uint')
                ):
                    self.log.debug(
                        f'Column {col} already integer type {col_dtype}, skipping coercion'
                    )
                    continue

                self.log.info(
                    f'Column {col}: dtype={col_dtype}, pg_type={pg_type}, '
                    f'bq_type={bq_type}, coercing to Int64'
                )
                if col_dtype in {'float64', 'float32'}:
                    df_copy[col] = df_copy[col].astype('Int64')
                elif col_dtype in {'object', 'str', 'string[pyarrow]'}:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype(
                        'Int64'
                    )
                elif hasattr(df_copy[col].dtype, 'pyarrow_dtype'):
                    import pyarrow as pa  # noqa: PLC0415

                    if pa.types.is_floating(df_copy[col].dtype.pyarrow_dtype):
                        df_copy[col] = df_copy[col].astype('Int64')
                    elif pa.types.is_string(df_copy[col].dtype.pyarrow_dtype):
                        df_copy[col] = pd.to_numeric(
                            df_copy[col], errors='coerce'
                        ).astype('Int64')
                    else:
                        self.log.debug(
                            f'Column {col} has pyarrow type {df_copy[col].dtype.pyarrow_dtype}, not floating/string'
                        )

            # Coerce to numeric/float
            elif is_numeric_target or is_numeric_source:
                self.log.info(
                    f'Column {col}: dtype={col_dtype}, pg_type={pg_type}, '
                    f'bq_type={bq_type}, coercing to float64'
                )
                if col_dtype in {'object', 'str', 'string[pyarrow]'}:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                elif hasattr(df_copy[col].dtype, 'pyarrow_dtype'):
                    import pyarrow as pa  # noqa: PLC0415

                    if pa.types.is_string(df_copy[col].dtype.pyarrow_dtype):
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

            # Coerce to boolean
            elif is_boolean_target or is_boolean_source:
                self.log.info(
                    f'Column {col}: dtype={col_dtype}, pg_type={pg_type}, '
                    f'bq_type={bq_type}, coercing to boolean'
                )
                if col_dtype in {'object', 'str', 'string[pyarrow]'}:
                    df_copy[col] = df_copy[col].map({
                        'true': True,
                        'True': True,
                        'TRUE': True,
                        'false': False,
                        'False': False,
                        'FALSE': False,
                        '1': True,
                        '0': False,
                    })

        return df_copy

    def _grant_table_privileges(self, pg_hook, schema, table_name_simple):
        """Grant all privileges on table to public."""
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql_mod = _get_sql_module(conn)

        try:
            table_identifier = (
                sql_mod.Identifier(schema, table_name_simple)
                if schema
                else sql_mod.Identifier(table_name_simple)
            )

            grant_sql = sql_mod.SQL('GRANT ALL PRIVILEGES ON {table} TO PUBLIC').format(
                table=table_identifier
            )

            self.log.info(
                f'Granting all privileges on {schema}.{table_name_simple} to PUBLIC'
            )
            cursor.execute(grant_sql)

            conn.commit()
            self.log.info('Table privileges granted successfully')

        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to grant table privileges: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _detect_file_format(filename: str) -> str:
        """Detect file format from filename extension.

        Args:
            filename: The file path/name to detect format from

        Returns:
            'parquet', 'jsonl', 'avro', or 'csv'
        """
        if filename.endswith('.parquet'):
            return 'parquet'
        if filename.endswith('.jsonl'):
            return 'jsonl'
        if filename.endswith('.avro'):
            return 'avro'
        return 'csv'

    def execute(self, context):  # noqa: PLR0912, PLR0914
        import pandas as pd  # noqa: PLC0415

        from airsql import __version__

        self.log.info(f'airsql version: {__version__}')

        start_time = time.time()
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        file_format = self._detect_file_format(self.object_name)
        object_names = self._resolve_object_names(gcs_hook)
        df = self._read_dataframe_from_gcs_objects(gcs_hook, object_names, file_format)

        if '.' in self.target_table_name:
            schema, table_name_simple = self.target_table_name.split('.', 1)
        else:
            schema = 'public'
            table_name_simple = self.target_table_name

        table_name_full = f'{schema}.{table_name_simple}'

        table_exists = GCSToPostgresOperator._table_exists(
            pg_hook, schema, table_name_simple
        )

        if df.empty:
            if self.create_if_empty and not table_exists:
                self.log.info(
                    f'Source is empty and create_if_empty=True. '
                    f'Creating empty table {table_name_full} with inferred schema.'
                )
                if not self._skip_execution:
                    if self.partition_column:
                        self._create_partitioned_parent_table(
                            pg_hook, schema, table_name_simple, df
                        )
                    else:
                        self._create_empty_table_from_schema(
                            pg_hook, schema, table_name_simple, df
                        )
                return table_name_full
            self.log.info('Source file is empty. No data to load.')
            return table_name_full

        if not table_exists:
            if self.create_if_missing:
                self.log.info(
                    f'Table {table_name_full} does not exist and create_if_missing=True. '
                    f'Creating table with inferred schema.'
                )
                if not self._skip_execution:
                    if self.partition_column:
                        self._create_partitioned_parent_table(
                            pg_hook, schema, table_name_simple, df
                        )
                    else:
                        self._create_table_from_dataframe(
                            pg_hook, schema, table_name_simple, df
                        )
                table_exists = True
            else:
                raise ValueError(
                    f'Table {table_name_full} does not exist. '
                    f'Either create the table first or set create_if_missing=True.'
                )

        self.log.info(f'Fetching schema for Postgres table: {table_name_full}')
        sql_get_columns = """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;"""
        columns_from_db_records = pg_hook.get_records(
            sql_get_columns, parameters=(schema, table_name_simple)
        )

        if not columns_from_db_records:
            raise ValueError(
                f'Could not retrieve column information for table {table_name_full}.'
                ' Ensure the table exists.'
            )

        model_columns = [rec[0] for rec in columns_from_db_records]
        column_types = {rec[0]: rec[1] for rec in columns_from_db_records}
        common_columns = [col for col in df.columns if col in model_columns]
        df_filtered = df[common_columns].where(pd.notna(df[common_columns]), None)

        self.log.info(f'PostgreSQL column types: {column_types}')
        if self.source_schema:
            self.log.info(f'Source schema (BigQuery): {self.source_schema}')

        df_filtered = self._coerce_column_types(df_filtered, column_types)

        json_columns = self._detect_json_columns(pg_hook, schema, table_name_simple)
        if json_columns and file_format == 'jsonl':
            self.log.info(f'Detected JSON columns, fixing quoting: {json_columns}')
            df_filtered = GCSToPostgresOperator._fix_json_quoting(
                df_filtered, json_columns
            )

        validation_result = DataValidator.validate_columns(
            df_filtered, expected_columns=common_columns
        )
        if validation_result.errors:
            for error in validation_result.errors:
                self.log.error(f'Validation error: {error}')
        for warning in validation_result.warnings:
            self.log.warning(f'Validation warning: {warning}')

        duration = time.time() - start_time
        if self.partition_column:
            operation_type = 'partition_exchange'
        else:
            operation_type = (
                'replace'
                if self.replace
                else ('upsert' if self.conflict_columns else 'append')
            )
        summary = OperationSummary(
            operation_type=operation_type,
            rows_extracted=len(df),
            rows_loaded=len(df_filtered) if not self._skip_execution else 0,
            duration_seconds=duration,
            file_size_mb=self._get_total_file_size_mb(gcs_hook, object_names),
            format_used=file_format,
            validation_errors=validation_result.errors,
            validation_warnings=validation_result.warnings,
            dry_run=self._skip_execution,
        )

        if not self._skip_execution:
            if self.partition_column:
                self.log.info(
                    f'Performing partition exchange for {table_name_full} '
                    f'on column {self.partition_column}'
                )
                self._partition_exchange(
                    pg_hook, schema, table_name_simple, df_filtered
                )
            elif self.replace:
                self.log.info(
                    f'Truncating and replacing data in table {table_name_full}'
                )
                self._truncate_and_insert_data(
                    pg_hook, schema, table_name_simple, df_filtered
                )
            elif not self.conflict_columns:
                engine = pg_hook.get_sqlalchemy_engine()
                self.log.info(
                    f'Appending DataFrame to Postgres table {table_name_full}'
                )
                df_to_insert = self._convert_json_columns_to_strings(df_filtered)
                df_to_insert.to_sql(
                    name=table_name_simple,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000,
                )
                self.log.info('Append to Postgres complete.')
            else:
                self._upsert_data(pg_hook, schema, table_name_simple, df_filtered)

            self._grant_table_privileges(pg_hook, schema, table_name_simple)
        else:
            self.log.info(
                f'[DRY RUN] Would {operation_type} {len(df_filtered)} rows to {table_name_full}'
            )

        self.log.info(summary.to_log_summary())

    def _truncate_and_insert_data(
        self, pg_hook, schema, table_name_simple, df_filtered
    ):
        """Truncate table and insert new data to preserve table permissions."""
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql_mod = _get_sql_module(conn)
        is_psycopg2 = _is_psycopg2_connection(conn)

        try:
            table_identifier = (
                sql_mod.Identifier(schema, table_name_simple)
                if schema
                else sql_mod.Identifier(table_name_simple)
            )

            truncate_sql = sql_mod.SQL('TRUNCATE TABLE {table}').format(
                table=table_identifier
            )
            self.log.info(f'Truncating table {schema}.{table_name_simple}')
            cursor.execute(truncate_sql.as_string(cursor))

            insert_cols_ident = [sql_mod.Identifier(col) for col in df_filtered.columns]

            data_tuples = self._dataframe_to_tuples(df_filtered)

            if not is_psycopg2:
                copy_sql = sql_mod.SQL('COPY {table} ({columns}) FROM STDIN').format(
                    table=table_identifier,
                    columns=sql_mod.SQL(', ').join(insert_cols_ident),
                )
                with cursor.copy(copy_sql.as_string(cursor)) as copy:
                    for row in data_tuples:
                        copy.write_row(row)
            else:
                from psycopg2.extras import execute_values  # noqa: PLC0415

                insert_sql_psycopg2 = sql_mod.SQL(
                    'INSERT INTO {table} ({columns}) VALUES %s'
                ).format(
                    table=table_identifier,
                    columns=sql_mod.SQL(', ').join(insert_cols_ident),
                )
                execute_values(
                    cursor,
                    insert_sql_psycopg2.as_string(cursor),
                    data_tuples,
                    page_size=1000,
                )

            conn.commit()
            self.log.info('Truncate and insert to Postgres complete.')

        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to truncate and insert records: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    def _upsert_data(self, pg_hook, schema, table_name_simple, df_filtered):
        """Perform upsert operation using ON CONFLICT."""
        table_full_name = f'{schema}.{table_name_simple}'
        self.log.info(f'Upserting DataFrame into Postgres table {table_full_name}')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql_mod = _get_sql_module(conn)
        is_psycopg2 = _is_psycopg2_connection(conn)
        conflict_columns = self.conflict_columns or []

        try:
            if not conflict_columns:
                raise ValueError('conflict_columns is required for upsert operations')

            table_identifier = (
                sql_mod.Identifier(schema, table_name_simple)
                if schema
                else sql_mod.Identifier(table_name_simple)
            )

            insert_cols_ident = [sql_mod.Identifier(col) for col in df_filtered.columns]

            conflict_cols_ident = [sql_mod.Identifier(col) for col in conflict_columns]

            audit_cols_to_exclude = {
                'criado_em',
                'atualizado_em',
                'created_at',
                'updated_at',
            }
            update_set_cols = [
                col
                for col in df_filtered.columns
                if col not in conflict_columns
                and col.lower() not in audit_cols_to_exclude
            ]

            if not update_set_cols:
                update_sql_part = sql_mod.SQL('NOTHING')
            else:
                set_statements = [
                    sql_mod.SQL('{col_to_update} = EXCLUDED.{col_to_update}').format(
                        col_to_update=sql_mod.Identifier(col)
                    )
                    for col in update_set_cols
                ]
                update_sql_part = sql_mod.SQL('UPDATE SET {}').format(
                    sql_mod.SQL(', ').join(set_statements)
                )

            data_tuples = self._dataframe_to_tuples(df_filtered)

            if not is_psycopg2:
                col_names = ', '.join(df_filtered.columns)
                placeholders = ', '.join(['%s'] * len(df_filtered.columns))

                conflict_col_names = ', '.join(conflict_columns)

                if not update_set_cols:
                    update_clause = 'NOTHING'
                else:
                    set_clauses = [
                        f'"{col}" = EXCLUDED."{col}"' for col in update_set_cols
                    ]
                    update_clause = f'UPDATE SET {", ".join(set_clauses)}'

                final_sql = f'INSERT INTO {schema}.{table_name_simple} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({conflict_col_names}) DO {update_clause}'

                for i in range(0, len(data_tuples), 1000):
                    batch = data_tuples[i : i + 1000]
                    cursor.executemany(final_sql, batch)
            else:
                from psycopg2.extras import execute_values  # noqa: PLC0415

                insert_sql_psycopg2 = sql_mod.SQL(
                    'INSERT INTO {table} ({columns}) VALUES %s'
                ).format(
                    table=table_identifier,
                    columns=sql_mod.SQL(', ').join(insert_cols_ident),
                )

                conflict_sql_part = sql_mod.SQL(
                    'ON CONFLICT ({conflict_cols}) DO '
                ).format(conflict_cols=sql_mod.SQL(', ').join(conflict_cols_ident))

                final_sql_query_psycopg2 = sql_mod.SQL(' ').join([
                    insert_sql_psycopg2,
                    conflict_sql_part,
                    update_sql_part,
                ])
                execute_values(
                    cursor,
                    final_sql_query_psycopg2.as_string(cursor),
                    data_tuples,
                    page_size=1000,
                )
            conn.commit()
            self.log.info('Upsert to Postgres complete.')
        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to upsert records into database: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    def _resolve_object_names(self, gcs_hook) -> list[str]:
        if '*' not in self.object_name:
            return [self.object_name]

        wildcard_index = self.object_name.index('*')
        prefix = self.object_name[:wildcard_index]
        listed_objects = gcs_hook.list(bucket_name=self.bucket_name, prefix=prefix)
        matched_objects = sorted(
            object_name
            for object_name in listed_objects
            if fnmatch.fnmatch(object_name, self.object_name)
        )
        if not matched_objects:
            raise FileNotFoundError(
                f'No objects matched gs://{self.bucket_name}/{self.object_name}'
            )
        return matched_objects

    def _read_dataframe_from_gcs_objects(
        self, gcs_hook, object_names: list[str], file_format: str
    ):
        import pandas as pd  # noqa: PLC0415

        dataframes = [
            self._read_dataframe_from_bytes(
                gcs_hook.download(
                    bucket_name=self.bucket_name, object_name=object_name
                ),
                file_format,
            )
            for object_name in object_names
        ]
        if len(dataframes) == 1:
            return dataframes[0]
        return pd.concat(dataframes, ignore_index=True)

    @staticmethod
    def _read_dataframe_from_bytes(file_data: bytes, file_format: str):
        import pandas as pd  # noqa: PLC0415

        if file_format == 'parquet':
            return pd.read_parquet(BytesIO(file_data), engine='pyarrow')
        if file_format == 'jsonl':
            return pd.read_json(BytesIO(file_data), lines=True, dtype_backend='pyarrow')
        if file_format == 'avro':
            avro = importlib.import_module('pyarrow.avro')
            avro_reader: Any = avro
            table = avro_reader.read_table(BytesIO(file_data))
            return table.to_pandas()
        return pd.read_csv(StringIO(file_data.decode('utf-8')), dtype_backend='pyarrow')

    def _get_total_file_size_mb(self, gcs_hook, object_names: list[str]) -> float:
        total_size_bytes = sum(
            len(
                gcs_hook.download(bucket_name=self.bucket_name, object_name=object_name)
            )
            for object_name in object_names
        )
        return total_size_bytes / (1024 * 1024)

    @staticmethod
    def _detect_json_columns(pg_hook, schema: str, table_name: str) -> set:
        """Detect JSON/JSONB columns in the target PostgreSQL table.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Table name

        Returns:
            Set of column names that are JSON/JSONB type
        """
        json_columns = set()
        try:
            sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            AND data_type IN ('json', 'jsonb')
            """
            records = pg_hook.get_records(sql, parameters=(schema, table_name))
            for record in records:
                json_columns.add(record[0])
        except Exception:  # noqa: S110
            pass
        return json_columns

    @staticmethod
    def _fix_json_quoting(df, json_columns: set):
        """Fix JSON quoting from BigQuery exports.

        BigQuery exports JSON values with single quotes (Python dict repr format),
        but PostgreSQL expects standard JSON with double quotes.

        This method detects values that look like Python dict/list literals
        and converts them to valid JSON strings.

        Args:
            df: DataFrame with potential JSON columns
            json_columns: Set of column names that should contain JSON data

        Returns:
            DataFrame with fixed JSON columns
        """
        import ast  # noqa: PLC0415

        import numpy as np  # noqa: PLC0415

        for col in json_columns:
            if col not in df.columns:
                continue

            def fix_json_value(val):
                if val is None:
                    return None
                try:
                    if isinstance(val, float) and np.isnan(val):
                        return None
                except (TypeError, ValueError):
                    pass
                if isinstance(val, (dict, list)):
                    return json.dumps(val)
                if isinstance(val, str):
                    try:
                        parsed = ast.literal_eval(val)
                        return json.dumps(parsed)
                    except (ValueError, SyntaxError):
                        return val
                return val

            df[col] = df[col].apply(fix_json_value)

        return df

    @staticmethod
    def _table_exists(pg_hook, schema: str, table_name: str) -> bool:
        """Check if a table exists in PostgreSQL.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """
        result = pg_hook.get_records(sql, parameters=(schema, table_name))
        return result[0][0] if result else False

    def _create_empty_table_from_schema(
        self, pg_hook, schema: str, table_name: str, df
    ) -> None:
        """Create an empty table based on DataFrame schema.

        Uses pandas to_sql with if_exists='fail' to create the table structure.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Table name
            df: DataFrame with schema to use (can be empty)
        """
        engine = pg_hook.get_sqlalchemy_engine()
        empty_df = df.iloc[0:0] if len(df) > 0 else df
        empty_df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='fail',
            index=False,
        )
        self.log.info(f'Created empty table {schema}.{table_name}')

        if self.grant_table_privileges:
            self._grant_table_privileges(pg_hook, schema, table_name)

    def _partition_exchange(self, pg_hook, schema: str, table_name: str, df) -> None:
        """Perform partition exchange for incremental data loads.

        Creates partitions for each unique value in partition_column. For each
        partition, creates a temp table, loads data, then creates/attaches the
        partition. If partition already exists, detaches/drops it first.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Parent table name
            df: DataFrame with data to load
        """
        if self.partition_column not in df.columns:
            raise ValueError(
                f"Partition column '{self.partition_column}' not found in data. "
                f'Available columns: {list(df.columns)}'
            )

        unique_values = df[self.partition_column].dropna().unique()
        if len(unique_values) == 0:
            raise ValueError(
                f"No values found in partition column '{self.partition_column}'"
            )

        preview_count = 10
        self.log.info(
            f'Found {len(unique_values)} unique partition values: '
            f'{unique_values[:preview_count]}{"..." if len(unique_values) > preview_count else ""}'
        )

        partition_pg_type = self._get_partition_pg_type()

        for partition_value in unique_values:
            partition_value_str = self._stringify_partition_bound(partition_value)

            partition_value_safe = (
                partition_value_str.replace('-', '').replace(' ', '_').replace(':', '')
            )
            partition_name = f'{table_name}_{partition_value_safe}'
            full_parent_table = f'{schema}.{table_name}' if schema else table_name
            full_partition = f'{schema}.{partition_name}' if schema else partition_name

            df_partition = df[df[self.partition_column] == partition_value]

            self.log.info(
                f'Processing partition {partition_name} with {len(df_partition)} rows'
            )

            temp_table_name = self._build_partition_temp_table_name(
                table_name, partition_value_safe, partition_value_str
            )

            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            try:
                cursor.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
                conn.commit()

                cursor.execute(
                    f'CREATE TEMP TABLE {temp_table_name} '
                    f'(LIKE {full_parent_table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'
                )
                conn.commit()

                cursor.close()
                conn.close()

                engine = pg_hook.get_sqlalchemy_engine()
                df_to_insert = self._convert_json_columns_to_strings(df_partition)
                df_to_insert.to_sql(
                    name=temp_table_name,
                    con=engine,
                    schema=None,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000,
                )

                conn = pg_hook.get_conn()
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND n.nspname = %s
                    """,
                    (partition_name, schema or 'public'),
                )
                partition_exists = cursor.fetchone() is not None

                if partition_exists:
                    self.log.info(f'Detaching existing partition {full_partition}')
                    cursor.execute(
                        f'ALTER TABLE {full_parent_table} DETACH PARTITION {full_partition}'
                    )
                    self.log.info(f'Dropping old partition {full_partition}')
                    cursor.execute(f'DROP TABLE IF EXISTS {full_partition}')
                    conn.commit()

                partition_start = self._stringify_partition_bound(partition_value)
                partition_end = GCSToPostgresOperator._get_next_partition_value(
                    partition_value
                )
                partition_start_sql = self._quote_sql_literal(partition_start)
                partition_end_sql = self._quote_sql_literal(partition_end)

                cursor.execute(
                    f"""
                    CREATE TABLE {full_partition} PARTITION OF {full_parent_table}
                    FOR VALUES FROM ({partition_start_sql}::{partition_pg_type})
                    TO ({partition_end_sql}::{partition_pg_type})
                    """
                )
                conn.commit()

                self.log.info(f'Inserting data into partition {full_partition}')
                cursor.execute(
                    f'INSERT INTO {full_partition} SELECT * FROM {temp_table_name}'
                )

                conn.commit()
                self.log.info(f'Partition exchange complete for {full_partition}')

            except Exception as e:
                conn.rollback()
                self.log.error(
                    f'Failed partition exchange for {partition_value_str}: {e}'
                )
                raise
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
                conn.commit()
            cursor.close()
            conn.close()

    @classmethod
    def _build_partition_temp_table_name(
        cls, table_name: str, partition_value_safe: str, partition_value_str: str
    ) -> str:
        hash_suffix = hashlib.sha1(partition_value_str.encode('utf-8')).hexdigest()[:12]
        suffix = f'_{hash_suffix}'
        max_base_length = cls.POSTGRES_IDENTIFIER_MAX_LENGTH - len('_temp_') - len(
            suffix
        )
        combined_name = f'{table_name}_{partition_value_safe}'
        truncated_name = combined_name[:max_base_length]
        return f'_temp_{truncated_name}{suffix}'

    @staticmethod
    def _stringify_partition_bound(value) -> str:
        import datetime  # noqa: PLC0415

        if isinstance(value, datetime.datetime):
            return value.isoformat(sep=' ')
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        return str(value)

    def _get_partition_pg_type(self) -> str:
        if self.source_schema and self.partition_column in self.source_schema:
            bq_type = self.source_schema[self.partition_column].upper()
            return self.BQ_TO_PG_TYPE_MAP.get(bq_type, 'TEXT')
        return 'DATE'

    @staticmethod
    def _quote_sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _get_next_partition_value(value) -> str:
        """Get the next value for partition boundary (exclusive upper bound).

        For DATE: next day
        For other types: stringify and add a sentinel character

        Args:
            value: The partition value

        Returns:
            String representation of the next value
        """
        import datetime  # noqa: PLC0415

        if isinstance(value, (datetime.date, datetime.datetime)):
            if isinstance(value, datetime.datetime):
                next_val = value + datetime.timedelta(days=1)
                return next_val.isoformat(sep=' ')
            next_val = value + datetime.timedelta(days=1)
            return next_val.strftime('%Y-%m-%d')

        return str(value) + chr(255)

    def _create_partitioned_parent_table(
        self, pg_hook, schema: str, table_name: str, df
    ) -> None:
        """Create a partitioned parent table based on DataFrame schema.

        Creates the parent table with PARTITION BY RANGE clause. Does not create
        any partitions - those are created during partition exchange.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Table name
            df: DataFrame with data to infer schema from
        """
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        full_table_name = f'{schema}.{table_name}' if schema else table_name

        try:
            columns_sql = []
            for col in df.columns:
                if self.source_schema and col in self.source_schema:
                    bq_type = self.source_schema[col].upper()
                    pg_type = self.BQ_TO_PG_TYPE_MAP.get(bq_type, 'TEXT')
                elif col == self.partition_column:
                    pg_type = 'DATE'
                else:
                    pg_type = 'TEXT'

                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    pg_type = 'JSONB'

                columns_sql.append(f'"{col}" {pg_type}')

            columns_clause = ', '.join(columns_sql)
            partition_clause = f'PARTITION BY RANGE ("{self.partition_column}")'

            create_sql = (
                f'CREATE TABLE {full_table_name} ({columns_clause}) {partition_clause}'
            )

            self.log.info(f'Creating partitioned parent table: {create_sql}')
            cursor.execute(create_sql)
            conn.commit()
            self.log.info(f'Created partitioned parent table {full_table_name}')

            if self.grant_table_privileges:
                self._grant_table_privileges(pg_hook, schema, table_name)

        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to create partitioned parent table: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    def _create_table_from_dataframe(
        self, pg_hook, schema: str, table_name: str, df
    ) -> None:
        """Create a table based on DataFrame schema and types.

        Creates the table with appropriate PostgreSQL types, including JSON/JSONB
        for dict/list columns.

        Args:
            pg_hook: PostgresHook instance
            schema: Schema name
            table_name: Table name
            df: DataFrame with data to infer schema from
        """

        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql_mod = _get_sql_module(conn)

        json_columns = [
            col
            for col in df.columns
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any()
        ]

        columns_sql = []
        for col in df.columns:
            col_ident = sql_mod.Identifier(col)
            if col in json_columns:
                columns_sql.append(sql_mod.SQL('{} JSONB').format(col_ident))
            else:
                columns_sql.append(sql_mod.SQL('{} TEXT').format(col_ident))

        table_ident = (
            sql_mod.Identifier(schema, table_name)
            if schema
            else sql_mod.Identifier(table_name)
        )

        create_sql = sql_mod.SQL('CREATE TABLE {} ({})').format(
            table_ident, sql_mod.SQL(', ').join(columns_sql)
        )

        self.log.info(f'Creating table {schema}.{table_name} with inferred schema')
        cursor.execute(create_sql.as_string(cursor))
        conn.commit()
        cursor.close()

        if self.grant_table_privileges:
            self._grant_table_privileges(pg_hook, schema, table_name)
