"""
Operator to transfer data from PostgreSQL to Google Cloud Storage.
"""

import json
import os
import tempfile
import time
import warnings
from io import BytesIO, StringIO
from typing import Dict, List, Optional, Sequence

import pandas as pd
import pyarrow as pa
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airsql.utils import DataValidator, OperationSummary, ValidationResult

# PostgreSQL to BigQuery type mapping
POSTGRES_TO_BQ_TYPE_MAP = {
    'bool': 'BOOL',
    'bytea': 'BYTES',
    'date': 'DATE',
    'float4': 'FLOAT',
    'float8': 'FLOAT',
    'int2': 'INTEGER',
    'int4': 'INTEGER',
    'int8': 'INTEGER',
    'json': 'JSON',
    'jsonb': 'JSON',
    'numeric': 'NUMERIC',
    'text': 'STRING',
    'varchar': 'STRING',
    'timestamp': 'TIMESTAMP',
    'timestamptz': 'TIMETIME',
    'time': 'TIME',
    'timetz': 'TIME',
    'uuid': 'STRING',
    'inet': 'STRING',
    'cidr': 'STRING',
    'macaddr': 'STRING',
}


def _build_schema_from_column_types(
    column_types: Dict[str, str], json_columns: set
) -> List[Dict]:
    """Build BigQuery schema from PostgreSQL column types.

    Args:
        column_types: Dict of column names to PostgreSQL data types
        json_columns: Set of column names that are JSON/JSONB type

    Returns:
        List of dicts compatible with BigQuery load schema
    """
    schema = []
    for col_name, col_type in column_types.items():
        if col_name in json_columns:
            schema.append({'name': col_name, 'type': 'JSON', 'mode': 'NULLABLE'})
        else:
            bq_type = POSTGRES_TO_BQ_TYPE_MAP.get(col_type, 'STRING')
            schema.append({'name': col_name, 'type': bq_type, 'mode': 'NULLABLE'})
    return schema


def _pa_table_to_bq_schema(
    df,
    detect_json: bool = False,
    sample_size: int = 100,
    threshold: float = 0.9,
    postgres_type_map: Optional[Dict[str, Dict]] = None,
    json_mode: bool = False,
) -> List[Dict]:
    """Convert a pandas (pyarrow-backed) DataFrame or pyarrow.Table to BigQuery schema.

    Returns a list of dicts compatible with BigQuery load schema JSON: [{"name":..., "type":..., "mode":..., "fields": [...]}, ...]
    """
    import pyarrow as pa  # noqa: PLC0415

    # Ensure we have a pyarrow.Table
    if isinstance(df, pa.Table):
        table = df
    else:
        # from_pandas will convert pandas with pyarrow backend nicely
        table = pa.Table.from_pandas(df, preserve_index=False)

    def _field_to_bq(field: pa.Field) -> Dict:
        name = field.name
        typ = field.type

        # If we have Postgres metadata for this column, prefer it first
        if postgres_type_map and name in postgres_type_map:
            pg_info = postgres_type_map[name]
            pg_typname = pg_info.get('typname')
            # json/jsonb -> JSON
            if pg_typname in {'json', 'jsonb'}:
                return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
            # arrays: typname starting with '_' or typelem provided
            if pg_info.get('is_array'):
                if json_mode:
                    return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
                # map element type
                elem_typname = pg_info.get('element_typname')
                # if element is composite/record treat as RECORD REPEATED
                if elem_typname and elem_typname not in {
                    'int4',
                    'text',
                    'varchar',
                    'numeric',
                    'json',
                    'jsonb',
                }:
                    return {'name': name, 'type': 'RECORD', 'mode': 'REPEATED'}
                # otherwise map using simple mapping
                mapped = _simple_type(typ)
                return {'name': name, 'type': mapped, 'mode': 'REPEATED'}

        # LIST / REPEATED
        if pa.types.is_list(typ) or pa.types.is_large_list(typ):
            if json_mode:
                return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
            value_type = typ.value_type
            # nested list of structs -> RECORD REPEATED
            if pa.types.is_struct(value_type):
                fields = [_field_to_bq(sub) for sub in value_type]
                return {
                    'name': name,
                    'type': 'RECORD',
                    'mode': 'REPEATED',
                    'fields': fields,
                }
            else:
                mapped = _simple_type(value_type)
                return {'name': name, 'type': mapped, 'mode': 'REPEATED'}

        # STRUCT / RECORD
        if pa.types.is_struct(typ):
            if json_mode:
                return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
            fields = [_field_to_bq(sub) for sub in typ]
            return {
                'name': name,
                'type': 'RECORD',
                'mode': 'NULLABLE',
                'fields': fields,
            }

        bq_type = _simple_type(typ)

        # Optional JSON detection for string-backed fields (sampling)
        if detect_json and bq_type == 'STRING':
            try:
                # Try to sample values from the pyarrow column
                arr = table.column(name)
                non_null_indices = [i for i in range(len(arr)) if not arr.is_null(i)]
                if non_null_indices:
                    # sample up to sample_size indices evenly/randomly
                    if len(non_null_indices) <= sample_size:
                        sample_idx = non_null_indices
                    else:
                        # pick uniformly distributed sample indices for large arrays
                        step = max(1, len(non_null_indices) // sample_size)
                        sample_idx = non_null_indices[::step][:sample_size]

                    success = 0
                    total = 0
                    for i in sample_idx:
                        val = arr[i].as_py()
                        if val is None:
                            continue
                        total += 1
                        try:
                            json.loads(val)
                            success += 1
                        except Exception:
                            print('Value in column %s is not valid JSON: %s', name, val)

                    if total > 0 and (success / total) >= threshold:
                        return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
            except Exception as e:
                print('Failed to sample/detect JSON for column %s: %s', name, e)

        return {'name': name, 'type': bq_type, 'mode': 'NULLABLE'}

    def _simple_type(typ) -> str:
        # Map pyarrow types to BigQuery types
        if (
            pa.types.is_string(typ)
            or pa.types.is_large_string(typ)
            or pa.types.is_unicode(typ)
        ):
            return 'STRING'
        if pa.types.is_integer(typ) or pa.types.is_unsigned_integer(typ):
            return 'INTEGER'
        if pa.types.is_floating(typ):
            return 'FLOAT'
        if pa.types.is_boolean(typ):
            return 'BOOLEAN'
        if pa.types.is_timestamp(typ):
            return 'TIMESTAMP'
        if pa.types.is_date(typ):
            return 'DATE'
        if pa.types.is_time(typ):
            return 'TIME'
        if pa.types.is_binary(typ) or pa.types.is_large_binary(typ):
            return 'BYTES'
        if pa.types.is_decimal(typ):
            return 'NUMERIC'
        # BigQuery doesn't support JSON type in Parquet loads, convert to STRING
        if pa.types.is_struct(typ):
            return 'STRING'
        # Fallback for complex/unrecognized types (maps, dictionaries, etc.)
        # Represent as STRING to avoid load failures; user can refine schema later
        return 'STRING'

    schema = [_field_to_bq(f) for f in table.schema]
    return schema


class PostgresToGCSOperator(BaseOperator):
    """
    Copies data from Postgres to Google Cloud Storage in CSV or Parquet format.

    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
    :param sql: The SQL query to be executed.
    :type sql: str
    :param bucket: The GCS bucket to upload to.
    :type bucket: str
    :param filename: The GCS filename to upload to (including a folder).
    :type filename: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param export_format: (Optional) The format to export the data to.
        Supported formats are 'csv' and 'parquet'. Default is 'parquet'.
    :type export_format: str
    :param schema_filename: (Optional) If set, a GCS file with the schema
        will be uploaded.
    :type schema_filename: str
    :param pandas_chunksize: (Optional) The number of rows to include in each
        chunk processed by pandas.
    :type pandas_chunksize: int
    :param csv_kwargs: (Optional) Arguments to pass to `pandas.DataFrame.to_csv()`.
    :type csv_kwargs: dict
    :param parquet_kwargs: (Optional) Arguments to pass to `DataFrame.to_parquet()`.
    :type parquet_kwargs: dict
    """

    template_fields: Sequence[str] = (
        'sql',
        'bucket',
        'filename',
        'schema_filename',
    )
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#0077b6'

    def __init__(  # noqa: PLR0913
        self,
        *,
        postgres_conn_id: str,
        sql: str,
        bucket: str,
        filename: str,
        gcp_conn_id: str = 'google_cloud_default',
        export_format: str = 'parquet',
        schema_filename: Optional[str] = None,
        pandas_chunksize: Optional[int] = None,
        use_copy: bool = False,
        use_temp_file: bool = False,
        csv_kwargs: Optional[dict] = None,
        parquet_kwargs: Optional[dict] = None,
        # How to detect JSON/complex types for schema generation.
        # Options: 'postgres' (use Postgres metadata), 'sampling' (sample values), 'none' (no JSON detection)
        schema_detection_mode: str = 'postgres',
        sampling_size: int = 100,
        sampling_threshold: float = 0.9,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.gcp_conn_id = gcp_conn_id
        self.export_format = export_format.lower()
        self.schema_filename = schema_filename
        self.pandas_chunksize = pandas_chunksize
        self.use_copy = use_copy
        self.use_temp_file = use_temp_file
        self.csv_kwargs = csv_kwargs or {}
        self.parquet_kwargs = parquet_kwargs or {}
        self.schema_detection_mode = schema_detection_mode
        self.sampling_size = sampling_size
        self.sampling_threshold = sampling_threshold
        self.dry_run = dry_run

        if self.export_format not in {'csv', 'parquet', 'jsonl'}:
            raise ValueError(
                f"Unsupported format: {self.export_format}. Must be 'csv', 'parquet', or 'jsonl'."
            )

    def _detect_json_columns(self, pg_hook) -> set:
        """Detect JSON/JSONB columns from the source PostgreSQL query.

        Returns a set of column names that are JSON/JSONB type.
        """
        json_columns = set()
        try:
            type_query = f'SELECT * FROM ({self.sql}) AS subquery LIMIT 0'  # noqa: S608
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.execute(type_query)
            desc = cur.description
            col_names = [d[0] for d in desc]

            for i, d in enumerate(desc):
                type_oid = d[1]
                cur2 = conn.cursor()
                cur2.execute(
                    'SELECT typname FROM pg_type WHERE oid = %s',
                    (type_oid,),
                )
                row = cur2.fetchone()
                if row and row[0] in {'json', 'jsonb'}:
                    json_columns.add(col_names[i])
                cur2.close()
            cur.close()
        except Exception as e:
            self.log.warning(f'Failed to detect JSON columns: {e}')

        return json_columns

    def _get_column_types(self, pg_hook) -> Dict[str, str]:
        """Get column types from the source PostgreSQL query.

        Returns a dict of {column_name: data_type}.
        """
        column_types: Dict[str, str] = {}
        try:
            type_query = f'SELECT * FROM ({self.sql}) AS subquery LIMIT 0'  # noqa: S608
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.execute(type_query)
            desc = cur.description

            for d in desc:
                col_name = d[0]
                type_oid = d[1]
                cur2 = conn.cursor()
                cur2.execute(
                    'SELECT typname FROM pg_type WHERE oid = %s',
                    (type_oid,),
                )
                row = cur2.fetchone()
                if row:
                    column_types[col_name] = row[0]
                cur2.close()
            cur.close()
        except Exception as e:
            self.log.warning(f'Failed to get column types: {e}')

        return column_types

    def _build_copy_query(self, column_types: Dict[str, str], json_columns: set) -> str:
        """Build COPY TO query with proper type handling.

        Args:
            column_types: Dict of column names to PostgreSQL data types
            json_columns: Set of column names that are JSON/JSONB type

        Returns:
            COPY TO SQL query string
        """
        # Determine if we need JSONL format
        use_jsonl = bool(json_columns) or self.export_format == 'jsonl'

        if use_jsonl:
            # For JSONL, use row_to_json to convert entire row to JSON
            # This handles all types including JSON columns properly
            return f'SELECT row_to_json(t) FROM ({self.sql}) AS t'  # noqa: S608

        # For CSV format, build column list with proper escaping
        # Handle special types that need casting to text
        postgres_text_types = {'uuid', 'inet', 'cidr', 'macaddr'}
        columns = []

        for col_name, col_type in column_types.items():
            quoted_col = f'"{col_name}"' if col_name.lower() != col_name else col_name
            if col_type in postgres_text_types:
                columns.append(f'CAST({quoted_col} AS TEXT) AS {quoted_col}')
            else:
                columns.append(quoted_col)

        columns_str = ', '.join(columns)
        return f'SELECT {columns_str} FROM ({self.sql}) AS subquery'  # noqa: S608

    def _stream_copy_to_gcs(
        self, pg_hook, gcs_hook, copy_query: str, use_jsonl: bool
    ) -> int:
        """Stream data from PostgreSQL to GCS using COPY TO.

        Args:
            pg_hook: PostgresHook instance
            gcs_hook: GCSHook instance
            copy_query: The COPY TO SQL query
            use_jsonl: Whether to use JSONL format

        Returns:
            Number of rows extracted
        """
        try:
            from google.cloud.storage.fileio import BlobWriter  # noqa: PLC0415
        except ImportError:
            self.log.warning(
                'BlobWriter not available in this google-cloud-storage version. '
                'Falling back to temp file approach.'
            )
            return self._stream_copy_to_gcs_with_temp_file(
                pg_hook, gcs_hook, copy_query, use_jsonl, []
            )

        from google.cloud.storage import Client as GCSClient  # noqa: PLC0415

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Determine format for COPY
            if use_jsonl:
                copy_sql = f'COPY ({copy_query}) TO STDOUT'
            else:
                copy_sql = (
                    f'COPY ({copy_query}) TO STDOUT WITH (FORMAT CSV, HEADER true)'
                )

            # Get GCS client for streaming upload
            gcs_client = GCSClient()
            bucket = gcs_client.bucket(self.bucket)
            blob = bucket.blob(self.filename)

            # Stream directly to GCS using BlobWriter
            rows_count = 0
            with BlobWriter(blob) as writer:
                with cursor.copy(copy_sql) as copy:
                    for row in copy:
                        if use_jsonl:
                            # row is already a string from row_to_json
                            writer.write((row + '\n').encode('utf-8'))
                        else:
                            # For CSV, join values
                            writer.write(
                                (
                                    ','.join('' if v is None else str(v) for v in row)
                                    + '\n'
                                ).encode('utf-8')
                            )
                        rows_count += 1

                        # Log progress every million rows
                        if rows_count % 1000000 == 0:
                            self.log.info(f'COPY progress: {rows_count} rows extracted')

            self.log.info(
                f'Uploaded {rows_count} rows to GCS: gs://{self.bucket}/{self.filename}'
            )

            return rows_count

        finally:
            cursor.close()
            conn.close()

    def _stream_copy_to_gcs_with_temp_file(
        self,
        pg_hook,
        gcs_hook,
        copy_query: str,
        use_jsonl: bool,
        column_names: List[str],
    ) -> int:
        """Stream data from PostgreSQL to GCS using COPY TO with temp file.

        This is a fallback for environments where direct streaming doesn't work.

        Args:
            pg_hook: PostgresHook instance
            gcs_hook: GCSHook instance
            copy_query: The COPY TO SQL query
            use_jsonl: Whether to use JSONL format
            column_names: List of column names for CSV header

        Returns:
            Number of rows extracted
        """

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Determine format for COPY
            if use_jsonl:
                copy_sql = f'COPY ({copy_query}) TO STDOUT'
                mime_type = 'application/x-ndjson'
                suffix = '.jsonl'
            else:
                copy_sql = (
                    f'COPY ({copy_query}) TO STDOUT WITH (FORMAT CSV, HEADER false)'
                )
                mime_type = 'text/csv'
                suffix = '.csv'

            # Use temp file for streaming
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp_path = tmp.name

            try:
                rows_count = 0
                with open(tmp_path, 'wb') as f:
                    # Write CSV header if not JSONL
                    if not use_jsonl and column_names:
                        f.write((','.join(column_names) + '\n').encode('utf-8'))

                    with cursor.copy(copy_sql) as copy:
                        for row in copy:
                            if use_jsonl:
                                # row is already a string from row_to_json
                                f.write((row + '\n').encode('utf-8'))
                            else:
                                # For CSV, join values
                                f.write(
                                    (
                                        ','.join(
                                            '' if v is None else str(v) for v in row
                                        )
                                        + '\n'
                                    ).encode('utf-8')
                                )
                            rows_count += 1

                            # Log progress every million rows
                            if rows_count % 1000000 == 0:
                                self.log.info(
                                    f'COPY progress: {rows_count} rows extracted'
                                )

                # Upload the temp file to GCS
                self.log.info(
                    f'Uploading {rows_count} rows to GCS: gs://{self.bucket}/{self.filename}'
                )
                gcs_hook.upload(
                    bucket_name=self.bucket,
                    object_name=self.filename,
                    filename=tmp_path,
                    mime_type=mime_type,
                )

            finally:
                # Clean up temp file
                try:
                    os.remove(tmp_path)
                except Exception as e:
                    self.log.warning(f'Failed to remove temp file: {e}')

            return rows_count

        finally:
            cursor.close()
            conn.close()

    def _fix_timestamp_precision(self, df):
        """Fix timestamp precision to avoid TIMESTAMP_NANOS errors in BigQuery.

        BigQuery Parquet loader only supports TIMESTAMP_MILLIS and TIMESTAMP_MICROS.
        This method casts nanosecond timestamps to microsecond precision.
        """
        try:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Cast to microsecond precision and ensure PyArrow type is set correctly
                    df[col] = df[col].dt.round('us')
                    # Convert to PyArrow timestamp[us] to ensure Parquet uses TIMESTAMP_MICROS
                    try:
                        df[col] = df[col].astype(
                            pd.ArrowDtype(pa.timestamp('us', tz=df[col].dtype.tz))
                        )
                    except Exception:
                        # If conversion fails, keep the pandas native datetime64[us] type
                        df[col] = df[col].astype('datetime64[us]')
        except Exception as e:
            self.log.warning(f'Failed to fix timestamp precision: {e}')

        return df

    def execute(self, context) -> str:
        start_time = time.time()
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info(f'Extracting data from Postgres using query: {self.sql}')

        # Handle COPY TO mode for memory-efficient streaming
        if self.use_copy:
            self.log.info('Using COPY TO for memory-efficient streaming')

            # Get column types for schema and type handling
            column_types = self._get_column_types(pg_hook)
            column_names = list(column_types.keys())

            # Detect JSON columns
            json_columns = self._detect_json_columns(pg_hook)

            # Determine if we need JSONL format
            use_jsonl = bool(json_columns) or self.export_format == 'jsonl'
            export_format = 'jsonl' if use_jsonl else self.export_format

            if json_columns and self.export_format != 'jsonl':
                self.log.info(
                    f'Detected JSON columns: {json_columns}. Using JSONL format.'
                )

            # Build the COPY query
            copy_query = self._build_copy_query(column_types, json_columns)

            # Generate schema file for downstream operators
            if self.schema_filename or export_format == 'parquet':
                schema_data = _build_schema_from_column_types(
                    column_types, json_columns
                )
                schema_filename = self.schema_filename or (
                    self.filename + '.schema.json'
                )
                self.log.info(f'Generating schema file: {schema_filename}')
                schema_json = json.dumps(schema_data, indent=2)
                gcs_hook.upload(
                    bucket_name=self.bucket,
                    object_name=schema_filename,
                    data=schema_json.encode('utf-8'),
                    mime_type='application/json',
                )

            # Stream data to GCS (direct streaming by default, temp file as fallback)
            if self.use_temp_file:
                rows_extracted = self._stream_copy_to_gcs_with_temp_file(
                    pg_hook, gcs_hook, copy_query, use_jsonl, column_names
                )
            else:
                rows_extracted = self._stream_copy_to_gcs(
                    pg_hook, gcs_hook, copy_query, use_jsonl
                )

            self.log.info(f'Extracted {rows_extracted} rows from Postgres via COPY.')

            return f'gs://{self.bucket}/{self.filename}'

        # Original pandas-based extraction path
        self.log.info(
            f'Using pandas_chunksize={self.pandas_chunksize} for chunked extraction'
        )
        engine = pg_hook.get_sqlalchemy_engine()

        json_columns = self._detect_json_columns(pg_hook)
        export_format = self.export_format
        if json_columns and self.export_format == 'parquet':
            self.log.info(
                f'Detected JSON columns: {json_columns}. Switching to JSONL format.'
            )
            export_format = 'jsonl'

        tmp_path: Optional[str] = None
        data_bytes: bytes = b''
        mime_type = 'application/octet-stream'
        rows_extracted = 0
        validation_result = ValidationResult(is_valid=True)
        schema_source_df: Optional[pd.DataFrame] = None

        if self.pandas_chunksize:
            suffix = (
                '.parquet'
                if export_format == 'parquet'
                else ('.jsonl' if export_format == 'jsonl' else '.csv')
            )
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp_path = tmp.name
            tmp.close()

            first_chunk = True
            parquet_writer = None
            try:
                if export_format == 'parquet':
                    import pyarrow.parquet as pq  # noqa: PLC0415

                for chunk in pd.read_sql(
                    self.sql,
                    engine,
                    chunksize=self.pandas_chunksize,
                    dtype_backend='pyarrow',
                ):
                    if chunk.empty:
                        continue

                    fixed_chunk = self._fix_timestamp_precision(chunk)
                    rows_extracted += len(fixed_chunk)
                    self.log.info(
                        f'Processed chunk: {len(fixed_chunk)} rows (total: {rows_extracted})'
                    )
                    if schema_source_df is None:
                        schema_source_df = fixed_chunk

                    if export_format == 'csv':
                        csv_kwargs = {'index': False, **self.csv_kwargs}
                        fixed_chunk.to_csv(
                            tmp_path,
                            mode='a',
                            header=first_chunk,
                            **csv_kwargs,
                        )
                        mime_type = 'text/csv'
                    elif export_format == 'jsonl':
                        with open(tmp_path, 'a', encoding='utf-8') as f:
                            fixed_chunk.to_json(
                                f,
                                orient='records',
                                lines=True,
                                date_format='iso',
                            )
                        mime_type = 'application/x-ndjson'
                    else:
                        table = pa.Table.from_pandas(fixed_chunk, preserve_index=False)
                        if first_chunk:
                            parquet_writer = pq.ParquetWriter(tmp_path, table.schema)
                        parquet_writer.write_table(table)
                        mime_type = 'application/octet-stream'

                    first_chunk = False

                if parquet_writer:
                    parquet_writer.close()

            except Exception as e:
                if parquet_writer:
                    parquet_writer.close()
                if tmp_path:
                    try:
                        os.remove(tmp_path)
                    except Exception as exc:  # noqa: PLC410
                        self.log.debug(
                            'Failed to remove tmp file %s: %s', tmp_path, exc
                        )
                self.log.error(f'Failed during streaming export: {e}')
                raise

            if rows_extracted == 0:
                if tmp_path:
                    try:
                        os.remove(tmp_path)
                    except Exception as exc:  # noqa: PLC410
                        self.log.debug(
                            'Failed to remove tmp file %s: %s', tmp_path, exc
                        )
                self.log.info(
                    'No data extracted from Postgres. Skipping upload to GCS.'
                )
                return f'gs://{self.bucket}/{self.filename}'

            self.log.info(f'Extracted {rows_extracted} rows from Postgres.')
            file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
            validation_result = ValidationResult(is_valid=True)
        else:
            df = pd.read_sql(self.sql, engine, dtype_backend='pyarrow')
            if df.empty:
                self.log.info(
                    'No data extracted from Postgres. Skipping upload to GCS.'
                )
                return f'gs://{self.bucket}/{self.filename}'

            self.log.info(f'Extracted {len(df)} rows from Postgres.')

            df = self._fix_timestamp_precision(df)
            schema_source_df = df
            rows_extracted = len(df)

            validation_result = DataValidator.validate_row_count(df, min_rows=1)

            if export_format == 'csv':
                csv_kwargs = {'index': False, **self.csv_kwargs}
                data_buffer = StringIO()
                df.to_csv(data_buffer, **csv_kwargs)
                data_bytes = data_buffer.getvalue().encode('utf-8')
                mime_type = 'text/csv'
            elif export_format == 'jsonl':
                data_buffer = StringIO()
                df.to_json(
                    data_buffer,
                    orient='records',
                    lines=True,
                    date_format='iso',
                )
                data_bytes = data_buffer.getvalue().encode('utf-8')
                mime_type = 'application/x-ndjson'
            else:
                parquet_kwargs = {
                    'index': False,
                    'engine': 'pyarrow',
                    'coerce_timestamps': 'us',
                    **self.parquet_kwargs,
                }
                data_buffer = BytesIO()
                df.to_parquet(data_buffer, **parquet_kwargs)
                data_bytes = data_buffer.getvalue()
                mime_type = 'application/octet-stream'

            file_size_mb = len(data_bytes) / (1024 * 1024)

        # If requested, generate and upload a BigQuery-compatible schema JSON
        if self.schema_filename:
            try:
                self.log.info(
                    'Generating BigQuery schema file: %s', self.schema_filename
                )
                # Determine Postgres metadata mapping for columns if available
                postgres_type_map = None
                try:
                    # Use a helper query to obtain column types for the SQL used.
                    # We try to build a temporary query that uses the original SQL as subquery
                    # and inspects its columns using pg_type and information_schema.
                    # This is a best-effort approach and may be skipped if the SQL is complex.
                    type_query = f'SELECT * FROM ({self.sql}) AS subquery LIMIT 0'  # noqa: S608
                    conn = pg_hook.get_conn()
                    cur = conn.cursor()
                    cur.execute(type_query)
                    # cursor.description provides type_code; map via pg_type
                    desc = cur.description
                    col_names = [d[0] for d in desc]
                    postgres_type_map = {}
                    for i, d in enumerate(desc):
                        # d[1] is type_code per DB-API
                        type_oid = d[1]
                        # Query pg_type to get name and array/elem info
                        cur2 = conn.cursor()
                        cur2.execute(
                            'SELECT typname, typtype, typelem, typcategory FROM pg_type WHERE oid = %s',
                            (type_oid,),
                        )
                        row = cur2.fetchone()
                        if row:
                            typname = row[0]
                            typelem = row[2]
                            is_array = typelem != 0
                            elem_name = None
                            if is_array:
                                cur3 = conn.cursor()
                                cur3.execute(
                                    'SELECT typname FROM pg_type WHERE oid = %s',
                                    (typelem,),
                                )
                                er = cur3.fetchone()
                                elem_name = er[0] if er else None
                            postgres_type_map[col_names[i]] = {
                                'typname': typname,
                                'is_array': is_array,
                                'element_typname': elem_name,
                            }
                        cur2.close()
                    cur.close()
                except Exception:
                    postgres_type_map = None

                if schema_source_df is None:
                    raise ValueError(
                        'Could not infer schema source dataframe for schema generation'
                    )

                schema_fields = _pa_table_to_bq_schema(
                    schema_source_df,
                    detect_json=False,
                    postgres_type_map=postgres_type_map,
                    json_mode=(export_format == 'jsonl'),
                )
                gcs_hook.upload(
                    bucket_name=self.bucket,
                    object_name=self.schema_filename,
                    data=json.dumps(schema_fields).encode('utf-8'),
                    mime_type='application/json',
                )
                self.log.info(
                    'Schema file uploaded to gs://%s/%s',
                    self.bucket,
                    self.schema_filename,
                )
            except Exception as e:
                warnings.warn(
                    f'Failed to generate/upload schema file {self.schema_filename}: {e}',
                    UserWarning,
                    stacklevel=2,
                )

        if validation_result.errors:
            for error in validation_result.errors:
                self.log.error(f'Validation error: {error}')
        for warning in validation_result.warnings:
            self.log.warning(f'Validation warning: {warning}')

        duration = time.time() - start_time
        rows_loaded = rows_extracted if not self.dry_run else 0
        summary = OperationSummary(
            operation_type='export',
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            duration_seconds=duration,
            file_size_mb=file_size_mb,
            format_used=export_format,
            validation_errors=validation_result.errors,
            validation_warnings=validation_result.warnings,
            dry_run=self.dry_run,
        )
        if not self.dry_run:
            self.log.info(
                f'Uploading data as {export_format.upper()} to GCS: gs://{self.bucket}/{self.filename}'
            )
            # If we streamed into a temp file, upload from file to avoid huge memory usage
            if tmp_path:
                gcs_hook.upload(
                    bucket_name=self.bucket,
                    object_name=self.filename,
                    filename=tmp_path,
                    mime_type=mime_type,
                )
                # Cleanup tmp file
                try:
                    os.remove(tmp_path)
                except Exception:
                    self.log.debug('Failed to remove temporary file: %s', tmp_path)
            else:
                gcs_hook.upload(
                    bucket_name=self.bucket,
                    object_name=self.filename,
                    data=data_bytes,
                    mime_type=mime_type,
                )
            self.log.info('Upload complete.')
        else:
            self.log.info(
                f'[DRY RUN] Would upload {rows_extracted} rows as {export_format.upper()} to GCS: gs://{self.bucket}/{self.filename}'
            )
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    self.log.debug('Failed to remove temporary file: %s', tmp_path)

        self.log.info(summary.to_log_summary())
        return f'gs://{self.bucket}/{self.filename}'
