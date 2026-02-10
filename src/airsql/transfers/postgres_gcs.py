"""
Operator to transfer data from PostgreSQL to Google Cloud Storage.
"""

import json
import time
import warnings
from io import BytesIO, StringIO
from typing import Dict, List, Optional, Sequence

import pyarrow as pa
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airsql.utils import DataValidator, OperationSummary


def _pa_table_to_bq_schema(
    df,
    detect_json: bool = False,
    sample_size: int = 100,
    threshold: float = 0.9,
    postgres_type_map: Optional[Dict[str, Dict]] = None,
) -> List[Dict]:
    """Convert a pandas (pyarrow-backed) DataFrame or pyarrow.Table to BigQuery schema.

    Returns a list of dicts compatible with BigQuery load schema JSON: [{"name":..., "type":..., "mode":..., "fields": [...]}, ...]
    """
    # Ensure we have a pyarrow.Table
    if isinstance(df, pa.Table):
        table = df
    else:
        # from_pandas will convert pandas with pyarrow backend nicely
        table = pa.Table.from_pandas(df, preserve_index=False)

    def _field_to_bq(field: pa.Field) -> Dict:
        name = field.name
        typ = field.type

        # LIST / REPEATED
        if pa.types.is_list(typ) or pa.types.is_large_list(typ):
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
            fields = [_field_to_bq(sub) for sub in typ]
            return {
                'name': name,
                'type': 'RECORD',
                'mode': 'NULLABLE',
                'fields': fields,
            }

        # SIMPLE TYPES
        # If we have Postgres metadata for this column, prefer it
        if postgres_type_map and name in postgres_type_map:
            pg_info = postgres_type_map[name]
            pg_typname = pg_info.get('typname')
            # json/jsonb -> JSON
            if pg_typname in {'json', 'jsonb'}:
                return {'name': name, 'type': 'JSON', 'mode': 'NULLABLE'}
            # arrays: typname starting with '_' or typelem provided
            if pg_info.get('is_array'):
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
        self.csv_kwargs = csv_kwargs or {}
        self.parquet_kwargs = parquet_kwargs or {}
        self.schema_detection_mode = schema_detection_mode
        self.sampling_size = sampling_size
        self.sampling_threshold = sampling_threshold
        self.dry_run = dry_run

        if self.export_format not in {'csv', 'parquet'}:
            raise ValueError(
                f"Unsupported format: {self.export_format}. Must be 'csv' or 'parquet'."
            )

    def execute(self, context) -> str:
        start_time = time.time()
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info(f'Extracting data from Postgres using query: {self.sql}')
        # Use PyArrow backend for better performance and type handling
        df = pg_hook.get_pandas_df(
            sql=self.sql, chunksize=self.pandas_chunksize, dtype_backend='pyarrow'
        )

        if df.empty:
            self.log.info('No data extracted from Postgres. Skipping upload to GCS.')
            return f'gs://{self.bucket}/{self.filename}'

        self.log.info(f'Extracted {len(df)} rows from Postgres.')

        # Validate DataFrame
        validation_result = DataValidator.validate_row_count(df, min_rows=1)
        if validation_result.errors:
            for error in validation_result.errors:
                self.log.error(f'Validation error: {error}')
        for warning in validation_result.warnings:
            self.log.warning(f'Validation warning: {warning}')

        if self.export_format == 'csv':
            csv_kwargs = {'index': False, **self.csv_kwargs}
            data_buffer = StringIO()
            df.to_csv(data_buffer, **csv_kwargs)
            data_bytes = data_buffer.getvalue().encode('utf-8')
            mime_type = 'text/csv'
        elif self.export_format == 'parquet':
            # PyArrow is the default and optimal engine for parquet with PyArrow-backed DataFrames
            parquet_kwargs = {
                'index': False,
                'engine': 'pyarrow',
                **self.parquet_kwargs,
            }
            data_buffer = BytesIO()
            df.to_parquet(data_buffer, **parquet_kwargs)
            data_bytes = data_buffer.getvalue()
            mime_type = 'application/octet-stream'

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

                schema_fields = _pa_table_to_bq_schema(
                    df, detect_json=False, postgres_type_map=postgres_type_map
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

        duration = time.time() - start_time
        summary = OperationSummary(
            operation_type='export',
            rows_extracted=len(df),
            rows_loaded=len(df) if not self.dry_run else 0,
            duration_seconds=duration,
            file_size_mb=len(data_bytes) / (1024 * 1024),
            format_used=self.export_format,
            validation_errors=validation_result.errors,
            validation_warnings=validation_result.warnings,
            dry_run=self.dry_run,
        )
        if not self.dry_run:
            self.log.info(
                f'Uploading data as {self.export_format.upper()} to GCS: gs://{self.bucket}/{self.filename}'
            )
            gcs_hook.upload(
                bucket_name=self.bucket,
                object_name=self.filename,
                data=data_bytes,
                mime_type=mime_type,
            )
            self.log.info('Upload complete.')
        else:
            self.log.info(
                f'[DRY RUN] Would upload {len(df)} rows as {self.export_format.upper()} to GCS: gs://{self.bucket}/{self.filename}'
            )

        self.log.info(summary.to_log_summary())
        return f'gs://{self.bucket}/{self.filename}'
