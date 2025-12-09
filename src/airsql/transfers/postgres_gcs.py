"""
Operator to transfer data from PostgreSQL to Google Cloud Storage.
"""

import time
import warnings
from io import BytesIO, StringIO
from typing import Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airsql.utils import DataValidator, OperationSummary


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

            # TODO: Implement schema_filename upload if needed
            if self.schema_filename:
                warnings.warn(
                    'Schema file upload is not yet implemented in PostgresToGCSOperator.',
                    UserWarning,
                    stacklevel=2,
                )
        else:
            self.log.info(
                f'[DRY RUN] Would upload {len(df)} rows as {self.export_format.upper()} to GCS: gs://{self.bucket}/{self.filename}'
            )

        self.log.info(summary.to_log_summary())
        return f'gs://{self.bucket}/{self.filename}'
