"""
Enhanced PostgreSQL to BigQuery transfer operator with sensor validation
and asset emission.
"""

from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, Context

from airsql.sensors.postgres import PostgresSqlSensor
from airsql.transfers.postgres_gcs import PostgresToGCSOperator

# Constants for destination table parsing
_FULL_TABLE_PARTS = 3  # project.dataset.table
_PARTIAL_TABLE_PARTS = 2  # dataset.table


class PostgresToBigQueryOperator(BaseOperator):
    """
    Enhanced operator that transfers data from PostgreSQL to BigQuery with:
    - Table existence and data validation using sensors
    - Temporary GCS staging with automatic cleanup
    - Asset emission for lineage tracking

    This operator combines PostgreSQL→GCS→BigQuery transfer with proper validation.
    """

    template_fields = ['sql', 'destination_table', 'gcs_temp_path']
    ui_color = '#4285f4'

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        sql: str | None = '',
        source_project_dataset_table: str | None = None,
        destination_project_dataset_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        gcs_bucket: str,
        gcs_temp_path: Optional[str] = None,
        export_format: str = 'parquet',
        schema_filename: Optional[str] = None,
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        write_disposition: str = 'WRITE_TRUNCATE',
        create_disposition: str = 'CREATE_IF_NEEDED',
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql or f'SELECT * FROM {source_project_dataset_table}'  # noqa: S608
        self.destination_table = destination_project_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.export_format = export_format.lower()
        self.schema_filename = schema_filename
        # Generate temp path with appropriate extension
        if self.export_format == 'parquet':
            file_ext = '.parquet'
        elif self.export_format == 'jsonl':
            file_ext = '.jsonl'
        else:
            file_ext = '.csv'
        self.gcs_temp_path = (
            gcs_temp_path or f'temp/postgres_to_bq/{self.task_id}/data{file_ext}'
        )

        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files
        self.dry_run = dry_run

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

    def execute(self, context: Context) -> Any:
        """Execute the PostgreSQL to BigQuery transfer."""

        if self.dry_run:
            self.log.info('[DRY RUN] PostgreSQL to BigQuery transfer')

        if self.check_source_exists:
            self._check_source_data(context)

        self.log.info(
            f'Extracting data from PostgreSQL to GCS: gs://{self.gcs_bucket}/{self.gcs_temp_path}'
        )
        # Choose schema filename: prefer user-provided, else derive from temp path
        derived_schema_name = self.schema_filename or (
            (self.gcs_temp_path + '.schema.json')
            if self.export_format == 'parquet'
            else None
        )

        pg_to_gcs = PostgresToGCSOperator(
            task_id=f'{self.task_id}_extract',
            postgres_conn_id=self.postgres_conn_id,
            sql=self.sql,
            bucket=self.gcs_bucket,
            filename=self.gcs_temp_path,
            gcp_conn_id=self.gcp_conn_id,
            export_format=self.export_format,
            schema_filename=derived_schema_name,
            dry_run=self.dry_run,
        )
        pg_to_gcs.execute(context)

        if not self.dry_run:
            self.log.info(
                f'Loading data from GCS to BigQuery: {self.destination_table}'
            )

            # Ensure destination dataset exists before loading
            self._ensure_bigquery_dataset()

            # Build GCSToBigQueryOperator kwargs based on format
            gcs_to_bq_kwargs = {
                'task_id': f'{self.task_id}_load',
                'bucket': self.gcs_bucket,
                'source_objects': [self.gcs_temp_path],
                'destination_project_dataset_table': self.destination_table,
                'gcp_conn_id': self.gcp_conn_id,
                'write_disposition': self.write_disposition,
                'create_disposition': self.create_disposition,
            }

            if self.export_format == 'csv':
                gcs_to_bq_kwargs['source_format'] = 'CSV'
                gcs_to_bq_kwargs['skip_leading_rows'] = 1
            elif self.export_format == 'jsonl':
                gcs_to_bq_kwargs['source_format'] = 'NEWLINE_DELIMITED_JSON'
                # BigQuery auto-detects schema for JSONL
                gcs_to_bq_kwargs['autodetect'] = True
            else:  # parquet
                gcs_to_bq_kwargs['source_format'] = 'PARQUET'
                # If we generated a schema file for parquet, pass it to BigQuery operator
                if derived_schema_name:
                    gcs_to_bq_kwargs['schema_object'] = derived_schema_name
                    gcs_to_bq_kwargs['schema_object_bucket'] = self.gcs_bucket
                    # Prefer explicit schema over autodetect to preserve types
                    gcs_to_bq_kwargs['autodetect'] = False

            gcs_to_bq = GCSToBigQueryOperator(**gcs_to_bq_kwargs)
            gcs_to_bq.execute(context)

            if self.cleanup_temp_files:
                self._cleanup_temp_files()

            self.log.info(
                f'Successfully transferred data from PostgreSQL to BigQuery table: {self.destination_table}'
            )
        else:
            self.log.info(
                f'[DRY RUN] Would load data from GCS to BigQuery: {self.destination_table}'
            )

        return self.destination_table

    def _check_source_data(self, context: Context) -> None:
        """Check if source table exists and has data."""
        if self.source_table_check_sql:
            check_sql = self.source_table_check_sql
        else:
            check_sql = f'SELECT 1 FROM ({self.sql}) AS subquery LIMIT 1'  # noqa: S608

        self.log.info('Checking source data availability')
        sensor = PostgresSqlSensor(
            task_id=f'{self.task_id}_source_check',
            conn_id=self.postgres_conn_id,
            sql=check_sql,
            retries=1,
            poke_interval=30,
            timeout=300,
        )
        sensor.execute(context)
        self.log.info('Source data validation successful')

    def _cleanup_temp_files(self) -> None:
        """Clean up temporary files from GCS."""
        try:
            self.log.info(
                f'Cleaning up temporary file: gs://{self.gcs_bucket}/{self.gcs_temp_path}'
            )
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            gcs_hook.delete(bucket_name=self.gcs_bucket, object_name=self.gcs_temp_path)
            self.log.info('Temporary file cleanup completed')
        except Exception as e:
            self.log.warning(f'Failed to cleanup temporary file: {e}')

    def _ensure_bigquery_dataset(self) -> None:
        """Ensure the destination BigQuery dataset exists, creating if necessary."""
        from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
            BigQueryHook,
        )
        from google.cloud import bigquery  # noqa: PLC0415

        try:
            # Parse project_id, dataset_id from destination_table
            # Format: project.dataset.table or dataset.table
            parts = self.destination_table.split('.')

            if len(parts) == _FULL_TABLE_PARTS:
                project_id, dataset_id, _ = parts
            elif len(parts) == _PARTIAL_TABLE_PARTS:
                dataset_id, _ = parts
                # Get project_id from BigQuery hook
                bq_hook = BigQueryHook(
                    gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False
                )
                project_id = bq_hook.project_id
            else:
                self.log.warning(
                    f'Could not parse destination table: {self.destination_table}. '
                    'Expected format: [project.]dataset.table'
                )
                return

            bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)

            # Create dataset if it doesn't exist using exists_ok parameter
            dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')
            dataset.location = 'US'  # Default location; can be customized

            self.log.info(
                f'Ensuring BigQuery dataset exists: {project_id}.{dataset_id}'
            )
            bq_hook.get_client(
                project_id=project_id, location=dataset.location
            ).create_dataset(dataset=dataset, exists_ok=True)
            self.log.info(f'BigQuery dataset ready: {project_id}.{dataset_id}')
        except Exception as e:
            self.log.error(f'Failed to ensure BigQuery dataset exists: {e}')
            raise
