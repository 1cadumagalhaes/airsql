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
from airflow.sdk import Asset
from airflow.utils.context import Context

from airsql.sensors.postgres import PostgresSqlSensor
from airsql.transfers.postgres_gcs import PostgresToGCSOperator


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
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        write_disposition: str = 'WRITE_TRUNCATE',
        create_disposition: str = 'CREATE_IF_NEEDED',
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql or f'SELECT * FROM {source_project_dataset_table}'  # noqa: S608
        self.destination_table = destination_project_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_temp_path = (
            gcs_temp_path or f'temp/postgres_to_bq/{self.task_id}/data.csv'
        )

        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

    def execute(self, context: Context) -> Any:
        """Execute the PostgreSQL to BigQuery transfer."""

        if self.check_source_exists:
            self._check_source_data(context)

        self.log.info(
            f'Extracting data from PostgreSQL to GCS: gs://{self.gcs_bucket}/{self.gcs_temp_path}'
        )
        pg_to_gcs = PostgresToGCSOperator(
            task_id=f'{self.task_id}_extract',
            postgres_conn_id=self.postgres_conn_id,
            sql=self.sql,
            bucket=self.gcs_bucket,
            filename=self.gcs_temp_path,
            gcp_conn_id=self.gcp_conn_id,
            export_format='csv',
        )
        pg_to_gcs.execute(context)

        self.log.info(f'Loading data from GCS to BigQuery: {self.destination_table}')
        gcs_to_bq = GCSToBigQueryOperator(
            task_id=f'{self.task_id}_load',
            bucket=self.gcs_bucket,
            source_objects=[self.gcs_temp_path],
            destination_project_dataset_table=self.destination_table,
            gcp_conn_id=self.gcp_conn_id,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            source_format='CSV',
            skip_leading_rows=1,
        )
        gcs_to_bq.execute(context)

        if self.cleanup_temp_files:
            self._cleanup_temp_files()

        self.log.info(
            f'Successfully transferred data from PostgreSQL to BigQuery table: {self.destination_table}'
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
