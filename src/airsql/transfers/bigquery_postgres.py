from typing import Any, List, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.sdk import Asset
from airflow.utils.context import Context

from airsql.sensors.bigquery import BigQuerySqlSensor
from airsql.transfers.gcs_postgres import GCSToPostgresOperator


class BigQueryToPostgresOperator(BaseOperator):
    """
    Enhanced operator that transfers data from BigQuery to PostgreSQL with:
    - Table existence and data validation using sensors
    - Temporary GCS staging with automatic cleanup
    - Asset emission for lineage tracking

    This operator combines BigQuery→GCS→PostgreSQL transfer with proper validation.
    """

    template_fields = [
        'source_table',
        'destination_table',
        'gcs_temp_path',
    ]
    ui_color = '#336791'

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        postgres_conn_id: str,
        destination_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        gcs_bucket: str,
        gcs_temp_path: Optional[str] = None,
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        conflict_columns: Optional[List[str]] = None,
        replace: bool = True,
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_project_dataset_table = source_project_dataset_table
        self.source_table = source_project_dataset_table
        self.postgres_conn_id = postgres_conn_id
        self.destination_table = destination_table
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_temp_path = (
            gcs_temp_path or f'temp/bq_to_postgres/{self.task_id}/data.csv'
        )
        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.conflict_columns = conflict_columns
        self.replace = replace
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

    def execute(self, context: Context) -> Any:
        """Execute the BigQuery to PostgreSQL transfer."""

        if self.check_source_exists:
            self._check_source_data(context)

        self.log.info(
            f'Extracting data from BigQuery to GCS: gs://{self.gcs_bucket}/{self.gcs_temp_path}'
        )
        bq_to_gcs = BigQueryToGCSOperator(
            task_id=f'{self.task_id}_extract',
            source_project_dataset_table=self.source_table,
            destination_cloud_storage_uris=[
                f'gs://{self.gcs_bucket}/{self.gcs_temp_path}'
            ],
            gcp_conn_id=self.gcp_conn_id,
            export_format='CSV',
            print_header=True,
        )
        bq_to_gcs.execute(context)

        self.log.info(f'Loading data from GCS to PostgreSQL: {self.destination_table}')
        gcs_to_pg = GCSToPostgresOperator(
            task_id=f'{self.task_id}_load',
            target_table_name=self.destination_table,
            bucket_name=self.gcs_bucket,
            object_name=self.gcs_temp_path,
            postgres_conn_id=self.postgres_conn_id,
            gcp_conn_id=self.gcp_conn_id,
            conflict_columns=self.conflict_columns,
            replace=self.replace,
        )
        gcs_to_pg.execute(context)

        if self.cleanup_temp_files:
            self._cleanup_temp_files()

        self.log.info(
            'Successfully transferred data from BigQuery '
            'to PostgreSQL table: {self.destination_table}'
        )
        return self.destination_table

    def _check_source_data(self, context: Context) -> None:
        """Check if source table exists and has data."""
        if self.source_table_check_sql:
            check_sql = self.source_table_check_sql
        else:
            check_sql = f'SELECT 1 FROM `{self.source_table}` LIMIT 1'  # noqa: S608

        self.log.info('Checking source table existence and data availability')
        sensor = BigQuerySqlSensor(
            task_id=f'{self.task_id}_source_check',
            conn_id=self.gcp_conn_id,
            sql=check_sql,
            retries=1,
            poke_interval=30,
            timeout=300,
        )
        sensor.execute(context)
        self.log.info('Source table validation successful')

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
