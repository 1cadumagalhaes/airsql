from typing import Any, List, Optional, Set

from airflow.models import BaseOperator
from airflow.sdk import Asset, Context


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

    VALID_EXPORT_FORMATS = {'parquet', 'csv', 'jsonl', 'avro'}

    FORMAT_EXTENSION_MAP = {
        'parquet': '.parquet',
        'csv': '.csv',
        'jsonl': '.jsonl',
        'avro': '.avro',
    }

    FORMAT_API_MAP = {
        'parquet': 'PARQUET',
        'csv': 'CSV',
        'jsonl': 'NEWLINE_DELIMITED_JSON',
        'avro': 'AVRO',
    }

    JSON_COLUMN_TYPES = {'JSON', 'JSONB'}

    TABLE_NAME_WITH_PROJECT_PARTS = 3
    TABLE_NAME_WITHOUT_PROJECT_PARTS = 2

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        postgres_conn_id: str,
        destination_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        gcs_bucket: str,
        gcs_temp_path: Optional[str] = None,
        export_format: str = 'parquet',
        auto_detect_json_columns: bool = True,
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        conflict_columns: Optional[List[str]] = None,
        replace: bool = True,
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_project_dataset_table = source_project_dataset_table
        self.source_table = source_project_dataset_table
        self.postgres_conn_id = postgres_conn_id
        self.destination_table = destination_table
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.export_format = export_format.lower()
        self.auto_detect_json_columns = auto_detect_json_columns

        if self.export_format not in self.VALID_EXPORT_FORMATS:
            raise ValueError(
                f"Unsupported export_format: '{self.export_format}'. "
                f'Must be one of: {", ".join(sorted(self.VALID_EXPORT_FORMATS))}'
            )

        file_ext = self.FORMAT_EXTENSION_MAP.get(self.export_format, '.parquet')
        self.gcs_temp_path = (
            gcs_temp_path or f'temp/bq_to_postgres/{self.task_id}/data{file_ext}'
        )
        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.conflict_columns = conflict_columns
        self.replace = replace
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files
        self.dry_run = dry_run

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

    def execute(self, context: Context) -> Any:
        """Execute the BigQuery to PostgreSQL transfer."""

        if self.dry_run:
            self.log.info('[DRY RUN] BigQuery to PostgreSQL transfer')

        if self.check_source_exists:
            self._check_source_data(context)

        actual_export_format = self.export_format
        actual_gcs_temp_path = self.gcs_temp_path

        if (
            not self.dry_run
            and self.auto_detect_json_columns
            and self.export_format == 'parquet'
        ):
            try:
                from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
                    BigQueryHook,
                )

                bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
                json_columns = self._detect_json_columns(bq_hook)
                if json_columns:
                    self.log.info(
                        f'Detected JSON columns: {json_columns}. Switching to JSONL format.'
                    )
                    actual_export_format = 'jsonl'
                    actual_gcs_temp_path = self.gcs_temp_path.replace(
                        '.parquet', '.jsonl'
                    )
            except Exception as e:
                self.log.warning(f'Failed to detect JSON columns: {e}')

        if not self.dry_run:
            self.log.info(
                f'Extracting data from BigQuery to GCS: gs://{self.gcs_bucket}/{actual_gcs_temp_path}'
            )

            # Lazy import - only load when actually executing
            from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (  # noqa: PLC0415
                BigQueryToGCSOperator,
            )

            # Map user-friendly format to BigQuery API format
            api_format = self.FORMAT_API_MAP.get(actual_export_format, 'PARQUET')

            # Build BigQueryToGCSOperator kwargs based on format
            bq_to_gcs_kwargs = {
                'task_id': f'{self.task_id}_extract',
                'source_project_dataset_table': self.source_table,
                'destination_cloud_storage_uris': [
                    f'gs://{self.gcs_bucket}/{actual_gcs_temp_path}'
                ],
                'gcp_conn_id': self.gcp_conn_id,
                'export_format': api_format,
            }
            # Only add print_header for CSV format
            if actual_export_format == 'csv':
                bq_to_gcs_kwargs['print_header'] = True

            bq_to_gcs = BigQueryToGCSOperator(**bq_to_gcs_kwargs)
            bq_to_gcs.execute(context)

            self.log.info(
                f'Loading data from GCS to PostgreSQL: {self.destination_table}'
            )
        else:
            self.log.info(
                f'[DRY RUN] Would extract data from BigQuery to GCS: gs://{self.gcs_bucket}/{actual_gcs_temp_path}'
            )
            self.log.info(
                f'[DRY RUN] Would load data from GCS to PostgreSQL: {self.destination_table}'
            )

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator  # noqa: PLC0415

        gcs_to_pg = GCSToPostgresOperator(
            task_id=f'{self.task_id}_load',
            target_table_name=self.destination_table,
            bucket_name=self.gcs_bucket,
            object_name=actual_gcs_temp_path,
            postgres_conn_id=self.postgres_conn_id,
            gcp_conn_id=self.gcp_conn_id,
            conflict_columns=self.conflict_columns,
            replace=self.replace,
            dry_run=self.dry_run,
        )
        gcs_to_pg.execute(context)

        if not self.dry_run and self.cleanup_temp_files:
            self._cleanup_temp_files(actual_gcs_temp_path)

        if not self.dry_run:
            self.log.info(
                f'Successfully transferred data from BigQuery to PostgreSQL table: {self.destination_table}'
            )

        return self.destination_table

    def _check_source_data(self, context: Context) -> None:
        """Check if source table exists and has data."""
        from airsql.sensors.bigquery import BigQuerySqlSensor  # noqa: PLC0415

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

    def _detect_json_columns(self, bq_hook: Any) -> Set[str]:
        """Detect JSON columns in the BigQuery table schema."""

        json_columns: Set[str] = set()

        try:
            parts = self.source_table.split('.')
            if len(parts) == self.TABLE_NAME_WITH_PROJECT_PARTS:
                project_id, dataset_id, table_id = parts
            elif len(parts) == self.TABLE_NAME_WITHOUT_PROJECT_PARTS:
                dataset_id, table_id = parts
                project_id = bq_hook.project_id
            else:
                self.log.warning(f'Invalid table name format: {self.source_table}')
                return json_columns

            client = bq_hook.get_client()
            table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
            table_obj = client.get_table(table_ref)

            for field in table_obj.schema:
                if field.field_type.upper() in self.JSON_COLUMN_TYPES:
                    json_columns.add(field.name)

        except Exception as e:
            self.log.warning(f'Failed to detect JSON columns: {e}')

        return json_columns

    def _cleanup_temp_files(self, gcs_temp_path: str) -> None:
        """Clean up temporary files from GCS."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: PLC0415

        try:
            self.log.info(
                f'Cleaning up temporary file: gs://{self.gcs_bucket}/{gcs_temp_path}'
            )
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            gcs_hook.delete(bucket_name=self.gcs_bucket, object_name=gcs_temp_path)
            self.log.info('Temporary file cleanup completed')
        except Exception as e:
            self.log.warning(f'Failed to cleanup temporary file: {e}')
