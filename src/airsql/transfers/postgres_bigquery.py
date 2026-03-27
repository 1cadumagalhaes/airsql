"""
Enhanced PostgreSQL to BigQuery transfer operator with sensor validation
and asset emission.
"""

from typing import Any, Iterable, List, Optional

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Context

from airsql.enums import (
    CreateDisposition,
    PartitionType,
    PostgresExportFormat,
    WriteDisposition,
)

# Constants for destination table parsing
_FULL_TABLE_PARTS = 3  # project.dataset.table
_PARTIAL_TABLE_PARTS = 2  # dataset.table


class PostgresToBigQueryOperator(BaseOperator):
    """Transfer data from PostgreSQL to BigQuery via GCS staging.

    This operator extracts data from PostgreSQL, uploads to a temporary GCS
    location, then loads into BigQuery. Includes source validation, automatic
    schema detection, and cleanup.

    Args:
        postgres_conn_id: PostgreSQL connection ID.
        sql: SQL query to extract data. Mutually exclusive with
            source_project_dataset_table.
        where: WHERE clause to filter data. Only applies when
            source_project_dataset_table is used. Mutually exclusive with sql.
        source_project_dataset_table: Source table (postgres.schema.table).
            Used if sql is not provided.
        destination_project_dataset_table: BigQuery destination table
            (project.dataset.table or dataset.table).
        gcp_conn_id: GCP connection ID. Defaults to 'google_cloud_default'.
        gcs_bucket: GCS bucket for temporary staging.
        gcs_temp_path: GCS path for temp files. Auto-generated if not provided.
        export_format: Export format: 'csv' or 'jsonl'. Defaults to 'csv'.
        schema_filename: GCS path for BigQuery schema JSON file. Optional.
        pandas_chunksize: Rows per chunk for large exports. Defaults to 100000.
        use_copy: If True, use PostgreSQL COPY for streaming. Defaults to False.
        use_temp_file: If True, use temporary file instead of streaming.
        check_source_exists: If True, validate source has data before transfer.
        source_table_check_sql: Custom SQL for source validation. Optional.
        write_disposition: BigQuery write disposition. Defaults to 'WRITE_TRUNCATE'.
        create_disposition: BigQuery create disposition. Defaults to 'CREATE_IF_NEEDED'.
        emit_asset: If True, emit Airflow asset for lineage. Defaults to True.
        cleanup_temp_files: If True, delete GCS temp files after load. Defaults to True.
        partition_by: BigQuery partition column. Optional.
        partition_type: Partition type: 'DAY', 'HOUR', 'MONTH', 'YEAR'.
            Defaults to 'DAY'.
        cluster_fields: BigQuery clustering columns. Optional.
        dataset_location: BigQuery dataset location. Defaults to 'us-central1'.
        create_if_empty: If True, create empty table when source is empty.
        auto_switch_format: If True, switch to JSONL for JSON columns.
        dry_run: If True, simulate the operation without writing data.
    """

    template_fields = ['sql', 'where', 'destination_table', 'gcs_temp_path']
    ui_color = '#4285f4'

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        sql: str | None = '',
        where: Optional[str] = None,
        source_project_dataset_table: str | None = None,
        destination_project_dataset_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        gcs_bucket: str,
        gcs_temp_path: Optional[str] = None,
        export_format: str = PostgresExportFormat.CSV,
        schema_filename: Optional[str] = None,
        pandas_chunksize: int = 100000,
        use_copy: bool = False,
        use_temp_file: bool = False,
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        write_disposition: str = WriteDisposition.WRITE_TRUNCATE,
        create_disposition: str = CreateDisposition.CREATE_IF_NEEDED,
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        partition_by: Optional[str] = None,
        partition_type: str = PartitionType.DAY,
        cluster_fields: Optional[List[str]] = None,
        dataset_location: str = 'us-central1',
        create_if_empty: bool = False,
        auto_switch_format: bool = True,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if sql and where:
            raise ValueError(
                'where clause cannot be used with sql parameter. '
                'Include the WHERE condition in your sql query instead.'
            )
        if sql and source_project_dataset_table:
            raise ValueError(
                'sql and source_project_dataset_table are mutually exclusive'
            )
        if where and not source_project_dataset_table:
            raise ValueError('where can only be used with source_project_dataset_table')

        if export_format not in PostgresExportFormat.values():
            raise ValueError(
                f"Invalid export_format: '{export_format}'. "
                f'Supported formats: {PostgresExportFormat.values()}. '
                f'Note: Parquet is not supported for Postgres export.'
            )

        self.postgres_conn_id = postgres_conn_id
        self.where = where
        self.source_project_dataset_table = source_project_dataset_table

        if sql:
            self.sql = sql
        elif where:
            self.sql = f'SELECT * FROM {source_project_dataset_table} WHERE {where}'
        else:
            self.sql = sql or f'SELECT * FROM {source_project_dataset_table}'  # noqa: S608

        self.destination_table = destination_project_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.export_format = export_format.lower()
        self.schema_filename = schema_filename
        self.pandas_chunksize = pandas_chunksize
        self.use_copy = use_copy
        self.use_temp_file = use_temp_file
        if self.export_format == 'parquet':
            file_ext = '.parquet'
        elif self.export_format == 'jsonl':
            file_ext = '.jsonl'
        else:
            file_ext = '.csv'
        self.gcs_temp_path = (
            gcs_temp_path
            or f'temp/postgres_to_bq/{self.task_id}/{{{{ ts_nodash }}}}/data{file_ext}'
        )

        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files
        self.partition_by = partition_by
        self.partition_type = partition_type.upper()
        self.cluster_fields = cluster_fields or []
        self.dataset_location = dataset_location
        self.create_if_empty = create_if_empty
        self.auto_switch_format = auto_switch_format
        self._skip_execution = dry_run

        if self.partition_type not in {'DAY', 'HOUR', 'MONTH', 'YEAR'}:
            raise ValueError('partition_type must be one of: DAY, HOUR, MONTH, YEAR')
        if self.cluster_fields and any(not f for f in self.cluster_fields):
            raise ValueError('cluster_fields cannot contain empty values')

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

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

    def execute(self, context: Context) -> Any:
        """Execute the PostgreSQL to BigQuery transfer."""

        if self._skip_execution:
            self.log.info('[DRY RUN] PostgreSQL to BigQuery transfer')

        if self.check_source_exists:
            self._check_source_data(context)

        # Detect JSON columns and adjust format if needed
        actual_export_format = self.export_format
        actual_gcs_temp_path = self.gcs_temp_path
        actual_schema_filename = self.schema_filename or self._get_schema_path(
            self.gcs_temp_path
        )

        # COPY mode produces CSV/JSONL, not parquet
        if self.use_copy:
            # JSON columns detection still applies
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            json_columns = self._detect_json_columns(pg_hook)
            if json_columns:
                actual_export_format = 'jsonl'
                actual_gcs_temp_path = self.gcs_temp_path.replace('.parquet', '.jsonl')
                actual_schema_filename = self.schema_filename or self._get_schema_path(
                    actual_gcs_temp_path
                )
                self.log.info(
                    f'Detected JSON columns: {json_columns}. Using JSONL format.'
                )
            else:
                actual_export_format = 'csv'
                actual_gcs_temp_path = self.gcs_temp_path.replace('.parquet', '.csv')
                actual_schema_filename = self.schema_filename or self._get_schema_path(
                    actual_gcs_temp_path
                )
                self.log.info('Using COPY mode: CSV format.')
        elif self.export_format == 'parquet':
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            json_columns = self._detect_json_columns(pg_hook)
            if json_columns:
                self.log.info(
                    f'Detected JSON columns: {json_columns}. Switching to JSONL format.'
                )
                actual_export_format = 'jsonl'
                # Change file extension from .parquet to .jsonl
                actual_gcs_temp_path = self.gcs_temp_path.replace('.parquet', '.jsonl')
                actual_schema_filename = self.schema_filename or self._get_schema_path(
                    actual_gcs_temp_path
                )

        self.log.info(
            f'Extracting data from PostgreSQL to GCS: gs://{self.gcs_bucket}/{actual_gcs_temp_path}'
        )

        from airsql.transfers.postgres_gcs import PostgresToGCSOperator  # noqa: PLC0415

        pg_to_gcs = PostgresToGCSOperator(
            task_id=f'{self.task_id}_extract',
            postgres_conn_id=self.postgres_conn_id,
            sql=self.sql,
            bucket=self.gcs_bucket,
            filename=actual_gcs_temp_path,
            gcp_conn_id=self.gcp_conn_id,
            export_format=actual_export_format,
            schema_filename=actual_schema_filename,
            pandas_chunksize=self.pandas_chunksize,
            use_copy=self.use_copy,
            use_temp_file=self.use_temp_file,
            auto_switch_format=self.auto_switch_format,
            dry_run=self._skip_execution,
        )
        actual_gcs_path = pg_to_gcs.execute(context)

        # Get actual export format and schema filename from the operator (may have changed during export)
        actual_export_format = (
            getattr(pg_to_gcs, 'actual_export_format', None) or actual_export_format
        )
        actual_schema_filename = (
            getattr(pg_to_gcs, 'schema_filename', None) or actual_schema_filename
        )
        actual_gcs_object = actual_gcs_path.replace(f'gs://{self.gcs_bucket}/', '')

        if not self._skip_execution:
            self.log.info(
                f'Loading data from GCS to BigQuery: {self.destination_table}'
            )

            # Ensure destination dataset exists before loading
            self._ensure_bigquery_dataset()

            # Lazy import - only load when actually executing
            from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (  # noqa: PLC0415
                GCSToBigQueryOperator,
            )

            # Build GCSToBigQueryOperator kwargs based on actual format
            gcs_to_bq_kwargs: dict[str, Any] = {
                'task_id': f'{self.task_id}_load',
                'bucket': self.gcs_bucket,
                'source_objects': [actual_gcs_object],
                'destination_project_dataset_table': self.destination_table,
                'gcp_conn_id': self.gcp_conn_id,
                'write_disposition': self.write_disposition,
                'create_disposition': self.create_disposition,
            }

            if actual_export_format == 'csv':
                gcs_to_bq_kwargs['source_format'] = 'CSV'
                gcs_to_bq_kwargs['skip_leading_rows'] = 1
                gcs_to_bq_kwargs['quote_character'] = '"'
                gcs_to_bq_kwargs['allow_quoted_newlines'] = True
                if actual_schema_filename:
                    gcs_to_bq_kwargs['schema_object'] = actual_schema_filename
                    gcs_to_bq_kwargs['schema_object_bucket'] = self.gcs_bucket
                    gcs_to_bq_kwargs['autodetect'] = False
            elif actual_export_format == 'jsonl':
                gcs_to_bq_kwargs['source_format'] = 'NEWLINE_DELIMITED_JSON'
                if actual_schema_filename:
                    gcs_to_bq_kwargs['schema_object'] = actual_schema_filename
                    gcs_to_bq_kwargs['schema_object_bucket'] = self.gcs_bucket
                    gcs_to_bq_kwargs['autodetect'] = False
                else:
                    gcs_to_bq_kwargs['autodetect'] = True
            else:  # parquet
                gcs_to_bq_kwargs['source_format'] = 'PARQUET'
                # If we have a schema file for parquet, pass it to BigQuery operator
                if actual_schema_filename:
                    gcs_to_bq_kwargs['schema_object'] = actual_schema_filename
                    gcs_to_bq_kwargs['schema_object_bucket'] = self.gcs_bucket
                    # Prefer explicit schema over autodetect to preserve types
                    gcs_to_bq_kwargs['autodetect'] = False

            if self.partition_by:
                time_partitioning = {
                    'type': self.partition_type,
                    'field': self.partition_by,
                }
                gcs_to_bq_kwargs['time_partitioning'] = time_partitioning

            if self.cluster_fields:
                gcs_to_bq_kwargs['cluster_fields'] = self.cluster_fields

            gcs_to_bq = GCSToBigQueryOperator(**gcs_to_bq_kwargs)
            gcs_to_bq.execute(context)

            if self.cleanup_temp_files:
                self._cleanup_temp_files([actual_gcs_object, actual_schema_filename])

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
        from airsql.sensors.postgres import PostgresSqlSensor  # noqa: PLC0415

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

    @staticmethod
    def _get_schema_path(gcs_temp_path: str) -> str:
        return f'{gcs_temp_path}.schema.json'

    def _cleanup_temp_files(
        self, temp_path: Optional[str | Iterable[str]] = None
    ) -> None:
        """Clean up temporary files from GCS.

        Args:
            temp_path: Path or paths to clean up. If not provided, uses the
                default data path and derived schema path.
        """
        from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: PLC0415

        if temp_path is None:
            cleanup_paths = [
                self.gcs_temp_path,
                self.schema_filename or self._get_schema_path(self.gcs_temp_path),
            ]
        elif isinstance(temp_path, str):
            cleanup_paths = [temp_path]
        else:
            cleanup_paths = list(temp_path)

        unique_cleanup_paths = list(
            dict.fromkeys(path for path in cleanup_paths if path)
        )

        try:
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            for cleanup_path in unique_cleanup_paths:
                self.log.info(
                    f'Cleaning up temporary file: gs://{self.gcs_bucket}/{cleanup_path}'
                )
                gcs_hook.delete(bucket_name=self.gcs_bucket, object_name=cleanup_path)
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
            dataset.location = self.dataset_location

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
