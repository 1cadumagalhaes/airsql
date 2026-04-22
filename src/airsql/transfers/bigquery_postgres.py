import fnmatch
from typing import Any, List, Optional, Set

from airflow.models import BaseOperator
from airflow.sdk import Asset, Context

from airsql.enums import BigQueryExportFormat


class BigQueryToPostgresOperator(BaseOperator):
    """Transfer data from BigQuery to PostgreSQL via GCS staging.

    This operator extracts data from BigQuery to a temporary GCS location,
    then loads into PostgreSQL. Includes source validation, automatic format
    detection, and cleanup.

    Args:
        source_project_dataset_table: BigQuery source table
            (project.dataset.table or dataset.table). Required if sql is not provided.
        sql: SQL query to export. Mutually exclusive with source_project_dataset_table.
            If provided, the query result is exported to PostgreSQL.
        where: WHERE clause to filter data. Only applies when source_project_dataset_table
            is used. Mutually exclusive with sql.
        postgres_conn_id: PostgreSQL connection ID.
        destination_table: PostgreSQL destination table (schema.table).
        gcp_conn_id: GCP connection ID. Defaults to 'google_cloud_default'.
        gcs_bucket: GCS bucket for temporary staging.
        gcs_temp_path: GCS path for temp files. Auto-generated if not provided.
        export_format: Export format: 'parquet', 'csv', 'jsonl', or 'avro'.
            Defaults to 'parquet'.
        auto_detect_json_columns: If True, detect JSON columns from BigQuery.
            Defaults to True.
        check_source_exists: If True, validate source has data before transfer.
        source_table_check_sql: Custom SQL for source validation. Optional.
        conflict_columns: Columns for upsert conflict resolution. Optional.
        replace: If True, replace table content. If False and conflict_columns
            provided, perform upsert. Defaults to True.
        create_if_empty: If True, create table when source is empty.
            Defaults to False.
        create_if_missing: If True, create table if it doesn't exist.
            Defaults to False.
        partition_column: Column name for partitioning. When set, creates one
            partition per unique value in the column. Requires replace=False.
            Defaults to None.
        emit_asset: If True, emit Airflow asset for lineage. Defaults to True.
        cleanup_temp_files: If True, delete GCS temp files after load. Defaults to True.
        dry_run: If True, simulate the operation without writing data.
    """

    template_fields = [
        'source_table',
        'destination_table',
        'gcs_temp_path',
        'sql',
        'where',
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

    BQ_TO_PG_TYPE_MAP = {
        'INTEGER': 'integer',
        'INT64': 'integer',
        'FLOAT': 'double precision',
        'FLOAT64': 'double precision',
        'NUMERIC': 'numeric',
        'BIGNUMERIC': 'numeric',
        'BOOLEAN': 'boolean',
        'BOOL': 'boolean',
        'STRING': 'text',
        'BYTES': 'bytea',
        'DATE': 'date',
        'DATETIME': 'timestamp',
        'TIME': 'time',
        'TIMESTAMP': 'timestamptz',
        'JSON': 'jsonb',
    }

    TABLE_NAME_WITH_PROJECT_PARTS = 3
    TABLE_NAME_WITHOUT_PROJECT_PARTS = 2

    def __init__(
        self,
        *,
        source_project_dataset_table: Optional[str] = None,
        sql: Optional[str] = None,
        where: Optional[str] = None,
        postgres_conn_id: str,
        destination_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        gcs_bucket: str,
        gcs_temp_path: Optional[str] = None,
        export_format: str = BigQueryExportFormat.PARQUET,
        auto_detect_json_columns: bool = True,
        check_source_exists: bool = True,
        source_table_check_sql: Optional[str] = None,
        conflict_columns: Optional[List[str]] = None,
        replace: bool = True,
        create_if_empty: bool = False,
        create_if_missing: bool = False,
        partition_column: Optional[str] = None,
        emit_asset: bool = True,
        cleanup_temp_files: bool = True,
        dry_run: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if not source_project_dataset_table and not sql:
            raise ValueError(
                'Either source_project_dataset_table or sql must be provided'
            )
        if source_project_dataset_table and sql:
            raise ValueError(
                'source_project_dataset_table and sql are mutually exclusive'
            )
        if where and sql:
            raise ValueError(
                'where clause cannot be used with sql parameter. '
                'Include the WHERE condition in your sql query instead.'
            )
        if partition_column and replace:
            raise ValueError(
                'partition_column requires replace=False. '
                'Partition exchange is an incremental operation.'
            )

        self.source_project_dataset_table = source_project_dataset_table
        self.sql = sql
        self.where = where
        self.source_table = source_project_dataset_table or ''
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
        sanitized_run_id = "{{ run_id | replace(':', '_') | replace('+', '_') }}"
        self.gcs_temp_path = (
            gcs_temp_path
            or f'temp/bq_to_postgres/{self.task_id}/{sanitized_run_id}/data{file_ext}'
        )
        self.check_source_exists = check_source_exists
        self.source_table_check_sql = source_table_check_sql
        self.conflict_columns = conflict_columns
        self.replace = replace
        self.create_if_empty = create_if_empty
        self.create_if_missing = create_if_missing
        self.partition_column = partition_column
        self.emit_asset = emit_asset
        self.cleanup_temp_files = cleanup_temp_files
        self._skip_execution = dry_run

        if self.emit_asset:
            self.outlets = [Asset(f'airsql://database/{self.destination_table}')]

    def execute(self, context: Context) -> Any:
        """Execute the BigQuery to PostgreSQL transfer."""

        from airsql import __version__

        self.log.info(f'airsql version: {__version__}')

        if self._skip_execution:
            self.log.info('[DRY RUN] BigQuery to PostgreSQL transfer')

        if self.check_source_exists:
            self._check_source_data(context)

        actual_export_format = self.export_format
        actual_gcs_temp_path = self.gcs_temp_path
        load_gcs_temp_path = actual_gcs_temp_path
        bq_schema = {}

        if not self._skip_execution:
            from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
                BigQueryHook,
            )

            bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

            # Get BigQuery schema for type coercion
            bq_schema = self._get_bq_schema(bq_hook)
            if bq_schema:
                self.log.info(f'Fetched BigQuery schema: {bq_schema}')

            if self.auto_detect_json_columns and self.export_format == 'parquet':
                json_columns = self._detect_json_columns(bq_hook)
                if json_columns:
                    self.log.info(
                        f'Detected JSON columns: {json_columns}. Switching to JSONL format.'
                    )
                    actual_export_format = 'jsonl'
                    actual_gcs_temp_path = self.gcs_temp_path.replace(
                        '.parquet', '.jsonl'
                    )

        if not self._skip_execution:
            if self.sql or self.where:
                load_gcs_temp_path = self._get_query_export_path(actual_gcs_temp_path)

            self.log.info(
                f'Extracting data from BigQuery to GCS: gs://{self.gcs_bucket}/{load_gcs_temp_path}'
            )

            if self.sql or self.where:
                self._export_query_to_gcs(
                    context, actual_export_format, actual_gcs_temp_path
                )
            else:
                self._export_table_to_gcs(
                    context, actual_export_format, actual_gcs_temp_path
                )

            self.log.info(
                f'Loading data from GCS to PostgreSQL: {self.destination_table}'
            )
        else:
            self.log.info(
                f'[DRY RUN] Would extract data from BigQuery to GCS: gs://{self.gcs_bucket}/{load_gcs_temp_path}'
            )
            self.log.info(
                f'[DRY RUN] Would load data from GCS to PostgreSQL: {self.destination_table}'
            )

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator  # noqa: PLC0415

        gcs_to_pg = GCSToPostgresOperator(
            task_id=f'{self.task_id}_load',
            target_table_name=self.destination_table,
            bucket_name=self.gcs_bucket,
            object_name=load_gcs_temp_path,
            postgres_conn_id=self.postgres_conn_id,
            gcp_conn_id=self.gcp_conn_id,
            conflict_columns=self.conflict_columns,
            replace=self.replace,
            create_if_empty=self.create_if_empty,
            create_if_missing=self.create_if_missing,
            partition_column=self.partition_column,
            source_schema=bq_schema,
            dry_run=self._skip_execution,
        )
        gcs_to_pg.execute(context)

        if not self._skip_execution and self.cleanup_temp_files:
            self._cleanup_temp_files(load_gcs_temp_path)

        if not self._skip_execution:
            self.log.info(
                f'Successfully transferred data from BigQuery to PostgreSQL table: {self.destination_table}'
            )

        return self.destination_table

    def _get_source_query(self) -> str:
        """Get the source query based on sql, where, or table."""
        if self.sql:
            return self.sql
        if self.where:
            return f'SELECT * FROM `{self.source_table}` WHERE {self.where}'
        return f'SELECT * FROM `{self.source_table}`'

    def _export_table_to_gcs(
        self, context: Context, export_format: str, gcs_path: str
    ) -> None:
        """Export table to GCS using BigQueryToGCSOperator."""
        from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (  # noqa: PLC0415
            BigQueryToGCSOperator,
        )

        api_format = self.FORMAT_API_MAP.get(export_format, 'PARQUET')

        bq_to_gcs = BigQueryToGCSOperator(
            task_id=f'{self.task_id}_extract',
            source_project_dataset_table=self.source_table,
            destination_cloud_storage_uris=[f'gs://{self.gcs_bucket}/{gcs_path}'],
            gcp_conn_id=self.gcp_conn_id,
            export_format=api_format,
            print_header=export_format == 'csv',
        )
        bq_to_gcs.execute(context)

    def _export_query_to_gcs(
        self, context: Context, export_format: str, gcs_path: str
    ) -> None:
        """Export query results to GCS using EXPORT DATA statement."""
        from airflow.providers.google.cloud.operators.bigquery import (  # noqa: PLC0415
            BigQueryInsertJobOperator,
        )

        source_query = self._get_source_query()
        api_format = self.FORMAT_API_MAP.get(export_format, 'PARQUET')
        export_path = self._get_query_export_path(gcs_path)

        export_sql = f"""
        EXPORT DATA OPTIONS(
            uri='gs://{self.gcs_bucket}/{export_path}',
            format='{api_format}',
            overwrite=true
        ) AS {source_query}
        """

        job_operator = BigQueryInsertJobOperator(
            task_id=f'{self.task_id}_extract',
            configuration={'query': {'query': export_sql}},
            gcp_conn_id=self.gcp_conn_id,
        )
        job_operator.execute(context)

    @staticmethod
    def _get_query_export_path(gcs_path: str) -> str:
        if '*' in gcs_path:
            return gcs_path

        if '.' not in gcs_path:
            return f'{gcs_path}-*.data'

        base_path, extension = gcs_path.rsplit('.', 1)
        return f'{base_path}-*.{extension}'

    def _check_source_data(self, context: Context) -> None:
        """Check if source table/query exists and has data."""
        from airsql.sensors.bigquery import BigQuerySqlSensor  # noqa: PLC0415

        if self.source_table_check_sql:
            check_sql = self.source_table_check_sql
        elif self.sql:
            check_sql = f'SELECT 1 FROM ({self.sql}) AS subquery LIMIT 1'
        elif self.where:
            check_sql = (
                f'SELECT 1 FROM `{self.source_table}` WHERE {self.where} LIMIT 1'
            )
        else:
            check_sql = f'SELECT 1 FROM `{self.source_table}` LIMIT 1'

        self.log.info('Checking source data availability')
        sensor = BigQuerySqlSensor(
            task_id=f'{self.task_id}_source_check',
            conn_id=self.gcp_conn_id,
            sql=check_sql,
            retries=1,
            poke_interval=30,
            timeout=300,
        )
        sensor.execute(context)
        self.log.info('Source validation successful')

    def _detect_json_columns(self, bq_hook: Any) -> Set[str]:
        """Detect JSON columns in the BigQuery schema."""

        json_columns: Set[str] = set()

        try:
            if self.sql or self.where:
                source_query = self._get_source_query()
                schema_query = f'SELECT * FROM ({source_query}) AS subquery LIMIT 0'

                client = bq_hook.get_client()
                query_job = client.query(schema_query)
                query_job.result()

                for field in query_job.schema:
                    if field.field_type.upper() in self.JSON_COLUMN_TYPES:
                        json_columns.add(field.name)
            else:
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
                table_ref = client.dataset(dataset_id, project=project_id).table(
                    table_id
                )
                table_obj = client.get_table(table_ref)

                for field in table_obj.schema:
                    if field.field_type.upper() in self.JSON_COLUMN_TYPES:
                        json_columns.add(field.name)

        except Exception as e:
            self.log.warning(f'Failed to detect JSON columns: {e}')

        return json_columns

    def _get_bq_schema(self, bq_hook: Any) -> dict:
        """Get BigQuery schema as a dict mapping column names to types.

        Returns:
            Dict mapping column names to BigQuery field types (uppercase)
        """
        try:
            if self.sql or self.where:
                source_query = self._get_source_query()
                schema_query = f'SELECT * FROM ({source_query}) AS subquery LIMIT 0'

                client = bq_hook.get_client()
                query_job = client.query(schema_query)
                query_job.result()

                return {
                    field.name: field.field_type.upper() for field in query_job.schema
                }
            else:
                parts = self.source_table.split('.')
                if len(parts) == self.TABLE_NAME_WITH_PROJECT_PARTS:
                    project_id, dataset_id, table_id = parts
                elif len(parts) == self.TABLE_NAME_WITHOUT_PROJECT_PARTS:
                    dataset_id, table_id = parts
                    project_id = bq_hook.project_id
                else:
                    self.log.warning(f'Invalid table name format: {self.source_table}')
                    return {}

                client = bq_hook.get_client()
                table_ref = client.dataset(dataset_id, project=project_id).table(
                    table_id
                )
                table_obj = client.get_table(table_ref)

                return {
                    field.name: field.field_type.upper() for field in table_obj.schema
                }

        except Exception as e:
            self.log.warning(f'Failed to get BigQuery schema: {e}')
            return {}

    def _cleanup_temp_files(self, gcs_temp_path: str) -> None:
        """Clean up temporary files from GCS."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: PLC0415

        try:
            self.log.info(
                f'Cleaning up temporary file: gs://{self.gcs_bucket}/{gcs_temp_path}'
            )
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            if '*' in gcs_temp_path:
                wildcard_index = gcs_temp_path.index('*')
                prefix = gcs_temp_path[:wildcard_index]
                object_names = gcs_hook.list(bucket_name=self.gcs_bucket, prefix=prefix)
                for object_name in object_names:
                    if fnmatch.fnmatch(object_name, gcs_temp_path):
                        gcs_hook.delete(
                            bucket_name=self.gcs_bucket, object_name=object_name
                        )
            else:
                gcs_hook.delete(bucket_name=self.gcs_bucket, object_name=gcs_temp_path)
            self.log.info('Temporary file cleanup completed')
        except Exception as e:
            self.log.warning(f'Failed to cleanup temporary file: {e}')
