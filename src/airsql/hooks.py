"""
Hook manager for handling different database connections and operations.
"""

import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context
from airflow.sdk.bases.hook import BaseHook

from airsql.table import Table

logger = logging.getLogger(__name__)

BIGQUERY_TABLE_NAME_PARTS = 2
DEFAULT_TIMESTAMP_COLUMNS = ['updated_at', 'atualizado_em']
DEFAULT_BIGQUERY_LOCATION = 'us-central1'


@lru_cache(maxsize=128)
def _get_connection_type(conn_id: str) -> str:
    """Cached connection type lookup to avoid repeated DB queries."""
    try:
        conn = BaseHook.get_connection(conn_id)
        return conn.conn_type.lower() if conn.conn_type else 'unknown'
    except Exception:
        return 'unknown'


class SQLHookManager:
    """Manages database hooks and operations across different database types."""

    @staticmethod
    def get_hook(conn_id: str) -> Any:
        """Get the appropriate hook for a connection ID."""
        try:
            connection = BaseHook.get_connection(conn_id)
        except Exception as e:
            raise ValueError(
                f"Failed to get connection '{conn_id}': {e}. "
                'Make sure the connection is configured in Airflow.'
            ) from e

        conn_type = connection.conn_type.lower() if connection.conn_type else 'unknown'

        if conn_type in {'google_cloud_platform', 'gccpigquery'}:
            from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
                BigQueryHook,  # noqa: PLC0415
            )

            return BigQueryHook(gcp_conn_id=conn_id)
        elif conn_type in {'postgres', 'postgresql'}:
            from airflow.providers.postgres.hooks.postgres import (  # noqa: PLC0415
                PostgresHook,  # noqa: PLC0415
            )

            return PostgresHook(postgres_conn_id=conn_id)
        else:
            raise ValueError(
                f"Unsupported connection type '{conn_type}' "
                f"for conn_id '{conn_id}'. Supported types are "
                "'google_cloud_platform' (for BigQuery) and 'postgres'/'postgresql'. "
                f'Please check your Airflow connection configuration.'
            )

    def get_table_schema(self, table: Table) -> List[Dict[str, Any]]:
        """Get the schema of a table."""
        hook = self.get_hook(table.conn_id)

        if table.is_bigquery:
            return self._get_bigquery_schema(hook, table)
        elif table.is_postgres:
            return self._get_postgres_schema(hook, table)
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    def _add_automatic_timestamps(
        self, df, table: Table, timestamp_column: Optional[str] = None
    ):
        """Add automatic timestamp columns to
        DataFrame if they exist in target table."""
        import pandas as pd  # noqa: PLC0415

        try:
            try:
                get_current_context()
            except (RuntimeError, ImportError):
                return df

            schema = self.get_table_schema(table)
            column_names = [col['name'].lower() for col in schema]
            timestamp_cols_to_check = []
            if timestamp_column:
                timestamp_cols_to_check.append(timestamp_column.lower())
            timestamp_cols_to_check.extend(DEFAULT_TIMESTAMP_COLUMNS)
            target_timestamp_col = None

            for col in timestamp_cols_to_check:
                if col in column_names:
                    target_timestamp_col = col
                    break

            if target_timestamp_col:
                original_case_col = next(
                    col['name']
                    for col in schema
                    if col['name'].lower() == target_timestamp_col
                )
                df = df.copy()
                df[original_case_col] = pd.Timestamp.now()

            return df
        except Exception:
            return df

    def write_dataframe_to_table(
        self,
        df,
        table: Table,
        if_exists: str = 'append',
        timestamp_column: Optional[str] = None,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Write a DataFrame to a table with automatic timestamp handling.

        Args:
            df: DataFrame to write
            table: Target table reference
            if_exists: What to do if table exists ('append', 'replace', 'fail', 'truncate')
            timestamp_column: Column to set automatic timestamp
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        logger = logging.getLogger(__name__)
        logger.info(f'Writing DataFrame with {len(df)} rows to {table.table_name}')
        logger.debug(f'DataFrame columns: {list(df.columns)}')
        logger.debug(f'If exists strategy: {if_exists}')

        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if if_exists == 'truncate':
            self.truncate_table_content(
                df_with_timestamps, table, timestamp_column, dataset_location
            )
        elif table.is_bigquery:
            self._write_to_bigquery(
                df_with_timestamps, table, if_exists, dataset_location
            )
        elif table.is_postgres:
            self._write_to_postgres(df_with_timestamps, table, if_exists)
        else:
            try:
                connection = BaseHook.get_connection(table.conn_id)
                conn_type = connection.conn_type if connection.conn_type else 'unknown'
                raise ValueError(
                    f'Unsupported database type for table: {table}. '
                    f'Connection ID "{table.conn_id}" has type "{conn_type}". '
                    f'Supported types are: google_cloud_platform, gccpigquery, bigquery (BigQuery), postgres, postgresql'
                )
            except Exception as e:
                if 'Unsupported database type' in str(e):
                    raise e
                raise ValueError(
                    f'Unsupported database type for table: {table}. Error getting connection: {e}'
                ) from e

    def replace_table_content(
        self,
        df,
        table: Table,
        timestamp_column: Optional[str] = None,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Replace the content of a table with DataFrame data.

        Args:
            df: DataFrame to write
            table: Target table reference
            timestamp_column: Column to set automatic timestamp
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._replace_bigquery_table(df_with_timestamps, table, dataset_location)
        elif table.is_postgres:
            self._replace_postgres_table(df_with_timestamps, table)
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    def merge_dataframe_to_table(
        self,
        df,
        table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Merge/upsert DataFrame data into a table.

        Args:
            df: DataFrame to merge
            table: Target table reference
            conflict_columns: Columns to use for conflict detection
            update_columns: Columns to update on conflict (defaults to all except conflict columns)
            timestamp_column: Column to set automatic timestamp
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        logger = logging.getLogger(__name__)
        logger.info(f'Merging DataFrame with {len(df)} rows into {table.table_name}')
        logger.debug(f'Conflict columns: {conflict_columns}')
        logger.debug(f'Update columns: {update_columns or "all columns"}')
        logger.debug(f'DataFrame columns: {list(df.columns)}')

        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._merge_bigquery_table(
                df_with_timestamps,
                table,
                conflict_columns,
                update_columns,
                dataset_location,
            )
        elif table.is_postgres:
            self._merge_postgres_table(
                df_with_timestamps, table, conflict_columns, update_columns
            )
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    def truncate_table_content(
        self,
        df,
        table: Table,
        timestamp_column: Optional[str] = None,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Truncate the content of a table and insert new data, preserving table structure and sequences.

        Args:
            df: DataFrame to write
            table: Target table reference
            timestamp_column: Column to set automatic timestamp
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._replace_bigquery_table(df_with_timestamps, table, dataset_location)
        elif table.is_postgres:
            self._truncate_postgres_table(df_with_timestamps, table)
        else:
            # Get connection type for better error message
            try:
                connection = BaseHook.get_connection(table.conn_id)
                conn_type = connection.conn_type if connection.conn_type else 'unknown'
                raise ValueError(
                    f'Unsupported database type for table: {table}. '
                    f'Connection ID "{table.conn_id}" has type "{conn_type}". '
                    f'Supported types are: google_cloud_platform, gccpigquery, bigquery (BigQuery), postgres, postgresql'
                )
            except Exception as e:
                if 'Unsupported database type' in str(e):
                    raise e
                raise ValueError(
                    f'Unsupported database type for table: {table}. Error getting connection: {e}'
                ) from e

    @staticmethod
    def drop_table(table: Table) -> None:
        """Drop a table (for temporary tables cleanup)."""
        if not table.temporary:
            logger.info(f'Table {table} is not marked as temporary, skipping drop')
            return

        if table.is_bigquery:
            try:
                from google.cloud import bigquery  # noqa: PLC0415

                client = bigquery.Client()
                dataset_id = table.dataset or 'default_dataset'
                table_ref = client.dataset(dataset_id).table(table.table_name)
                client.delete_table(table_ref)
                logger.info(f'Dropped BigQuery table: {table}')
            except Exception as e:
                logger.error(f'Failed to drop BigQuery table {table}: {e}')
                raise
        elif table.is_postgres:
            try:
                hook = PostgresHook(postgres_conn_id=table.conn_id)
                conn = hook.get_conn()
                cursor = conn.cursor()

                schema, table_name = (
                    table.table_name.split('.', 1)
                    if '.' in table.table_name
                    else ('public', table.table_name)
                )

                drop_sql = f'DROP TABLE IF EXISTS {schema}.{table_name}'
                cursor.execute(drop_sql)
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f'Dropped Postgres table: {schema}.{table_name}')
            except Exception as e:
                logger.error(f'Failed to drop Postgres table {table}: {e}')
                raise
        else:
            logger.warning(f'Drop not supported for table type: {table}')

    @staticmethod
    def _get_bigquery_schema(hook: Any, table: Table) -> List[Dict[str, Any]]:
        """Get BigQuery table schema."""
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')
        client = hook.get_client()
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table_obj = client.get_table(table_ref)

        return [
            {'name': field.name, 'type': field.field_type} for field in table_obj.schema
        ]

    @staticmethod
    def _get_postgres_schema(hook, table: Table) -> List[Dict[str, Any]]:
        """Get Postgres table schema."""
        if '.' in table.table_name:
            schema_name, table_name = table.table_name.split('.', 1)
        else:
            schema_name = 'public'
            table_name = table.table_name
        sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

        records = hook.get_records(sql, parameters=[schema_name, table_name])
        return [
            {'name': record[0], 'type': record[1], 'nullable': record[2] == 'YES'}
            for record in records
        ]

    @staticmethod
    def _ensure_bigquery_dataset(
        hook: Any,
        project_id: str,
        dataset_id: str,
        location: str = DEFAULT_BIGQUERY_LOCATION,
    ) -> None:
        """Ensure BigQuery dataset exists, creating if necessary.

        Args:
            hook: BigQueryHook instance
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            location: BigQuery location/region (defaults to 'us-central1')
        """
        from google.cloud import bigquery  # noqa: PLC0415

        try:
            client = hook.get_client(project_id=project_id, location=location)
            dataset_ref = bigquery.Dataset(f'{project_id}.{dataset_id}')
            dataset_ref.location = location
            client.create_dataset(dataset_ref, exists_ok=True)
            logger.info(f'Ensured BigQuery dataset exists: {project_id}.{dataset_id}')
        except Exception as e:
            logger.warning(f'Failed to ensure BigQuery dataset exists: {e}')

    @staticmethod
    def _write_to_bigquery(
        df,
        table: Table,
        if_exists: str = 'append',
        dataset_location: Optional[str] = None,
    ) -> None:
        """Write DataFrame to BigQuery table.

        Args:
            df: DataFrame to write
            table: Target table reference
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
            BigQueryHook,
        )
        from google.cloud import bigquery  # noqa: PLC0415

        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')

        location = dataset_location or table.location or DEFAULT_BIGQUERY_LOCATION
        SQLHookManager._ensure_bigquery_dataset(hook, project_id, dataset_id, location)

        client = hook.get_client(project_id=project_id, location=location)
        destination_table_ref = client.dataset(dataset_id).table(table_id)

        # Map if_exists to BigQuery write disposition
        write_disposition_map = {
            'append': bigquery.WriteDisposition.WRITE_APPEND,
            'replace': bigquery.WriteDisposition.WRITE_TRUNCATE,
            'fail': bigquery.WriteDisposition.WRITE_EMPTY,
        }
        write_disposition = write_disposition_map.get(
            if_exists, bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        if table.partition_by:
            job_config.time_partitioning = bigquery.TimePartitioning(
                field=table.partition_by
            )
        if table.cluster_by:
            job_config.clustering_fields = table.cluster_by
        if table.schema_fields:
            job_config.schema = [
                bigquery.SchemaField(field['name'], field['type'])
                for field in table.schema_fields
            ]

        job = client.load_table_from_dataframe(
            df, destination_table_ref, job_config=job_config, location=location
        )
        job.result()

    @staticmethod
    def _write_to_postgres(df, table: Table, if_exists: str = 'append') -> None:
        """Write DataFrame to Postgres table using PyArrow for optimization."""
        from airflow.providers.postgres.hooks.postgres import (  # noqa: PLC0415
            PostgresHook,  # noqa: PLC0415
        )

        hook = PostgresHook(postgres_conn_id=table.conn_id)
        engine = hook.get_sqlalchemy_engine()
        if '.' in table.table_name:
            schema_name, table_name = table.table_name.split('.', 1)
        else:
            schema_name = None
            table_name = table.table_name
        df.to_sql(
            table_name,
            engine,
            schema=schema_name,
            if_exists=if_exists,
            index=False,
            method='multi',
        )

    @staticmethod
    def _replace_bigquery_table(
        df,
        table: Table,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Replace BigQuery table content using WRITE_TRUNCATE.

        Args:
            df: DataFrame to write
            table: Target table reference
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
            BigQueryHook,
        )
        from google.cloud import bigquery  # noqa: PLC0415

        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')

        location = dataset_location or table.location or DEFAULT_BIGQUERY_LOCATION
        SQLHookManager._ensure_bigquery_dataset(hook, project_id, dataset_id, location)

        client = hook.get_client(project_id=project_id, location=location)
        destination_table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        if table.partition_by:
            job_config.time_partitioning = bigquery.TimePartitioning(
                field=table.partition_by
            )
        if table.cluster_by:
            job_config.clustering_fields = table.cluster_by
        if table.schema_fields:
            job_config.schema = [
                bigquery.SchemaField(field['name'], field['type'])
                for field in table.schema_fields
            ]

        job = client.load_table_from_dataframe(
            df, destination_table_ref, job_config=job_config, location=location
        )
        job.result()

    @staticmethod
    def _replace_postgres_table(df, table: Table) -> None:
        """Replace Postgres table content using PyArrow for optimization."""
        from airflow.providers.postgres.hooks.postgres import (  # noqa: PLC0415
            PostgresHook,  # noqa: PLC0415
        )

        hook = PostgresHook(postgres_conn_id=table.conn_id)
        engine = hook.get_sqlalchemy_engine()
        if '.' in table.table_name:
            schema_name, table_name = table.table_name.split('.', 1)
        else:
            schema_name = None
            table_name = table.table_name

        df.to_sql(
            table_name,
            engine,
            schema=schema_name,
            if_exists='replace',
            index=False,
            method='multi',
        )

    @staticmethod
    def _merge_bigquery_table(  # noqa: PLR0914
        df,
        table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        dataset_location: Optional[str] = None,
    ) -> None:
        """Merge DataFrame into BigQuery table using MERGE statement.

        Args:
            df: DataFrame to merge
            table: Target table reference
            conflict_columns: Columns to use for conflict detection
            update_columns: Columns to update on conflict (defaults to all except conflict columns)
            dataset_location: BigQuery dataset location/region (defaults to table.location or 'us-central1')
        """
        import pandas as pd  # noqa: PLC0415
        from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
            BigQueryHook,
        )
        from google.cloud import bigquery  # noqa: PLC0415

        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')
        temp_table_id = f'{table_id}_temp_{int(pd.Timestamp.now().timestamp())}'

        location = dataset_location or table.location or DEFAULT_BIGQUERY_LOCATION
        SQLHookManager._ensure_bigquery_dataset(hook, project_id, dataset_id, location)

        client = hook.get_client(project_id=project_id, location=location)
        temp_table_ref = client.dataset(dataset_id).table(temp_table_id)

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            job = client.load_table_from_dataframe(
                df, temp_table_ref, job_config=job_config, location=location
            )
            job.result()
            temp_table_full = f'{project_id}.{dataset_id}.{temp_table_id}'
            all_columns = df.columns.tolist()

            if update_columns is None:
                columns_to_update = [
                    col for col in all_columns if col not in conflict_columns
                ]
            else:
                missing_cols = [col for col in update_columns if col not in all_columns]
                if missing_cols:
                    raise ValueError(
                        f'Update columns not found in DataFrame: {missing_cols}'
                    )
                columns_to_update = update_columns

            merge_sql = f"""
MERGE `{project_id}.{dataset_id}.{table_id}` AS target
USING `{temp_table_full}` AS source
ON {' AND '.join([f'target.{col} = source.{col}' for col in conflict_columns])}
WHEN MATCHED THEN
  UPDATE SET {', '.join([f'{col} = source.{col}' for col in columns_to_update])}
WHEN NOT MATCHED THEN
  INSERT ({', '.join(all_columns)})
  VALUES ({', '.join([f'source.{col}' for col in all_columns])})
"""  # noqa: S608
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False,
                location=location,
            )
            query_job = client.query(merge_sql, job_config=job_config)
            query_job.result()

        finally:
            try:
                temp_table_full = f'{project_id}.{dataset_id}.{temp_table_id}'
                hook.delete_table(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=temp_table_id,
                )
            except Exception as e:
                print(f'Warning: Failed to cleanup temp table {temp_table_full}: {e}')

    def _merge_postgres_table(  # noqa: PLR0914
        self,
        df,
        table: Table,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> None:
        """Merge DataFrame into Postgres table using ON CONFLICT."""
        from airflow.providers.postgres.hooks.postgres import (  # noqa: PLC0415
            USE_PSYCOPG3,
            PostgresHook,  # noqa: PLC0415
        )

        if USE_PSYCOPG3:
            from psycopg import sql as psycopg_sql  # noqa: PLC0415
        else:
            from psycopg2 import sql as psycopg_sql  # noqa: PLC0415
            from psycopg2.extras import execute_values  # noqa: PLC0415

        hook = PostgresHook(postgres_conn_id=table.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            schema = self._get_postgres_schema(hook, table)
            column_names = [col['name'] for col in schema]
            common_columns = [col for col in df.columns if col in column_names]
            df_filtered = df[common_columns]
            if '.' in table.table_name:
                schema_name, table_name = table.table_name.split('.', 1)
                table_identifier = psycopg_sql.Identifier(schema_name, table_name)
            else:
                schema_name, table_name = 'public', table.table_name
                table_identifier = psycopg_sql.Identifier(table_name)

            # Determine which columns to update
            if update_columns is None:
                # Default behavior: update all columns except conflict columns and audit columns
                audit_columns = {
                    'created_at',
                    'updated_at',
                    'criado_em',
                    'atualizado_em',
                }
                update_cols = [
                    col
                    for col in common_columns
                    if col not in conflict_columns and col not in audit_columns
                ]
            else:
                # Use provided update_columns, validate they exist in DataFrame and table
                missing_in_df = [col for col in update_columns if col not in df.columns]
                missing_in_table = [
                    col for col in update_columns if col not in common_columns
                ]
                if missing_in_df:
                    raise ValueError(
                        f'Update columns not found in DataFrame: {missing_in_df}'
                    )
                if missing_in_table:
                    raise ValueError(
                        f'Update columns not found in table: {missing_in_table}'
                    )
                update_cols = update_columns

            # Convert DataFrame to tuples, handling pandas NA values for psycopg3
            import numpy as np  # noqa: PLC0415
            import pandas as pd  # noqa: PLC0415

            df_clean = df_filtered[common_columns].replace({pd.NA: None, np.nan: None})
            data_tuples = [tuple(x) for x in df_clean.values.tolist()]

            insert_sql = psycopg_sql.SQL(
                'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
            ).format(
                table=table_identifier,
                columns=psycopg_sql.SQL(', ').join([
                    psycopg_sql.Identifier(col) for col in common_columns
                ]),
                placeholders=', '.join(['%s'] * len(common_columns)),
            )

            conflict_sql_part = psycopg_sql.SQL(
                'ON CONFLICT ({conflict_cols}) DO '
            ).format(
                conflict_cols=psycopg_sql.SQL(', ').join([
                    psycopg_sql.Identifier(col) for col in conflict_columns
                ])
            )

            if not update_cols:
                update_sql_part = psycopg_sql.SQL('NOTHING')
            else:
                set_statements = [
                    psycopg_sql.SQL(
                        '{col_to_update} = EXCLUDED.{col_to_update}'
                    ).format(col_to_update=psycopg_sql.Identifier(col))
                    for col in update_cols
                ]
                update_sql_part = psycopg_sql.SQL('UPDATE SET {}').format(
                    psycopg_sql.SQL(', ').join(set_statements)
                )

            final_sql_query = psycopg_sql.SQL(' ').join([
                insert_sql,
                conflict_sql_part,
                update_sql_part,
            ])

            if USE_PSYCOPG3:
                for i in range(0, len(data_tuples), 1000):
                    batch = data_tuples[i : i + 1000]
                    cursor.executemany(final_sql_query.as_string(cursor), batch)
            else:
                execute_values(
                    cursor, final_sql_query.as_string(cursor), data_tuples
                )  # Pass as string to execute_values
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f'Error during Postgres merge: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _truncate_postgres_table(df, table: Table) -> None:
        """Truncate Postgres table content, preserving table structure and sequences using PyArrow."""
        from airflow.providers.postgres.hooks.postgres import (  # noqa: PLC0415
            PostgresHook,  # noqa: PLC0415
        )
        from sqlalchemy import text  # noqa: PLC0415

        hook = PostgresHook(postgres_conn_id=table.conn_id)
        engine = hook.get_sqlalchemy_engine()

        if '.' in table.table_name:
            schema_name, table_name = table.table_name.split('.', 1)
            full_table_name = f'"{schema_name}"."{table_name}"'
        else:
            schema_name = None
            table_name = table.table_name
            full_table_name = f'"{table_name}"'

        # Use a transaction to ensure atomicity
        with engine.begin() as conn:
            # TRUNCATE preserves table structure and resets sequences
            # RESTART IDENTITY resets any auto-increment sequences
            truncate_sql = f'TRUNCATE TABLE {full_table_name} RESTART IDENTITY'
            conn.execute(text(truncate_sql))

            # Insert new data with PyArrow optimization
            df.to_sql(
                table_name,
                conn,
                schema=schema_name,
                if_exists='append',
                index=False,
                method='multi',
            )
