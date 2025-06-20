"""
Hook manager for handling different database connections and operations.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context
from psycopg2 import sql as psycopg2_sql
from psycopg2.extras import execute_values

from airsql.table import Table

BIGQUERY_TABLE_NAME_PARTS = 2
DEFAULT_TIMESTAMP_COLUMNS = ['updated_at', 'atualizado_em']


class SQLHookManager:
    """Manages database hooks and operations across different database types."""

    @staticmethod
    def get_hook(conn_id: str) -> Union[PostgresHook, BigQueryHook]:
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
            return BigQueryHook(gcp_conn_id=conn_id)
        elif conn_type in {'postgres', 'postgresql'}:
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
        self, df: pd.DataFrame, table: Table, timestamp_column: Optional[str] = None
    ) -> pd.DataFrame:
        """Add automatic timestamp columns to
        DataFrame if they exist in target table."""
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
        self, df: pd.DataFrame, table: Table, timestamp_column: Optional[str] = None
    ) -> None:
        """Write a DataFrame to a table with automatic timestamp handling."""
        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._write_to_bigquery(df_with_timestamps, table)
        elif table.is_postgres:
            self._write_to_postgres(df_with_timestamps, table)
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    def replace_table_content(
        self, df: pd.DataFrame, table: Table, timestamp_column: Optional[str] = None
    ) -> None:
        """Replace the content of a table with DataFrame data."""
        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._replace_bigquery_table(df_with_timestamps, table)
        elif table.is_postgres:
            self._replace_postgres_table(df_with_timestamps, table)
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    def merge_dataframe_to_table(
        self,
        df: pd.DataFrame,
        table: Table,
        conflict_columns: List[str],
        timestamp_column: Optional[str] = None,
    ) -> None:
        """Merge/upsert DataFrame data into a table."""
        df_with_timestamps = self._add_automatic_timestamps(df, table, timestamp_column)

        if table.is_bigquery:
            self._merge_bigquery_table(df_with_timestamps, table, conflict_columns)
        elif table.is_postgres:
            self._merge_postgres_table(df_with_timestamps, table, conflict_columns)
        else:
            raise ValueError(f'Unsupported database type for table: {table}')

    @staticmethod
    def _get_bigquery_schema(hook: BigQueryHook, table: Table) -> List[Dict[str, Any]]:
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
    def _get_postgres_schema(hook: PostgresHook, table: Table) -> List[Dict[str, Any]]:
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
    def _write_to_bigquery(df: pd.DataFrame, table: Table) -> None:
        """Write DataFrame to BigQuery table."""
        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')
        destination_table = f'{project_id}.{dataset_id}.{table_id}'
        job_config = {
            'write_disposition': 'WRITE_APPEND',
            'create_disposition': 'CREATE_IF_NEEDED',
        }
        if table.partition_by:
            job_config['time_partitioning'] = {'field': table.partition_by}
        if table.cluster_by:
            job_config['clustering'] = {'fields': table.cluster_by}

        hook.load_dataframe(
            dataframe=df,
            destination_project_dataset_table=destination_table,
            table_schema=table.schema_fields,
            location=table.location,
            write_disposition='WRITE_APPEND',  # Explicitly set, though often default
            create_disposition='CREATE_IF_NEEDED',
        )

    @staticmethod
    def _write_to_postgres(df: pd.DataFrame, table: Table) -> None:
        """Write DataFrame to Postgres table."""
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
            if_exists='append',
            index=False,
            method='multi',
        )

    @staticmethod
    def _replace_bigquery_table(df: pd.DataFrame, table: Table) -> None:
        """Replace BigQuery table content using WRITE_TRUNCATE."""
        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')
        table_id_full = f'{project_id}.{dataset_id}.{table_id}'
        job_config = {
            'write_disposition': 'WRITE_TRUNCATE',
            'create_disposition': 'CREATE_IF_NEEDED',
        }
        if table.partition_by:
            job_config['time_partitioning'] = {'field': table.partition_by}

        if table.cluster_by:
            job_config['clustering'] = {'fields': table.cluster_by}

        if table.location:
            job_config['job_location'] = table.location
        hook.load_dataframe(
            dataframe=df,
            destination_project_dataset_table=table_id_full,
            table_schema=table.schema_fields,
            location=table.location,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
        )

    @staticmethod
    def _replace_postgres_table(df: pd.DataFrame, table: Table) -> None:
        """Replace Postgres table content."""
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
    def _merge_bigquery_table(
        df: pd.DataFrame, table: Table, conflict_columns: List[str]
    ) -> None:
        """Merge DataFrame into BigQuery table using MERGE statement."""
        hook = BigQueryHook(gcp_conn_id=table.conn_id)
        parts = table.table_name.split('.')
        if len(parts) == BIGQUERY_TABLE_NAME_PARTS:
            dataset_id, table_id = parts
            project_id = table.project or hook.project_id
        else:
            raise ValueError(f'Invalid BigQuery table name: {table.table_name}')
        temp_table_id = f'{table_id}_temp_{int(pd.Timestamp.now().timestamp())}'
        temp_table_full = f'{project_id}.{dataset_id}.{temp_table_id}'

        try:
            hook.load_dataframe(
                dataframe=df,
                destination_project_dataset_table=temp_table_full,
                location=table.location,
                write_disposition='WRITE_TRUNCATE',  # Ensure temp table is fresh
            )
            all_columns = df.columns.tolist()
            update_columns = [col for col in all_columns if col not in conflict_columns]
            merge_sql = f"""
MERGE `{project_id}.{dataset_id}.{table_id}` AS target
USING `{temp_table_full}` AS source
ON {' AND '.join([f'target.{col} = source.{col}' for col in conflict_columns])}
WHEN MATCHED THEN
  UPDATE SET {', '.join([f'{col} = source.{col}' for col in update_columns])}
WHEN NOT MATCHED THEN
  INSERT ({', '.join(all_columns)})
  VALUES ({', '.join([f'source.{col}' for col in all_columns])})
"""  # noqa: S608
            hook.run_query(
                sql=merge_sql,
                location=table.location,
                use_legacy_sql=False,
            )

        finally:
            try:
                hook.delete_table(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=temp_table_id,
                )
            except Exception as e:
                print(f'Warning: Failed to cleanup temp table {temp_table_full}: {e}')

    def _merge_postgres_table(  # noqa: PLR0914
        self, df: pd.DataFrame, table: Table, conflict_columns: List[str]
    ) -> None:
        """Merge DataFrame into Postgres table using ON CONFLICT."""
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
                table_identifier = psycopg2_sql.Identifier(schema_name, table_name)
            else:
                schema_name, table_name = 'public', table.table_name
                table_identifier = psycopg2_sql.Identifier(table_name)

            audit_columns = {'created_at', 'updated_at', 'criado_em', 'atualizado_em'}
            update_cols = [
                col
                for col in common_columns
                if col not in conflict_columns and col not in audit_columns
            ]
            data_tuples = [tuple(x) for x in df_filtered[common_columns].to_numpy()]

            insert_sql = psycopg2_sql.SQL(
                'INSERT INTO {table} ({columns}) VALUES %s'
            ).format(
                table=table_identifier,
                columns=psycopg2_sql.SQL(', ').join([
                    psycopg2_sql.Identifier(col) for col in common_columns
                ]),
            )

            conflict_sql_part = psycopg2_sql.SQL(
                'ON CONFLICT ({conflict_cols}) DO '
            ).format(
                conflict_cols=psycopg2_sql.SQL(', ').join([
                    psycopg2_sql.Identifier(col) for col in conflict_columns
                ])
            )

            if not update_cols:
                update_sql_part = psycopg2_sql.SQL('NOTHING')
            else:
                set_statements = [
                    psycopg2_sql.SQL(
                        '{col_to_update} = EXCLUDED.{col_to_update}'
                    ).format(col_to_update=psycopg2_sql.Identifier(col))
                    for col in update_cols
                ]
                update_sql_part = psycopg2_sql.SQL('UPDATE SET {}').format(
                    psycopg2_sql.SQL(', ').join(set_statements)
                )

            final_sql_query = psycopg2_sql.SQL(' ').join([
                insert_sql,
                conflict_sql_part,
                update_sql_part,
            ])

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
