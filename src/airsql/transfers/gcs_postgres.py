import time
from io import BytesIO, StringIO

from airflow.models import BaseOperator
from psycopg2 import sql as psycopg2_sql
from psycopg2.extras import execute_values

from airsql.utils import DataValidator, OperationSummary


class GCSToPostgresOperator(BaseOperator):
    def __init__(
        self,
        target_table_name: str,
        bucket_name: str,
        object_name: str,
        postgres_conn_id: str,
        gcp_conn_id: str,
        conflict_columns=None,
        replace=False,
        grant_table_privileges: bool = True,
        audit_cols_to_exclude=None,
        dry_run: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.postgres_conn_id = postgres_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.target_table_name = target_table_name
        self.conflict_columns = conflict_columns
        self.replace = replace
        self.grant_table_privileges = grant_table_privileges
        self.dry_run = dry_run
        self.audit_cols_to_exclude = audit_cols_to_exclude or {
            'criado_em',
            'atualizado_em',
            'created_at',
            'updated_at',
        }

    @staticmethod
    def _dataframe_to_tuples(df):
        """Convert DataFrame to tuples with proper type conversion for PostgreSQL.

        Handles both PyArrow-backed and standard pandas DataFrames efficiently.
        """
        import numpy as np  # noqa: PLC0415
        import pandas as pd  # noqa: PLC0415

        # For PyArrow-backed DataFrames, convert to numpy with None for nulls
        # For standard pandas DataFrames, convert_dtypes handles NA values
        if hasattr(df, '__arrow_c_stream__'):  # PyArrow-backed DataFrame
            # Use to_numpy to preserve null handling
            df_clean = df.where(pd.notna(df), None)
        else:
            # Standard pandas DataFrame
            df_clean = df.convert_dtypes().replace({pd.NA: None, np.nan: None})

        return [tuple(row) for row in df_clean.values.tolist()]

    def _grant_table_privileges(self, pg_hook, schema, table_name_simple):
        """Grant all privileges on table to public."""
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            table_identifier = (
                psycopg2_sql.Identifier(schema, table_name_simple)
                if schema
                else psycopg2_sql.Identifier(table_name_simple)
            )

            grant_sql = psycopg2_sql.SQL(
                'GRANT ALL PRIVILEGES ON {table} TO PUBLIC'
            ).format(table=table_identifier)

            self.log.info(
                f'Granting all privileges on {schema}.{table_name_simple} to PUBLIC'
            )
            cursor.execute(grant_sql)

            conn.commit()
            self.log.info('Table privileges granted successfully')

        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to grant table privileges: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _detect_file_format(filename: str) -> str:
        """Detect file format from filename extension.

        Args:
            filename: The file path/name to detect format from

        Returns:
            'parquet' or 'csv'
        """
        if filename.endswith('.parquet'):
            return 'parquet'
        return 'csv'

    def execute(self, context):  # noqa: PLR0912, PLR0914
        import numpy as np  # noqa: PLC0415
        import pandas as pd  # noqa: PLC0415

        from airflow.providers.google.cloud.hooks.gcs import GCSHook  # noqa: PLC0415
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa: PLC0415

        start_time = time.time()
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        file_data = gcs_hook.download(
            bucket_name=self.bucket_name, object_name=self.object_name
        )
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Auto-detect format from file extension
        file_format = self._detect_file_format(self.object_name)
        if file_format == 'parquet':
            df = pd.read_parquet(BytesIO(file_data), engine='pyarrow')
        else:  # csv
            df = pd.read_csv(
                StringIO(file_data.decode('utf-8')), dtype_backend='pyarrow'
            )

        if '.' in self.target_table_name:
            schema, table_name_simple = self.target_table_name.split('.', 1)
        else:
            schema = 'public'  # Default schema in Postgres
            table_name_simple = self.target_table_name

        table_name_full = f'{schema}.{table_name_simple}'

        # Get actual column names from the target Postgres table
        self.log.info(f'Fetching schema for Postgres table: {table_name_full}')
        sql_get_columns = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;"""
        columns_from_db_records = pg_hook.get_records(
            sql_get_columns, parameters=(schema, table_name_simple)
        )

        if not columns_from_db_records:
            raise ValueError(
                f'Could not retrieve column information for table {table_name_full}.'
                ' Ensure the table exists.'
            )

        model_columns = [rec[0] for rec in columns_from_db_records]
        common_columns = [col for col in df.columns if col in model_columns]
        df_filtered = df[common_columns].replace({np.nan: None})

        # Validate DataFrame
        validation_result = DataValidator.validate_columns(
            df_filtered, expected_columns=common_columns
        )
        if validation_result.errors:
            for error in validation_result.errors:
                self.log.error(f'Validation error: {error}')
        for warning in validation_result.warnings:
            self.log.warning(f'Validation warning: {warning}')

        duration = time.time() - start_time
        operation_type = (
            'replace'
            if self.replace
            else ('upsert' if self.conflict_columns else 'append')
        )
        summary = OperationSummary(
            operation_type=operation_type,
            rows_extracted=len(df),
            rows_loaded=len(df_filtered) if not self.dry_run else 0,
            duration_seconds=duration,
            file_size_mb=len(file_data) / (1024 * 1024),
            format_used=file_format,
            validation_errors=validation_result.errors,
            validation_warnings=validation_result.warnings,
            dry_run=self.dry_run,
        )

        if not self.dry_run:
            if self.replace:
                self.log.info(
                    f'Truncating and replacing data in table {table_name_full}'
                )
                self._truncate_and_insert_data(
                    pg_hook, schema, table_name_simple, df_filtered
                )
            elif not self.conflict_columns:
                engine = pg_hook.get_sqlalchemy_engine()
                self.log.info(
                    f'Appending DataFrame to Postgres table {table_name_full}'
                )
                df_filtered.to_sql(
                    name=table_name_simple,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000,
                    dtype_backend='pyarrow',
                )
                self.log.info('Append to Postgres complete.')
            else:
                self._upsert_data(pg_hook, schema, table_name_simple, df_filtered)

            self._grant_table_privileges(pg_hook, schema, table_name_simple)
        else:
            self.log.info(
                f'[DRY RUN] Would {operation_type} {len(df_filtered)} rows to {table_name_full}'
            )

        self.log.info(summary.to_log_summary())

    def _truncate_and_insert_data(
        self, pg_hook, schema, table_name_simple, df_filtered
    ):
        """Truncate table and insert new data to preserve table permissions."""
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            table_identifier = (
                psycopg2_sql.Identifier(schema, table_name_simple)
                if schema
                else psycopg2_sql.Identifier(table_name_simple)
            )

            truncate_sql = psycopg2_sql.SQL('TRUNCATE TABLE {table}').format(
                table=table_identifier
            )
            self.log.info(f'Truncating table {schema}.{table_name_simple}')
            cursor.execute(truncate_sql)

            insert_cols_ident = [
                psycopg2_sql.Identifier(col) for col in df_filtered.columns
            ]

            insert_sql = psycopg2_sql.SQL(
                'INSERT INTO {table} ({columns}) VALUES %s'
            ).format(
                table=table_identifier,
                columns=psycopg2_sql.SQL(', ').join(insert_cols_ident),
            )

            data_tuples = self._dataframe_to_tuples(df_filtered)

            execute_values(
                cursor,
                insert_sql.as_string(cursor),
                data_tuples,
                page_size=1000,
            )

            conn.commit()
            self.log.info('Truncate and insert to Postgres complete.')

        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to truncate and insert records: {e}')
            raise
        finally:
            cursor.close()
            conn.close()

    def _upsert_data(self, pg_hook, schema, table_name_simple, df_filtered):
        """Perform upsert operation using ON CONFLICT."""
        table_full_name = f'{schema}.{table_name_simple}'
        self.log.info(f'Upserting DataFrame into Postgres table {table_full_name}')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            table_identifier = (
                psycopg2_sql.Identifier(schema, table_name_simple)
                if schema
                else psycopg2_sql.Identifier(table_name_simple)
            )

            insert_cols_ident = [
                psycopg2_sql.Identifier(col) for col in df_filtered.columns
            ]

            conflict_cols_ident = [
                psycopg2_sql.Identifier(col) for col in self.conflict_columns
            ]

            audit_cols_to_exclude = {
                'criado_em',
                'atualizado_em',
                'created_at',
                'updated_at',
            }
            update_set_cols = [
                col
                for col in df_filtered.columns
                if col not in self.conflict_columns
                and col.lower() not in audit_cols_to_exclude
            ]

            if not update_set_cols:
                update_sql_part = psycopg2_sql.SQL('NOTHING')
            else:
                set_statements = [
                    psycopg2_sql.SQL(
                        '{col_to_update} = EXCLUDED.{col_to_update}'
                    ).format(col_to_update=psycopg2_sql.Identifier(col))
                    for col in update_set_cols
                ]
                update_sql_part = psycopg2_sql.SQL('UPDATE SET {}').format(
                    psycopg2_sql.SQL(', ').join(set_statements)
                )

            insert_sql = psycopg2_sql.SQL(
                'INSERT INTO {table} ({columns}) VALUES %s'
            ).format(
                table=table_identifier,
                columns=psycopg2_sql.SQL(', ').join(insert_cols_ident),
            )

            conflict_sql_part = psycopg2_sql.SQL(
                'ON CONFLICT ({conflict_cols}) DO '
            ).format(conflict_cols=psycopg2_sql.SQL(', ').join(conflict_cols_ident))

            final_sql_query = psycopg2_sql.SQL(' ').join([
                insert_sql,
                conflict_sql_part,
                update_sql_part,
            ])

            data_tuples = self._dataframe_to_tuples(df_filtered)

            execute_values(
                cursor,
                final_sql_query.as_string(cursor),
                data_tuples,
                page_size=1000,
            )
            conn.commit()
            self.log.info('Upsert to Postgres complete.')
        except Exception as e:
            conn.rollback()
            self.log.error(f'Failed to upsert records into database: {e}')
            raise
        finally:
            cursor.close()
            conn.close()
