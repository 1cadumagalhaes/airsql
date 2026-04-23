from unittest.mock import MagicMock, patch

import pytest

from airsql.transfers.bigquery_postgres import BigQueryToPostgresOperator


class TestBigQueryToPostgresOperatorInit:
    def test_requires_source_table_or_sql(self):
        with pytest.raises(
            ValueError,
            match='Either source_project_dataset_table or sql must be provided',
        ):
            BigQueryToPostgresOperator(
                task_id='test',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
            )

    def test_source_table_and_sql_mutually_exclusive(self):
        with pytest.raises(ValueError, match='mutually exclusive'):
            BigQueryToPostgresOperator(
                task_id='test',
                source_project_dataset_table='dataset.table',
                sql='SELECT * FROM dataset.table',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
            )

    def test_where_and_sql_mutually_exclusive(self):
        with pytest.raises(
            ValueError, match='where clause cannot be used with sql parameter'
        ):
            BigQueryToPostgresOperator(
                task_id='test',
                sql='SELECT * FROM dataset.table',
                where='date > CURRENT_DATE()',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
            )

    def test_where_requires_source_table(self):
        with pytest.raises(
            ValueError,
            match='Either source_project_dataset_table or sql must be provided',
        ):
            BigQueryToPostgresOperator(
                task_id='test',
                where='date > CURRENT_DATE()',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
            )

    def test_default_gcs_temp_path_uses_run_scoped_directory(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )
        expected_path = (
            'temp/bq_to_postgres/test/'
            "{{ run_id | replace(':', '_') | replace('+', '_') }}"
            '/data.parquet'
        )

        assert op.gcs_temp_path == expected_path

    def test_stores_postgres_type_overrides(self):
        overrides = {'followers_count': 'BIGINT'}
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            postgres_type_overrides=overrides,
            emit_asset=False,
        )

        assert op.postgres_type_overrides == overrides


class TestGetSourceQuery:
    def test_get_source_query_with_table(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        result = op._get_source_query()
        assert result == 'SELECT * FROM `dataset.table`'

    def test_get_source_query_with_where(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            where='date >= CURRENT_DATE()',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        result = op._get_source_query()
        assert result == 'SELECT * FROM `dataset.table` WHERE date >= CURRENT_DATE()'

    def test_get_source_query_with_sql(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            sql='SELECT id, name FROM dataset.table WHERE active = true',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        result = op._get_source_query()
        assert result == 'SELECT id, name FROM dataset.table WHERE active = true'

    def test_get_source_query_sql_takes_precedence_over_where(self):
        with pytest.raises(
            ValueError, match='where clause cannot be used with sql parameter'
        ):
            BigQueryToPostgresOperator(
                task_id='test',
                sql='SELECT * FROM dataset.table',
                where='should raise',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
                emit_asset=False,
            )


class TestCheckSourceData:
    def test_check_source_data_with_table(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            check_source_exists=True,
            emit_asset=False,
        )

        mock_sensor = MagicMock()
        mock_sensor.execute.return_value = None

        mock_sensor_class = MagicMock(return_value=mock_sensor)

        with patch.dict(
            'sys.modules',
            {'airsql.sensors.bigquery': MagicMock(BigQuerySqlSensor=mock_sensor_class)},
        ):
            op._check_source_data({})

        assert (
            mock_sensor_class.call_args[1]['sql']
            == 'SELECT 1 FROM `dataset.table` LIMIT 1'
        )

    def test_check_source_data_with_sql(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            sql='SELECT * FROM dataset.table WHERE id > 100',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            check_source_exists=True,
            emit_asset=False,
        )

        mock_sensor = MagicMock()
        mock_sensor.execute.return_value = None

        mock_sensor_class = MagicMock(return_value=mock_sensor)

        with patch.dict(
            'sys.modules',
            {'airsql.sensors.bigquery': MagicMock(BigQuerySqlSensor=mock_sensor_class)},
        ):
            op._check_source_data({})

        assert mock_sensor_class.call_args[1]['sql'] == (
            'SELECT 1 FROM (SELECT * FROM dataset.table WHERE id > 100) AS subquery LIMIT 1'
        )

    def test_check_source_data_with_where(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            where='id > 100',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            check_source_exists=True,
            emit_asset=False,
        )

        mock_sensor = MagicMock()
        mock_sensor.execute.return_value = None

        mock_sensor_class = MagicMock(return_value=mock_sensor)

        with patch.dict(
            'sys.modules',
            {'airsql.sensors.bigquery': MagicMock(BigQuerySqlSensor=mock_sensor_class)},
        ):
            op._check_source_data({})

        assert mock_sensor_class.call_args[1]['sql'] == (
            'SELECT 1 FROM `dataset.table` WHERE id > 100 LIMIT 1'
        )


class TestDetectJsonColumns:
    def test_detect_json_columns_from_query(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            sql='SELECT id, metadata FROM dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_field = MagicMock()
        mock_field.name = 'metadata'
        mock_field.field_type = 'JSON'

        mock_schema = [mock_field]

        mock_query_job = MagicMock()
        mock_query_job.schema = mock_schema
        mock_query_job.result.return_value = None

        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_hook = MagicMock()
        mock_hook.get_client.return_value = mock_client

        result = op._detect_json_columns(mock_hook)

        assert 'metadata' in result

    def test_detect_json_columns_from_table(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='project.dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_field = MagicMock()
        mock_field.name = 'config'
        mock_field.field_type = 'JSONB'

        mock_table = MagicMock()
        mock_table.schema = [mock_field]

        mock_client = MagicMock()
        mock_client.get_table.return_value = mock_table
        mock_client.dataset.return_value.table.return_value = 'table_ref'

        mock_hook = MagicMock()
        mock_hook.get_client.return_value = mock_client
        mock_hook.project_id = 'project'

        result = op._detect_json_columns(mock_hook)

        assert 'config' in result


class TestGetBqSchema:
    def test_get_bq_schema_from_query(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            sql='SELECT id, name FROM dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_field1 = MagicMock()
        mock_field1.name = 'id'
        mock_field1.field_type = 'INTEGER'

        mock_field2 = MagicMock()
        mock_field2.name = 'name'
        mock_field2.field_type = 'STRING'

        mock_query_job = MagicMock()
        mock_query_job.schema = [mock_field1, mock_field2]
        mock_query_job.result.return_value = None

        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job

        mock_hook = MagicMock()
        mock_hook.get_client.return_value = mock_client

        result = op._get_bq_schema(mock_hook)

        assert result == {'id': 'INTEGER', 'name': 'STRING'}

    def test_get_bq_schema_from_table(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='project.dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_field1 = MagicMock()
        mock_field1.name = 'id'
        mock_field1.field_type = 'INT64'

        mock_field2 = MagicMock()
        mock_field2.name = 'value'
        mock_field2.field_type = 'FLOAT64'

        mock_table = MagicMock()
        mock_table.schema = [mock_field1, mock_field2]

        mock_client = MagicMock()
        mock_client.get_table.return_value = mock_table
        mock_client.dataset.return_value.table.return_value = 'table_ref'

        mock_hook = MagicMock()
        mock_hook.get_client.return_value = mock_client
        mock_hook.project_id = 'project'

        result = op._get_bq_schema(mock_hook)

        assert result == {'id': 'INT64', 'value': 'FLOAT64'}


class TestCreateIfMissingParameter:
    def test_create_if_missing_defaults_to_false(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        assert op.create_if_missing is False

    def test_create_if_missing_can_be_set(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            create_if_missing=True,
            emit_asset=False,
        )

        assert op.create_if_missing is True

    def test_create_if_missing_and_create_if_empty_both_set(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            create_if_missing=True,
            create_if_empty=True,
            emit_asset=False,
        )

        assert op.create_if_missing is True
        assert op.create_if_empty is True


class TestPartitionParameters:
    def test_partition_column_requires_replace_false(self):
        with pytest.raises(ValueError, match='partition_column requires replace=False'):
            BigQueryToPostgresOperator(
                task_id='test',
                source_project_dataset_table='dataset.table',
                postgres_conn_id='pg',
                destination_table='public.table',
                gcs_bucket='bucket',
                partition_column='event_date',
                replace=True,
                emit_asset=False,
            )

    def test_partition_parameters_set_correctly(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            partition_column='event_date',
            replace=False,
            emit_asset=False,
        )

        assert op.partition_column == 'event_date'
        assert op.replace is False


class TestQueryExportPaths:
    def test_get_query_export_path_adds_wildcard_before_extension(self):
        assert (
            BigQueryToPostgresOperator._get_query_export_path(
                'temp/export/data.parquet'
            )
            == 'temp/export/data-*.parquet'
        )

    def test_export_query_to_gcs_uses_wildcard_uri(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='project.dataset.table',
            where="event_date = DATE('2026-03-25')",
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_job_operator = MagicMock()
        mock_job_operator.execute.return_value = None
        mock_job_operator_class = MagicMock(return_value=mock_job_operator)

        with patch.dict(
            'sys.modules',
            {
                'airflow.providers.google.cloud.operators.bigquery': MagicMock(
                    BigQueryInsertJobOperator=mock_job_operator_class
                )
            },
        ):
            op._export_query_to_gcs({}, 'parquet', 'temp/export/data.parquet')

        export_sql = mock_job_operator_class.call_args[1]['configuration']['query'][
            'query'
        ]
        assert "uri='gs://bucket/temp/export/data-*.parquet'" in export_sql
        assert 'overwrite=true' in export_sql

    def test_cleanup_temp_files_deletes_wildcard_matches(self):
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='project.dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            emit_asset=False,
        )

        mock_gcs_hook = MagicMock()
        mock_gcs_hook.list.return_value = [
            'temp/export/data-000000000000.parquet',
            'temp/export/data-000000000001.parquet',
        ]

        with patch(
            'airflow.providers.google.cloud.hooks.gcs.GCSHook',
            return_value=mock_gcs_hook,
        ):
            op._cleanup_temp_files('temp/export/data-*.parquet')

        assert mock_gcs_hook.delete.call_count == 2


class TestExecuteParameterForwarding:
    def test_execute_passes_postgres_type_overrides_to_gcs_loader(self):
        overrides = {'followers_count': 'BIGINT'}
        op = BigQueryToPostgresOperator(
            task_id='test',
            source_project_dataset_table='project.dataset.table',
            postgres_conn_id='pg',
            destination_table='public.table',
            gcs_bucket='bucket',
            check_source_exists=False,
            cleanup_temp_files=False,
            dry_run=True,
            postgres_type_overrides=overrides,
            emit_asset=False,
        )

        mock_loader = MagicMock()
        mock_loader.execute.return_value = None
        mock_loader_class = MagicMock(return_value=mock_loader)

        with patch.dict(
            'sys.modules',
            {
                'airsql.transfers.gcs_postgres': MagicMock(
                    GCSToPostgresOperator=mock_loader_class
                )
            },
        ):
            op.execute({})

        assert (
            mock_loader_class.call_args.kwargs['postgres_type_overrides'] == overrides
        )
