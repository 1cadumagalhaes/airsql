from unittest.mock import MagicMock, call, patch

from airsql.transfers.postgres_bigquery import PostgresToBigQueryOperator


class TestPostgresToBigQueryPaths:
    def test_default_gcs_temp_path_uses_run_scoped_directory(self):
        op = PostgresToBigQueryOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            gcs_bucket='bucket',
            destination_project_dataset_table='project.dataset.table',
            emit_asset=False,
            check_source_exists=False,
        )

        assert op.gcs_temp_path == 'temp/postgres_to_bq/test/{{ ts_nodash }}/data.csv'

    def test_get_schema_path_appends_suffix(self):
        assert (
            PostgresToBigQueryOperator._get_schema_path('temp/export/data.csv')
            == 'temp/export/data.csv.schema.json'
        )

    def test_write_disposition_is_listed_in_template_fields(self):
        op = PostgresToBigQueryOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            gcs_bucket='bucket',
            destination_project_dataset_table='project.dataset.table',
            write_disposition="{{ 'WRITE_TRUNCATE' if params.full_refresh else 'WRITE_APPEND' }}",
            emit_asset=False,
            check_source_exists=False,
        )

        assert 'write_disposition' in op.template_fields

    def test_schema_overrides_are_passed_to_extract_operator(self):
        op = PostgresToBigQueryOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            gcs_bucket='bucket',
            destination_project_dataset_table='project.dataset.table',
            schema_overrides={'chat_slow_mode_wait_time': 'INTEGER'},
            emit_asset=False,
            check_source_exists=False,
            dry_run=True,
        )

        mock_pg_to_gcs = MagicMock()
        mock_pg_to_gcs.execute.return_value = 'gs://bucket/temp/data.csv'

        with patch(
            'airsql.transfers.postgres_gcs.PostgresToGCSOperator',
            return_value=mock_pg_to_gcs,
        ) as mock_operator:
            op.execute({})

        assert mock_operator.call_args.kwargs['schema_overrides'] == {
            'chat_slow_mode_wait_time': 'INTEGER'
        }


class TestPostgresToBigQueryCleanup:
    def test_cleanup_temp_files_deletes_data_and_schema_paths(self):
        op = PostgresToBigQueryOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            gcs_bucket='bucket',
            destination_project_dataset_table='project.dataset.table',
            emit_asset=False,
            check_source_exists=False,
        )

        mock_gcs_hook = MagicMock()

        with patch(
            'airflow.providers.google.cloud.hooks.gcs.GCSHook',
            return_value=mock_gcs_hook,
        ):
            op._cleanup_temp_files()

        assert mock_gcs_hook.delete.call_args_list == [
            call(
                bucket_name='bucket',
                object_name='temp/postgres_to_bq/test/{{ ts_nodash }}/data.csv',
            ),
            call(
                bucket_name='bucket',
                object_name='temp/postgres_to_bq/test/{{ ts_nodash }}/data.csv.schema.json',
            ),
        ]

    def test_execute_cleans_updated_data_and_schema_paths(self):
        op = PostgresToBigQueryOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            gcs_bucket='bucket',
            gcs_temp_path='temp/data.parquet',
            destination_project_dataset_table='project.dataset.table',
            emit_asset=False,
            check_source_exists=False,
        )

        mock_pg_to_gcs = MagicMock()
        mock_pg_to_gcs.execute.return_value = 'gs://bucket/temp/data.jsonl'
        mock_pg_to_gcs.actual_export_format = 'jsonl'
        mock_pg_to_gcs.schema_filename = 'temp/data.jsonl.schema.json'

        mock_gcs_to_bq = MagicMock()

        with (
            patch.object(op, '_ensure_bigquery_dataset'),
            patch.object(op, '_cleanup_temp_files') as mock_cleanup,
            patch(
                'airsql.transfers.postgres_gcs.PostgresToGCSOperator',
                return_value=mock_pg_to_gcs,
            ),
            patch.dict(
                'sys.modules',
                {
                    'airflow.providers.google.cloud.transfers.gcs_to_bigquery': MagicMock(
                        GCSToBigQueryOperator=MagicMock(return_value=mock_gcs_to_bq)
                    )
                },
            ),
        ):
            op.execute({})

        mock_cleanup.assert_called_once_with([
            'temp/data.jsonl',
            'temp/data.jsonl.schema.json',
        ])
