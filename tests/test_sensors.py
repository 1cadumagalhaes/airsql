from typing import Any
from unittest.mock import MagicMock, patch

from airsql.sensors import BigQuerySqlSensor, PostgresSqlSensor


class TestPostgresSqlSensor:
    def test_instantiation(self) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT COUNT(*) FROM users WHERE active = true',
            conn_id='postgres_conn',
        )
        assert sensor.task_id == 'test_sensor'
        assert sensor.sql == 'SELECT COUNT(*) FROM users WHERE active = true'
        assert sensor.conn_id == 'postgres_conn'

    def test_retries_default(self) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='postgres_conn',
        )
        assert sensor.retries == 1

    def test_retries_custom(self) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='postgres_conn',
            retries=5,
        )
        assert sensor.retries == 5

    def test_poke_count_initialized(self) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='postgres_conn',
        )
        assert sensor.poke_count == 0

    def test_poke_increments_count(self) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='postgres_conn',
        )
        context: dict[str, Any] = {}
        sensor.poke(context)
        assert sensor.poke_count == 1
        sensor.poke(context)
        assert sensor.poke_count == 2

    @patch('airsql.sensors.postgres.PostgresHook')
    def test_get_hook_returns_postgres_hook(self, mock_hook_class: MagicMock) -> None:
        sensor = PostgresSqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='postgres_conn',
        )
        sensor._get_hook()
        mock_hook_class.assert_called_once_with(postgres_conn_id='postgres_conn')


class TestBigQuerySqlSensor:
    def test_instantiation(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT COUNT(*) FROM `project.dataset.table`',
            conn_id='bigquery_conn',
        )
        assert sensor.task_id == 'test_sensor'
        assert sensor.sql == 'SELECT COUNT(*) FROM `project.dataset.table`'
        assert sensor.conn_id == 'bigquery_conn'

    def test_location_default(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
        )
        assert sensor.location == 'us-central1'

    def test_location_custom(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
            location='europe-west1',
        )
        assert sensor.location == 'europe-west1'

    def test_retries_default(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
        )
        assert sensor.retries == 1

    def test_retries_custom(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
            retries=3,
        )
        assert sensor.retries == 3

    def test_poke_count_initialized(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
        )
        assert sensor.poke_count == 0

    def test_poke_increments_count(self) -> None:
        sensor = BigQuerySqlSensor(
            task_id='test_sensor',
            sql='SELECT 1',
            conn_id='bigquery_conn',
        )
        context: dict[str, Any] = {}
        sensor.poke(context)
        assert sensor.poke_count == 1
        sensor.poke(context)
        assert sensor.poke_count == 2

    def test_get_hook_returns_bigquery_hook(self) -> None:
        with patch(
            'airflow.providers.google.cloud.hooks.bigquery.BigQueryHook'
        ) as mock_hook_class:
            mock_hook_class.return_value = MagicMock()
            sensor = BigQuerySqlSensor(
                task_id='test_sensor',
                sql='SELECT 1',
                conn_id='bigquery_conn',
                location='US',
            )
            sensor._get_hook()
            mock_hook_class.assert_called_once()
            call_kwargs = mock_hook_class.call_args[1]
            assert call_kwargs['gcp_conn_id'] == 'bigquery_conn'
            assert call_kwargs['use_legacy_sql'] is False
            assert call_kwargs['location'] == 'US'
