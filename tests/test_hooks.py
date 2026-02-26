from unittest.mock import MagicMock

from airsql.hooks import DEFAULT_BIGQUERY_LOCATION, SQLHookManager
from airsql.table import Table


class TestEnsureBigQueryDataset:
    def test_ensure_bigquery_dataset_calls_create_dataset(self):
        mock_hook = MagicMock()
        mock_client = MagicMock()
        mock_hook.get_client.return_value = mock_client

        SQLHookManager._ensure_bigquery_dataset(
            mock_hook, 'test-project', 'test_dataset', 'us-central1'
        )

        mock_client.create_dataset.assert_called_once()
        call_args = mock_client.create_dataset.call_args
        assert call_args[1]['exists_ok'] is True

    def test_ensure_bigquery_dataset_uses_location(self):
        mock_hook = MagicMock()
        mock_client = MagicMock()
        mock_hook.get_client.return_value = mock_client

        SQLHookManager._ensure_bigquery_dataset(
            mock_hook, 'test-project', 'test_dataset', 'europe-west1'
        )

        get_client_call = mock_hook.get_client.call_args
        assert get_client_call[1]['location'] == 'europe-west1'

    def test_ensure_bigquery_dataset_default_location(self):
        mock_hook = MagicMock()
        mock_client = MagicMock()
        mock_hook.get_client.return_value = mock_client

        SQLHookManager._ensure_bigquery_dataset(
            mock_hook, 'test-project', 'test_dataset'
        )

        get_client_call = mock_hook.get_client.call_args
        assert get_client_call[1]['location'] == DEFAULT_BIGQUERY_LOCATION


class TestDatasetLocationResolution:
    def test_location_resolution_dataset_location_overrides_table(self):
        table = Table(
            conn_id='bq_conn', table_name='dataset.table', location='asia-east1'
        )
        result = 'us-east1'
        assert result == 'us-east1'

    def test_location_resolution_uses_table_location(self):
        table = Table(
            conn_id='bq_conn', table_name='dataset.table', location='asia-east1'
        )
        dataset_location = None
        result = dataset_location or table.location or DEFAULT_BIGQUERY_LOCATION
        assert result == 'asia-east1'

    def test_location_resolution_uses_default(self):
        table = Table(conn_id='bq_conn', table_name='dataset.table')
        dataset_location = None
        result = dataset_location or table.location or DEFAULT_BIGQUERY_LOCATION
        assert result == DEFAULT_BIGQUERY_LOCATION
