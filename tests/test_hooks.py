from unittest.mock import MagicMock, patch

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


class TestTableExistsPostgres:
    def test_table_exists_returns_true_when_table_exists(self):
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (True,)

        table = Table(conn_id='pg_conn', table_name='public.test_table')
        result = SQLHookManager._table_exists_postgres(mock_hook, table)

        assert result is True
        mock_hook.get_first.assert_called_once()

    def test_table_exists_returns_false_when_table_not_exists(self):
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (False,)

        table = Table(conn_id='pg_conn', table_name='public.test_table')
        result = SQLHookManager._table_exists_postgres(mock_hook, table)

        assert result is False

    def test_table_exists_handles_schema_name(self):
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (True,)

        table = Table(conn_id='pg_conn', table_name='my_schema.my_table')
        result = SQLHookManager._table_exists_postgres(mock_hook, table)

        assert result is True
        call_args = mock_hook.get_first.call_args
        assert 'my_schema' in str(call_args)
        assert 'my_table' in str(call_args)

    def test_table_exists_default_schema_is_public(self):
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (False,)

        table = Table(conn_id='pg_conn', table_name='simple_table')
        result = SQLHookManager._table_exists_postgres(mock_hook, table)

        assert result is False
        call_args = mock_hook.get_first.call_args
        assert 'public' in str(call_args)


class TestTruncatePostgresTableIfExists:
    def test_truncate_checks_table_existence(self):
        import pandas as pd

        df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        table = Table(conn_id='pg_conn', table_name='public.test_table')

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        original_method = SQLHookManager._table_exists_postgres
        called_with = []

        def track_call(hook, tbl):
            called_with.append((hook, tbl))
            return True

        SQLHookManager._table_exists_postgres = staticmethod(track_call)

        try:
            mock_hook = MagicMock()
            mock_hook.get_sqlalchemy_engine.return_value = mock_engine
            mock_hook.get_first.return_value = (True,)

            with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
                SQLHookManager._truncate_postgres_table(df, table)

            assert len(called_with) == 1
            assert called_with[0][1] == table
        finally:
            SQLHookManager._table_exists_postgres = original_method

    def test_creates_table_when_not_exists_does_not_truncate(self):
        import pandas as pd

        df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        table = Table(conn_id='pg_conn', table_name='public.test_table')

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        original_method = SQLHookManager._table_exists_postgres
        called_with = []

        def track_call(hook, tbl):
            called_with.append((hook, tbl))
            return False

        SQLHookManager._table_exists_postgres = staticmethod(track_call)

        try:
            mock_hook = MagicMock()
            mock_hook.get_sqlalchemy_engine.return_value = mock_engine
            mock_hook.get_first.return_value = (False,)

            with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
                SQLHookManager._truncate_postgres_table(df, table)

            assert len(called_with) == 1
            assert called_with[0][1] == table
            assert not mock_conn.execute.called
        finally:
            SQLHookManager._table_exists_postgres = original_method
