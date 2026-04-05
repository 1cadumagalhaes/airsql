from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

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

        called_with = []

        def track_call(hook, tbl):
            called_with.append((hook, tbl))
            return True

        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine
        mock_hook.get_first.return_value = (True,)

        with (
            patch.object(SQLHookManager, 'get_hook', return_value=mock_hook),
            patch.object(
                SQLHookManager, '_table_exists_postgres', side_effect=track_call
            ),
        ):
            SQLHookManager._truncate_postgres_table(df, table)

        assert len(called_with) == 1
        assert called_with[0][1] == table

    def test_creates_table_when_not_exists_does_not_truncate(self):
        import pandas as pd

        df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        table = Table(conn_id='pg_conn', table_name='public.test_table')

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.return_value = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        called_with = []

        def track_call(hook, tbl):
            called_with.append((hook, tbl))
            return False

        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine
        mock_hook.get_first.return_value = (False,)

        with (
            patch.object(SQLHookManager, 'get_hook', return_value=mock_hook),
            patch.object(
                SQLHookManager, '_table_exists_postgres', side_effect=track_call
            ),
        ):
            SQLHookManager._truncate_postgres_table(df, table)

        assert len(called_with) == 1
        assert called_with[0][1] == table
        assert not mock_conn.execute.called


class TestPartitionedPostgresTable:
    """Tests for PostgreSQL partitioned table functionality."""

    @pytest.fixture
    def mock_pg_hook(self):
        """Create a mock Postgres hook."""
        mock_hook = MagicMock()
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Set up connection cursor
        mock_conn.cursor.return_value = mock_cursor
        mock_hook.get_conn.return_value = mock_conn
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine

        return mock_hook, mock_engine, mock_conn, mock_cursor

    def test_ensure_partitioned_postgres_table_creates_range_partitioned_table(
        self, mock_pg_hook
    ):
        """Test creating a RANGE partitioned table."""
        mock_hook, mock_engine, mock_conn, mock_cursor = mock_pg_hook

        # Table doesn't exist
        mock_cursor.fetchone.return_value = (False,)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'created_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'value': [100, 200, 300],
        })

        table = Table(
            conn_id='pg_conn',
            table_name='public.partitioned_table',
            postgres_partition_by='created_at',
            postgres_partition_type='RANGE',
        )

        with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
            SQLHookManager._ensure_partitioned_postgres_table(
                mock_hook, table, df, 'public', 'partitioned_table'
            )

        # Verify CREATE TABLE was called
        create_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if 'CREATE TABLE' in str(call)
        ]
        assert len(create_calls) > 0

        # Verify PARTITION BY RANGE clause
        create_sql = str(create_calls[0])
        assert 'PARTITION BY RANGE' in create_sql
        assert 'created_at' in create_sql

    def test_ensure_partitioned_postgres_table_creates_list_partitioned_table(
        self, mock_pg_hook
    ):
        """Test creating a LIST partitioned table."""
        mock_hook, mock_engine, mock_conn, mock_cursor = mock_pg_hook

        mock_cursor.fetchone.return_value = (False,)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'region': ['north', 'south', 'east'],
            'value': [100, 200, 300],
        })

        table = Table(
            conn_id='pg_conn',
            table_name='public.partitioned_table',
            postgres_partition_by='region',
            postgres_partition_type='LIST',
        )

        with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
            SQLHookManager._ensure_partitioned_postgres_table(
                mock_hook, table, df, 'public', 'partitioned_table'
            )

        create_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if 'CREATE TABLE' in str(call)
        ]
        assert len(create_calls) > 0

        create_sql = str(create_calls[0])
        assert 'PARTITION BY LIST' in create_sql
        assert 'region' in create_sql

    def test_ensure_partitioned_postgres_table_creates_hash_partitioned_table(
        self, mock_pg_hook
    ):
        """Test creating a HASH partitioned table."""
        mock_hook, mock_engine, mock_conn, mock_cursor = mock_pg_hook

        mock_cursor.fetchone.return_value = (False,)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'user_id': [1001, 1002, 1003],
            'value': [100, 200, 300],
        })

        table = Table(
            conn_id='pg_conn',
            table_name='public.partitioned_table',
            postgres_partition_by='user_id',
            postgres_partition_type='HASH',
        )

        with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
            SQLHookManager._ensure_partitioned_postgres_table(
                mock_hook, table, df, 'public', 'partitioned_table'
            )

        create_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if 'CREATE TABLE' in str(call)
        ]
        assert len(create_calls) > 0

        create_sql = str(create_calls[0])
        assert 'PARTITION BY HASH' in create_sql
        assert 'user_id' in create_sql

    def test_ensure_partitioned_postgres_table_skips_if_table_exists(
        self, mock_pg_hook
    ):
        """Test that partitioned table creation is skipped if table already exists."""
        mock_hook, mock_engine, mock_conn, mock_cursor = mock_pg_hook

        # Table already exists
        mock_cursor.fetchone.return_value = (True,)

        df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})

        table = Table(
            conn_id='pg_conn',
            table_name='public.existing_table',
            postgres_partition_by='id',
            postgres_partition_type='RANGE',
        )

        with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
            SQLHookManager._ensure_partitioned_postgres_table(
                mock_hook, table, df, 'public', 'existing_table'
            )

        # Verify no CREATE TABLE was executed
        create_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if 'CREATE TABLE' in str(call)
        ]
        assert len(create_calls) == 0

    def test_ensure_partitioned_postgres_table_with_expression(self, mock_pg_hook):
        """Test creating partitioned table with partition expression."""
        mock_hook, mock_engine, mock_conn, mock_cursor = mock_pg_hook

        mock_cursor.fetchone.return_value = (False,)

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'created_at': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01']),
        })

        table = Table(
            conn_id='pg_conn',
            table_name='public.partitioned_table',
            postgres_partition_by='created_at',
            postgres_partition_type='RANGE',
            postgres_partition_expression="date_trunc('month', created_at)",
        )

        with patch.object(SQLHookManager, 'get_hook', return_value=mock_hook):
            SQLHookManager._ensure_partitioned_postgres_table(
                mock_hook, table, df, 'public', 'partitioned_table'
            )

        create_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if 'CREATE TABLE' in str(call)
        ]
        assert len(create_calls) > 0

        create_sql = str(create_calls[0])
        assert 'PARTITION BY RANGE' in create_sql
        # The SQL contains escaped quotes in the repr, so check for both versions
        assert 'date_trunc' in create_sql
        assert 'month' in create_sql
        assert 'created_at' in create_sql

    def test_table_class_has_postgres_partition_fields(self):
        """Test that Table class has the new partition fields."""
        table = Table(
            conn_id='pg_conn',
            table_name='public.partitioned_table',
            postgres_partition_by='created_at',
            postgres_partition_type='RANGE',
            postgres_partition_expression="date_trunc('month', created_at)",
        )

        assert table.postgres_partition_by == 'created_at'
        assert table.postgres_partition_type == 'RANGE'
        assert table.postgres_partition_expression == "date_trunc('month', created_at)"


class TestBigQueryPartitionType:
    """Tests for BigQuery partition type support."""

    def test_table_class_has_partition_type_field(self):
        """Test that Table class supports partition_type for BigQuery."""
        table = Table(
            conn_id='bq_conn',
            table_name='dataset.table',
            partition_by='event_timestamp',
            partition_type='HOUR',
        )

        assert table.partition_by == 'event_timestamp'
        assert table.partition_type == 'HOUR'

    def test_table_class_partition_type_defaults_to_none(self):
        """Test that partition_type defaults to None."""
        table = Table(
            conn_id='bq_conn',
            table_name='dataset.table',
            partition_by='date',
        )

        assert table.partition_by == 'date'
        assert table.partition_type is None
