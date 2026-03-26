import json
from unittest.mock import MagicMock, patch

import pandas as pd

from airsql.transfers.gcs_postgres import GCSToPostgresOperator


class TestDetectJsonColumns:
    def test_detect_json_columns_returns_json_columns(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('metadata',), ('config',)]

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == {'metadata', 'config'}

    def test_detect_json_columns_returns_empty_set_when_no_json_columns(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == set()

    def test_detect_json_columns_handles_exception(self):
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = Exception('DB error')

        result = GCSToPostgresOperator._detect_json_columns(
            mock_hook, 'public', 'test_table'
        )

        assert result == set()


class TestFixJsonQuoting:
    def test_fix_json_quoting_converts_single_quotes_to_double(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': ["{'key': 'value'}", "{'name': 'test', 'count': 1}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"key": "value"}'
        assert result['metadata'][1] == '{"name": "test", "count": 1}'

    def test_fix_json_quoting_handles_nested_structures(self):
        df = pd.DataFrame({
            'id': [1],
            'config': ["{'nested': {'deep': 'value'}}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'config'})

        assert result['config'][0] == '{"nested": {"deep": "value"}}'

    def test_fix_json_quoting_handles_lists(self):
        df = pd.DataFrame({
            'id': [1],
            'items': ["['a', 'b', 'c']"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'items'})

        assert result['items'][0] == '["a", "b", "c"]'

    def test_fix_json_quoting_handles_null_values(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': [None, "{'key': 'value'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert pd.isna(result['metadata'][0])
        assert result['metadata'][1] == '{"key": "value"}'

    def test_fix_json_quoting_handles_nan_values(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'metadata': [float('nan'), "{'key': 'value'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert pd.isna(result['metadata'][0])
        assert result['metadata'][1] == '{"key": "value"}'

    def test_fix_json_quoting_leaves_valid_json_unchanged(self):
        df = pd.DataFrame({
            'id': [1],
            'metadata': ['{"already": "valid"}'],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"already": "valid"}'

    def test_fix_json_quoting_handles_mixed_types(self):
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'metadata': [
                "{'key': 'value'}",
                'plain string',
                '123',
            ],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['metadata'][0] == '{"key": "value"}'
        assert result['metadata'][1] == 'plain string'
        assert result['metadata'][2] == '123'

    def test_fix_json_quoting_ignores_non_json_columns(self):
        df = pd.DataFrame({
            'id': [1],
            'name': ["{'this': 'looks like json'}"],
            'metadata': ["{'actual': 'json'}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'metadata'})

        assert result['name'][0] == "{'this': 'looks like json'}"
        assert result['metadata'][0] == '{"actual": "json"}'

    def test_fix_json_quoting_handles_missing_column(self):
        df = pd.DataFrame({
            'id': [1],
            'name': ['test'],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'nonexistent'})

        assert 'name' in result.columns
        assert result['name'][0] == 'test'

    def test_fix_json_quoting_handles_complex_nested_json(self):
        df = pd.DataFrame({
            'id': [1],
            'data': ["{'outer': {'inner': ['a', 'b']}, 'nums': [1, 2, 3]}"],
        })

        result = GCSToPostgresOperator._fix_json_quoting(df, {'data'})

        parsed = json.loads(result['data'][0])
        assert parsed == {'outer': {'inner': ['a', 'b']}, 'nums': [1, 2, 3]}


class TestTableExists:
    def test_table_exists_returns_true(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(True,)]

        result = GCSToPostgresOperator._table_exists(mock_hook, 'public', 'test_table')

        assert result is True

    def test_table_exists_returns_false(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(False,)]

        result = GCSToPostgresOperator._table_exists(mock_hook, 'public', 'test_table')

        assert result is False

    def test_table_exists_handles_empty_result(self):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []

        result = GCSToPostgresOperator._table_exists(mock_hook, 'public', 'test_table')

        assert result is False


class TestCreateIfEmptyParameter:
    def test_create_if_empty_default_is_false(self):
        op = GCSToPostgresOperator(
            task_id='test_task',
            target_table_name='public.test_table',
            bucket_name='test-bucket',
            object_name='test.jsonl',
            postgres_conn_id='postgres',
            gcp_conn_id='gcp',
        )
        assert op.create_if_empty is False

    def test_create_if_empty_can_be_set(self):
        op = GCSToPostgresOperator(
            task_id='test_task',
            target_table_name='public.test_table',
            bucket_name='test-bucket',
            object_name='test.jsonl',
            postgres_conn_id='postgres',
            gcp_conn_id='gcp',
            create_if_empty=True,
        )
        assert op.create_if_empty is True


class TestCoerceColumnTypes:
    """Test _coerce_column_types for BigQuery float-to-integer conversion."""

    def test_coerce_float_to_integer_from_pg_schema(self):
        """Test converting float columns to integer based on PostgreSQL schema."""
        df = pd.DataFrame({
            'id': [1.0, 2.0, 3.0],
            'name': ['Alice', 'Bob', 'Charlie'],
            'count': [10.0, 20.0, 30.0],
        })

        column_types = {'id': 'integer', 'name': 'text', 'count': 'integer'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )

        result = op._coerce_column_types(df, column_types)

        # Int64 is pandas nullable integer type (handles NaN)
        assert str(result['id'].dtype) == 'Int64'
        assert str(result['count'].dtype) == 'Int64'
        assert result['id'].tolist() == [1, 2, 3]
        assert result['count'].tolist() == [10, 20, 30]

    def test_coerce_float_to_integer_from_bq_schema(self):
        """Test converting float columns to integer based on BigQuery schema."""
        df = pd.DataFrame({
            'id': [1.0, 2.0, 3.0],
            'name': ['Alice', 'Bob', 'Charlie'],
            'count': [10.0, 20.0, 30.0],
        })

        column_types = {'id': 'bigint', 'name': 'text', 'count': 'bigint'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            source_schema={'id': 'INTEGER', 'count': 'INT64'},
        )

        result = op._coerce_column_types(df, column_types)

        # Int64 is pandas nullable integer type (handles NaN)
        assert str(result['id'].dtype) == 'Int64'
        assert str(result['count'].dtype) == 'Int64'

    def test_coerce_preserves_float_for_float_columns(self):
        """Test that float columns are not converted when target is float."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.5, 20.5, 30.5],
        })

        column_types = {'id': 'integer', 'value': 'double precision'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )

        result = op._coerce_column_types(df, column_types)

        assert result['id'].dtype == 'int64'
        assert result['value'].dtype == 'float64'
        assert result['value'].tolist() == [10.5, 20.5, 30.5]

    def test_coerce_handles_nan_values(self):
        """Test that NaN values are handled during float-to-int conversion."""
        import numpy as np

        df = pd.DataFrame({
            'id': [1.0, 2.0, None],
            'count': [10.0, np.nan, 30.0],
        })

        column_types = {'id': 'integer', 'count': 'integer'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )

        result = op._coerce_column_types(df, column_types)

        assert result['id'].tolist()[0] == 1
        assert result['id'].tolist()[1] == 2
        assert pd.isna(result['id'].tolist()[2])

    def test_coerce_with_pyarrow_backend(self):
        """Test conversion with PyArrow-backed DataFrame."""
        df = pd.DataFrame({
            'id': [1.0, 2.0, 3.0],
            'name': ['Alice', 'Bob', 'Charlie'],
        }).convert_dtypes(dtype_backend='pyarrow')

        column_types = {'id': 'integer', 'name': 'text'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )

        result = op._coerce_column_types(df, column_types)

        assert result['id'].tolist() == [1, 2, 3]

    def test_coerce_object_dtype_string_numbers(self):
        """Test conversion of object dtype containing string numbers like '0.0'."""
        df = pd.DataFrame({
            'id': ['1.0', '2.0', '3.0'],
            'count': ['10.0', '20.0', None],
        })

        column_types = {'id': 'integer', 'count': 'integer'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            source_schema={'id': 'INTEGER', 'count': 'INTEGER'},
        )

        result = op._coerce_column_types(df, column_types)

        assert str(result['id'].dtype) == 'Int64'
        assert str(result['count'].dtype) == 'Int64'
        assert result['id'].tolist() == [1, 2, 3]
        assert result['count'].tolist()[0] == 10
        assert result['count'].tolist()[1] == 20
        assert pd.isna(result['count'].tolist()[2])

    def test_coerce_numeric_from_string(self):
        """Test conversion of string numbers to numeric/float types."""
        df = pd.DataFrame({
            'price': ['100.50', '200.75', '300.00'],
            'discount': ['0.1', '0.2', None],
        })

        column_types = {'price': 'numeric', 'discount': 'double precision'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            source_schema={'price': 'FLOAT', 'discount': 'FLOAT64'},
        )

        result = op._coerce_column_types(df, column_types)

        assert result['price'].tolist() == [100.50, 200.75, 300.00]
        assert result['discount'].tolist()[0] == 0.1
        assert result['discount'].tolist()[1] == 0.2

    def test_coerce_boolean_from_string(self):
        """Test conversion of string booleans to boolean type."""
        df = pd.DataFrame({
            'active': ['true', 'false', 'TRUE'],
            'enabled': ['True', 'False', None],
        })

        column_types = {'active': 'boolean', 'enabled': 'boolean'}

        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            source_schema={'active': 'BOOLEAN', 'enabled': 'BOOL'},
        )

        result = op._coerce_column_types(df, column_types)

        assert result['active'].tolist() == [True, False, True]
        assert result['enabled'].tolist()[0] is True
        assert result['enabled'].tolist()[1] is False


class TestPartitionParameters:
    def test_partition_column_defaults_to_none(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )
        assert op.partition_column is None

    def test_partition_parameters_can_be_set(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            partition_column='event_date',
        )
        assert op.partition_column == 'event_date'


class TestPartitionExchange:
    def test_partition_exchange_casts_date_partition_bounds(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            partition_column='event_date',
            source_schema={'event_date': 'DATE'},
        )
        df = pd.DataFrame({
            'event_date': [pd.Timestamp('2025-02-02').date()],
            'id': [1],
        })

        first_cursor = MagicMock()
        second_cursor = MagicMock()
        second_cursor.fetchone.return_value = None

        first_conn = MagicMock()
        first_conn.cursor.return_value = first_cursor

        second_conn = MagicMock()
        second_conn.cursor.return_value = second_cursor

        mock_hook = MagicMock()
        mock_hook.get_conn.side_effect = [first_conn, second_conn]
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch.object(pd.DataFrame, 'to_sql', autospec=True, return_value=None):
            op._partition_exchange(mock_hook, 'public', 'test_table', df)

        create_partition_calls = [
            call
            for call in second_cursor.execute.call_args_list
            if 'FOR VALUES FROM' in call.args[0]
        ]

        assert len(create_partition_calls) == 1
        sql = create_partition_calls[0].args[0]
        assert "FOR VALUES FROM ('2025-02-02'::DATE)" in sql
        assert "TO ('2025-02-03'::DATE)" in sql

    def test_quote_sql_literal_escapes_single_quotes(self):
        assert GCSToPostgresOperator._quote_sql_literal("O'Brien") == "'O''Brien'"

    def test_get_partition_pg_type_uses_source_schema(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
            partition_column='event_at',
            source_schema={'event_at': 'TIMESTAMP'},
        )

        assert op._get_partition_pg_type() == 'TIMESTAMPTZ'


class TestGCSObjectResolution:
    def test_resolve_object_names_returns_single_name_without_wildcard(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='temp/export/data.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )

        assert op._resolve_object_names(MagicMock()) == ['temp/export/data.parquet']

    def test_resolve_object_names_matches_wildcard_objects(self):
        op = GCSToPostgresOperator(
            task_id='test',
            target_table_name='public.test',
            bucket_name='bucket',
            object_name='temp/export/data-*.parquet',
            postgres_conn_id='pg',
            gcp_conn_id='gcp',
        )
        mock_gcs_hook = MagicMock()
        mock_gcs_hook.list.return_value = [
            'temp/export/data-000000000001.parquet',
            'temp/export/ignore.csv',
            'temp/export/data-000000000000.parquet',
        ]

        result = op._resolve_object_names(mock_gcs_hook)

        assert result == [
            'temp/export/data-000000000000.parquet',
            'temp/export/data-000000000001.parquet',
        ]
