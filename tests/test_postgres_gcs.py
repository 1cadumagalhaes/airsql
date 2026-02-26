from unittest.mock import MagicMock, patch

import pandas as pd

from airsql.transfers.postgres_gcs import (
    POSTGRES_TO_BQ_TYPE_MAP,
    PostgresToGCSOperator,
    _build_schema_from_column_types,
    _format_number,
    _pa_table_to_bq_schema,
)


class TestFormatNumber:
    def test_format_number_small(self):
        assert _format_number(100) == '100'

    def test_format_number_thousands(self):
        assert _format_number(1000) == '1_000'

    def test_format_number_millions(self):
        assert _format_number(1000000) == '1_000_000'

    def test_format_number_zero(self):
        assert _format_number(0) == '0'


class TestBuildSchemaFromColumnTypes:
    def test_basic_types(self):
        column_types = {'id': 'int4', 'name': 'text', 'created_at': 'timestamp'}
        json_columns = set()
        result = _build_schema_from_column_types(column_types, json_columns)
        assert len(result) == 3
        assert {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'} in result
        assert {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'} in result
        assert {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'} in result

    def test_json_columns(self):
        column_types = {'id': 'int4', 'metadata': 'json', 'config': 'jsonb'}
        json_columns = {'metadata', 'config'}
        result = _build_schema_from_column_types(column_types, json_columns)
        assert {'name': 'metadata', 'type': 'JSON', 'mode': 'NULLABLE'} in result
        assert {'name': 'config', 'type': 'JSON', 'mode': 'NULLABLE'} in result

    def test_unknown_type_falls_back_to_string(self):
        column_types = {'custom_col': 'custom_type'}
        json_columns = set()
        result = _build_schema_from_column_types(column_types, json_columns)
        assert {'name': 'custom_col', 'type': 'STRING', 'mode': 'NULLABLE'} in result


class TestPaTableToBqSchema:
    def test_simple_types(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['a', 'b'],
            'value': [1.5, 2.5],
            'active': [True, False],
        })
        schema = _pa_table_to_bq_schema(df)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['id']['type'] == 'INTEGER'
        assert schema_dict['name']['type'] == 'STRING'
        assert schema_dict['value']['type'] == 'FLOAT'
        assert schema_dict['active']['type'] == 'BOOLEAN'

    def test_list_type_as_json_in_json_mode(self):
        df = pd.DataFrame({
            'id': [1],
            'items': [[1, 2, 3]],
        })
        schema = _pa_table_to_bq_schema(df, json_mode=True)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['items']['type'] == 'JSON'
        assert schema_dict['items']['mode'] == 'NULLABLE'

    def test_struct_type_as_json_in_json_mode(self):
        df = pd.DataFrame({
            'id': [1],
            'data': [{'key': 'value'}],
        })
        schema = _pa_table_to_bq_schema(df, json_mode=True)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['data']['type'] == 'JSON'

    def test_postgres_type_map_json_column(self):
        df = pd.DataFrame({'id': [1], 'data': ['some text']})
        postgres_type_map = {
            'data': {'typname': 'json', 'is_array': False, 'element_typname': None}
        }
        schema = _pa_table_to_bq_schema(df, postgres_type_map=postgres_type_map)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['data']['type'] == 'JSON'

    def test_postgres_type_map_jsonb_column(self):
        df = pd.DataFrame({'id': [1], 'data': ['some text']})
        postgres_type_map = {
            'data': {'typname': 'jsonb', 'is_array': False, 'element_typname': None}
        }
        schema = _pa_table_to_bq_schema(df, postgres_type_map=postgres_type_map)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['data']['type'] == 'JSON'

    def test_postgres_type_map_array_column(self):
        df = pd.DataFrame({'id': [1], 'tags': [['a', 'b']]})
        postgres_type_map = {
            'tags': {'typname': '_text', 'is_array': True, 'element_typname': 'text'}
        }
        schema = _pa_table_to_bq_schema(df, postgres_type_map=postgres_type_map)
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['tags']['mode'] == 'REPEATED'

    def test_postgres_type_map_array_json_in_json_mode(self):
        df = pd.DataFrame({'id': [1], 'items': [['a', 'b']]})
        postgres_type_map = {
            'items': {'typname': '_json', 'is_array': True, 'element_typname': 'json'}
        }
        schema = _pa_table_to_bq_schema(
            df, postgres_type_map=postgres_type_map, json_mode=True
        )
        schema_dict = {f['name']: f for f in schema}
        assert schema_dict['items']['type'] == 'JSON'


class TestPostgresToBqTypeMap:
    def test_type_map_has_expected_types(self):
        assert POSTGRES_TO_BQ_TYPE_MAP['bool'] == 'BOOL'
        assert POSTGRES_TO_BQ_TYPE_MAP['int4'] == 'INTEGER'
        assert POSTGRES_TO_BQ_TYPE_MAP['int8'] == 'INTEGER'
        assert POSTGRES_TO_BQ_TYPE_MAP['float8'] == 'FLOAT'
        assert POSTGRES_TO_BQ_TYPE_MAP['text'] == 'STRING'
        assert POSTGRES_TO_BQ_TYPE_MAP['json'] == 'JSON'
        assert POSTGRES_TO_BQ_TYPE_MAP['jsonb'] == 'JSON'
        assert POSTGRES_TO_BQ_TYPE_MAP['uuid'] == 'STRING'
        assert POSTGRES_TO_BQ_TYPE_MAP['inet'] == 'STRING'


class TestDetectJsonColumns:
    def test_detect_json_columns_returns_json_columns(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        call_count = [0]

        def make_cursor():
            cursor = MagicMock()
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        main_cursor = make_cursor()
        main_cursor.description = [
            ('id', 23, None, None, None, None, None),
            ('metadata', 114, None, None, None, None, None),
            ('config', 3802, None, None, None, None, None),
        ]
        main_cursor.execute = MagicMock()
        main_cursor.close = MagicMock()

        type_responses = [('int4',), ('json',), ('jsonb',)]

        def mock_cursor_factory():
            call_count[0] += 1
            if call_count[0] == 1:
                return main_cursor
            idx = call_count[0] - 2
            if idx < len(type_responses):
                c = make_cursor()
                c.fetchone.return_value = type_responses[idx]
                c.execute = MagicMock()
                c.close = MagicMock()
                return c
            return make_cursor()

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = mock_cursor_factory
        mock_conn.close = MagicMock()

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        result = op._detect_json_columns(mock_hook)

        assert 'metadata' in result
        assert 'config' in result
        assert 'id' not in result

    def test_detect_json_columns_returns_empty_set_when_no_json(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        call_count = [0]

        def make_cursor():
            cursor = MagicMock()
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        main_cursor = make_cursor()
        main_cursor.description = [
            ('id', 23, None, None, None, None, None),
            ('name', 25, None, None, None, None, None),
        ]
        main_cursor.execute = MagicMock()
        main_cursor.close = MagicMock()

        type_responses = [('int4',), ('text',)]

        def mock_cursor_factory():
            call_count[0] += 1
            if call_count[0] == 1:
                return main_cursor
            idx = call_count[0] - 2
            if idx < len(type_responses):
                c = make_cursor()
                c.fetchone.return_value = type_responses[idx]
                c.execute = MagicMock()
                c.close = MagicMock()
                return c
            return make_cursor()

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = mock_cursor_factory
        mock_conn.close = MagicMock()

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        result = op._detect_json_columns(mock_hook)

        assert result == set()

    def test_detect_json_columns_handles_exception(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_hook.get_conn.side_effect = Exception('connection error')

        result = op._detect_json_columns(mock_hook)

        assert result == set()


class TestGetColumnTypes:
    def test_get_column_types_returns_types(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ('id', 23, None, None, None, None, None),
            ('name', 25, None, None, None, None, None),
            ('price', 700, None, None, None, None, None),
        ]
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor2 = MagicMock()
        mock_cursor2.fetchone.side_effect = [('int4',), ('text',), ('numeric',)]
        mock_conn.cursor.side_effect = [
            mock_cursor,
            mock_cursor2,
            mock_cursor2,
            mock_cursor2,
        ]

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        result = op._get_column_types(mock_hook)

        assert result == {'id': 'int4', 'name': 'text', 'price': 'numeric'}

    def test_get_column_types_handles_exception(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_hook.get_conn.side_effect = Exception('db error')

        result = op._get_column_types(mock_hook)

        assert result == {}


class TestBuildCopyQuery:
    def test_build_copy_query_csv(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT id, name FROM users',
            bucket='bucket',
            filename='file.csv',
        )

        column_types = {'id': 'int4', 'name': 'text'}
        json_columns = set()

        result = op._build_copy_query(column_types, json_columns)

        assert 'SELECT id, name FROM (SELECT id, name FROM users) AS subquery' == result

    def test_build_copy_query_jsonl(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT id, data FROM users',
            bucket='bucket',
            filename='file.jsonl',
        )

        column_types = {'id': 'int4', 'data': 'json'}
        json_columns = {'data'}

        result = op._build_copy_query(column_types, json_columns)

        assert 'row_to_json' in result
        assert 'SELECT row_to_json(t) FROM (SELECT id, data FROM users) AS t' == result

    def test_build_copy_query_casts_uuid_to_text(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT id FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        column_types = {'id': 'uuid'}
        json_columns = set()

        result = op._build_copy_query(column_types, json_columns)

        assert 'CAST(id AS TEXT)' in result

    def test_build_copy_query_casts_inet_to_text(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT ip FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        column_types = {'ip': 'inet'}
        json_columns = set()

        result = op._build_copy_query(column_types, json_columns)

        assert 'CAST(ip AS TEXT)' in result


class TestDetectProblematicColumns:
    def test_detects_double_quote(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            ('text with "quote"', 'normal text'),
        ])

        result = op._detect_problematic_columns(mock_hook, ['col1', 'col2'])

        assert 'col1' in result
        assert 'col2' not in result

    def test_detects_newline(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            ('text\nwith\nnewlines',),
        ])

        result = op._detect_problematic_columns(mock_hook, ['col1'])

        assert 'col1' in result

    def test_detects_carriage_return(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            ('text\rwith\rcarriage',),
        ])

        result = op._detect_problematic_columns(mock_hook, ['col1'])

        assert 'col1' in result

    def test_no_problematic_chars(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            ('normal text', 'also normal'),
            ('another row', 'more text'),
        ])

        result = op._detect_problematic_columns(mock_hook, ['col1', 'col2'])

        assert result == set()

    def test_handles_null_values(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_cursor = MagicMock()
        mock_hook.get_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.__iter__ = lambda self: iter([
            (None, 'normal'),
        ])

        result = op._detect_problematic_columns(mock_hook, ['col1', 'col2'])

        assert result == set()

    def test_handles_empty_list(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()

        result = op._detect_problematic_columns(mock_hook, [])

        assert result == set()

    def test_handles_exception(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )

        mock_hook = MagicMock()
        mock_hook.get_conn.side_effect = Exception('connection error')

        result = op._detect_problematic_columns(mock_hook, ['col1'])

        assert result == set()


class TestAutoSwitchFormatParameter:
    def test_auto_switch_format_default_is_true(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
        )
        assert op.auto_switch_format is True

    def test_auto_switch_format_can_be_disabled(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT * FROM t',
            bucket='bucket',
            filename='file.csv',
            auto_switch_format=False,
        )
        assert op.auto_switch_format is False


class TestProblematicCharactersConstant:
    def test_problematic_characters_defined(self):
        assert PostgresToGCSOperator.PROBLEMATIC_CHARACTERS == {'"', '\n', '\r'}


class TestCopyCommandWithQuoting:
    def test_copy_csv_with_quoting_options(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT id, name FROM t',
            bucket='bucket',
            filename='file.csv',
            use_copy=True,
        )

        with (
            patch.object(op, '_get_column_types') as mock_types,
            patch.object(op, '_detect_json_columns') as mock_json,
            patch.object(op, '_detect_problematic_columns') as mock_problematic,
            patch.object(op, '_stream_copy_to_gcs') as mock_stream,
            patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg_hook,
            patch('airsql.transfers.postgres_gcs.GCSHook') as mock_gcs_hook,
        ):
            mock_types.return_value = {'id': 'int4', 'name': 'text'}
            mock_json.return_value = set()
            mock_problematic.return_value = set()
            mock_stream.return_value = 10

            mock_pg_hook.return_value.get_conn.return_value.cursor.return_value.__enter__ = MagicMock()
            mock_pg_hook.return_value.get_conn.return_value.cursor.return_value.__exit__ = MagicMock()

            with patch('google.cloud.storage.fileio.BlobWriter'):
                from airsql.transfers.postgres_gcs import PostgresToGCSOperator as Op

                with patch.object(Op, '_stream_copy_to_gcs') as m:
                    m.return_value = 10
                    op.execute({})

    def test_copy_command_format_for_csv(self):
        op = PostgresToGCSOperator(
            task_id='test',
            postgres_conn_id='pg',
            sql='SELECT id, name FROM t',
            bucket='bucket',
            filename='file.csv',
            use_copy=True,
            auto_switch_format=False,
        )

        with (
            patch.object(op, '_get_column_types') as mock_types,
            patch.object(op, '_detect_json_columns') as mock_json,
            patch.object(op, '_stream_copy_to_gcs') as mock_stream,
            patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg_hook,
            patch('airsql.transfers.postgres_gcs.GCSHook') as mock_gcs_hook,
            patch('google.cloud.storage.fileio.BlobWriter'),
        ):
            mock_types.return_value = {'id': 'int4', 'name': 'text'}
            mock_json.return_value = set()
            mock_stream.return_value = 10

            op.execute({})

            call_args = mock_stream.call_args
            assert call_args is not None
            assert call_args[0][3] is False
