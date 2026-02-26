from unittest.mock import MagicMock, patch

from airsql.transfers.postgres_gcs import PostgresToGCSOperator


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
