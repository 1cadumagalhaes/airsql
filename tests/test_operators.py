from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from airsql.operators import (
    BaseSQLOperator,
    DataFrameLoadOperator,
    DataFrameMergeOperator,
    SQLAppendOperator,
    SQLCheckOperator,
    SQLDataFrameOperator,
    SQLMergeOperator,
    SQLQueryOperator,
    SQLReplaceOperator,
    SQLTruncateOperator,
)
from tests.fixtures.data import (
    CONN_ID,
    DF_MERGE,
    DF_SIMPLE,
    SQL_SIMPLE,
    TABLE_SIMPLE,
    TABLE_TEMP,
)

CONTEXT: dict[str, Any] = {}


class TestBaseSQLOperator:
    def test_instantiation(self) -> None:
        op = BaseSQLOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID)
        assert op.task_id == 'test_task'
        assert op.sql == SQL_SIMPLE
        assert op.source_conn == CONN_ID

    def test_dry_run_compat_kwarg(self) -> None:
        op = BaseSQLOperator(task_id='test_task', sql=SQL_SIMPLE, dry_run=True)
        assert op.is_dry_run is True

    def test_dynamic_params_stored_as_attrs(self) -> None:
        op = BaseSQLOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            dynamic_params={'region': 'us-east', 'date': '2025-01-01'},
        )
        assert op.region == 'us-east'
        assert op.date == '2025-01-01'

    def test_hook_manager_initialized(self) -> None:
        op = BaseSQLOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID)
        assert hasattr(op, 'hook_manager')


class TestSQLQueryOperator:
    def test_instantiation(self) -> None:
        op = SQLQueryOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=True,
            pre_truncate=True,
        )
        assert op.task_id == 'test_task'
        assert op.output_table == TABLE_SIMPLE
        assert op.is_dry_run is True
        assert op.pre_truncate is True

    def test_defaults(self) -> None:
        op = SQLQueryOperator(
            task_id='test_task', sql=SQL_SIMPLE, output_table=TABLE_SIMPLE
        )
        assert op.is_dry_run is False
        assert op.pre_truncate is False
        assert op.source_conn is None

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_dry_run_skips_write(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLQueryOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=True,
        )
        result = op.execute(CONTEXT)
        assert result == str(TABLE_SIMPLE)
        mock_manager.return_value.write_dataframe_to_table.assert_not_called()

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_calls_write_with_append(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLQueryOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=False,
        )
        result = op.execute(CONTEXT)
        assert result == str(TABLE_SIMPLE)
        mock_manager.return_value.write_dataframe_to_table.assert_called_once()

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_pre_truncate_passed(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLQueryOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            pre_truncate=True,
        )
        op.execute(CONTEXT)
        call_args = mock_manager.return_value.write_dataframe_to_table.call_args
        assert call_args[1]['if_exists'] == 'truncate'

    def test_execute_missing_source_conn_raises(self) -> None:
        op = SQLQueryOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
        )
        with pytest.raises(ValueError, match='source_conn is required'):
            op.execute(CONTEXT)


class TestSQLAppendOperator:
    def test_instantiation(self) -> None:
        op = SQLAppendOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=True,
        )
        assert op.task_id == 'test_task'
        assert op.output_table == TABLE_SIMPLE
        assert op.is_dry_run is True

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_calls_write_append(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLAppendOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.write_dataframe_to_table.assert_called_once()
        call_args = mock_manager.return_value.write_dataframe_to_table.call_args
        assert call_args[1]['if_exists'] == 'append'

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_dry_run_skips_write(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLAppendOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=True,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.write_dataframe_to_table.assert_not_called()

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_temp_table_dropped_after_append(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLAppendOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_TEMP,
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.drop_table.assert_called_once_with(TABLE_TEMP)


class TestSQLDataFrameOperator:
    def test_instantiation(self) -> None:
        op = SQLDataFrameOperator(
            task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID
        )
        assert op.task_id == 'test_task'
        assert op.sql == SQL_SIMPLE
        assert op.source_conn == CONN_ID

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_returns_dataframe(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        expected_df = DF_SIMPLE
        mock_read_df.return_value = expected_df
        op = SQLDataFrameOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            source_conn=CONN_ID,
        )
        result = op.execute(CONTEXT)
        assert isinstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, expected_df)

    def test_execute_missing_source_conn_raises(self) -> None:
        op = SQLDataFrameOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=None)
        with pytest.raises(ValueError, match='source_conn is required'):
            op.execute(CONTEXT)


class TestSQLReplaceOperator:
    def test_instantiation(self) -> None:
        op = SQLReplaceOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=False,
        )
        assert op.task_id == 'test_task'
        assert op.output_table == TABLE_SIMPLE
        assert op.is_dry_run is False

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_calls_replace_table_content(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLReplaceOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.replace_table_content.assert_called_once()

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_dry_run_skips_replace(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLReplaceOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
            dry_run_flag=True,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.replace_table_content.assert_not_called()


class TestSQLTruncateOperator:
    def test_instantiation(self) -> None:
        op = SQLTruncateOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
        )
        assert op.task_id == 'test_task'
        assert op.output_table == TABLE_SIMPLE
        assert op.is_dry_run is False

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_calls_truncate_table_content(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLTruncateOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.truncate_table_content.assert_called_once()

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_temp_table_dropped_after_truncate(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_SIMPLE
        op = SQLTruncateOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_TEMP,
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.drop_table.assert_called_once_with(TABLE_TEMP)


class TestSQLMergeOperator:
    def test_instantiation(self) -> None:
        op = SQLMergeOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['value'],
            source_conn=CONN_ID,
            pre_truncate=True,
            dry_run_flag=True,
        )
        assert op.task_id == 'test_task'
        assert op.conflict_columns == ['id']
        assert op.update_columns == ['value']
        assert op.pre_truncate is True
        assert op.is_dry_run is True

    def test_defaults(self) -> None:
        op = SQLMergeOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
        )
        assert op.update_columns is None
        assert op.pre_truncate is False
        assert op.is_dry_run is False

    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_calls_merge_dataframe_to_table(
        self, mock_read_df: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_MERGE
        mock_manager = MagicMock()
        op = SQLMergeOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['value'],
            source_conn=CONN_ID,
        )
        op.hook_manager = mock_manager
        op.execute(CONTEXT)
        mock_manager.merge_dataframe_to_table.assert_called_once()
        call_args = mock_manager.merge_dataframe_to_table.call_args
        assert call_args[0][2] == ['id']
        assert call_args[1]['update_columns'] == ['value']

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_execute_with_pre_truncate(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_MERGE
        op = SQLMergeOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            source_conn=CONN_ID,
            pre_truncate=True,
        )
        op.execute(CONTEXT)
        assert mock_manager.return_value.truncate_table_content.called

    @patch('airsql.operators.SQLHookManager')
    @patch('airsql.operators._read_dataframe_from_hook')
    def test_temp_table_dropped_after_merge(
        self, mock_read_df: MagicMock, mock_manager: MagicMock
    ) -> None:
        mock_read_df.return_value = DF_MERGE
        op = SQLMergeOperator(
            task_id='test_task',
            sql=SQL_SIMPLE,
            output_table=TABLE_TEMP,
            conflict_columns=['id'],
            source_conn=CONN_ID,
        )
        op.execute(CONTEXT)
        mock_manager.drop_table.assert_called_once_with(TABLE_TEMP)


class TestDataFrameLoadOperator:
    def test_instantiation(self) -> None:
        op = DataFrameLoadOperator(
            task_id='test_task',
            dataframe=DF_SIMPLE,
            output_table=TABLE_SIMPLE,
            timestamp_column='updated_at',
            if_exists='replace',
            dry_run_flag=True,
        )
        assert op.task_id == 'test_task'
        assert op.dataframe is DF_SIMPLE
        assert op.output_table == TABLE_SIMPLE
        assert op.timestamp_column == 'updated_at'
        assert op.if_exists == 'replace'
        assert op.is_dry_run is True

    def test_defaults(self) -> None:
        op = DataFrameLoadOperator(
            task_id='test_task',
            dataframe=DF_SIMPLE,
            output_table=TABLE_SIMPLE,
        )
        assert op.if_exists == 'append'
        assert op.timestamp_column is None
        assert op.is_dry_run is False

    @patch('airsql.operators.SQLHookManager')
    def test_execute_calls_write_dataframe_to_table(
        self, mock_manager: MagicMock
    ) -> None:
        op = DataFrameLoadOperator(
            task_id='test_task',
            dataframe=DF_SIMPLE,
            output_table=TABLE_SIMPLE,
            if_exists='replace',
        )
        op.execute(CONTEXT)
        mock_manager.return_value.write_dataframe_to_table.assert_called_once()
        call_args = mock_manager.return_value.write_dataframe_to_table.call_args
        assert call_args[1]['if_exists'] == 'replace'

    @patch('airsql.operators.SQLHookManager')
    def test_dry_run_skips_write(self, mock_manager: MagicMock) -> None:
        op = DataFrameLoadOperator(
            task_id='test_task',
            dataframe=DF_SIMPLE,
            output_table=TABLE_SIMPLE,
            dry_run_flag=True,
        )
        op.execute(CONTEXT)
        mock_manager.return_value.write_dataframe_to_table.assert_not_called()

    @patch('airsql.operators.SQLHookManager')
    def test_temp_table_dropped_after_load(self, mock_manager: MagicMock) -> None:
        op = DataFrameLoadOperator(
            task_id='test_task',
            dataframe=DF_SIMPLE,
            output_table=TABLE_TEMP,
        )
        op.execute(CONTEXT)
        mock_manager.drop_table.assert_called_once_with(TABLE_TEMP)


class TestDataFrameMergeOperator:
    def test_instantiation(self) -> None:
        op = DataFrameMergeOperator(
            task_id='test_task',
            dataframe=DF_MERGE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['value'],
            dry_run_flag=True,
        )
        assert op.task_id == 'test_task'
        assert op.dataframe is DF_MERGE
        assert op.conflict_columns == ['id']
        assert op.update_columns == ['value']
        assert op.is_dry_run is True

    def test_defaults(self) -> None:
        op = DataFrameMergeOperator(
            task_id='test_task',
            dataframe=DF_MERGE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
        )
        assert op.update_columns is None
        assert op.pre_truncate is False
        assert op.is_dry_run is False

    @patch('airsql.operators.SQLHookManager')
    def test_execute_calls_merge_dataframe_to_table(
        self, mock_manager: MagicMock
    ) -> None:
        op = DataFrameMergeOperator(
            task_id='test_task',
            dataframe=DF_MERGE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['name', 'value'],
        )
        op.execute(CONTEXT)
        mock_manager.return_value.merge_dataframe_to_table.assert_called_once()
        call_args = mock_manager.return_value.merge_dataframe_to_table.call_args
        assert call_args[1]['conflict_columns'] == ['id']
        assert call_args[1]['update_columns'] == ['name', 'value']

    @patch('airsql.operators.SQLHookManager')
    def test_execute_with_pre_truncate(self, mock_manager: MagicMock) -> None:
        op = DataFrameMergeOperator(
            task_id='test_task',
            dataframe=DF_MERGE,
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            pre_truncate=True,
        )
        op.execute(CONTEXT)
        assert mock_manager.return_value.truncate_table_content.called

    @patch('airsql.operators.SQLHookManager')
    def test_temp_table_dropped_after_merge(self, mock_manager: MagicMock) -> None:
        op = DataFrameMergeOperator(
            task_id='test_task',
            dataframe=DF_MERGE,
            output_table=TABLE_TEMP,
            conflict_columns=['id'],
        )
        op.execute(CONTEXT)
        mock_manager.drop_table.assert_called_once_with(TABLE_TEMP)


class TestSQLCheckOperator:
    def test_instantiation(self) -> None:
        op = SQLCheckOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID)
        assert op.task_id == 'test_task'
        assert op.sql == SQL_SIMPLE
        assert op.conn_id == CONN_ID

    def test_retries_default(self) -> None:
        op = SQLCheckOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID)
        assert op.retries == 1

    def test_source_conn_maps_to_conn_id(self) -> None:
        op = SQLCheckOperator(task_id='test_task', sql=SQL_SIMPLE, source_conn=CONN_ID)
        assert op.conn_id == CONN_ID


class TestReadDataframeFromHook:
    def test_bigquery_converts_pyarrow_to_numpy_dtypes(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'BigQueryHook'

        import pyarrow as pa

        df_with_pyarrow = pd.DataFrame({
            'id': [1, 2, 3],
            'date_col': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'value': [10.5, 20.3, 30.1],
        })
        df_with_pyarrow = df_with_pyarrow.astype({
            'id': pd.ArrowDtype(pa.int64()),
            'date_col': pd.ArrowDtype(pa.timestamp('ns')),
            'value': pd.ArrowDtype(pa.float64()),
        })

        mock_hook.get_pandas_df.return_value = df_with_pyarrow

        result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

        for col in result.columns:
            assert not isinstance(result[col].dtype, pd.ArrowDtype), (
                f'Column {col} still has ArrowDtype'
            )

    def test_postgres_converts_pyarrow_to_numpy_dtypes(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_pyarrow = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
        })
        df_with_pyarrow = df_with_pyarrow.astype({
            'id': pd.ArrowDtype(pa.int64()),
            'name': pd.ArrowDtype(pa.string()),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_pyarrow

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            for col in result.columns:
                assert not isinstance(result[col].dtype, pd.ArrowDtype), (
                    f'Column {col} still has ArrowDtype'
                )

    def test_bigquery_converts_dbdate_to_object_dtype(self) -> None:
        from datetime import date

        import db_dtypes

        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'BigQueryHook'

        df_with_dbdate = pd.DataFrame({
            'id': [1, 2, 3],
            'date_col': pd.Series(
                [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
                dtype=db_dtypes.DateDtype(),
            ),
        })

        mock_hook.get_pandas_df.return_value = df_with_dbdate

        result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

        assert str(result['date_col'].dtype) == 'object', (
            f'Expected object dtype, got {result["date_col"].dtype}'
        )
        assert result['date_col'].tolist() == [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]

    def test_bigquery_converts_dbtime_to_object_dtype(self) -> None:
        from datetime import time

        import db_dtypes

        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'BigQueryHook'

        df_with_dbtime = pd.DataFrame({
            'id': [1, 2, 3],
            'time_col': pd.Series(
                [time(10, 30, 0), time(11, 45, 0), time(14, 0, 0)],
                dtype=db_dtypes.TimeDtype(),
            ),
        })

        mock_hook.get_pandas_df.return_value = df_with_dbtime

        result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

        assert str(result['time_col'].dtype) == 'object', (
            f'Expected object dtype, got {result["time_col"].dtype}'
        )
        assert result['time_col'].tolist() == [
            time(10, 30, 0),
            time(11, 45, 0),
            time(14, 0, 0),
        ]

    def test_bigquery_handles_empty_dataframe(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'BigQueryHook'

        empty_df = pd.DataFrame()
        mock_hook.get_pandas_df.return_value = empty_df

        result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table WHERE 1=0')

        assert isinstance(result, pd.DataFrame)

    def test_pyarrow_int_with_nulls_converts_to_Int64(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'id': pd.array([1, 2, None], dtype=pd.ArrowDtype(pa.int64())),
            'value': pd.array([10, None, 30], dtype=pd.ArrowDtype(pa.int64())),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['id'].dtype) == 'Int64'
            assert str(result['value'].dtype) == 'Int64'
            assert result['id'].isna().sum() == 1
            assert result['value'].isna().sum() == 1

    def test_pyarrow_float_with_nulls_converts_to_Float64(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'price': pd.array([1.5, None, 3.7], dtype=pd.ArrowDtype(pa.float64())),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['price'].dtype) == 'Float64'
            assert result['price'].isna().sum() == 1

    def test_pyarrow_string_with_nulls_converts_to_string(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'name': pd.array(
                ['alice', None, 'charlie'], dtype=pd.ArrowDtype(pa.string())
            ),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert isinstance(result['name'].dtype, pd.StringDtype)
            assert result['name'].isna().sum() == 1

    def test_pyarrow_timestamp_with_nulls_converts_to_datetime64(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'ts': pd.array(
                [pd.Timestamp('2024-01-01'), None, pd.Timestamp('2024-01-03')],
                dtype=pd.ArrowDtype(pa.timestamp('ns')),
            ),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['ts'].dtype) == 'datetime64[ns]'
            assert result['ts'].isna().sum() == 1

    def test_pyarrow_date_with_nulls_converts_to_object(self) -> None:
        from datetime import date

        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'dt': pd.array(
                [date(2024, 1, 1), None, date(2024, 1, 3)],
                dtype=pd.ArrowDtype(pa.date32()),
            ),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['dt'].dtype) == 'object'
            assert result['dt'].isna().sum() == 1

    def test_pyarrow_bool_with_nulls_converts_to_boolean(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'flag': pd.array([True, None, False], dtype=pd.ArrowDtype(pa.bool_())),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['flag'].dtype) == 'boolean'
            assert result['flag'].isna().sum() == 1

    def test_pyarrow_int32_with_nulls_converts_to_Int64(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'id': pd.array([1, None, 3], dtype=pd.ArrowDtype(pa.int32())),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['id'].dtype) == 'Int64'
            assert result['id'].isna().sum() == 1

    def test_pyarrow_float32_with_nulls_converts_to_Float64(self) -> None:
        from airsql.operators import _read_dataframe_from_hook

        mock_hook = MagicMock()
        mock_hook.__class__.__name__ = 'PostgresHook'
        mock_hook.get_sqlalchemy_engine.side_effect = NotImplementedError()

        mock_conn = MagicMock()
        import pyarrow as pa

        df_with_nulls = pd.DataFrame({
            'value': pd.array([1.5, None, 3.7], dtype=pd.ArrowDtype(pa.float32())),
        })

        mock_conn.close = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        with patch('airsql.operators.pd.read_sql') as mock_read_sql:
            mock_read_sql.return_value = df_with_nulls

            result = _read_dataframe_from_hook(mock_hook, 'SELECT * FROM table')

            assert str(result['value'].dtype) == 'Float64'
            assert result['value'].isna().sum() == 1
