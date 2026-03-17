import pandas as pd
import pytest

from airsql.decorators import SQLDecorators
from airsql.operators import (
    DataFrameLoadOperator,
    DataFrameMergeOperator,
    SQLAppendOperator,
    SQLCheckOperator,
    SQLMergeOperator,
    SQLQueryOperator,
    SQLReplaceOperator,
    SQLTruncateOperator,
)
from tests.fixtures.data import (
    CONN_ID,
    DF_MERGE,
    DF_SIMPLE,
    TABLE_SIMPLE,
)

sql = SQLDecorators()


class TestQueryDecorator:  # noqa: PLR0904
    def test_is_callable(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_query() -> str:
            return 'SELECT 1'

        assert callable(my_query)

    def test_returns_operator(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_query() -> str:
            return 'SELECT 1'

        op = my_query()
        assert isinstance(op, SQLQueryOperator)
        assert op.task_id == 'my_query'
        assert op.source_conn == CONN_ID
        assert op.output_table == TABLE_SIMPLE

    def test_pre_truncate(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID, pre_truncate=True)
        def my_query() -> str:
            return 'SELECT 1'

        op = my_query()
        assert op.pre_truncate is True

    def test_dry_run(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID, dry_run=True)
        def my_query() -> str:
            return 'SELECT 1'

        op = my_query()
        assert op.is_dry_run is True

    def test_missing_output_table_raises(self) -> None:
        @sql.query(source_conn=CONN_ID)
        def my_query() -> str:
            return 'SELECT 1'

        with pytest.raises(ValueError, match='output_table is required'):
            my_query()

    def test_jinja_template_rendering(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID, table_name='users')
        def my_query() -> str:
            return 'SELECT * FROM {{ table_name }}'

        op = my_query()
        assert op.sql == 'SELECT * FROM users'

    def test_jinja_template_from_func_args(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_query(table_name: str) -> str:
            return 'SELECT * FROM {{ table_name }}'

        op = my_query(table_name='orders')
        assert op.sql == 'SELECT * FROM orders'

    def test_template_vars_passed_to_operator(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID, region='us-east')
        def my_query() -> str:
            return 'SELECT 1'

        op = my_query()
        assert hasattr(op, 'region')
        assert op.region == 'us-east'

    def test_jinja_conditional_with_none_param(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_query(dominio: str | None) -> str:
            return """
                SELECT * FROM users
                WHERE active = true
                {% if dominio %}
                AND dominio = '{{ dominio }}'
                {% endif %}
                ORDER BY id
            """

        op_no_filter = my_query(dominio=None)
        assert 'AND dominio' not in op_no_filter.sql
        assert 'WHERE active = true' in op_no_filter.sql

        op_with_filter = my_query(dominio='instagram')
        assert "AND dominio = 'instagram'" in op_with_filter.sql

    def test_jinja_conditional_with_empty_string(self) -> None:
        @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_query(status: str | None) -> str:
            return """
                SELECT * FROM users
                WHERE active = true
                {% if status %}
                AND status = '{{ status }}'
                {% endif %}
            """

        op_no_filter = my_query(status=None)
        assert 'AND status' not in op_no_filter.sql

        op_empty = my_query(status='')
        assert 'AND status' not in op_empty.sql

        op_with_filter = my_query(status='pending')
        assert "status = 'pending'" in op_with_filter.sql

    def test_airflow_params_template_rendering(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'test_user', 'id_inventario': '123'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None, id_inventario: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                    {% if id_inventario %}
                    AND id = {{ id_inventario }}
                    {% endif %}
                """

            op = my_query(
                username='{{ params.username }}',
                id_inventario='{{ params.id_inventario }}',
            )
            assert "username = 'test_user'" in op.sql
            assert 'AND id = 123' in op.sql

    def test_airflow_params_with_none_value(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': None, 'id_inventario': None}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None, id_inventario: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                    {% if id_inventario %}
                    AND id = {{ id_inventario }}
                    {% endif %}
                """

            op = my_query(
                username='{{ params.username }}',
                id_inventario='{{ params.id_inventario }}',
            )
            assert 'AND username' not in op.sql
            assert 'AND id' not in op.sql

    def test_airflow_params_with_empty_string(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': '', 'id_inventario': ''}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None, id_inventario: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                    {% if id_inventario %}
                    AND id = {{ id_inventario }}
                    {% endif %}
                """

            op = my_query(
                username='{{ params.username }}',
                id_inventario='{{ params.id_inventario }}',
            )
            assert 'AND username' not in op.sql
            assert 'AND id' not in op.sql

    def test_airflow_params_mixed_values(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'instagram', 'id_inventario': None}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None, id_inventario: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                    {% if id_inventario %}
                    AND id = {{ id_inventario }}
                    {% endif %}
                """

            op = my_query(
                username='{{ params.username }}',
                id_inventario='{{ params.id_inventario }}',
            )
            assert "username = 'instagram'" in op.sql
            assert 'AND id' not in op.sql

    def test_airflow_ds_template(self) -> None:
        from unittest.mock import patch

        mock_context = {'ds': '2025-01-15', 'params': {}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(execution_date: str) -> str:
                return """
                    SELECT * FROM events
                    WHERE event_date = '{{ execution_date }}'
                """

            op = my_query(execution_date='{{ ds }}')
            assert "event_date = '2025-01-15'" in op.sql

    def test_airflow_params_with_list_values(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'canais': 'instagram,facebook,twitter'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(canais: str) -> str:
                return """
                    SELECT * FROM users
                    WHERE canal IN ({{ canais }})
                """

            op = my_query(canais='{{ params.canais }}')
            assert 'canal IN (instagram,facebook,twitter)' in op.sql

    def test_airflow_params_with_dict_values(self) -> None:
        from unittest.mock import patch

        mock_context = {
            'params': {'filters': {'status': 'active', 'region': 'us-east'}}
        }

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(status: str, region: str) -> str:
                return """
                    SELECT * FROM users
                    WHERE status = '{{ status }}'
                    AND region = '{{ region }}'
                """

            op = my_query(
                status='{{ params.filters.status }}',
                region='{{ params.filters.region }}',
            )
            assert "status = 'active'" in op.sql
            assert "region = 'us-east'" in op.sql

    def test_airflow_params_static_value_overrides_template(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'from_params'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str) -> str:
                return """
                    SELECT * FROM users
                    WHERE username = '{{ username }}'
                """

            op = my_query(username='static_value')
            assert "username = 'static_value'" in op.sql

    def test_airflow_params_missing_key_returns_empty_string(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                """

            op = my_query(username='{{ params.username }}')
            assert 'AND username' not in op.sql

    def test_airflow_params_direct_access_in_template(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'instagram', 'id_inventario': '123'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query() -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if params.username %}
                    AND username = '{{ params.username }}'
                    {% endif %}
                    {% if params.id_inventario %}
                    AND id = {{ params.id_inventario }}
                    {% endif %}
                """

            op = my_query()
            assert "username = 'instagram'" in op.sql
            assert 'AND id = 123' in op.sql

    def test_airflow_params_direct_access_with_none(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': None, 'id_inventario': None}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query() -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if params.username %}
                    AND username = '{{ params.username }}'
                    {% endif %}
                    {% if params.id_inventario %}
                    AND id = {{ params.id_inventario }}
                    {% endif %}
                """

            op = my_query()
            assert 'AND username' not in op.sql
            assert 'AND id' not in op.sql

    def test_airflow_params_string_none_sanitized(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'None', 'id_inventario': 'None'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query() -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if params.username %}
                    AND username = '{{ params.username }}'
                    {% endif %}
                    {% if params.id_inventario %}
                    AND id = {{ params.id_inventario }}
                    {% endif %}
                """

            op = my_query()
            assert 'AND username' not in op.sql
            assert 'AND id' not in op.sql

    def test_airflow_params_via_args_string_none_sanitized(self) -> None:
        from unittest.mock import patch

        mock_context = {'params': {'username': 'None', 'id_inventario': 'None'}}

        with patch('airsql.decorators.get_current_context', return_value=mock_context):

            @sql.query(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
            def my_query(username: str | None, id_inventario: str | None) -> str:
                return """
                    SELECT * FROM users
                    WHERE active = true
                    {% if username %}
                    AND username = '{{ username }}'
                    {% endif %}
                    {% if id_inventario %}
                    AND id = {{ id_inventario }}
                    {% endif %}
                """

            op = my_query(
                username='{{ params.username }}',
                id_inventario='{{ params.id_inventario }}',
            )
            assert 'AND username' not in op.sql
            assert 'AND id' not in op.sql


class TestAppendDecorator:
    def test_is_callable(self) -> None:
        @sql.append(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_append() -> str:
            return 'SELECT 1'

        assert callable(my_append)

    def test_returns_operator(self) -> None:
        @sql.append(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_append() -> str:
            return 'SELECT 1'

        op = my_append()
        assert isinstance(op, SQLAppendOperator)
        assert op.task_id == 'my_append'
        assert op.output_table == TABLE_SIMPLE

    def test_dry_run(self) -> None:
        @sql.append(output_table=TABLE_SIMPLE, source_conn=CONN_ID, dry_run=True)
        def my_append() -> str:
            return 'SELECT 1'

        op = my_append()
        assert op.is_dry_run is True

    def test_jinja_template_rendering(self) -> None:
        @sql.append(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_append(status: str) -> str:
            return "SELECT * FROM orders WHERE status = '{{ status }}'"

        op = my_append(status='completed')
        assert op.sql == "SELECT * FROM orders WHERE status = 'completed'"


class TestReplaceDecorator:
    def test_is_callable(self) -> None:
        @sql.replace(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_replace() -> str:
            return 'SELECT 1'

        assert callable(my_replace)

    def test_returns_operator(self) -> None:
        @sql.replace(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_replace() -> str:
            return 'SELECT 1'

        op = my_replace()
        assert isinstance(op, SQLReplaceOperator)
        assert op.task_id == 'my_replace'
        assert op.output_table == TABLE_SIMPLE

    def test_method_truncate_returns_truncate_operator(self) -> None:
        @sql.replace(output_table=TABLE_SIMPLE, source_conn=CONN_ID, method='truncate')
        def my_replace() -> str:
            return 'SELECT 1'

        op = my_replace()
        assert isinstance(op, SQLTruncateOperator)

    def test_dry_run(self) -> None:
        @sql.replace(output_table=TABLE_SIMPLE, source_conn=CONN_ID, dry_run=True)
        def my_replace() -> str:
            return 'SELECT 1'

        op = my_replace()
        assert op.is_dry_run is True


class TestTruncateDecorator:
    def test_is_callable(self) -> None:
        @sql.truncate(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_truncate() -> str:
            return 'SELECT 1'

        assert callable(my_truncate)

    def test_returns_operator(self) -> None:
        @sql.truncate(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_truncate() -> str:
            return 'SELECT 1'

        op = my_truncate()
        assert isinstance(op, SQLTruncateOperator)
        assert op.task_id == 'my_truncate'
        assert op.output_table == TABLE_SIMPLE


class TestMergeDecorator:
    def test_is_callable(self) -> None:
        @sql.merge(
            output_table=TABLE_SIMPLE, conflict_columns=['id'], source_conn=CONN_ID
        )
        def my_merge() -> str:
            return 'SELECT 1'

        assert callable(my_merge)

    def test_returns_operator(self) -> None:
        @sql.merge(
            output_table=TABLE_SIMPLE, conflict_columns=['id'], source_conn=CONN_ID
        )
        def my_merge() -> str:
            return 'SELECT 1'

        op = my_merge()
        assert isinstance(op, SQLMergeOperator)
        assert op.task_id == 'my_merge'
        assert op.conflict_columns == ['id']

    def test_with_update_columns(self) -> None:
        @sql.merge(
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['name', 'value'],
            source_conn=CONN_ID,
        )
        def my_merge() -> str:
            return 'SELECT 1'

        op = my_merge()
        assert op.update_columns == ['name', 'value']

    def test_pre_truncate(self) -> None:
        @sql.merge(
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            source_conn=CONN_ID,
            pre_truncate=True,
        )
        def my_merge() -> str:
            return 'SELECT 1'

        op = my_merge()
        assert op.pre_truncate is True


class TestLoadDataframeDecorator:
    def test_is_callable(self) -> None:
        @sql.load_dataframe(output_table=TABLE_SIMPLE, dataframe=DF_SIMPLE)
        def load_data() -> None:
            pass

        assert callable(load_data)

    def test_returns_operator_with_provided_df(self) -> None:
        @sql.load_dataframe(output_table=TABLE_SIMPLE, dataframe=DF_SIMPLE)
        def load_data() -> None:
            pass

        op = load_data()
        assert isinstance(op, DataFrameLoadOperator)
        assert op.task_id == 'load_data'
        assert op.output_table == TABLE_SIMPLE

    def test_returns_operator_from_func(self) -> None:
        @sql.load_dataframe(output_table=TABLE_SIMPLE)
        def load_data() -> pd.DataFrame:
            return DF_SIMPLE

        op = load_data()
        assert isinstance(op, DataFrameLoadOperator)
        pd.testing.assert_frame_equal(op.dataframe, DF_SIMPLE)

    def test_dry_run(self) -> None:
        @sql.load_dataframe(
            output_table=TABLE_SIMPLE, dataframe=DF_SIMPLE, dry_run=True
        )
        def load_data() -> None:
            pass

        op = load_data()
        assert op.is_dry_run is True

    def test_if_exists_parameter(self) -> None:
        @sql.load_dataframe(
            output_table=TABLE_SIMPLE, dataframe=DF_SIMPLE, if_exists='replace'
        )
        def load_data() -> None:
            pass

        op = load_data()
        assert op.if_exists == 'replace'

    def test_non_dataframe_raises(self) -> None:
        @sql.load_dataframe(output_table=TABLE_SIMPLE)
        def load_data() -> str:
            return 'not a dataframe'

        with pytest.raises(ValueError, match='must return a pandas DataFrame'):
            load_data()


class TestMergeDataframeDecorator:
    def test_is_callable(self) -> None:
        @sql.merge_dataframe(
            output_table=TABLE_SIMPLE, conflict_columns=['id'], dataframe=DF_MERGE
        )
        def merge_data() -> None:
            pass

        assert callable(merge_data)

    def test_returns_operator_with_provided_df(self) -> None:
        @sql.merge_dataframe(
            output_table=TABLE_SIMPLE, conflict_columns=['id'], dataframe=DF_MERGE
        )
        def merge_data() -> None:
            pass

        op = merge_data()
        assert isinstance(op, DataFrameMergeOperator)
        assert op.task_id == 'merge_data'
        assert op.conflict_columns == ['id']

    def test_returns_operator_from_func(self) -> None:
        @sql.merge_dataframe(output_table=TABLE_SIMPLE, conflict_columns=['id'])
        def merge_data() -> pd.DataFrame:
            return DF_MERGE

        op = merge_data()
        assert isinstance(op, DataFrameMergeOperator)
        pd.testing.assert_frame_equal(op.dataframe, DF_MERGE)

    def test_with_update_columns(self) -> None:
        @sql.merge_dataframe(
            output_table=TABLE_SIMPLE,
            conflict_columns=['id'],
            update_columns=['name'],
            dataframe=DF_MERGE,
        )
        def merge_data() -> None:
            pass

        op = merge_data()
        assert op.update_columns == ['name']


class TestCheckDecorator:
    def test_is_callable(self) -> None:
        @sql.check(source_conn=CONN_ID)
        def my_check() -> str:
            return 'SELECT COUNT(*) FROM public.test WHERE id IS NULL'

        assert callable(my_check)

    def test_returns_operator(self) -> None:
        @sql.check(source_conn=CONN_ID)
        def my_check() -> str:
            return 'SELECT COUNT(*) FROM public.test WHERE id IS NULL'

        op = my_check()
        assert isinstance(op, SQLCheckOperator)
        assert op.task_id == 'my_check'
        assert op.conn_id == CONN_ID

    def test_conn_id_parameter(self) -> None:
        @sql.check(conn_id='my_bigquery_conn')
        def my_check() -> str:
            return 'SELECT 1'

        op = my_check()
        assert op.conn_id == 'my_bigquery_conn'

    def test_jinja_template_rendering(self) -> None:
        @sql.check(source_conn=CONN_ID)
        def my_check(table_name: str) -> str:
            return 'SELECT COUNT(*) FROM {{ table_name }} WHERE id IS NULL'

        op = my_check(table_name='users')
        assert op.sql == 'SELECT COUNT(*) FROM users WHERE id IS NULL'


class TestDdlDecorator:
    def test_is_callable(self) -> None:
        @sql.ddl(source_conn=CONN_ID)
        def my_ddl() -> str:
            return 'CREATE OR REPLACE VIEW my_view AS SELECT 1'

        assert callable(my_ddl)

    def test_returns_operator(self) -> None:
        @sql.ddl(source_conn=CONN_ID)
        def my_ddl() -> str:
            return 'CREATE OR REPLACE VIEW my_view AS SELECT 1'

        op = my_ddl()
        assert isinstance(op, SQLQueryOperator)
        assert op.task_id == 'my_ddl'
        assert op.source_conn == CONN_ID

    def test_with_output_table(self) -> None:
        @sql.ddl(output_table=TABLE_SIMPLE, source_conn=CONN_ID)
        def my_ddl() -> str:
            return 'CREATE OR REPLACE VIEW my_view AS SELECT 1'

        op = my_ddl()
        assert op.output_table is None


class TestSqlFileParameter:
    def test_missing_sql_file_raises(self) -> None:
        @sql.query(
            output_table=TABLE_SIMPLE, source_conn=CONN_ID, sql_file='nonexistent.sql'
        )
        def my_query() -> str:
            return 'SELECT 1'

        with pytest.raises(FileNotFoundError):
            my_query()
