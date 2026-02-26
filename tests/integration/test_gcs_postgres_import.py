from io import BytesIO

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

pytestmark = pytest.mark.integration


@pytest.fixture(scope='module')
def postgres_container():
    """Start a PostgreSQL container for integration tests."""
    container = PostgresContainer('postgres:16')
    container.start()
    yield container
    container.stop()


@pytest.fixture
def pg_engine(postgres_container):
    """Get SQLAlchemy engine for direct database operations."""
    url = postgres_container.get_connection_url()
    engine = create_engine(url)
    yield engine
    engine.dispose()


@pytest.fixture
def real_postgres_hook(postgres_container, pg_engine):
    """Create a real PostgresHook that uses the test container."""

    class RealPostgresHook:
        def __init__(self, postgres_conn_id):
            self.postgres_conn_id = postgres_conn_id
            self._connection_url = postgres_container.get_connection_url().replace(
                '+psycopg2', ''
            )

        def get_conn(self):
            import psycopg

            return psycopg.connect(self._connection_url)

        def get_sqlalchemy_engine(self):
            return pg_engine

        def get_records(self, sql, parameters=None):
            import psycopg

            conn = psycopg.connect(self._connection_url)
            try:
                cursor = conn.cursor()
                if parameters:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql)
                results = cursor.fetchall()
                cursor.close()
                return results
            finally:
                conn.close()

        def run(self, sql, parameters=None):
            import psycopg

            conn = psycopg.connect(self._connection_url)
            try:
                cursor = conn.cursor()
                if parameters:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql)
                conn.commit()
                cursor.close()
            finally:
                conn.close()

    return RealPostgresHook


class TestImportCSV:
    """Tests for importing CSV files into PostgreSQL."""

    def test_import_integer_types(self, pg_engine, real_postgres_hook):
        """Test importing INTEGER columns from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_integers;
                CREATE TABLE import_integers (
                    id INTEGER PRIMARY KEY,
                    small_val SMALLINT,
                    int_val INTEGER,
                    big_val BIGINT
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,small_val,int_val,big_val
1,100,100000,9223372036854775807
2,-50,-50000,-9223372036854775808
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_integers',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_integers ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][1] == 100
            assert rows[0][2] == 100000
            assert rows[0][3] == 9223372036854775807

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_integers'))
                conn.commit()

    def test_import_decimal_float_types(self, pg_engine, real_postgres_hook):
        """Test importing DECIMAL and DOUBLE PRECISION from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_decimals;
                CREATE TABLE import_decimals (
                    id INTEGER PRIMARY KEY,
                    decimal_val DECIMAL(10, 4),
                    float_val DOUBLE PRECISION
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,decimal_val,float_val
1,12345.6789,3.14159265358979
2,-999.9999,-2.71828
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_decimals',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_decimals ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert float(rows[0][1]) == 12345.6789
            assert abs(rows[0][2] - 3.14159265358979) < 0.0001

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_decimals'))
                conn.commit()

    def test_import_text_types(self, pg_engine, real_postgres_hook):
        """Test importing TEXT and VARCHAR from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_text;
                CREATE TABLE import_text (
                    id INTEGER PRIMARY KEY,
                    text_val TEXT,
                    varchar_val VARCHAR(50)
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,text_val,varchar_val
1,Hello World,varchar test
2,Unicode: \xc3\xa9\xc3\xa0\xc3\xbc,special!@#
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_text',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_text ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][1] == 'Hello World'
            assert 'Unicode' in rows[1][1]

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_text'))
                conn.commit()

    def test_import_boolean_type(self, pg_engine, real_postgres_hook):
        """Test importing BOOLEAN from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_boolean;
                CREATE TABLE import_boolean (
                    id INTEGER PRIMARY KEY,
                    bool_val BOOLEAN
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,bool_val
1,True
2,False
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_boolean',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_boolean ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][1] is True
            assert rows[1][1] is False

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_boolean'))
                conn.commit()

    def test_import_date_types(self, pg_engine, real_postgres_hook):
        """Test importing DATE and TIMESTAMP from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_dates;
                CREATE TABLE import_dates (
                    id INTEGER PRIMARY KEY,
                    date_val DATE,
                    timestamp_val TIMESTAMP
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,date_val,timestamp_val
1,2024-01-15,2024-01-15 10:30:45
2,2024-12-31,2024-12-31 23:59:59
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_dates',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_dates ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert '2024-01-15' in str(rows[0][1])
            assert '2024-01-15' in str(rows[0][2])

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_dates'))
                conn.commit()

    def test_import_uuid_type(self, pg_engine, real_postgres_hook):
        """Test importing UUID from CSV."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_uuid;
                CREATE TABLE import_uuid (
                    id INTEGER PRIMARY KEY,
                    uuid_val UUID
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,uuid_val
1,550e8400-e29b-41d4-a716-446655440000
2,00000000-0000-0000-0000-000000000000
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_uuid',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_uuid ORDER BY id'))
                rows = list(result)

            assert len(rows) == 2
            assert '550e8400' in str(rows[0][1])

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_uuid'))
                conn.commit()

    def test_import_with_nulls(self, pg_engine, real_postgres_hook):
        """Test importing CSV with NULL values."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_nulls;
                CREATE TABLE import_nulls (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                );
            """)
            )
            conn.commit()

        try:
            csv_data = b"""id,name,value
1,Alice,100
2,,
3,Charlie,300
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_nulls',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM import_nulls ORDER BY id'))
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][1] == 'Alice'
            assert rows[1][1] is None
            assert rows[1][2] is None

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_nulls'))
                conn.commit()


class TestImportJSONL:
    """Tests for importing JSONL files into PostgreSQL."""

    def test_import_basic_jsonl(self, pg_engine, real_postgres_hook):
        """Test importing a basic JSONL file."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_jsonl_basic;
                CREATE TABLE import_jsonl_basic (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value DOUBLE PRECISION
                );
            """)
            )
            conn.commit()

        try:
            jsonl_data = b"""{"id": 1, "name": "Alice", "value": 100.5}
{"id": 2, "name": "Bob", "value": 200.75}
{"id": 3, "name": "Charlie", "value": 300.0}
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_jsonl_basic',
                bucket_name='test-bucket',
                object_name='data.jsonl',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = jsonl_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM import_jsonl_basic ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][1] == 'Alice'
            assert rows[1][1] == 'Bob'

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_jsonl_basic'))
                conn.commit()

    def test_import_jsonl_with_json_column(self, pg_engine, real_postgres_hook):
        """Test importing JSONL with JSON/JSONB columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_jsonl_json;
                CREATE TABLE import_jsonl_json (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    metadata JSONB
                );
            """)
            )
            conn.commit()

        try:
            jsonl_data = b"""{"id": 1, "name": "item1", "metadata": {"key": "value", "count": 42}}
{"id": 2, "name": "item2", "metadata": {"nested": {"deep": true}}}
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_jsonl_json',
                bucket_name='test-bucket',
                object_name='data.jsonl',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = jsonl_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM import_jsonl_json ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][1] == 'item1'
            assert rows[0][2]['key'] == 'value'
            assert rows[0][2]['count'] == 42

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_jsonl_json'))
                conn.commit()

    def test_import_jsonl_with_json_array(self, pg_engine, real_postgres_hook):
        """Test importing JSONL with JSON arrays."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_jsonl_array;
                CREATE TABLE import_jsonl_array (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    tags JSONB
                );
            """)
            )
            conn.commit()

        try:
            jsonl_data = b"""{"id": 1, "name": "item1", "tags": ["tag1", "tag2", "tag3"]}
{"id": 2, "name": "item2", "tags": [1, 2, 3]}
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_jsonl_array',
                bucket_name='test-bucket',
                object_name='data.jsonl',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = jsonl_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM import_jsonl_array ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][2] == ['tag1', 'tag2', 'tag3']
            assert rows[1][2] == [1, 2, 3]

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_jsonl_array'))
                conn.commit()


class TestImportParquet:
    """Tests for importing Parquet files into PostgreSQL."""

    def test_import_basic_parquet(self, pg_engine, real_postgres_hook):
        """Test importing a basic Parquet file."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_parquet_basic;
                CREATE TABLE import_parquet_basic (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value DOUBLE PRECISION,
                    active BOOLEAN
                );
            """)
            )
            conn.commit()

        try:
            df = pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie'],
                'value': [100.5, 200.75, 300.0],
                'active': [True, False, True],
            })

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_data = buffer.getvalue()

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_parquet_basic',
                bucket_name='test-bucket',
                object_name='data.parquet',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = parquet_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM import_parquet_basic ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][1] == 'Alice'
            assert rows[0][3] is True

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_parquet_basic'))
                conn.commit()

    def test_import_parquet_with_nulls(self, pg_engine, real_postgres_hook):
        """Test importing Parquet with NULL values."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS import_parquet_nulls;
                CREATE TABLE import_parquet_nulls (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value DOUBLE PRECISION
                );
            """)
            )
            conn.commit()

        try:
            df = pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Alice', None, 'Charlie'],
                'value': [100.5, 200.75, None],
            })

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_data = buffer.getvalue()

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_import',
                target_table_name='import_parquet_nulls',
                bucket_name='test-bucket',
                object_name='data.parquet',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                replace=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = parquet_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM import_parquet_nulls ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][1] == 'Alice'
            assert rows[1][1] is None
            assert rows[2][2] is None

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS import_parquet_nulls'))
                conn.commit()


class TestImportUpsert:
    """Tests for upsert operations."""

    @pytest.fixture(autouse=True)
    def setup_table(self, pg_engine):
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS upsert_test;
                CREATE TABLE upsert_test (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                );
            """)
            )
            conn.commit()
        yield
        with pg_engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS upsert_test'))
            conn.commit()

    def test_upsert_inserts_new_rows(self, pg_engine, real_postgres_hook):
        """Test upsert inserts rows that don't exist."""
        csv_data = b"""id,name,value
1,Alice,100
2,Bob,200
"""

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        op = GCSToPostgresOperator(
            task_id='test_upsert',
            target_table_name='upsert_test',
            bucket_name='test-bucket',
            object_name='data.csv',
            postgres_conn_id='postgres_default',
            gcp_conn_id='NOT_USED',
            conflict_columns=['id'],
            replace=False,
        )

        from unittest.mock import MagicMock, patch

        mock_gcs = MagicMock()
        mock_gcs.download.return_value = csv_data

        with (
            patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
            patch(
                'airsql.transfers.gcs_postgres.PostgresHook',
                return_value=real_postgres_hook('postgres_default'),
            ),
        ):
            op.execute({})

        with pg_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM upsert_test'))
            count = result.scalar()

        assert count == 2

    def test_upsert_updates_existing_rows(self, pg_engine, real_postgres_hook):
        """Test upsert updates rows that already exist."""
        with pg_engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO upsert_test (id, name, value) VALUES (1, 'Old Name', 50)"
                )
            )
            conn.commit()

        csv_data = b"""id,name,value
1,New Name,100
2,Bob,200
"""

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        op = GCSToPostgresOperator(
            task_id='test_upsert',
            target_table_name='upsert_test',
            bucket_name='test-bucket',
            object_name='data.csv',
            postgres_conn_id='postgres_default',
            gcp_conn_id='NOT_USED',
            conflict_columns=['id'],
            replace=False,
        )

        from unittest.mock import MagicMock, patch

        mock_gcs = MagicMock()
        mock_gcs.download.return_value = csv_data

        with (
            patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
            patch(
                'airsql.transfers.gcs_postgres.PostgresHook',
                return_value=real_postgres_hook('postgres_default'),
            ),
        ):
            op.execute({})

        with pg_engine.connect() as conn:
            result = conn.execute(
                text('SELECT name, value FROM upsert_test WHERE id = 1')
            )
            row = result.fetchone()

        assert row[0] == 'New Name'
        assert row[1] == 100


class TestImportReplace:
    """Tests for replace (truncate + insert) operations."""

    @pytest.fixture(autouse=True)
    def setup_table(self, pg_engine):
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS replace_test;
                CREATE TABLE replace_test (
                    id INTEGER,
                    name TEXT
                );
            """)
            )
            conn.commit()
        yield
        with pg_engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS replace_test'))
            conn.commit()

    def test_replace_truncates_before_insert(self, pg_engine, real_postgres_hook):
        """Test replace mode truncates existing data."""
        with pg_engine.connect() as conn:
            conn.execute(text("INSERT INTO replace_test VALUES (999, 'old data')"))
            conn.commit()

        csv_data = b"""id,name
1,Alice
2,Bob
"""

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        op = GCSToPostgresOperator(
            task_id='test_replace',
            target_table_name='replace_test',
            bucket_name='test-bucket',
            object_name='data.csv',
            postgres_conn_id='postgres_default',
            gcp_conn_id='NOT_USED',
            replace=True,
        )

        from unittest.mock import MagicMock, patch

        mock_gcs = MagicMock()
        mock_gcs.download.return_value = csv_data

        with (
            patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
            patch(
                'airsql.transfers.gcs_postgres.PostgresHook',
                return_value=real_postgres_hook('postgres_default'),
            ),
        ):
            op.execute({})

        with pg_engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM replace_test'))
            count = result.scalar()
            result2 = conn.execute(
                text("SELECT id FROM replace_test WHERE name = 'old data'")
            )
            old_data = result2.fetchone()

        assert count == 2
        assert old_data is None


class TestFileFormatDetection:
    """Tests for automatic file format detection."""

    def test_detect_parquet(self):
        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        assert GCSToPostgresOperator._detect_file_format('data.parquet') == 'parquet'

    def test_detect_jsonl(self):
        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        assert GCSToPostgresOperator._detect_file_format('data.jsonl') == 'jsonl'

    def test_detect_csv(self):
        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        assert GCSToPostgresOperator._detect_file_format('data.csv') == 'csv'

    def test_detect_avro(self):
        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        assert GCSToPostgresOperator._detect_file_format('data.avro') == 'avro'

    def test_default_to_csv(self):
        from airsql.transfers.gcs_postgres import GCSToPostgresOperator

        assert GCSToPostgresOperator._detect_file_format('data.unknown') == 'csv'
        assert GCSToPostgresOperator._detect_file_format('data') == 'csv'


class TestCreateIfMissing:
    """Tests for create_if_missing parameter."""

    def test_create_if_missing_creates_table(self, pg_engine, real_postgres_hook):
        """Test that create_if_missing creates table when it doesn't exist."""
        with pg_engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS auto_created_table'))
            conn.commit()

        try:
            csv_data = b"""id,name,value
1,Alice,100.5
2,Bob,200.75
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_create',
                target_table_name='auto_created_table',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                create_if_missing=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT * FROM auto_created_table ORDER BY id')
                )
                rows = list(result)

            assert len(rows) == 2
            assert rows[0][1] == 'Alice'

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS auto_created_table'))
                conn.commit()

    def test_create_if_missing_false_raises_error(self, pg_engine, real_postgres_hook):
        """Test that missing table raises error when create_if_missing=False."""
        with pg_engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS nonexistent_table'))
            conn.commit()

        try:
            csv_data = b"""id,name
1,test
"""

            from airsql.transfers.gcs_postgres import GCSToPostgresOperator

            op = GCSToPostgresOperator(
                task_id='test_create',
                target_table_name='nonexistent_table',
                bucket_name='test-bucket',
                object_name='data.csv',
                postgres_conn_id='postgres_default',
                gcp_conn_id='NOT_USED',
                create_if_missing=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.download.return_value = csv_data

            with (
                patch('airsql.transfers.gcs_postgres.GCSHook', return_value=mock_gcs),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                import pytest

                with pytest.raises(ValueError, match='does not exist'):
                    op.execute({})

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS nonexistent_table'))
                conn.commit()


class TestRoundTrip:
    """Tests for round-trip: Postgres -> Export -> Import -> Postgres."""

    def test_roundtrip_csv_basic_types(
        self, pg_engine, real_postgres_hook, postgres_container
    ):
        """Test round-trip with basic types via CSV."""
        from unittest.mock import MagicMock, patch

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator
        from airsql.transfers.postgres_gcs import PostgresToGCSOperator

        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS source_table;
                DROP TABLE IF EXISTS dest_table;
                CREATE TABLE source_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    int_val INTEGER,
                    float_val DOUBLE PRECISION,
                    bool_val BOOLEAN,
                    created_at TIMESTAMP
                );
                INSERT INTO source_table (name, int_val, float_val, bool_val, created_at)
                VALUES
                    ('Alice', 100, 100.5, true, '2024-01-15 10:30:00'),
                    ('Bob', 200, 200.75, false, '2024-01-16 11:45:00'),
                    ('Charlie', 300, 300.0, true, '2024-01-17 09:00:00');
            """)
            )
            conn.commit()

        try:
            import tempfile

            csv_data = b''

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            def make_fake_upload(path):
                def fake_upload(
                    bucket_name, object_name, data=None, filename=None, **kwargs
                ):
                    if data:
                        nonlocal csv_data
                        csv_data = data

                return fake_upload

            mock_gcs_export = MagicMock()
            mock_gcs_export.upload = make_fake_upload(tmp_path)

            import psycopg

            user = postgres_container.username
            password = postgres_container.password
            host = postgres_container.get_container_host_ip()
            port = postgres_container.get_exposed_port(5432)
            database = postgres_container.dbname
            psycopg_str = f'host={host} port={port} dbname={database} user={user} password={password}'

            def get_conn():
                return psycopg.connect(psycopg_str)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch(
                    'airsql.transfers.postgres_gcs.GCSHook',
                    return_value=mock_gcs_export,
                ),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = get_conn

                export_op = PostgresToGCSOperator(
                    task_id='export',
                    postgres_conn_id='NOT_USED',
                    sql='SELECT name, int_val, float_val, bool_val, created_at FROM source_table ORDER BY id',
                    bucket='',
                    filename='test.csv',
                    export_format='csv',
                    use_copy=False,
                )
                export_op.execute({})

            mock_gcs_import = MagicMock()
            mock_gcs_import.download.return_value = csv_data

            with (
                patch(
                    'airsql.transfers.gcs_postgres.GCSHook',
                    return_value=mock_gcs_import,
                ),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                import_op = GCSToPostgresOperator(
                    task_id='import',
                    target_table_name='dest_table',
                    bucket_name='test-bucket',
                    object_name='test.csv',
                    postgres_conn_id='postgres_default',
                    gcp_conn_id='NOT_USED',
                    create_if_missing=True,
                )
                import_op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(text('SELECT * FROM dest_table ORDER BY name'))
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][0] == 'Alice'
            assert int(rows[0][1]) == 100
            assert abs(float(rows[0][2]) - 100.5) < 0.01

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS source_table'))
                conn.execute(text('DROP TABLE IF EXISTS dest_table'))
                conn.commit()

    def test_roundtrip_jsonl_with_json_columns(
        self, pg_engine, real_postgres_hook, postgres_container
    ):
        """Test round-trip with JSON columns via JSONL."""
        from unittest.mock import MagicMock, patch

        from airsql.transfers.gcs_postgres import GCSToPostgresOperator
        from airsql.transfers.postgres_gcs import PostgresToGCSOperator

        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS source_json;
                DROP TABLE IF EXISTS dest_json;
                CREATE TABLE source_json (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    metadata JSONB,
                    tags JSONB
                );
                INSERT INTO source_json (name, metadata, tags) VALUES
                    ('item1', '{"role": "admin", "level": 5}', '["tag1", "tag2"]'),
                    ('item2', '{"role": "user"}', '[1, 2, 3]'),
                    ('item3', '{}', '[]');
            """)
            )
            conn.commit()

        try:
            import tempfile

            jsonl_data = b''

            with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp:
                tmp_path = tmp.name

            def make_fake_upload(path):
                def fake_upload(
                    bucket_name, object_name, data=None, filename=None, **kwargs
                ):
                    if data:
                        nonlocal jsonl_data
                        jsonl_data = data

                return fake_upload

            mock_gcs_export = MagicMock()
            mock_gcs_export.upload = make_fake_upload(tmp_path)

            import psycopg

            user = postgres_container.username
            password = postgres_container.password
            host = postgres_container.get_container_host_ip()
            port = postgres_container.get_exposed_port(5432)
            database = postgres_container.dbname
            psycopg_str = f'host={host} port={port} dbname={database} user={user} password={password}'

            def get_conn():
                return psycopg.connect(psycopg_str)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch(
                    'airsql.transfers.postgres_gcs.GCSHook',
                    return_value=mock_gcs_export,
                ),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = get_conn

                export_op = PostgresToGCSOperator(
                    task_id='export',
                    postgres_conn_id='NOT_USED',
                    sql='SELECT name, metadata, tags FROM source_json ORDER BY id',
                    bucket='',
                    filename='test.jsonl',
                    export_format='jsonl',
                    use_copy=False,
                )
                export_op.execute({})

            mock_gcs_import = MagicMock()
            mock_gcs_import.download.return_value = jsonl_data

            with (
                patch(
                    'airsql.transfers.gcs_postgres.GCSHook',
                    return_value=mock_gcs_import,
                ),
                patch(
                    'airsql.transfers.gcs_postgres.PostgresHook',
                    return_value=real_postgres_hook('postgres_default'),
                ),
            ):
                import_op = GCSToPostgresOperator(
                    task_id='import',
                    target_table_name='dest_json',
                    bucket_name='test-bucket',
                    object_name='test.jsonl',
                    postgres_conn_id='postgres_default',
                    gcp_conn_id='NOT_USED',
                    create_if_missing=True,
                )
                import_op.execute({})

            with pg_engine.connect() as conn:
                result = conn.execute(
                    text('SELECT name, metadata, tags FROM dest_json ORDER BY name')
                )
                rows = list(result)

            assert len(rows) == 3
            assert rows[0][0] == 'item1'
            assert rows[0][1]['role'] == 'admin'
            assert rows[0][2] == ['tag1', 'tag2']

        finally:
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS source_json'))
                conn.execute(text('DROP TABLE IF EXISTS dest_json'))
                conn.commit()
