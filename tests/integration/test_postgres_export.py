import os
import tempfile

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
def pg_conn_factory(postgres_container):
    """Get a factory for psycopg connections."""
    import psycopg

    url = postgres_container.get_connection_url().replace('+psycopg2', '')

    def get_conn():
        return psycopg.connect(url)

    return get_conn


def make_fake_upload(tmp_path):
    """Create a fake upload function that writes data to the temp file."""

    def fake_upload(bucket_name, object_name, data=None, filename=None, **kwargs):
        if data:
            with open(object_name, 'wb') as f:
                f.write(data)
        elif filename:
            with open(filename, 'rb') as src:
                content = src.read()
            with open(object_name, 'wb') as dst:
                dst.write(content)

    return fake_upload


class TestPostgresExportCSV:
    """Tests for exporting PostgreSQL tables to CSV format using real DB."""

    def test_export_integer_types(self, pg_engine):
        """Test exporting INTEGER, BIGINT, SMALLINT columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_integers;
                CREATE TABLE test_integers (
                    id SERIAL PRIMARY KEY,
                    small_val SMALLINT,
                    int_val INTEGER,
                    big_val BIGINT
                );
                INSERT INTO test_integers (small_val, int_val, big_val) VALUES
                    (100, 100000, 9223372036854775807),
                    (-50, -50000, -9223372036854775808),
                    (NULL, NULL, NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT small_val, int_val, big_val FROM test_integers',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert df['small_val'].iloc[0] == 100
            assert df['int_val'].iloc[0] == 100000
            assert df['big_val'].iloc[0] == 9223372036854775807
            assert df['small_val'].iloc[1] == -50

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_integers'))
                conn.commit()

    def test_export_decimal_float_types(self, pg_engine):
        """Test exporting DECIMAL and DOUBLE PRECISION columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_decimals;
                CREATE TABLE test_decimals (
                    id SERIAL PRIMARY KEY,
                    decimal_val DECIMAL(10, 4),
                    float_val DOUBLE PRECISION
                );
                INSERT INTO test_decimals (decimal_val, float_val) VALUES
                    (12345.6789, 3.14159265358979),
                    (-999.9999, -2.71828),
                    (NULL, NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT decimal_val, float_val FROM test_decimals',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert float(df['decimal_val'].iloc[0]) == 12345.6789
            assert abs(df['float_val'].iloc[0] - 3.14159265358979) < 0.0001

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_decimals'))
                conn.commit()

    def test_export_text_types(self, pg_engine):
        """Test exporting TEXT and VARCHAR columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_text;
                CREATE TABLE test_text (
                    id SERIAL PRIMARY KEY,
                    text_val TEXT,
                    varchar_val VARCHAR(50),
                    char_val CHAR(5)
                );
                INSERT INTO test_text (text_val, varchar_val, char_val) VALUES
                    ('Hello World', 'varchar test', 'abcde'),
                    ('Unicode: éàü', 'special!@#', 'xyz'),
                    ('', '', '');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT text_val, varchar_val, char_val FROM test_text',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert df['text_val'].iloc[0] == 'Hello World'
            assert 'Unicode' in df['text_val'].iloc[1]

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_text'))
                conn.commit()

    def test_export_boolean_type(self, pg_engine):
        """Test exporting BOOLEAN columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_boolean;
                CREATE TABLE test_boolean (
                    id SERIAL PRIMARY KEY,
                    bool_val BOOLEAN
                );
                INSERT INTO test_boolean (bool_val) VALUES
                    (TRUE),
                    (FALSE),
                    (NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT bool_val FROM test_boolean',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert df['bool_val'].iloc[0] is True
            assert df['bool_val'].iloc[1] is False

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_boolean'))
                conn.commit()

    def test_export_date_types(self, pg_engine):
        """Test exporting DATE, TIMESTAMP, TIMESTAMPTZ columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_dates;
                CREATE TABLE test_dates (
                    id SERIAL PRIMARY KEY,
                    date_val DATE,
                    timestamp_val TIMESTAMP,
                    timestamptz_val TIMESTAMPTZ
                );
                INSERT INTO test_dates (date_val, timestamp_val, timestamptz_val) VALUES
                    ('2024-01-15', '2024-01-15 10:30:45', '2024-01-15 10:30:45+00'),
                    ('2024-12-31', '2024-12-31 23:59:59', '2024-12-31 23:59:59-05'),
                    (NULL, NULL, NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT date_val, timestamp_val, timestamptz_val FROM test_dates',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert '2024-01-15' in df['date_val'].iloc[0]
            assert '2024-01-15' in df['timestamp_val'].iloc[0]

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_dates'))
                conn.commit()

    def test_export_uuid_type(self, pg_engine):
        """Test exporting UUID columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_uuid;
                CREATE TABLE test_uuid (
                    id SERIAL PRIMARY KEY,
                    uuid_val UUID
                );
                INSERT INTO test_uuid (uuid_val) VALUES
                    ('550e8400-e29b-41d4-a716-446655440000'),
                    ('00000000-0000-0000-0000-000000000000'),
                    (NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT uuid_val FROM test_uuid',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert '550e8400-e29b-41d4-a716-446655440000' in str(df['uuid_val'].iloc[0])

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_uuid'))
                conn.commit()

    def test_export_inet_type(self, pg_engine):
        """Test exporting INET columns (IP addresses)."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_inet;
                CREATE TABLE test_inet (
                    id SERIAL PRIMARY KEY,
                    inet_val INET
                );
                INSERT INTO test_inet (inet_val) VALUES
                    ('192.168.1.1'),
                    ('10.0.0.0/24'),
                    ('2001:db8::1'),
                    (NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT inet_val FROM test_inet',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 4
            assert '192.168.1.1' in str(df['inet_val'].iloc[0])

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_inet'))
                conn.commit()


class TestPostgresExportJSONL:
    """Tests for exporting PostgreSQL tables to JSONL format using real DB."""

    def test_export_json_columns(self, pg_engine):
        """Test exporting JSON and JSONB columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_json;
                CREATE TABLE test_json (
                    id SERIAL PRIMARY KEY,
                    json_val JSON,
                    jsonb_val JSONB
                );
                INSERT INTO test_json (json_val, jsonb_val) VALUES
                    ('{"key": "value", "number": 42}', '{"nested": {"deep": true}}'),
                    ('[]', '["a", "b", "c"]'),
                    (NULL, NULL);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, json_val, jsonb_val FROM test_json WHERE id = 1',
                bucket='',
                filename=tmp_path,
                export_format='jsonl',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn.return_value.cursor.return_value.description = []
                op.execute({})

            df = pd.read_json(tmp_path, lines=True)

            assert len(df) == 1

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_json'))
                conn.commit()

    def test_export_json_arrays(self, pg_engine, pg_conn_factory):
        """Test exporting JSON arrays."""
        import json

        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_json_arrays;
                CREATE TABLE test_json_arrays (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    tags JSONB
                );
                INSERT INTO test_json_arrays (name, tags) VALUES
                    ('item1', '["tag1", "tag2", "tag3"]'),
                    ('item2', '[1, 2, 3]'),
                    ('item3', '[]');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, name, tags FROM test_json_arrays',
                bucket='',
                filename=tmp_path,
                export_format='jsonl',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            df = pd.read_json(tmp_path, lines=True)

            assert len(df) == 3
            with open(tmp_path, encoding='utf-8') as f:
                content = f.read()
            tags_first = df['tags'].iloc[0]
            if isinstance(tags_first, str):
                tags_first = json.loads(tags_first)
            assert tags_first == ['tag1', 'tag2', 'tag3'], (
                f'Got {tags_first!r}, file content:\n{content}'
            )

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_json_arrays'))
                conn.commit()


class TestPostgresExportAutoSwitch:
    """Tests for automatic format switching based on data."""

    def test_auto_switch_on_quotes_in_text(self, pg_engine, pg_conn_factory):
        """Test auto-switch to JSONL when text contains quotes."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_quotes;
                CREATE TABLE test_quotes (
                    id SERIAL PRIMARY KEY,
                    description TEXT
                );
                INSERT INTO test_quotes (description) VALUES
                    ('Normal text'),
                    ('Text with "double quotes"'),
                    ('More normal text');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, description FROM test_quotes',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
                auto_switch_format=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            assert op.actual_export_format == 'jsonl'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_quotes'))
                conn.commit()

    def test_auto_switch_on_newlines_in_text(self, pg_engine, pg_conn_factory):
        """Test auto-switch to JSONL when text contains newlines."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_newlines;
                CREATE TABLE test_newlines (
                    id SERIAL PRIMARY KEY,
                    content TEXT
                );
                INSERT INTO test_newlines (content) VALUES
                    ('Line one
Line two
Line three'),
                    ('Normal single line');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, content FROM test_newlines',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
                auto_switch_format=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            assert op.actual_export_format == 'jsonl'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_newlines'))
                conn.commit()


class TestPostgresCopyExport:
    """Tests for COPY-based export (streaming mode)."""

    def test_copy_export_csv(self, pg_engine, pg_conn_factory):
        """Test COPY export to CSV format."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_copy_csv;
                CREATE TABLE test_copy_csv (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                );
                INSERT INTO test_copy_csv (name, value) VALUES
                    ('Alice', 100),
                    ('Bob', 200),
                    ('Charlie', 300);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT name, value FROM test_copy_csv ORDER BY id',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=True,
                use_temp_file=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_conn = pg_conn_factory
                mock_pg.return_value.run = lambda sql: None
                mock_pg.return_value.get_records = lambda sql: [(3,)]

                op.execute({})

            df = pd.read_csv(tmp_path)

            assert len(df) == 3
            assert list(df['name']) == ['Alice', 'Bob', 'Charlie']
            assert list(df['value']) == [100, 200, 300]

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_copy_csv'))
                conn.commit()

    def test_copy_export_jsonl_with_json_column(self, pg_engine, pg_conn_factory):
        """Test COPY export to JSONL when table has JSON column."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_copy_jsonl;
                CREATE TABLE test_copy_jsonl (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    metadata JSONB
                );
                INSERT INTO test_copy_jsonl (name, metadata) VALUES
                    ('Alice', '{"role": "admin"}'),
                    ('Bob', '{"role": "user"}');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT name, metadata FROM test_copy_jsonl ORDER BY id',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=True,
                use_temp_file=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_conn = pg_conn_factory
                mock_pg.return_value.run = lambda sql: None
                mock_pg.return_value.get_records = lambda sql: [(2,)]

                op.execute({})

            assert op.actual_export_format == 'jsonl'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_copy_jsonl'))
                conn.commit()


class TestExportFormatAutoInference:
    """Tests for automatic export format inference."""

    def test_auto_switch_to_jsonl_for_json_columns(self, pg_engine, pg_conn_factory):
        """Test that export auto-switches to JSONL when table has JSON columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_json_auto;
                CREATE TABLE test_json_auto (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    data JSONB
                );
                INSERT INTO test_json_auto (name, data) VALUES
                    ('item1', '{"key": "value"}');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT name, data FROM test_json_auto',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            assert op.actual_export_format == 'jsonl'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_json_auto'))
                conn.commit()

    def test_no_switch_when_no_json_columns(self, pg_engine, pg_conn_factory):
        """Test that export stays as CSV when no JSON columns exist."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_no_json;
                CREATE TABLE test_no_json (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                );
                INSERT INTO test_no_json (name, value) VALUES ('test', 100);
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT name, value FROM test_no_json',
                bucket='',
                filename=tmp_path,
                export_format='csv',
                use_copy=False,
                auto_switch_format=True,
            )

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = make_fake_upload(tmp_path)

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            assert op.actual_export_format == 'csv'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_no_json'))
                conn.commit()


class TestSchemaExport:
    """Tests for BigQuery schema export."""

    def test_schema_export_csv(self, pg_engine, pg_conn_factory):
        """Test that schema file is exported for CSV format."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_schema_csv;
                CREATE TABLE test_schema_csv (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value DOUBLE PRECISION,
                    active BOOLEAN,
                    created_at TIMESTAMP
                );
                INSERT INTO test_schema_csv VALUES (1, 'test', 100.5, true, NOW());
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            schema_content = None

            def capture_upload(bucket_name, object_name, data=None, **kwargs):
                nonlocal schema_content
                if data and object_name.endswith('.schema.json'):
                    schema_content = data

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = capture_upload

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, name, value, active, created_at FROM test_schema_csv',
                bucket='test-bucket',
                filename=tmp_path,
                export_format='csv',
                schema_filename='test.schema.json',
                use_copy=False,
            )

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            import json

            assert schema_content is not None
            schema = json.loads(schema_content)
            assert isinstance(schema, list)
            field_names = [f['name'] for f in schema]
            assert 'id' in field_names
            assert 'name' in field_names
            assert 'value' in field_names

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_schema_csv'))
                conn.commit()

    def test_schema_export_jsonl_with_json_column(self, pg_engine, pg_conn_factory):
        """Test that schema file correctly identifies JSON columns."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_schema_json;
                CREATE TABLE test_schema_json (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    metadata JSONB
                );
                INSERT INTO test_schema_json VALUES (1, 'test', '{"key": "value"}');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp:
                tmp_path = tmp.name

            schema_content = None

            def capture_upload(bucket_name, object_name, data=None, **kwargs):
                nonlocal schema_content
                if data and object_name.endswith('.schema.json'):
                    schema_content = data

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = capture_upload

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT id, name, metadata FROM test_schema_json',
                bucket='test-bucket',
                filename=tmp_path,
                export_format='jsonl',
                schema_filename='test.schema.json',
                use_copy=False,
            )

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            import json

            assert schema_content is not None
            schema = json.loads(schema_content)

            metadata_field = next((f for f in schema if f['name'] == 'metadata'), None)
            assert metadata_field is not None
            assert metadata_field['type'] == 'JSON'

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_schema_json'))
                conn.commit()

    def test_schema_filename_updated_on_format_switch(self, pg_engine, pg_conn_factory):
        """Test that schema_filename is updated when format auto-switches from CSV to JSONL."""
        with pg_engine.connect() as conn:
            conn.execute(
                text("""
                DROP TABLE IF EXISTS test_schema_switch;
                CREATE TABLE test_schema_switch (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    data JSONB
                );
                INSERT INTO test_schema_switch VALUES (1, 'test', '{"key": "value"}');
            """)
            )
            conn.commit()

        try:
            from airsql.transfers.postgres_gcs import PostgresToGCSOperator

            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                tmp_path = tmp.name

            uploads = {}

            def capture_upload(bucket_name, object_name, data=None, **kwargs):
                if data:
                    uploads[object_name] = data

            from unittest.mock import MagicMock, patch

            mock_gcs = MagicMock()
            mock_gcs.upload = capture_upload

            op = PostgresToGCSOperator(
                task_id='test_export',
                postgres_conn_id='NOT_USED',
                sql='SELECT name, data FROM test_schema_switch',
                bucket='test-bucket',
                filename='data.csv',
                export_format='csv',
                schema_filename='data.schema.json',
                auto_switch_format=True,
                use_copy=False,
            )

            with (
                patch('airsql.transfers.postgres_gcs.PostgresHook') as mock_pg,
                patch('airsql.transfers.postgres_gcs.GCSHook', return_value=mock_gcs),
            ):
                mock_pg.return_value.get_sqlalchemy_engine.return_value = pg_engine
                mock_pg.return_value.get_conn = pg_conn_factory
                op.execute({})

            import json

            assert op.actual_export_format == 'jsonl'
            assert op.filename == 'data.jsonl'
            assert op.schema_filename == 'data.jsonl.schema.json'

            schema_key = 'data.jsonl.schema.json'
            assert schema_key in uploads, (
                f'Expected {schema_key} in uploads, got {list(uploads.keys())}'
            )
            schema = json.loads(uploads[schema_key])
            assert isinstance(schema, list)

        finally:
            os.remove(tmp_path)
            with pg_engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS test_schema_switch'))
                conn.commit()
