import pytest
from sqlalchemy import create_engine
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
