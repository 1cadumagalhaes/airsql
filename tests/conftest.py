import logging
import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from airsql.table import Table


class _MockAsset:
    def __init__(self, *, uri: str) -> None:
        self.uri = uri


@pytest.fixture(autouse=True)
def mock_table_asset() -> None:
    original_as_asset = Table.as_asset

    def mock_as_asset(self) -> _MockAsset:
        return _MockAsset(uri=self.get_asset_uri())

    Table.as_asset = mock_as_asset
    yield
    Table.as_asset = original_as_asset


class _Connection:
    def __init__(
        self,
        conn_type: str = 'postgres',
        host: str = 'localhost',
        login: str = '',
        password: str = '',
        schema: str = '',
        port: int = 5432,
        extra: str = '',
    ) -> None:
        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra


class _BaseHook:
    @classmethod
    def get_connection(cls, conn_id: str) -> _Connection:
        return _Connection()


class _PostgresHook(_BaseHook):
    def __init__(
        self, postgres_conn_id: str = 'postgres_default', **kwargs: Any
    ) -> None:
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self) -> MagicMock:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.close.return_value = None
        return mock_conn

    def get_sqlalchemy_engine(self) -> MagicMock:
        return MagicMock()

    def get_pandas_df(self, sql: str, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})

    def get_records(self, sql: str, parameters=None) -> list:
        return []


class _BigQueryHook(_BaseHook):
    def __init__(
        self, gcp_conn_id: str = 'google_cloud_default', **kwargs: Any
    ) -> None:
        self.gcp_conn_id = gcp_conn_id
        self.project_id = 'test-project'

    def get_pandas_df(
        self, sql: str, dialect: str = 'standard', **kwargs: Any
    ) -> pd.DataFrame:
        return pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})

    def get_client(
        self, project_id: str | None = None, location: str | None = None
    ) -> MagicMock:
        mock_client = MagicMock()
        mock_client.dataset.return_value.table.return_value = MagicMock()
        mock_client.load_table_from_dataframe.return_value.result.return_value = None
        mock_client.query.return_value.result.return_value = None
        mock_client.create_dataset.return_value = None
        return mock_client


class _GCSHook:
    def __init__(
        self, gcp_conn_id: str = 'google_cloud_default', **kwargs: Any
    ) -> None:
        self.gcp_conn_id = gcp_conn_id


class _Asset:
    def __init__(self, *, uri: str) -> None:
        self.uri = uri


class _BaseOperator:
    log = logging.getLogger('airflow.task')

    def __init__(self, *, task_id: str, **kwargs: Any) -> None:
        self.task_id = task_id
        for k, v in kwargs.items():
            setattr(self, k, v)

    def execute(self, context: dict[str, Any]) -> Any:
        raise NotImplementedError


class _SQLCheckOperator(_BaseOperator):
    def __init__(self, *, sql: str, conn_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id


class _SqlSensor(_BaseOperator):
    def __init__(self, *, sql: str, conn_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id

    def poke(self, context: dict[str, Any]) -> bool:
        return True


def _get_current_context() -> dict[str, Any]:
    return {}


def _task(func: Any = None, *, task_id: str | None = None, **kwargs: Any) -> Any:
    if func is not None:
        return func

    def decorator(f: Any) -> Any:
        return f

    return decorator


Context = dict[str, Any]


def _make_module(name: str, **attrs: Any) -> ModuleType:
    m = ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


_make_module('airflow')
_make_module(
    'airflow.sdk',
    get_current_context=_get_current_context,
    task=_task,
    Asset=_Asset,
    Context=Context,
)
_make_module('airflow.sdk.bases')
_make_module('airflow.sdk.bases.hook', BaseHook=_BaseHook)
_make_module('airflow.models', BaseOperator=_BaseOperator)
_make_module('airflow.exceptions', AirflowSkipException=Exception)
_make_module('airflow.providers')
_make_module('airflow.providers.common')
_make_module('airflow.providers.common.sql')
_make_module('airflow.providers.common.sql.operators')
_make_module(
    'airflow.providers.common.sql.operators.sql',
    SQLCheckOperator=_SQLCheckOperator,
)
_make_module('airflow.providers.common.sql.sensors')
_make_module(
    'airflow.providers.common.sql.sensors.sql',
    SqlSensor=_SqlSensor,
)
_make_module('airflow.providers.postgres')
_make_module('airflow.providers.postgres.hooks')
_make_module(
    'airflow.providers.postgres.hooks.postgres',
    PostgresHook=_PostgresHook,
    USE_PSYCOPG3=False,
)
_make_module('airflow.providers.google')
_make_module('airflow.providers.google.cloud')
_make_module('airflow.providers.google.cloud.hooks')
_make_module(
    'airflow.providers.google.cloud.hooks.gcs',
    GCSHook=_GCSHook,
)
_make_module(
    'airflow.providers.google.cloud.hooks.bigquery',
    BigQueryHook=_BigQueryHook,
)


@pytest.fixture
def mock_hook_manager() -> MagicMock:
    with patch('airsql.operators.SQLHookManager') as mock:
        instance = MagicMock()
        mock.return_value = instance
        mock.get_hook.return_value = MagicMock()
        yield instance


@pytest.fixture
def mock_postgres_hook() -> MagicMock:
    hook = MagicMock(spec=_PostgresHook)
    hook.get_pandas_df.return_value = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['a', 'b', 'c'],
    })
    hook.get_conn.return_value = MagicMock()
    hook.get_sqlalchemy_engine.return_value = MagicMock()
    return hook


@pytest.fixture
def mock_bigquery_hook() -> MagicMock:
    hook = MagicMock(spec=_BigQueryHook)
    hook.get_pandas_df.return_value = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['a', 'b', 'c'],
    })
    hook.project_id = 'test-project'
    return hook


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['alice', 'bob', 'charlie'],
        'value': [100, 200, 300],
    })


@pytest.fixture
def sample_table() -> Table:
    return Table(conn_id='postgres_conn', table_name='public.test_table')
