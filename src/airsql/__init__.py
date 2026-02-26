"""
AirSQL Framework

A decorator-based SQL execution framework for Airflow that provides:
- Clean, Python-like syntax with decorators
- Flexible table references with database-specific configurations
- Cross-database query support via DataFusion
- Support for SQL files with Jinja templating
- Native Airflow connection integration
"""

# ruff: noqa: PLC2701
# Workaround for Airflow bug: pandas 3.x uses 'pandas.DataFrame' but serializer
# is registered with 'pandas.core.frame.DataFrame'. Add alias for compatibility.
try:
    from airflow.serialization.serde import (  # noqa: PLC0415
        _deserializers,
        _extra_allowed,
        _serializers,
    )
    from airflow.serialization.serializers import (  # noqa: PLC0415
        pandas as pd_serializer,
    )

    _serializers['pandas.DataFrame'] = pd_serializer
    _deserializers['pandas.DataFrame'] = pd_serializer
    _extra_allowed.add('pandas.DataFrame')
except ImportError:
    pass

from airsql.decorators import sql
from airsql.file import File
from airsql.table import Table

try:
    from importlib.metadata import version  # noqa: PLC0415

    __version__ = version('airsql')
except Exception:
    __version__ = 'unknown'

# Core exports
__all__ = [
    'sql',
    'Table',
    'File',
]


def main() -> None:
    print('Hello from airsql!')
