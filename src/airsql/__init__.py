"""
AirSQL Framework

A decorator-based SQL execution framework for Airflow that provides:
- Clean, Python-like syntax with decorators
- Flexible table references with database-specific configurations
- Cross-database query support via DataFusion
- Support for SQL files with Jinja templating
- Native Airflow connection integration
"""

from airsql.decorators import sql
from airsql.file import File
from airsql.table import Table

__version__ = '0.1.0'

# Core exports
__all__ = [
    'sql',
    'Table',
    'File',
]


def main() -> None:
    print('Hello from airsql!')
