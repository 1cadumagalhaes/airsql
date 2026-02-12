"""
AirSQL Sensors

Custom Airflow sensors for various data sources.
"""

__all__ = ['BigQuerySqlSensor', 'PostgresSqlSensor']


def __getattr__(name):
    if name == 'BigQuerySqlSensor':
        from airsql.sensors.bigquery import BigQuerySqlSensor  # noqa: PLC0415

        return BigQuerySqlSensor
    elif name == 'PostgresSqlSensor':
        from airsql.sensors.postgres import PostgresSqlSensor  # noqa: PLC0415

        return PostgresSqlSensor
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
