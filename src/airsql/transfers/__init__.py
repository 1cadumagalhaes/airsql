"""
AirSQL Transfer Operators

Collection of transfer operators for moving data between different systems.
"""

__all__ = [
    'GCSToPostgresOperator',
    'PostgresToGCSOperator',
    'PostgresToBigQueryOperator',
    'BigQueryToPostgresOperator',
]


def __getattr__(name):
    if name == 'BigQueryToPostgresOperator':
        from airsql.transfers.bigquery_postgres import (  # noqa: PLC0415
            BigQueryToPostgresOperator,  # noqa: PLC0415
        )

        return BigQueryToPostgresOperator
    elif name == 'GCSToPostgresOperator':
        from airsql.transfers.gcs_postgres import (  # noqa: PLC0415
            GCSToPostgresOperator,  # noqa: PLC0415
        )

        return GCSToPostgresOperator
    elif name == 'PostgresToBigQueryOperator':
        from airsql.transfers.postgres_bigquery import (  # noqa: PLC0415
            PostgresToBigQueryOperator,  # noqa: PLC0415
        )

        return PostgresToBigQueryOperator
    elif name == 'PostgresToGCSOperator':
        from airsql.transfers.postgres_gcs import (  # noqa: PLC0415
            PostgresToGCSOperator,  # noqa: PLC0415
        )

        return PostgresToGCSOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
