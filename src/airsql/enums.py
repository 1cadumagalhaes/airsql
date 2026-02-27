"""
Centralized enums for AirSQL operators.
"""

from enum import Enum


# PostgreSQL to GCS - only CSV and JSONL (Postgres COPY doesn't produce parquet)
class PostgresExportFormat(str, Enum):
    """Supported export formats for Postgres to GCS transfer.

    These values are used by the Postgres->GCS transfer operator to determine
    the serialization format when exporting data.
    """

    CSV = 'csv'
    JSONL = 'jsonl'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


# BigQuery to GCS - supports all formats
class BigQueryExportFormat(str, Enum):
    """Supported export formats for BigQuery to GCS transfer.

    These values are used by the BigQuery->GCS transfer operator to select
    the output format for exports.
    """

    PARQUET = 'parquet'
    CSV = 'csv'
    JSONL = 'jsonl'
    AVRO = 'avro'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class WriteDisposition(str, Enum):
    """BigQuery write dispositions.

    These constants map to BigQuery write disposition semantics used during
    load operations.
    """

    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class CreateDisposition(str, Enum):
    """BigQuery create dispositions.

    These constants determine whether BigQuery should create tables when
    loading data.
    """

    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
    CREATE_NEVER = 'CREATE_NEVER'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class PartitionType(str, Enum):
    """BigQuery partition types.

    Describes supported partition granularities for BigQuery tables.
    """

    DAY = 'DAY'
    HOUR = 'HOUR'
    MONTH = 'MONTH'
    YEAR = 'YEAR'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class SchemaDetectionMode(str, Enum):
    """Schema detection modes for Postgres to GCS.

    Controls how the Postgres->GCS transfer infers schema information.
    """

    POSTGRES = 'postgres'  # Use Postgres metadata
    SAMPLING = 'sampling'  # Sample values
    NONE = 'none'  # No JSON detection

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]
