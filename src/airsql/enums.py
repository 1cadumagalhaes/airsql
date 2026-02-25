"""
Centralized enums for AirSQL operators.
"""

from enum import Enum


# PostgreSQL to GCS - only CSV and JSONL (Postgres COPY doesn't produce parquet)
class PostgresExportFormat(str, Enum):
    """Supported export formats for Postgres to GCS transfer."""

    CSV = 'csv'
    JSONL = 'jsonl'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


# BigQuery to GCS - supports all formats
class BigQueryExportFormat(str, Enum):
    """Supported export formats for BigQuery to GCS transfer."""

    PARQUET = 'parquet'
    CSV = 'csv'
    JSONL = 'jsonl'
    AVRO = 'avro'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class WriteDisposition(str, Enum):
    """BigQuery write dispositions."""

    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class CreateDisposition(str, Enum):
    """BigQuery create dispositions."""

    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
    CREATE_NEVER = 'CREATE_NEVER'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class PartitionType(str, Enum):
    """BigQuery partition types."""

    DAY = 'DAY'
    HOUR = 'HOUR'
    MONTH = 'MONTH'
    YEAR = 'YEAR'

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class SchemaDetectionMode(str, Enum):
    """Schema detection modes for Postgres to GCS."""

    POSTGRES = 'postgres'  # Use Postgres metadata
    SAMPLING = 'sampling'  # Sample values
    NONE = 'none'  # No JSON detection

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]
