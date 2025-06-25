"""
Table reference class for airsql framework.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from airflow.hooks.base import BaseHook
from airflow.sdk import Asset, get_current_context
from pydantic import BaseModel, Field


class Table(BaseModel):
    """Represents a database table reference with connection and configuration.

    This class is JSON serializable for use with Airflow XCom and TaskFlow API.

    Examples:
        Table(conn_id="postgres_conn", table_name="analytics.user_summary")
        Table(
            conn_id="bigquery_conn",
            table_name="analytics.user_events",
            project="data-warehouse-proj",
            partition_by="event_date",
            cluster_by=["user_id", "event_type"],
            location="US"
        )
        Table(
            conn_id="bigquery_conn",
            table_name="analytics.events_*",
            date_range=("20250501", "20250529")
        )
        Table(
            conn_id="bigquery_conn",
            table_name="temp.analysis_{{run_id}}",
            temporary=True
        )
    """

    conn_id: str = Field(..., description='Airflow connection ID')
    table_name: str = Field(
        ...,
        description='Table name (schema.table, dataset.table for BigQuery)',
    )
    project: Optional[str] = Field(
        None,
        description='BigQuery project (uses connection default if not specified)',
    )
    schema_fields: Optional[List[Dict[str, Any]]] = Field(
        None,
        description='BigQuery table schema fields. '
        'E.g. [{"name": "col1", "type": "STRING"}]',
    )
    partition_by: Optional[str] = Field(None, description='BigQuery partition column')
    cluster_by: List[str] = Field(
        default_factory=list, description='BigQuery clustering columns'
    )
    location: Optional[str] = Field(None, description='BigQuery location/region')
    temporary: bool = Field(False, description='Whether this is a temporary table')
    date_range: Optional[Tuple[str, str]] = Field(
        None, description='Date range for sharded tables (start, end)'
    )
    table_type: Optional[str] = Field(
        None, description='BigQuery table type (e.g., EXTERNAL)'
    )
    dataset: Optional[str] = Field(None, description='Postgres dataset override')
    extra_config: Dict[str, Any] = Field(
        default_factory=dict, description='Additional database-specific configuration'
    )

    class Config:
        extra = 'allow'
        json_schema_extra = {
            'examples': [
                {'conn_id': 'postgres_conn', 'table_name': 'analytics.users'},
                {
                    'conn_id': 'bigquery_conn',
                    'table_name': 'analytics.events',
                    'project': 'my-project',
                    'partition_by': 'date',
                    'cluster_by': ['user_id'],
                },
            ]
        }

    def __str__(self) -> str:
        """Return the fully qualified table name for SQL queries."""
        if self.is_bigquery:
            return f'`{self.table_name}`'
        return self.table_name

    def __repr__(self) -> str:
        return f"Table(conn_id='{self.conn_id}', table_name='{self.table_name}')"

    @property
    def is_bigquery(self) -> bool:
        """Check if the table uses a BigQuery connection."""
        try:
            conn = BaseHook.get_connection(self.conn_id)
            conn_type = conn.conn_type.lower() if conn.conn_type else 'unknown'
            return conn_type in {'google_cloud_platform', 'gccpigquery', 'bigquery'}
        except Exception as e:
            logging.getLogger(__name__).warning(
                f'Failed to check connection type for {self.conn_id}: {e}'
            )
            return False

    @property
    def is_postgres(self) -> bool:
        """Check if the table uses a Postgres connection."""
        try:
            try:
                get_current_context()
            except (RuntimeError, ImportError):
                conn_id_lower = self.conn_id.lower()
                return any(
                    keyword in conn_id_lower for keyword in ['sql', 'postgres', 'pg']
                )

            conn = BaseHook.get_connection(self.conn_id)
            return conn.conn_type.lower() in {'postgres', 'postgresql'}
        except Exception as e:
            logging.getLogger(__name__).warning(
                f'Failed to check connection type for {self.conn_id}: {e}'
            )
            return False

    def get_create_options(self) -> Dict[str, Any]:
        """Get database-specific table creation options."""
        options = {}

        if self.is_bigquery:
            if self.partition_by:
                options['partition_by'] = self.partition_by
            if self.cluster_by:
                options['cluster_by'] = self.cluster_by
            if self.location:
                options['location'] = self.location
            if self.table_type:
                options['table_type'] = self.table_type

        return options

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Table':
        """Create Table instance from dictionary (for XCom deserialization)."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for XCom serialization."""
        return self.model_dump()

    def get_asset_uri(self) -> str:
        """Get a unique URI for this table to be used in Airflow Assets."""
        db_type = 'unknown'
        if self.is_bigquery:
            db_type = 'bigquery'
        elif self.is_postgres:
            db_type = 'postgres'
        # Add more conditions here if you support other database types
        return f'airsql://{db_type}/{self.table_name}'

    def as_asset(self) -> Asset:
        """Return an Airflow Asset representation of this table."""
        return Asset(uri=self.get_asset_uri())
