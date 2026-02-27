# DDL Operations

The `@sql.ddl` decorator executes DDL (Data Definition Language) statements like CREATE, ALTER, and DROP.

## Usage

```python
from airsql import sql

@sql.ddl(source_conn="postgres_conn")
def create_view():
    return """
    CREATE OR REPLACE VIEW active_users AS
    SELECT * FROM users WHERE active = true
    """

ddl_task = create_view()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_conn` | `str` | Yes | Connection ID for the database |
| `output_table` | `Table` | No | Optional table for lineage tracking |
| `sql_file` | `str` | No | Path to SQL file |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Create View

```python
@sql.ddl(source_conn="postgres_conn")
def create_user_summary_view():
    return """
    CREATE OR REPLACE VIEW v_user_summary AS
    SELECT 
        u.id,
        u.name,
        COUNT(o.id) as order_count,
        COALESCE(SUM(o.amount), 0) as total_spent
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.name
    """
```

### Create Table

```python
@sql.ddl(source_conn="postgres_conn")
def create_partitioned_table():
    return """
    CREATE TABLE IF NOT EXISTS events (
        event_id BIGSERIAL,
        event_type VARCHAR(50),
        event_data JSONB,
        event_time TIMESTAMP
    ) PARTITION BY RANGE (event_time)
    """
```

### Create Index

```python
@sql.ddl(source_conn="postgres_conn")
def create_index():
    return """
    CREATE INDEX IF NOT EXISTS idx_orders_user_date 
    ON orders (user_id, created_at)
    """
```

### Drop Table

```python
@sql.ddl(source_conn="postgres_conn")
def drop_temp_table():
    return "DROP TABLE IF EXISTS temp_staging"
```

### Alter Table

```python
@sql.ddl(source_conn="postgres_conn")
def add_column():
    return """
    ALTER TABLE users 
    ADD COLUMN IF NOT EXISTS last_login TIMESTAMP
    """
```

### Using Table References

```python
from airsql import sql, Table

@sql.ddl(source_conn="postgres_conn")
def create_view_from_table(source_table):
    return f"""
    CREATE OR REPLACE VIEW active_users AS
    SELECT * FROM {{ source_table }} WHERE active = true
    """

task = create_view_from_table(
    source_table=Table("postgres_conn", "public.users")
)
```

### With Template Variables

```python
@sql.ddl(
    source_conn="bigquery_conn",
    dataset="analytics"
)
def create_partitioned_view():
    return """
    CREATE OR REPLACE VIEW {{ dataset }}.daily_events AS
    SELECT * FROM events
    WHERE DATE(event_time) = CURRENT_DATE()
    """
```

### Using SQL Files

```python
@sql.ddl(
    source_conn="postgres_conn",
    sql_file="ddl/create_schema.sql"
)
def create_schema():
    pass
```

### BigQuery DDL

```python
@sql.ddl(source_conn="bigquery_conn")
def create_bigquery_table():
    return """
    CREATE TABLE IF NOT EXISTS `project.dataset.events` (
        event_id INT64,
        event_type STRING,
        event_time TIMESTAMP
    )
    PARTITION BY DATE(event_time)
    CLUSTER BY event_type
    """
```

## Lineage Tracking

Optionally track outputs for data lineage:

```python
@sql.ddl(
    source_conn="postgres_conn",
    output_table=Table("postgres_conn", "views.active_users")
)
def create_tracked_view():
    return """
    CREATE OR REPLACE VIEW active_users AS
    SELECT * FROM users WHERE active = true
    """
```

## Dynamic DDL

```python
from datetime import datetime

@sql.ddl(source_conn="postgres_conn")
def create_partition(date_value):
    return f"""
    CREATE TABLE IF NOT EXISTS events_{date_value.replace('-', '_')}
    PARTITION OF events
    FOR VALUES FROM ('{date_value}') TO ('{date_value}'::date + INTERVAL '1 day')
    """

task = create_partition(date_value="{{ ds }}")
```

## Important Notes

- DDL operations execute against the source database directly
- No result validation is performed
- Use caution with DROP and ALTER statements
- Consider wrapping in transactions where supported

## Related Operations

- [`@sql.query`](query.md) - Execute queries that write to tables
- [`@sql.check`](check.md) - Execute data quality checks