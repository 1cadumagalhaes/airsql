# Extract and Merge

The `@sql.extract_and_merge` decorator combines SQL extraction and DataFrame merging into a single TaskFlow-compatible task.

## Usage

```python
from airsql import sql, Table

@sql.extract_and_merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    source_conn="bigquery_conn"
)
def sync_users():
    return """
    SELECT id, name, email, updated_at
    FROM staging_users
    WHERE processed = true
    """

merge_task = sync_users()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `output_table` | `Table` | Yes | Destination table reference |
| `conflict_columns` | `List[str]` | Yes | Columns for conflict detection |
| `update_columns` | `List[str]` | No | Columns to update (default: all non-conflict) |
| `source_conn` | `str` | Yes | Connection ID for source database |
| `timestamp_column` | `str` | No | Column to populate with current timestamp |
| `sql_file` | `str` | No | Path to SQL file |
| `pre_truncate` | `bool` | No | If True, truncate before merge |
| `dry_run` | `bool` | No | If True, simulate without writing |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Single Column Conflict

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "inventory.products"),
    conflict_columns=["sku"],
    source_conn="bigquery_conn"
)
def sync_products():
    return """
    SELECT sku, name, quantity, price
    FROM product_updates
    WHERE DATE(updated_at) = '{{ ds }}'
    """
```

### Composite Key Conflict

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "analytics.daily_metrics"),
    conflict_columns=["metric_date", "metric_name"],
    update_columns=["metric_value"],
    source_conn="bigquery_conn"
)
def update_daily_metrics():
    return """
    SELECT
        metric_date,
        metric_name,
        metric_value
    FROM metrics_source
    WHERE metric_date = '{{ ds }}'
    """
```

### Selective Column Update

Only update specific columns on conflict:

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["user_id", "event_date"],
    update_columns=["event_count", "last_updated"],
    source_conn="bigquery_conn"
)
def merge_user_events():
    return """
    SELECT
        user_id,
        event_date,
        COUNT(*) as event_count,
        CURRENT_TIMESTAMP() as last_updated
    FROM raw_events
    WHERE event_date = '{{ ds }}'
    GROUP BY user_id, event_date
    """
```

### Pre-Truncate Before Merge

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "reporting.current_state"),
    conflict_columns=["entity_id"],
    source_conn="bigquery_conn",
    pre_truncate=True
)
def rebuild_current_state():
    return """
    SELECT entity_id, state_value, updated_at
    FROM entity_snapshot
    WHERE is_current = true
    """
```

### With Timestamp Tracking

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "audit.change_log"),
    conflict_columns=["change_id"],
    timestamp_column="loaded_at",
    source_conn="bigquery_conn"
)
def sync_changes():
    return """
    SELECT change_id, entity, action
    FROM change_events
    WHERE processed = false
    """
```

### Using SQL Files

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    sql_file="sql/user_merge.sql",
    source_conn="bigquery_conn"
)
def merge_users(run_date):
    pass
```

### Dry Run Mode

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "test.preview"),
    conflict_columns=["id"],
    source_conn="bigquery_conn",
    dry_run=True
)
def preview_merge():
    return "SELECT * FROM updates LIMIT 100"
```

## How It Works

1. Extracts data from the source database using SQL
2. Loads results into a pandas DataFrame
3. Performs a merge/upsert operation:
   - If conflict columns match: UPDATE specified columns
   - If no match: INSERT new row
4. Returns a summary string with row count

## TaskFlow Integration

```python
from airflow.decorators import dag
from airsql import sql, Table

@dag(schedule="@daily")
def my_dag():

    @sql.extract_and_merge(
        output_table=Table("postgres_conn", "warehouse.dimensions"),
        conflict_columns=["dim_id"],
        source_conn="bigquery_conn"
    )
    def sync_dimensions():
        return "SELECT * FROM dim_updates WHERE date = '{{ ds }}'"

    @sql.query(
        output_table=Table("postgres_conn", "reports.fact_table")
    )
    def process_facts():
        return "SELECT * FROM source_facts WHERE date = '{{ ds }}'"

    dimensions = sync_dimensions()
    facts = process_facts()
    facts.set_upstream(dimensions)

my_dag()
```

## Related Operations

- [`@sql.extract_and_load`](extract-and-load.md) - Extract and load (no merge)
- [`@sql.merge`](merge.md) - Merge SQL query results
- [`@sql.merge_dataframe`](merge-dataframe.md) - Merge DataFrame directly