# Truncate and Reload

The `@sql.truncate` decorator and `SQLTruncateOperator` truncate a table's data while preserving its structure, then insert new data.

## Operator Usage

```python
from airsql.operators import SQLTruncateOperator
from airsql import Table

truncate_task = SQLTruncateOperator(
    task_id="reload_daily_data",
    sql="SELECT * FROM source WHERE date = '{{ ds }}'",
    output_table=Table("postgres_conn", "warehouse.daily_events"),
    source_conn="postgres_conn"
)

truncate_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to execute |
| `output_table` | `Table` | Yes | Destination table reference |
| `source_conn` | `str` | Yes | Connection ID for the source database |
| `dry_run_flag` | `bool` | No | If True, simulate without writing |
| `dynamic_params` | `dict` | No | Parameters for dynamic task mapping |

## Decorator Usage

```python
from airsql import sql, Table

@sql.truncate(
    output_table=Table("postgres_conn", "warehouse.daily_events"),
    source_conn="postgres_conn"
)
def reload_daily_data():
    return "SELECT * FROM source WHERE date = '{{ ds }}'"

reload_task = reload_daily_data()
```

## Examples

### Daily Partition Reload

```python
@sql.truncate(
    output_table=Table("postgres_conn", "analytics.today_events")
)
def reload_todays_events():
    return """
    SELECT * FROM raw_events 
    WHERE DATE(event_time) = CURRENT_DATE
    """
```

### Staging Table Refresh

```python
@sql.truncate(
    output_table=Table("postgres_conn", "staging.import_data"),
    source_conn="postgres_conn"
)
def refresh_staging():
    return """
    SELECT * FROM external_source
    WHERE processed = false
    """
```

### Cache Table Reload

```python
@sql.truncate(
    output_table=Table("postgres_conn", "cache.user_summary"),
    source_conn="postgres_conn"
)
def rebuild_cache():
    return """
    SELECT 
        user_id,
        COUNT(*) as event_count,
        MAX(event_time) as last_event
    FROM events
    WHERE event_time > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY user_id
    """
```

### Cross-Database Reload

```python
@sql.truncate(
    output_table=Table("postgres_conn", "reporting.bq_sync")
)
def sync_from_bigquery(source):
    return "SELECT * FROM {{ source }}"

task = sync_from_bigquery(
    source=Table("bigquery_conn", "analytics.user_metrics")
)
```

## Truncate vs Replace

| Operation | Structure | Indexes | Constraints | Sequences |
|-----------|-----------|---------|-------------|-----------|
| `truncate` | Preserved | Preserved | Preserved | Reset |
| `replace` | Recreated | Lost | Lost | Lost |

Use `truncate` when:
- You want to preserve table structure (indexes, constraints)
- The schema is stable and you just need fresh data
- You want to maintain existing grants and permissions

## Behavior

1. Truncates the target table (faster than DELETE)
2. Resets sequences (auto-increment counters)
3. Preserves table structure, indexes, and constraints
4. Inserts the new data
5. Returns the table reference as a string

## Temporary Tables

When using tables marked as `temporary=True`, they are automatically dropped after the operation:

```python
temp_table = Table("postgres_conn", "temp.staging", temporary=True)

@sql.truncate(output_table=temp_table)
def stage_data():
    return "SELECT * FROM raw_data"
```

## Related Operations

- [`@sql.replace`](replace.md) - Drop and recreate table
- [`@sql.query`](query.md) - Query with optional pre-truncate
- [`@sql.append`](append.md) - Add to existing data