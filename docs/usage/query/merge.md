# Merge/Upsert

The `@sql.merge` decorator and `SQLMergeOperator` execute SQL and merge (upsert) results into a table, updating existing rows or inserting new ones based on conflict columns.

## Operator Usage

```python
from airsql.operators import SQLMergeOperator
from airsql import Table

merge_task = SQLMergeOperator(
    task_id="upsert_users",
    sql="SELECT id, name, email, updated_at FROM staging_users",
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    update_columns=["name", "email", "updated_at"],
    source_conn="postgres_conn",
    pre_truncate=False
)

merge_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to execute |
| `output_table` | `Table` | Yes | Destination table reference |
| `conflict_columns` | `List[str]` | Yes | Columns for conflict detection (ON clause) |
| `update_columns` | `List[str]` | No | Columns to update on conflict (default: all non-conflict) |
| `source_conn` | `str` | Yes | Connection ID for the source database |
| `pre_truncate` | `bool` | No | If True, truncate before merge |
| `dry_run_flag` | `bool` | No | If True, simulate without writing |
| `dynamic_params` | `dict` | No | Parameters for dynamic task mapping |

## Decorator Usage

```python
from airsql import sql, Table

@sql.merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    source_conn="postgres_conn"
)
def upsert_users():
    return "SELECT id, name, email, updated_at FROM staging_users"

merge_task = upsert_users()
```

## Examples

### Single Column Conflict

```python
@sql.merge(
    output_table=Table("postgres_conn", "public.products"),
    conflict_columns=["sku"],
    source_conn="postgres_conn"
)
def sync_products():
    return """
    SELECT sku, name, price, updated_at
    FROM staging_products
    """
```

### Composite Key Conflict

```python
@sql.merge(
    output_table=Table("postgres_conn", "analytics.daily_metrics"),
    conflict_columns=["metric_date", "metric_name"],
    source_conn="postgres_conn"
)
def update_metrics():
    return """
    SELECT 
        metric_date,
        metric_name,
        metric_value,
        updated_at
    FROM raw_metrics
    """
```

### Selective Column Update

Only update specific columns on conflict:

```python
@sql.merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    update_columns=["name", "email", "last_login"],
    source_conn="postgres_conn"
)
def update_user_profiles():
    return """
    SELECT id, name, email, last_login, created_at
    FROM user_updates
    """
```

### Pre-Truncate Before Merge

```python
@sql.merge(
    output_table=Table("postgres_conn", "reporting.current_state"),
    conflict_columns=["entity_id"],
    pre_truncate=True,
    source_conn="postgres_conn"
)
def rebuild_current_state():
    return """
    SELECT entity_id, state_value, updated_at
    FROM entity_snapshot
    WHERE is_current = true
    """
```

### Merge from BigQuery

```python
@sql.merge(
    output_table=Table("postgres_conn", "warehouse.synced_data"),
    conflict_columns=["id"]
)
def merge_from_bigquery(source_table):
    return "SELECT * FROM {{ source_table }}"

task = merge_from_bigquery(
    source_table=Table("bigquery_conn", "analytics.export_data")
)
```

### Using SQL Files

```python
@sql.merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["user_id", "event_date"],
    update_columns=["event_count", "last_updated"],
    sql_file="sql/user_events_merge.sql"
)
def merge_user_events(run_date):
    pass
```

## How Merge Works

1. Executes the SQL query to get the source data
2. Loads results into a pandas DataFrame
3. For each row in the DataFrame:
   - If `conflict_columns` match existing row: UPDATE specified columns
   - If no match: INSERT new row
4. Returns the table reference as a string

### PostgreSQL Example (Generated SQL)

```sql
INSERT INTO users (id, name, email, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (id) 
DO UPDATE SET 
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at
```

## Use Cases

| Scenario | Conflict Columns | Update Columns |
|----------|-----------------|----------------|
| User sync | `user_id` | All except `created_at` |
| Daily metrics | `date`, `metric_name` | `value` only |
| Product inventory | `sku` | `quantity`, `price` |
| Event deduplication | `event_id` | None (insert only) |

## Temporary Tables

Temporary tables are automatically dropped after merge:

```python
temp_table = Table("postgres_conn", "temp.staging", temporary=True)

@sql.merge(
    output_table=temp_table,
    conflict_columns=["id"]
)
def process_batch():
    return "SELECT * FROM batch_data"
```

## Related Operations

- [`@sql.query`](query.md) - Write results to table
- [`@sql.append`](append.md) - Append without conflict detection
- [`@sql.merge_dataframe`](merge-dataframe.md) - Merge DataFrame directly