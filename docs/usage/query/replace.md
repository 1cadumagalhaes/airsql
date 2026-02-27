# Replace Table

The `@sql.replace` decorator and `SQLReplaceOperator` execute SQL and completely replace the destination table content.

## Operator Usage

```python
from airsql.operators import SQLReplaceOperator
from airsql import Table

replace_task = SQLReplaceOperator(
    task_id="refresh_daily_report",
    sql="SELECT DATE(created_at) as date, COUNT(*) as total FROM orders GROUP BY 1",
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn"
)

replace_task.execute(context)
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

@sql.replace(
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn"
)
def refresh_daily_report():
    return """
    SELECT DATE(created_at) as date, COUNT(*) as total
    FROM orders
    GROUP BY DATE(created_at)
    """

report_task = refresh_daily_report()
```

## Examples

### Cache Refresh

```python
@sql.replace(
    output_table=Table("postgres_conn", "cache.active_users_cache"),
    source_conn="postgres_conn"
)
def refresh_user_cache():
    return """
    SELECT id, name, email, last_login
    FROM users 
    WHERE last_login > CURRENT_DATE - INTERVAL '30 days'
    """
```

### Materialized View Replacement

```python
@sql.replace(
    output_table=Table("bigquery_conn", "reporting.mart_user_metrics")
)
def rebuild_user_metrics():
    return """
    SELECT 
        user_id,
        COUNT(*) as total_orders,
        SUM(amount) as total_spent,
        MAX(order_date) as last_order_date
    FROM orders
    GROUP BY user_id
    """
```

### Cross-Database Replacement

```python
@sql.replace(
    output_table=Table("postgres_conn", "warehouse.external_data")
)
def sync_from_bigquery(source_table):
    return f"SELECT * FROM {{ source_table }}"

task = sync_from_bigquery(
    source_table=Table("bigquery_conn", "analytics.user_scores")
)
```

### Date-Partitioned Refresh

```python
@sql.replace(
    output_table=Table("postgres_conn", "reports.current_month_summary")
)
def monthly_summary():
    return """
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        COUNT(*) as total_orders,
        SUM(amount) as revenue
    FROM orders
    WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1
    """
```

### Using SQL Files

```python
@sql.replace(
    output_table=Table("postgres_conn", "analytics.kpi_dashboard"),
    sql_file="sql/kpi_dashboard.sql"
)
def refresh_kpis(report_date):
    pass
```

## Replace vs Truncate

| Operation | Behavior | Use Case |
|-----------|----------|----------|
| `replace` | Drops table and recreates with new data | Schema might change; complete refresh |
| `truncate` | Truncates table, preserves structure | Same schema; just new data |

The `replace` operation:

1. Drops the existing table completely
2. Creates a new table with the query results
3. Does NOT preserve indexes, constraints, or defaults

## Behavior

1. Executes the SQL query against the source database
2. Loads results into a pandas DataFrame
3. Drops the existing table
4. Creates a new table with the DataFrame data
5. Returns the table reference as a string

## Important Notes

- The table is **dropped and recreated** - all indexes, constraints, and defaults are lost
- Use `@sql.truncate` if you want to preserve table structure
- The operation is atomic - either complete success or no changes

## Related Operations

- [`@sql.truncate`](truncate.md) - Truncate while preserving structure
- [`@sql.query`](query.md) - Append mode with optional pre-truncate
- [`@sql.append`](append.md) - Add to existing data