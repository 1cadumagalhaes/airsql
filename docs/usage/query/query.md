# Query to Table

The `@sql.query` decorator and `SQLQueryOperator` execute SQL and write results to a destination table.

## Operator Usage

```python
from airsql.operators import SQLQueryOperator
from airsql import Table

query_task = SQLQueryOperator(
    task_id="create_daily_report",
    sql="SELECT DATE(created_at) as date, COUNT(*) as total FROM orders GROUP BY 1",
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn",
    pre_truncate=False
)

query_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to execute |
| `output_table` | `Table` | Yes | Destination table reference |
| `source_conn` | `str` | Yes | Connection ID for the source database |
| `dry_run_flag` | `bool` | No | If True, simulate without writing |
| `pre_truncate` | `bool` | No | If True, truncate destination before writing |
| `dynamic_params` | `dict` | No | Parameters for dynamic task mapping |

## Decorator Usage

```python
from airsql import sql, Table

@sql.query(
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn"
)
def create_daily_report():
    return """
    SELECT DATE(created_at) as date, SUM(amount) as total
    FROM orders
    GROUP BY DATE(created_at)
    """

report_task = create_daily_report()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `output_table` | `Table` | Yes | Destination table reference |
| `source_conn` | `str` | No | Connection ID for simple queries |
| `sql_file` | `str` | No | Path to SQL file |
| `dry_run` | `bool` | No | If True, simulate without writing |
| `pre_truncate` | `bool` | No | If True, truncate before writing |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Basic Query

```python
from airsql import sql, Table

@sql.query(
    output_table=Table("postgres_conn", "analytics.user_stats"),
    source_conn="postgres_conn"
)
def get_user_statistics():
    return """
    SELECT 
        u.id,
        u.name,
        COUNT(o.id) as order_count,
        SUM(o.amount) as total_spent
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.name
    """
```

### Query with Table References

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.cross_db_analysis")
)
def analyze_cross_source(users_table, orders_table):
    return """
    SELECT 
        u.id,
        u.name,
        COUNT(o.id) as order_count
    FROM {{ users_table }} u
    LEFT JOIN {{ orders_table }} o ON u.id = o.user_id
    GROUP BY u.id, u.name
    """

task = analyze_cross_source(
    users_table=Table("postgres_conn", "public.users"),
    orders_table=Table("bigquery_conn", "analytics.orders")
)
```

### Using SQL Files

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.monthly_summary"),
    source_conn="postgres_conn",
    sql_file="queries/monthly_report.sql"
)
def monthly_report(start_date, end_date):
    pass
```

### Pre-Truncate Before Writing

```python
@sql.query(
    output_table=Table("postgres_conn", "cache.user_cache"),
    source_conn="postgres_conn",
    pre_truncate=True
)
def refresh_user_cache():
    return """
    SELECT id, name, email, last_login
    FROM users 
    WHERE last_login > CURRENT_DATE - INTERVAL '30 days'
    """
```

### Dry Run Mode

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.test_report"),
    source_conn="postgres_conn",
    dry_run=True
)
def test_query():
    return "SELECT * FROM large_table LIMIT 1000000"
```

### Jinja Templating

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.custom_report"),
    source_conn="postgres_conn",
    region='us-west'
)
def custom_report(date_filter, status_filter):
    return """
    SELECT *
    FROM orders 
    WHERE created_at >= '{{ date_filter }}'
      AND status = '{{ status_filter }}'
      AND region = '{{ region }}'
    """
```

## Behavior

1. Executes the SQL query against the source database
2. Loads results into a pandas DataFrame
3. Writes the DataFrame to the output table using append mode
4. Returns the table reference as a string

## Related Operations

- [`@sql.append`](append.md) - Append to existing table (no truncation)
- [`@sql.replace`](replace.md) - Replace entire table content
- [`@sql.truncate`](truncate.md) - Truncate table structure preserved