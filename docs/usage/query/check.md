# Data Quality Checks

The `@sql.check` decorator and `SQLCheckOperator` execute SQL data quality checks that pass if all returned values are truthy.

## Operator Usage

```python
from airsql.operators import SQLCheckOperator

check_task = SQLCheckOperator(
    task_id="check_no_nulls",
    sql="SELECT COUNT(*) = 0 FROM users WHERE id IS NULL",
    source_conn="postgres_conn",
    retries=2
)

check_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL check query |
| `source_conn` | `str` | Yes | Connection ID for the database |
| `retries` | `int` | No | Number of retries (default: 1) |
| `conn_id` | `str` | No | Alternative connection parameter |

## Decorator Usage

```python
from airsql import sql

@sql.check(conn_id="postgres_conn")
def check_no_nulls():
    return "SELECT COUNT(*) = 0 FROM users WHERE id IS NULL"

check_task = check_no_nulls()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `conn_id` | `str` | No | Connection ID for the database |
| `source_conn` | `str` | No | Alternative connection parameter |
| `sql_file` | `str` | No | Path to SQL file |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Null Check

```python
@sql.check(conn_id="postgres_conn")
def test_no_null_ids():
    return "SELECT COUNT(*) = 0 FROM users WHERE id IS NULL"
```

### Row Count Check

```python
@sql.check(conn_id="postgres_conn")
def test_has_data():
    return "SELECT COUNT(*) > 0 FROM daily_data WHERE date = '{{ ds }}'"
```

### Data Range Check

```python
@sql.check(conn_id="bigquery_conn")
def test_value_range():
    return """
    SELECT COUNT(*) = 0 
    FROM metrics 
    WHERE value < 0 OR value > 1000
    """
```

### Uniqueness Check

```python
@sql.check(conn_id="postgres_conn")
def test_unique_emails():
    return """
    SELECT COUNT(*) = COUNT(DISTINCT email)
    FROM users
    """
```

### Referential Integrity Check

```python
@sql.check(conn_id="postgres_conn")
def test_foreign_keys():
    return """
    SELECT COUNT(*) = 0
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.id
    WHERE u.id IS NULL
    """
```

### Multiple Checks

```python
@sql.check(conn_id="postgres_conn")
def test_data_quality():
    return """
    SELECT 
        (SELECT COUNT(*) > 0 FROM users) as has_users,
        (SELECT COUNT(*) = 0 FROM users WHERE email IS NULL) as no_null_emails,
        (SELECT COUNT(*) = COUNT(DISTINCT email) FROM users) as unique_emails
    """
```

### Using Table References

```python
from airsql import sql, Table

@sql.check(conn_id="postgres_conn")
def test_table_quality(table):
    return f"""
    SELECT COUNT(*) = 0 
    FROM {{ table }} 
    WHERE id IS NULL OR created_at IS NULL
    """

check = test_table_quality(table=Table("postgres_conn", "public.orders"))
```

### Using SQL Files

```python
@sql.check(
    conn_id="postgres_conn",
    sql_file="checks/daily_quality_check.sql"
)
def daily_quality_check(date_param):
    pass
```

### With Template Variables

```python
@sql.check(
    conn_id="bigquery_conn",
    min_rows=1000
)
def test_min_rows(date_filter):
    return """
    SELECT COUNT(*) >= {{ min_rows }}
    FROM events
    WHERE DATE(event_time) = '{{ date_filter }}'
    """
```

## dbt Test Compatibility

AirSQL checks are compatible with dbt test patterns:

### Not Null Test

```python
@sql.check(conn_id="postgres_conn")
def test_id_not_null(table):
    return "SELECT COUNT(*) = 0 FROM {{ table }} WHERE id IS NULL"
```

### Unique Test

```python
@sql.check(conn_id="postgres_conn")
def test_id_unique(table):
    return "SELECT COUNT(*) = COUNT(DISTINCT id) FROM {{ table }}"
```

### Accepted Values Test

```python
@sql.check(conn_id="postgres_conn")
def test_status_values(table):
    return """
    SELECT COUNT(*) = 0 
    FROM {{ table }} 
    WHERE status NOT IN ('active', 'inactive', 'pending')
    """
```

## Behavior

The check passes when:
1. The query returns at least one row
2. All values in the returned row(s) are truthy (evaluate to `True` in Python)

The check fails when:
1. The query returns no rows
2. Any value in the returned row(s) is falsy (`0`, `NULL`, `FALSE`, empty string)

## Retries

Add retry logic for transient issues:

```python
from datetime import timedelta

@sql.check(conn_id="postgres_conn")
def check_with_retry():
    return "SELECT COUNT(*) > 0 FROM late_arriving_data"

check_task = check_with_retry()
check_task.retries = 3
check_task.retry_delay = timedelta(minutes=5)
```

## Related Operations

- Sensors for monitoring data availability: [Sensors Guide](../sensors/index.md)