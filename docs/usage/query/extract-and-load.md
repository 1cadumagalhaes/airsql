# Extract and Load

The `@sql.extract_and_load` decorator combines SQL extraction and DataFrame loading into a single TaskFlow-compatible task.

## Usage

```python
from airsql import sql, Table

@sql.extract_and_load(
    output_table=Table("postgres_conn", "analytics.daily_summary"),
    source_conn="bigquery_conn",
    if_exists="replace"
)
def extract_daily_summary():
    return """
    SELECT
        DATE(created_at) as summary_date,
        COUNT(*) as total_records,
        AVG(amount) as avg_amount
    FROM transactions
    WHERE DATE(created_at) = '{{ ds }}'
    GROUP BY 1
    """

load_task = extract_daily_summary()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `output_table` | `Table` | Yes | Destination table reference |
| `source_conn` | `str` | Yes | Connection ID for source database |
| `if_exists` | `str` | No | Behavior: 'append', 'replace', 'fail', 'truncate' |
| `timestamp_column` | `str` | No | Column to populate with current timestamp |
| `sql_file` | `str` | No | Path to SQL file |
| `dry_run` | `bool` | No | If True, simulate without writing |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Cross-Database Loading

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "warehouse.events"),
    source_conn="bigquery_conn",
    if_exists="replace"
)
def sync_events():
    return """
    SELECT *
    FROM analytics.events
    WHERE DATE(event_time) = '{{ ds }}'
    """
```

### Append Daily Data

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "archive.all_orders"),
    source_conn="postgres_staging",
    if_exists="append"
)
def archive_orders():
    return """
    SELECT * FROM orders
    WHERE created_at >= '{{ ds }}'
      AND created_at < '{{ tomorrow_ds }}'
    """
```

### With Timestamp Tracking

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "audit.sync_log"),
    source_conn="bigquery_conn",
    if_exists="append",
    timestamp_column="loaded_at"
)
def log_sync():
    return """
    SELECT
        table_name,
        row_count,
        CURRENT_TIMESTAMP() as sync_time
    FROM metadata.table_stats
    """
```

### Using SQL Files

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "reports.complex_report"),
    source_conn="bigquery_conn",
    sql_file="sql/complex_report.sql",
    if_exists="replace"
)
def generate_report(report_date):
    pass
```

### With Template Variables

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "reports.{{ region }}_metrics"),
    source_conn="bigquery_conn",
    if_exists="replace",
    region="us-west"
)
def regional_metrics(date_filter):
    return """
    SELECT * FROM metrics
    WHERE region = '{{ region }}'
      AND date = '{{ date_filter }}'
    """
```

### Dry Run Mode

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "test.preview"),
    source_conn="bigquery_conn",
    dry_run=True
)
def preview_data():
    return "SELECT * FROM large_table LIMIT 1000"
```

## How It Works

1. Extracts data from the source database using the SQL query
2. Loads results into a pandas DataFrame
3. Writes the DataFrame to the destination table
4. Returns a summary string with row count

## TaskFlow Integration

This decorator is TaskFlow-compatible and can be used in dependencies:

```python
from airflow.decorators import dag
from airsql import sql, Table

@dag(schedule="@daily")
def my_dag():

    @sql.extract_and_load(
        output_table=Table("postgres_conn", "staging.raw_data"),
        source_conn="bigquery_conn",
        if_exists="replace"
    )
    def extract_raw():
        return "SELECT * FROM source WHERE date = '{{ ds }}'"

    @sql.query(
        output_table=Table("postgres_conn", "reports.final")
    )
    def process(raw_data):
        return f"SELECT * FROM {raw_data} WHERE valid = true"

    raw_data = extract_raw()
    process(raw_data)

my_dag()
```

## Related Operations

- [`@sql.extract_and_merge`](extract-and-merge.md) - Extract and merge/upsert
- [`@sql.query`](query.md) - Execute SQL and write to table
- [`@sql.load_dataframe`](load-dataframe.md) - Load DataFrame directly