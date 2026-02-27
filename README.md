<p align="center">
  <strong>A decorator-based SQL execution framework for Apache Airflow</strong>
</p>

<p align="center">
  <a href="https://github.com/1cadumagalhaes/airsql/actions"><img
    src="https://github.com/1cadumagalhaes/airsql/actions/workflows/test.yml/badge.svg"
    alt="Build"
  /></a>
  <a href="https://pypistats.org/packages/airsql"><img
    src="https://img.shields.io/pypi/dm/airsql.svg"
    alt="Downloads"
  /></a>
  <a href="https://pypi.org/project/airsql"><img
    src="https://img.shields.io/pypi/v/airsql?logo=python&logoColor=white&label=PyPI"
    alt="Python Package Index"
  /></a>
</p>

---

## What is AirSQL?

AirSQL is a modern Python framework that provides clean, intuitive decorators for SQL operations in Airflow. It simplifies data pipeline development by offering a Python-like syntax for common data operations while maintaining full compatibility with Airflow's ecosystem.

## Features

- Decorator-based syntax for clean, Pythonic SQL operations
- Native Airflow integration using connections and patterns
- Multi-database support: PostgreSQL, BigQuery, and more
- SQL file support with Jinja templating
- Flexible outputs: DataFrames, tables, or files
- Smart operations: replace, merge/upsert, truncate
- SQL sensors with retry logic for BigQuery and PostgreSQL
- Transfer operators for BigQuery, Postgres, and GCS

## Requirements

AirSQL is built for **Apache Airflow 3** and is not compatible with Airflow 2.x. API changes are planned, but no roadmap is set yet.

## Installation

```bash
pip install airsql
```

Or with uv:

```bash
uv add airsql
```

## Quick Start

### Basic DataFrame Query

```python
from airsql import sql

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

# Use in DAG
df_task = get_active_users()
```

### Query with Table Output

```python
from airsql import sql, Table

@sql.query(output_table=Table("postgres_conn", "reports.daily_summary"))
def create_daily_report():
    return """
    SELECT DATE(created_at) as date, COUNT(*) as total
    FROM orders
    GROUP BY DATE(created_at)
    """

report_task = create_daily_report()
```

### Replace Table Content

```python
@sql.replace(output_table=Table("postgres_conn", "cache.user_cache"))
def refresh_cache():
    return """
    SELECT * FROM users 
    WHERE last_login > CURRENT_DATE - INTERVAL '30 days'
    """

cache_task = refresh_cache()
```

### Merge/Upsert

```python
@sql.merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    update_columns=["name", "email", "updated_at"]
)
def sync_users():
    return "SELECT id, name, email, updated_at FROM staging_users"

sync_task = sync_users()
```

### Data Quality Checks

```python
@sql.check(conn_id="postgres_conn")
def test_no_nulls():
    return "SELECT COUNT(*) = 0 FROM users WHERE id IS NULL"

@sql.check(conn_id="bigquery_conn")
def test_row_count():
    return "SELECT COUNT(*) > 0 FROM daily_data WHERE date = '{{ ds }}'"
```

## Query Operations

AirSQL provides decorators for SQL query operations. Each operation type is available as both a decorator and a direct operator class.

### Return DataFrame

Execute SQL and return a pandas DataFrame:

```python
from airsql import sql

@sql.dataframe(source_conn="postgres_conn")
def get_user_stats():
    return """
    SELECT COUNT(*) as total_users, 
           AVG(age) as avg_age
    FROM users
    """
```

### Query to Table

Execute SQL and write results to a table:

```python
@sql.query(
    output_table=Table("postgres_conn", "analytics.user_stats"),
    source_conn="postgres_conn"
)
def calculate_stats():
    return """
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
    """
```

### Append to Table

Append query results to an existing table:

```python
@sql.append(output_table=Table("postgres_conn", "archive.events"))
def archive_daily():
    return """
    SELECT * FROM events 
    WHERE created_at >= '{{ ds }}'
    """
```

### Replace Table

Replace entire table content:

```python
@sql.replace(output_table=Table("postgres_conn", "reports.current_state"))
def refresh_state():
    return "SELECT * FROM current_state_view"
```

### Truncate and Reload

Truncate table (preserving structure) and insert new data:

```python
@sql.truncate(output_table=Table("postgres_conn", "staging.daily_import"))
def reload_daily():
    return """
    SELECT * FROM source_data 
    WHERE import_date = '{{ ds }}'
    """
```

### Merge/Upsert

Merge with conflict resolution:

```python
@sql.merge(
    output_table=Table("postgres_conn", "public.products"),
    conflict_columns=["sku"],
    update_columns=["name", "price", "quantity"]
)
def sync_products():
    return "SELECT sku, name, price, quantity FROM product_updates"
```

### Load DataFrame Directly

Load a pandas DataFrame into a table:

```python
import pandas as pd

@sql.load_dataframe(
    output_table=Table("postgres_conn", "staging.metrics"),
    if_exists="replace"
)
def generate_metrics():
    return pd.DataFrame({
        'metric': ['users', 'orders'],
        'value': [1000, 500]
    })
```

### Merge DataFrame

Merge DataFrame with upsert logic:

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "public.inventory"),
    conflict_columns=["sku"]
)
def update_inventory():
    return pd.DataFrame({
        'sku': ['SKU001', 'SKU002'],
        'quantity': [100, 50],
        'price': [19.99, 29.99]
    })
```

### Extract and Load

Extract via SQL and load into a table in one task:

```python
@sql.extract_and_load(
    output_table=Table("postgres_conn", "warehouse.events"),
    source_conn="bigquery_conn",
    if_exists="replace"
)
def sync_events():
    return """
    SELECT * FROM analytics.events 
    WHERE DATE(event_time) = '{{ ds }}'
    """
```

### Extract and Merge

Extract via SQL and merge with upsert:

```python
@sql.extract_and_merge(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    source_conn="bigquery_conn"
)
def sync_users():
    return "SELECT id, name, email FROM staging_users"
```

## Table References

The `Table` class encapsulates database table references with connection information:

### Basic Usage

```python
from airsql import Table

# Simple reference
users = Table(conn_id="postgres_conn", table_name="public.users")

# With dataset (BigQuery style)
events = Table(conn_id="bigquery_conn", table_name="analytics.events")

# Full reference
full_table = Table(
    conn_id="bigquery_conn",
    table_name="analytics.events",
    project="my-project",
    location="US"
)
```

### Table Parameters

```python
@sql.query(output_table=Table("postgres_conn", "reports.{{ region }}_summary"))
def regional_summary(region):
    return f"""
    SELECT * FROM orders 
    WHERE region = '{region}'
    """

us_task = regional_summary(region="us")
eu_task = regional_summary(region="eu")
```

### Cross-Database Queries

Join tables from different databases:

```python
@sql.dataframe
def analyze_cross_source(users_table, events_table):
    return """
    SELECT u.id, u.name, COUNT(e.id) as event_count
    FROM {{ users_table }} u
    LEFT JOIN {{ events_table }} e ON u.id = e.user_id
    GROUP BY u.id, u.name
    """

analysis = analyze_cross_source(
    users_table=Table("postgres_conn", "public.users"),
    events_table=Table("bigquery_conn", "analytics.events")
)
```

## SQL Files and Templates

Store SQL in separate files with Jinja templating:

### Using SQL Files

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.complex_report"),
    sql_file="sql/complex_report.sql"
)
def generate_report(start_date, end_date):
    pass
```

### Template Variables

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.filtered"),
    region="us-west",
    status="active"
)
def filtered_data(date_filter):
    return """
    SELECT * FROM orders 
    WHERE region = '{{ region }}'
      AND status = '{{ status }}'
      AND date = '{{ date_filter }}'
    """
```

## Direct Operator Usage

For maximum control, use operators directly:

```python
from airsql.operators import (
    SQLQueryOperator,
    SQLDataFrameOperator,
    SQLReplaceOperator,
    SQLMergeOperator,
)
from airsql import Table

df_task = SQLDataFrameOperator(
    task_id="get_users",
    sql="SELECT * FROM users WHERE active = true",
    source_conn="postgres_conn"
)

query_task = SQLQueryOperator(
    task_id="create_report",
    sql="SELECT * FROM orders GROUP BY date",
    output_table=Table("postgres_conn", "reports.daily"),
    source_conn="postgres_conn"
)

replace_task = SQLReplaceOperator(
    task_id="refresh_cache",
    sql="SELECT * FROM active_users",
    output_table=Table("postgres_conn", "cache.users"),
    source_conn="postgres_conn"
)

merge_task = SQLMergeOperator(
    task_id="upsert_products",
    sql="SELECT * FROM product_updates",
    output_table=Table("postgres_conn", "public.products"),
    conflict_columns=["sku"],
    source_conn="postgres_conn"
)
```

## Sensors

SQL-based sensors that wait for conditions:

### PostgreSQL Sensor

```python
from airsql.sensors import PostgresSqlSensor

wait_for_data = PostgresSqlSensor(
    task_id='wait_for_data',
    sql="SELECT COUNT(*) > 0 FROM daily_data WHERE date = '{{ ds }}'",
    conn_id='postgres_default',
    retries=3,
    poke_interval=60
)
```

### BigQuery Sensor

```python
from airsql.sensors import BigQuerySqlSensor

wait_for_etl = BigQuerySqlSensor(
    task_id='wait_for_etl',
    sql="SELECT COUNT(*) > 0 FROM staging WHERE processed = true",
    conn_id='bigquery_default',
    location='us-central1'
)
```

## Transfer Operators

Move data between systems:

### BigQuery to PostgreSQL

```python
from airsql.transfers import BigQueryToPostgresOperator

transfer = BigQueryToPostgresOperator(
    task_id='bq_to_pg',
    source_project_dataset_table='my-project.analytics.users',
    destination_table='warehouse.users',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket'
)
```

### PostgreSQL to BigQuery

```python
from airsql.transfers import PostgresToBigQueryOperator

export = PostgresToBigQueryOperator(
    task_id='pg_to_bq',
    sql='SELECT * FROM orders WHERE date = {{ ds }}',
    destination_project_dataset_table='my-project.staging.orders',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket'
)
```

### PostgreSQL to GCS

```python
from airsql.transfers import PostgresToGCSOperator

export = PostgresToGCSOperator(
    task_id='export_to_gcs',
    sql='SELECT * FROM large_table',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='exports/{{ ds }}/data.csv'
)
```

### GCS to PostgreSQL

```python
from airsql.transfers import GCSToPostgresOperator

import_task = GCSToPostgresOperator(
    task_id='import_from_gcs',
    gcs_bucket='data-bucket',
    object_name='imports/users.jsonl',
    target_table_name='staging.users',
    postgres_conn_id='postgres_default'
)
```

## Complete DAG Example

```python
from airflow.decorators import dag
from airsql import sql, Table
from airsql.sensors import PostgresSqlSensor
from airsql.transfers import PostgresToBigQueryOperator
from datetime import datetime

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def analytics_pipeline():

    wait_for_source = PostgresSqlSensor(
        task_id='wait_for_source',
        sql="SELECT COUNT(*) > 0 FROM raw_events WHERE date = '{{ ds }}'",
        conn_id='postgres_default'
    )

    @sql.query(output_table=Table("postgres_conn", "staging.daily_events"))
    def stage_events():
        return """
        SELECT * FROM raw_events 
        WHERE date = '{{ ds }}'
        """

    @sql.merge(
        output_table=Table("postgres_conn", "warehouse.events"),
        conflict_columns=["event_id"],
        update_columns=["status", "updated_at"]
    )
    def merge_events():
        return """
        SELECT * FROM staging.daily_events
        """

    @sql.check(conn_id="postgres_conn")
    def validate_events():
        return """
        SELECT COUNT(*) > 0 
        FROM warehouse.events 
        WHERE date = '{{ ds }}'
        """

    export_to_bq = PostgresToBigQueryOperator(
        task_id='export_to_bigquery',
        sql="SELECT * FROM warehouse.events WHERE date = '{{ ds }}'",
        destination_project_dataset_table='my-project.analytics.events',
        postgres_conn_id='postgres_default',
        gcs_bucket='temp-bucket'
    )

    wait_for_source >> stage_events() >> merge_events() >> validate_events() >> export_to_bq

analytics_pipeline()
```

## TaskFlow API Integration

AirSQL decorators work seamlessly with Airflow's TaskFlow API:

```python
from airflow.decorators import dag, task
from airsql import sql, Table

@dag(schedule="@daily")
def my_dag():

    @sql.dataframe(source_conn="postgres_conn")
    def extract_users():
        return "SELECT * FROM users WHERE active = true"

    @task
    def process_users(df):
        return df.groupby("region").size().to_dict()

    @sql.load_dataframe(
        output_table=Table("postgres_conn", "reports.user_counts"),
        if_exists="replace"
    )
    def save_counts(counts):
        import pandas as pd
        return pd.DataFrame([
            {"region": k, "count": v} for k, v in counts.items()
        ])

    users_df = extract_users()
    counts = process_users(users_df)
    save_counts(counts)

my_dag()
```

## Dry Run Mode

Test operations without writing data:

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.test"),
    source_conn="postgres_conn",
    dry_run=True
)
def test_query():
    return "SELECT * FROM large_table LIMIT 1000000"
```

## Documentation

For full documentation, visit the [AirSQL Documentation](https://1cadumagalhaes.github.io/airsql/).

## Migration from retize.sql

This package is the evolution of `retize.sql`. Main changes:

- Package renamed from `retize.sql` to `airsql`
- Table class `schema` field renamed to `dataset`
- Asset URIs changed from `rtz://` to `airsql://`
- Improved organization with sensors and transfers submodules

## License

This project is licensed under the MIT License.
