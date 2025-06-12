# AirSQL

A decorator-based SQL execution framework for Airflow that provides clean, Python-like syntax for data operations.

## Features

- 🎯 **Decorator-based syntax** - Clean, intuitive Python decorators for SQL operations
- 🔗 **Native Airflow integration** - Uses Airflow connections and follows Airflow patterns
- 🗃️ **Multi-database support** - Works with Postgres, BigQuery, and more
- 📄 **SQL file support** - Keep complex queries in separate `.sql` files with Jinja templating
- ⚡ **Flexible outputs** - Write to tables, return DataFrames, or save to files
- 🔄 **Smart operations** - Built-in support for replace, merge/upsert operations
- 🌐 **Cross-database queries** - Query across different databases (planned with DataFusion)
- 🔍 **Data quality checks** - Built-in SQL check operators compatible with dbt tests
- 📊 **Transfer operators** - Move data between BigQuery, Postgres, and GCS
- 👁️ **Smart sensors** - SQL sensors with retry logic for BigQuery and Postgres

## Installation

```bash
pip install airsql
```

Or if you're using uv:

```bash
uv add airsql
```

## Quick Start

### Basic Usage

#### 1. Simple DataFrame Query

```python
from airsql import sql, Table, File

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

# Use in DAG
df_task = get_active_users()
```

#### 2. Query with Table References

```python
@sql.dataframe
def user_activity_analysis(users_table, events_table):
    return """
    SELECT u.id, u.name, COUNT(e.id) as event_count
    FROM {{ users_table }} u
    LEFT JOIN {{ events_table }} e ON u.id = e.user_id
    GROUP BY u.id, u.name
    """

# Use in DAG
analysis_task = user_activity_analysis(
    users_table=Table("postgres_conn", "users.active_users"),
    events_table=Table("bigquery_conn", "analytics.user_events")
)
```

#### 3. Replace Table Content

```python
@sql.replace(output_table=Table("postgres_conn", "reports.daily_summary"))
def create_daily_report(transactions_table):
    return """
    SELECT DATE(created_at) as date, SUM(amount) as total
    FROM {{ transactions_table }}
    GROUP BY DATE(created_at)
    """

# Use in DAG
report_task = create_daily_report(
    transactions_table=Table("postgres_conn", "transactions.orders")
)
```

#### 4. Data Quality Checks

```python
@sql.check(conn_id="bigquery_conn")
def test_no_nulls(table):
    return "SELECT COUNT(*) FROM {{ table }} WHERE id IS NULL"

@sql.check(conn_id="postgres_conn")
def test_row_count(table):
    return "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM {{ table }}"

# Use in DAG
null_check = test_no_nulls(table=Table("bigquery_conn", "analytics.users"))
count_check = test_row_count(table=Table("postgres_conn", "staging.orders"))
```

#### 5. Transfer Operations

```python
from airsql import BigQueryToPostgresOperator, PostgresToBigQueryOperator

# Transfer from BigQuery to Postgres
bq_to_pg = BigQueryToPostgresOperator(
    task_id="transfer_users",
    source_project_dataset_table="my-project.analytics.users",
    postgres_conn_id="postgres_default",
    destination_table="staging.users",
    gcs_bucket="temp-bucket",
    gcp_conn_id="google_cloud_default"
)

# Transfer from Postgres to BigQuery
pg_to_bq = PostgresToBigQueryOperator(
    task_id="transfer_orders",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM orders WHERE date >= '2024-01-01'",
    destination_project_dataset_table="my-project.staging.orders",
    gcs_bucket="temp-bucket",
    gcp_conn_id="google_cloud_default"
)
```

For more examples and detailed documentation, see the [full documentation](src/airsql/).

## Migration from retize.sql

This package is the evolution of `retize.sql`. The main changes:
- Package renamed from `retize.sql` to `airsql`
- Table class `schema` field renamed to `dataset` (avoids Pydantic warnings)
- Asset URIs changed from `rtz://` to `airsql://`
- Improved organization with sensors and transfers in submodules

## License

This project is licensed under the MIT License.