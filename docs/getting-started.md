---
icon: lucide/play
---

# Getting Started with AirSQL

## Installation

Install AirSQL using uv:

```bash
uv add airsql
```

Or with pip:

```bash
pip install airsql
```

## Basic Concepts

AirSQL provides a decorator-based approach to SQL operations in Airflow. The main components are:

1. **Tables** - Database table references with connection information
2. **Decorators** - Python decorators that create Airflow operators
3. **Files** - SQL file handling with Jinja templating

## First Example

Here's a simple example to get you started:

```python
from airsql import sql, Table

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

# Use in DAG
df_task = get_active_users()
```

## Table References

Tables are the primary way to reference database tables in AirSQL:

```python
from airsql import Table

# Simple table reference
table = Table(conn_id="postgres_conn", table_name="users.active_users")

# BigQuery table with advanced options
bq_table = Table(
    conn_id="bigquery_conn",
    table_name="analytics.user_events",
    project="my-project",
    partition_by="event_date",
    cluster_by=["user_id", "event_type"]
)
```

## Next Steps

- Learn about [query operations](guides/query/index.md)
- Explore [sensor usage](guides/sensors/index.md)
- Understand [data transfer options](guides/transfers/index.md)
- Check out [complete examples](examples/index.md)