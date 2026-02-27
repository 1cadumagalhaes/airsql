---
icon: lucide/lightbulb
---

# Core Concepts

Understanding the fundamental concepts of AirSQL is essential for effective usage.

## Tables

The `Table` class is central to AirSQL's approach to database interactions. Unlike traditional approaches that use plain string table names, AirSQL's `Table` objects encapsulate both the table reference and connection information.

### Basic Table Usage

```python
from airsql import Table

# Simple table reference
users_table = Table(
    conn_id="postgres_conn", 
    table_name="public.users"
)

# BigQuery table with advanced configuration
analytics_table = Table(
    conn_id="bigquery_conn",
    table_name="analytics.user_events",
    project="my-gcp-project",
    partition_by="event_date",
    cluster_by=["user_id", "event_type"],
    location="US"
)
```

### Table Properties

Tables can represent various database objects:

- Regular tables
- Views
- Temporary tables
- Partitioned tables (BigQuery)
- Sharded tables

### Cross-Database References

One powerful feature is the ability to reference tables from different databases within the same workflow:

```python
@sql.dataframe
def analyze_user_behavior(postgres_users, bigquery_events):
    return """
    SELECT 
        u.id,
        u.name,
        COUNT(e.event_id) as event_count
    FROM {{ postgres_users }} u
    LEFT JOIN {{ bigquery_events }} e ON u.id = e.user_id
    GROUP BY u.id, u.name
    """

# Usage in DAG
analysis = analyze_user_behavior(
    postgres_users=Table("postgres_conn", "public.users"),
    bigquery_events=Table("bigquery_conn", "analytics.user_events")
)
```

## Files and Templates

AirSQL uses `File` objects for managing SQL files with Jinja templating support.

### File Locations

SQL files can be located in several places:

- Absolute paths
- Relative to the calling DAG's directory
- In configured search paths (`dags/git_sql`, `sql/`, `dags/sql`)

### Template Variables

Templates can use variables from:

- Function arguments
- Explicitly provided template variables
- Runtime context

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn",
    sql_file="queries/daily_report.sql",
    region="us-west",  # Available in template as {{ region }}
    environment="production"
)
def daily_report(date_param):  # Available in template as {{ date_param }}
    # Variables passed to template:
    # - date_param (from function argument)
    # - region (from decorator)
    # - environment (from decorator)
    pass
```

## Connections

AirSQL leverages Airflow's connection management system. Define connections in the Airflow UI or configuration files.

### Supported Databases

Currently supported database backends:

- PostgreSQL
- BigQuery
- Google Cloud Storage (for transfers)

### Connection Configuration

Connections should be configured with appropriate credentials for each database type. AirSQL automatically detects the connection type based on the `conn_type` field.

## Decorators vs Direct Operators

AirSQL provides two ways to create operators:

1. **Decorators** - Higher-level, Pythonic approach
2. **Direct Operators** - Lower-level, explicit class instantiation

### Decorator Approach

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.summary"),
    source_conn="postgres_conn"
)
def create_summary():
    return "SELECT COUNT(*) as total FROM orders"
```

### Direct Operator Approach

```python
from airsql.operators import SQLQueryOperator
from airsql import Table

summary_task = SQLQueryOperator(
    task_id="create_summary",
    sql="SELECT COUNT(*) as total FROM orders",
    output_table=Table("postgres_conn", "reports.summary"),
    source_conn="postgres_conn"
)
```

The decorator approach is recommended for most use cases as it provides a cleaner syntax and handles many common patterns automatically.

## Dynamic Task Mapping

AirSQL supports Airflow's dynamic task mapping through the `dynamic_params` parameter:

```python
@sql.query(
    output_table=Table("postgres_conn", "reports.{{ region }}_summary"),
    source_conn="postgres_conn"
)
def regional_summary(region):
    return f"SELECT COUNT(*) as total FROM orders WHERE region = '{region}'"

# Dynamic mapping
regions = ["us-east", "us-west", "eu-central"]
tasks = regional_summary.expand(region=regions)
```

This enables creating multiple task instances dynamically based on input data.