# Return DataFrame

The `@sql.dataframe` decorator and `SQLDataFrameOperator` execute SQL queries and return the results as a pandas DataFrame.

## Operator Usage

```python
from airsql.operators import SQLDataFrameOperator

df_task = SQLDataFrameOperator(
    task_id="get_active_users",
    sql="SELECT * FROM users WHERE active = true",
    source_conn="postgres_conn"
)

df_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to execute |
| `source_conn` | `str` | Yes | Connection ID for the source database |
| `dynamic_params` | `dict` | No | Parameters for dynamic task mapping |

## Decorator Usage

```python
from airsql import sql

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

df_task = get_active_users()
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_conn` | `str` | No | Connection ID for simple queries |
| `sql_file` | `str` | No | Path to SQL file |
| `**template_vars` | `Any` | No | Variables passed to Jinja template |

## Examples

### Basic DataFrame Query

```python
from airsql import sql

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

# Use in DAG
with DAG("example_dag", ...) as dag:
    df_task = get_active_users()
```

### Query with Table References

```python
from airsql import sql, Table

@sql.dataframe
def analyze_user_behavior(users_table, events_table):
    return """
    SELECT 
        u.id,
        u.name,
        COUNT(e.event_id) as event_count
    FROM {{ users_table }} u
    LEFT JOIN {{ events_table }} e ON u.id = e.user_id
    GROUP BY u.id, u.name
    """

# Use with different database tables
analysis = analyze_user_behavior(
    users_table=Table("postgres_conn", "public.users"),
    events_table=Table("bigquery_conn", "analytics.user_events")
)
```

### Using SQL Files

```python
@sql.dataframe(
    source_conn="postgres_conn",
    sql_file="queries/complex_analysis.sql"
)
def complex_analysis(start_date, end_date):
    pass
```

### Using in TaskFlow API

```python
from airflow.decorators import dag, task
from airsql import sql

@dag(schedule="@daily")
def my_dag():

    @sql.dataframe(source_conn="postgres_conn")
    def extract_users():
        return "SELECT * FROM users WHERE created_at >= '{{ ds }}'"

    @task
    def process_users(df):
        return df.groupby("region").size().to_dict()

    @task
    def save_results(results):
        print(f"Users by region: {results}")

    users_df = extract_users()
    processed = process_users(users_df)
    save_results(processed)

my_dag()
```

### Cross-Database Queries

```python
@sql.dataframe
def compare_databases(source_a, source_b):
    return """
    WITH a_data AS (
        SELECT 'source_a' as source, COUNT(*) as count FROM {{ source_a }}
    ),
    b_data AS (
        SELECT 'source_b' as source, COUNT(*) as count FROM {{ source_b }}
    )
    SELECT * FROM a_data
    UNION ALL
    SELECT * FROM b_data
    """

result = compare_databases(
    source_a=Table("postgres_prod", "public.orders"),
    source_b=Table("postgres_staging", "public.orders")
)
```

## Returns

The decorated function returns an Airflow operator that, when executed, returns a `pandas.DataFrame` containing the query results.

## Important Notes

- The `source_conn` parameter is required for queries without table parameters
- When using table parameters, the connection is inferred from the `Table` references
- The DataFrame uses PyArrow as the dtype backend for better performance
- Results are not cached; each execution runs the query fresh