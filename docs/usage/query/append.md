# Append to Table

The `@sql.append` decorator and `SQLAppendOperator` execute SQL and append results to an existing table.

## Operator Usage

```python
from airsql.operators import SQLAppendOperator
from airsql import Table

append_task = SQLAppendOperator(
    task_id="append_daily_data",
    sql="SELECT * FROM source WHERE date = '{{ ds }}'",
    output_table=Table("postgres_conn", "warehouse.daily_events"),
    source_conn="postgres_conn"
)

append_task.execute(context)
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

@sql.append(
    output_table=Table("postgres_conn", "warehouse.daily_events"),
    source_conn="postgres_conn"
)
def append_daily_data():
    return "SELECT * FROM source_table WHERE date = '{{ ds }}'"

append_task = append_daily_data()
```

## Examples

### Incremental Data Loading

```python
@sql.append(
    output_table=Table("postgres_conn", "analytics.events_archive"),
    source_conn="postgres_conn"
)
def archive_events():
    return """
    SELECT * FROM events 
    WHERE created_at >= '{{ ds }}' 
      AND created_at < '{{ tomorrow_ds }}'
    """
```

### Multi-Source Append

```python
@sql.append(
    output_table=Table("bigquery_conn", "warehouse.combined_data")
)
def combine_sources(source_a, source_b):
    return """
    SELECT * FROM {{ source_a }}
    UNION ALL
    SELECT * FROM {{ source_b }}
    """

task = combine_sources(
    source_a=Table("postgres_conn", "public.data_v1"),
    source_b=Table("postgres_conn", "public.data_v2")
)
```

### Using SQL Files

```python
@sql.append(
    output_table=Table("postgres_conn", "staging.incoming"),
    sql_file="sql/incremental_load.sql"
)
def incremental_load(batch_id):
    pass
```

### Dry Run Mode

```python
@sql.append(
    output_table=Table("postgres_conn", "archive.data"),
    source_conn="postgres_conn",
    dry_run=True
)
def test_append():
    return "SELECT * FROM source LIMIT 1000"
```

## Behavior

1. Executes the SQL query against the source database
2. Loads results into a pandas DataFrame
3. Appends the DataFrame to the existing table
4. Does NOT truncate or modify existing data
5. Returns the table reference as a string

## Temporary Tables

When using tables marked as `temporary=True`, they are automatically dropped after the append operation:

```python
temp_table = Table("postgres_conn", "temp.staging", temporary=True)

@sql.append(output_table=temp_table)
def stage_data():
    return "SELECT * FROM raw_data WHERE processed = false"
```

## Use Cases

| Scenario | Description |
|----------|-------------|
| **Incremental Loading** | Add new data to existing historical tables |
| **Event Archiving** | Append daily/hourly events to archive tables |
| **Data Consolidation** | Combine data from multiple sources into one table |
| **Staging** | Append to staging tables before further processing |

## Related Operations

- [`@sql.query`](query.md) - Write with optional pre-truncate
- [`@sql.replace`](replace.md) - Replace entire table content
- [`@sql.merge`](merge.md) - Upsert with conflict resolution