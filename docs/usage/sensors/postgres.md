# PostgreSQL Sensor

The `PostgresSqlSensor` executes SQL queries against PostgreSQL and succeeds when results are truthy.

## Operator Usage

```python
from airsql.sensors import PostgresSqlSensor

wait_for_data = PostgresSqlSensor(
    task_id='wait_for_processing',
    sql="SELECT COUNT(*) FROM processing_queue WHERE status = 'complete'",
    conn_id='postgres_default',
    retries=2
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to check condition |
| `conn_id` | `str` | Yes | PostgreSQL connection ID |
| `retries` | `int` | No | Number of retries before failing (default: 1) |
| `poke_interval` | `int` | No | Seconds between checks (default: 60) |
| `timeout` | `int` | No | Maximum seconds to wait |

## Examples

### Wait for Row Count

```python
wait_for_rows = PostgresSqlSensor(
    task_id='wait_for_min_rows',
    sql="""
        SELECT COUNT(*) >= 1000
        FROM daily_data
        WHERE date = '{{ ds }}'
    """,
    conn_id='postgres_default'
)
```

### Wait for Parent Task

```python
wait_for_parent = PostgresSqlSensor(
    task_id='wait_for_upstream',
    sql="""
        SELECT EXISTS (
            SELECT 1 FROM job_status
            WHERE job_name = 'parent_job'
              AND status = 'success'
              AND run_date = '{{ ds }}'
        )
    """,
    conn_id='postgres_default',
    poke_interval=30
)
```

### Wait for Processing Completion

```python
wait_for_queue = PostgresSqlSensor(
    task_id='wait_for_queue_empty',
    sql="SELECT COUNT(*) = 0 FROM processing_queue",
    conn_id='postgres_default',
    retries=10,
    poke_interval=60
)
```

### Data Quality Check

```python
check_no_nulls = PostgresSqlSensor(
    task_id='check_no_nulls',
    sql="""
        SELECT COUNT(*) = 0
        FROM staging_data
        WHERE id IS NULL OR created_at IS NULL
    """,
    conn_id='postgres_default'
)
```

### Complex Validation

```python
validate_data = PostgresSqlSensor(
    task_id='validate_data',
    sql="""
        SELECT
            COUNT(*) > 0
            AND COUNT(CASE WHEN user_id IS NULL THEN 1 END) = 0
            AND COUNT(CASE WHEN email IS NULL THEN 1 END) = 0
        FROM users
        WHERE created_at >= '{{ ds }}'
    """,
    conn_id='postgres_default',
    timeout=3600
)
```

## With Retries and Timeout

```python
from datetime import timedelta

sensor = PostgresSqlSensor(
    task_id='robust_wait',
    sql="SELECT processed = true FROM etl_status WHERE date = '{{ ds }}'",
    conn_id='postgres_default',
    retries=5,
    poke_interval=120,
    timeout=7200,
    retry_delay=timedelta(minutes=2)
)
```

## Integration in DAG

```python
from airflow.decorators import dag
from airsql.sensors import PostgresSqlSensor
from airsql import sql, Table

@dag(schedule="@daily")
def pipeline():

    wait_for_source = PostgresSqlSensor(
        task_id='wait_for_source',
        sql="SELECT COUNT(*) > 0 FROM raw_data WHERE date = '{{ ds }}'",
        conn_id='postgres_default'
    )

    @sql.query(output_table=Table("postgres_conn", "warehouse.processed"))
    def process():
        return "SELECT * FROM raw_data WHERE date = '{{ ds }}'"

    wait_for_source >> process()

pipeline()
```