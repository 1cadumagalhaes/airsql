# BigQuery Sensor

The `BigQuerySqlSensor` executes SQL queries against BigQuery and succeeds when results are truthy.

## Operator Usage

```python
from airsql.sensors import BigQuerySqlSensor

wait_for_data = BigQuerySqlSensor(
    task_id='wait_for_user_data',
    sql="SELECT COUNT(*) FROM analytics.user_events WHERE dt = '{{ ds }}'",
    conn_id='bigquery_default',
    location='us-central1'
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `sql` | `str` | Yes | SQL query to check condition |
| `conn_id` | `str` | Yes | BigQuery connection ID |
| `location` | `str` | No | BigQuery location (default: 'us-central1') |
| `retries` | `int` | No | Number of retries before failing (default: 1) |
| `poke_interval` | `int` | No | Seconds between checks (default: 60) |
| `timeout` | `int` | No | Maximum seconds to wait |

## Examples

### Wait for Row Count

```python
wait_for_rows = BigQuerySqlSensor(
    task_id='wait_for_min_rows',
    sql="""
        SELECT COUNT(*) >= 1000
        FROM `project.dataset.daily_data`
        WHERE date = '{{ ds }}'
    """,
    conn_id='bigquery_default',
    location='US'
)
```

### Wait for ETL Completion

```python
wait_for_etl = BigQuerySqlSensor(
    task_id='wait_for_etl_completion',
    sql="""
        SELECT COUNT(*) > 0
        FROM etl_status
        WHERE job_name = 'daily_import'
          AND status = 'complete'
          AND date = '{{ ds }}'
    """,
    conn_id='bigquery_default',
    retries=5,
    poke_interval=300
)
```

### Partition Check

```python
wait_for_partition = BigQuerySqlSensor(
    task_id='wait_for_partition',
    sql="""
        SELECT COUNT(*) > 0
        FROM `project.dataset.events`
        WHERE DATE(event_time) = '{{ ds }}'
    """,
    conn_id='bigquery_default'
)
```

### Data Quality Validation

```python
validate_data = BigQuerySqlSensor(
    task_id='validate_data_quality',
    sql="""
        SELECT
            COUNT(*) > 0
            AND COUNT(CASE WHEN user_id IS NULL THEN 1 END) = 0
        FROM `project.analytics.users`
        WHERE date = '{{ ds }}'
    """,
    conn_id='bigquery_default'
)
```

### Complex Validation Query

```python
validate_completeness = BigQuerySqlSensor(
    task_id='validate_completeness',
    sql="""
        SELECT
            (SELECT COUNT(*) FROM source_a WHERE date = '{{ ds }}')
            =
            (SELECT COUNT(*) FROM source_b WHERE date = '{{ ds }}')
    """,
    conn_id='bigquery_default',
    location='us-central1'
)
```

## With Retries and Timeout

```python
from datetime import timedelta

sensor = BigQuerySqlSensor(
    task_id='robust_wait',
    sql="SELECT processed = true FROM etl_status WHERE date = '{{ ds }}'",
    conn_id='bigquery_default',
    location='US',
    retries=5,
    poke_interval=180,
    timeout=5400,
    retry_delay=timedelta(minutes=3)
)
```

## Integration in DAG

```python
from airflow.decorators import dag
from airsql.sensors import BigQuerySqlSensor
from airsql.transfers import BigQueryToPostgresOperator

@dag(schedule="@daily")
def bq_to_pg_pipeline():

    wait_for_source = BigQuerySqlSensor(
        task_id='wait_for_source',
        sql="""
            SELECT COUNT(*) > 0
            FROM `project.staging.data_{{ ds_nodash }}`
        """,
        conn_id='bigquery_default',
        location='US'
    )

    transfer = BigQueryToPostgresOperator(
        task_id='transfer_to_pg',
        source_project_dataset_table='project.staging.data_{{ ds_nodash }}',
        destination_table='warehouse.daily_data',
        postgres_conn_id='postgres_default',
        gcs_bucket='temp-bucket'
    )

    wait_for_source >> transfer

bq_to_pg_pipeline()
```

## Multiple Location Support

```python
# US multi-region
sensor_us = BigQuerySqlSensor(
    task_id='check_us',
    sql="SELECT COUNT(*) > 0 FROM `project.us_dataset.table`",
    conn_id='bigquery_default',
    location='US'
)

# EU region
sensor_eu = BigQuerySqlSensor(
    task_id='check_eu',
    sql="SELECT COUNT(*) > 0 FROM `project.eu_dataset.table`",
    conn_id='bigquery_default',
    location='EU'
)

# Specific region
sensor_regional = BigQuerySqlSensor(
    task_id='check_regional',
    sql="SELECT COUNT(*) > 0 FROM `project.dataset.table`",
    conn_id='bigquery_default',
    location='us-central1'
)
```