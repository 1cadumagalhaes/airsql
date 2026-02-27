---
icon: lucide/arrow-right-left
---

# Transfer Operators Guide

AirSQL provides operators for transferring data between different systems, particularly BigQuery, PostgreSQL, and Google Cloud Storage.

## Overview

Transfer operators handle ETL workflows with built-in validation, automatic format conversion, and error handling.

## Available Operators

| Operator | Source | Destination | Description |
|----------|--------|-------------|-------------|
| [`BigQueryToPostgresOperator`](bigquery-to-postgres.md) | BigQuery | PostgreSQL | Transfer via GCS |
| [`PostgresToBigQueryOperator`](postgres-to-bigquery.md) | PostgreSQL | BigQuery | Transfer via GCS |
| [`PostgresToGCSOperator`](postgres-to-gcs.md) | PostgreSQL | GCS | Export to files |
| [`GCSToPostgresOperator`](gcs-to-postgres.md) | GCS | PostgreSQL | Import from files |

## Quick Example

```python
from airsql.transfers import PostgresToBigQueryOperator

transfer = PostgresToBigQueryOperator(
    task_id='export_to_bq',
    sql='SELECT * FROM orders WHERE date = {{ ds }}',
    destination_project_dataset_table='my-project.staging.orders',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket'
)
```

## Common Features

### Dry Run Mode

All transfer operators support dry run mode:

```python
transfer = PostgresToBigQueryOperator(
    task_id='test_transfer',
    sql='SELECT * FROM large_table',
    destination_project_dataset_table='project.staging.data',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    dry_run=True
)
```

### Automatic Cleanup

Temporary GCS files are automatically cleaned up:

```python
transfer = BigQueryToPostgresOperator(
    task_id='transfer',
    source_project_dataset_table='project.dataset.table',
    destination_table='staging.table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    cleanup_temp_files=True
)
```

### Error Handling

Built-in retry logic:

```python
from datetime import timedelta

transfer = PostgresToGCSOperator(
    task_id='export',
    sql='SELECT * FROM data',
    postgres_conn_id='postgres_default',
    bucket='export-bucket',
    filename='data.csv',
    retries=3,
    retry_delay=timedelta(minutes=5)
)
```

## Transfer Patterns

### Full Pipeline: PostgreSQL to BigQuery

```python
from airflow.decorators import dag
from airsql.transfers import PostgresToBigQueryOperator
from airsql.sensors import BigQuerySqlSensor

@dag(schedule="@daily")
def etl_pipeline():

    export = PostgresToBigQueryOperator(
        task_id='export_to_bq',
        sql='SELECT * FROM source WHERE date = {{ ds }}',
        destination_project_dataset_table='project.staging.data_{{ ds_nodash }}',
        postgres_conn_id='postgres_default',
        gcs_bucket='temp-bucket'
    )

    validate = BigQuerySqlSensor(
        task_id='validate_data',
        sql="SELECT COUNT(*) > 0 FROM project.staging.data_{{ ds_nodash }}",
        conn_id='bigquery_default'
    )

    export >> validate

etl_pipeline()
```

### Full Pipeline: BigQuery to PostgreSQL

```python
from airflow.decorators import dag
from airsql.transfers import BigQueryToPostgresOperator
from airsql.sensors import PostgresSqlSensor

@dag(schedule="@daily")
def bq_to_pg_pipeline():

    transfer = BigQueryToPostgresOperator(
        task_id='bq_to_pg',
        source_project_dataset_table='project.analytics.data_{{ ds_nodash }}',
        destination_table='warehouse.daily_data',
        postgres_conn_id='postgres_default',
        gcs_bucket='temp-bucket'
    )

    validate = PostgresSqlSensor(
        task_id='validate_transfer',
        sql="SELECT COUNT(*) > 0 FROM warehouse.daily_data WHERE date = {{ ds }}",
        conn_id='postgres_default'
    )

    transfer >> validate

bq_to_pg_pipeline()
```