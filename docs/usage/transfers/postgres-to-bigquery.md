# PostgreSQL to BigQuery

Transfer data from PostgreSQL to BigQuery via Google Cloud Storage with automatic format handling and schema detection.

## Operator Usage

```python
from airsql.transfers import PostgresToBigQueryOperator

transfer = PostgresToBigQueryOperator(
    task_id='pg_to_bq_export',
    sql='SELECT * FROM analytics.monthly_report',
    destination_project_dataset_table='my-project.analytics.monthly_reports',
    postgres_conn_id='postgres_default',
    gcp_conn_id='google_cloud_default',
    gcs_bucket='temp-export-bucket'
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | `str` | Yes* | SQL query to extract data |
| `source_project_dataset_table` | `str` | Yes* | Source table (alternative to sql) |
| `destination_project_dataset_table` | `str` | Yes | BigQuery destination (project.dataset.table) |
| `postgres_conn_id` | `str` | Yes | PostgreSQL connection ID |
| `gcp_conn_id` | `str` | No | GCP connection ID |
| `gcs_bucket` | `str` | Yes | GCS bucket for staging |
| `gcs_temp_path` | `str` | No | GCS path for temp files |
| `export_format` | `str` | No | Format: csv, jsonl (default: csv) |
| `schema_filename` | `str` | No | Path for BigQuery schema JSON |
| `pandas_chunksize` | `int` | No | Rows per chunk for large exports |
| `use_copy` | `bool` | No | Use COPY for streaming (default: False) |
| `write_disposition` | `str` | No | WRITE_TRUNCATE, WRITE_APPEND (default: WRITE_TRUNCATE) |
| `partition_by` | `str` | No | Partition column for BigQuery |
| `partition_type` | `str` | No | DAY, HOUR, MONTH, YEAR (default: DAY) |
| `cluster_fields` | `List[str]` | No | Clustering fields |
| `dataset_location` | `str` | No | BigQuery dataset location (default: us-central1) |
| `dry_run` | `bool` | No | Simulate without writing |

*Either `sql` or `source_project_dataset_table` is required.

## Examples

### Basic Export

```python
export = PostgresToBigQueryOperator(
    task_id='export_orders',
    sql='SELECT * FROM orders WHERE created_at >= CURRENT_DATE',
    destination_project_dataset_table='my-project.staging.orders',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket'
)
```

### Using Source Table

```python
export = PostgresToBigQueryOperator(
    task_id='export_table',
    source_project_dataset_table='public.users',
    destination_project_dataset_table='my-project.staging.users',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket'
)
```

### With Partitioning

```python
export = PostgresToBigQueryOperator(
    task_id='export_partitioned',
    sql='SELECT * FROM events',
    destination_project_dataset_table='my-project.analytics.events',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    partition_by='event_date',
    partition_type='DAY',
    cluster_fields=['user_id', 'event_type']
)
```

### Incremental Load

```python
export = PostgresToBigQueryOperator(
    task_id='incremental_export',
    sql="""
        SELECT * FROM transactions 
        WHERE created_at >= '{{ ds }}' 
          AND created_at < '{{ tomorrow_ds }}'
    """,
    destination_project_dataset_table='my-project.analytics.transactions',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    write_disposition='WRITE_APPEND'
)
```

### Large Dataset with Streaming

```python
export = PostgresToBigQueryOperator(
    task_id='large_export',
    sql='SELECT * FROM huge_table',
    destination_project_dataset_table='my-project.staging.large_table',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    use_copy=True,
    pandas_chunksize=50000
)
```

### JSONL Format with Schema

```python
export = PostgresToBigQueryOperator(
    task_id='jsonl_export',
    sql='SELECT * FROM events WHERE date = {{ ds }}',
    destination_project_dataset_table='my-project.staging.events',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    export_format='jsonl',
    schema_filename='schemas/events.json'
)
```

### Custom GCS Path

```python
export = PostgresToBigQueryOperator(
    task_id='custom_path_export',
    sql='SELECT * FROM data',
    destination_project_dataset_table='my-project.staging.data',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    gcs_temp_path='exports/{{ ds }}/data.csv'
)
```

### Dry Run

```python
export = PostgresToBigQueryOperator(
    task_id='test_export',
    sql='SELECT * FROM large_table LIMIT 1000000',
    destination_project_dataset_table='my-project.staging.test',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    dry_run=True
)
```

## How It Works

1. Validates source data exists (optional)
2. Detects JSON columns and selects format
3. Exports PostgreSQL data to GCS (CSV/JSONL)
4. Generates BigQuery schema automatically
5. Loads data from GCS to BigQuery
6. Cleans up temporary GCS files

## Format Selection

| Format | Use Case | JSON Support |
|--------|----------|--------------|
| `csv` | Simple data, smaller files | No |
| `jsonl` | JSON/JSONB columns, nested data | Yes |

JSON columns are automatically detected and format is switched to JSONL.

## Partitioning

Partition large tables for better query performance:

```python
export = PostgresToBigQueryOperator(
    task_id='partitioned_export',
    sql='SELECT *, DATE(created_at) as event_date FROM events',
    destination_project_dataset_table='my-project.analytics.events',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    partition_by='event_date',
    partition_type='DAY',
    cluster_fields=['user_id']
)
```

## Chunked Exports

For large datasets, use chunked exports:

```python
export = PostgresToBigQueryOperator(
    task_id='chunked_export',
    sql='SELECT * FROM very_large_table',
    destination_project_dataset_table='my-project.staging.large',
    postgres_conn_id='postgres_default',
    gcs_bucket='export-bucket',
    pandas_chunksize=100000
)
```