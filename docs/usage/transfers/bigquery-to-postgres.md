# BigQuery to PostgreSQL

Transfer data from BigQuery to PostgreSQL via Google Cloud Storage with automatic validation and cleanup.

## Operator Usage

```python
from airsql.transfers import BigQueryToPostgresOperator

transfer = BigQueryToPostgresOperator(
    task_id='bq_to_pg_transfer',
    source_project_dataset_table='my-project.analytics.users',
    destination_table='staging.users',
    postgres_conn_id='postgres_default',
    gcp_conn_id='google_cloud_default',
    gcs_bucket='temp-export-bucket'
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_project_dataset_table` | `str` | Yes | BigQuery table (project.dataset.table or dataset.table) |
| `destination_table` | `str` | Yes | PostgreSQL table (schema.table) |
| `postgres_conn_id` | `str` | Yes | PostgreSQL connection ID |
| `gcp_conn_id` | `str` | No | GCP connection ID (default: google_cloud_default) |
| `gcs_bucket` | `str` | Yes | GCS bucket for temporary storage |
| `gcs_temp_path` | `str` | No | GCS path for temp files (auto-generated) |
| `export_format` | `str` | No | Format: parquet, csv, jsonl (default: parquet) |
| `conflict_columns` | `List[str]` | No | Columns for upsert conflict resolution |
| `replace` | `bool` | No | Replace table content (default: True) |
| `check_source_exists` | `bool` | No | Validate source exists (default: True) |
| `auto_detect_json_columns` | `bool` | No | Auto-detect JSON columns (default: True) |
| `cleanup_temp_files` | `bool` | No | Clean up GCS temp files (default: True) |
| `dry_run` | `bool` | No | Simulate without writing (default: False) |

## Examples

### Basic Transfer

```python
transfer = BigQueryToPostgresOperator(
    task_id='transfer_users',
    source_project_dataset_table='my-project.analytics.users',
    destination_table='warehouse.users',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket'
)
```

### With Upsert

```python
transfer = BigQueryToPostgresOperator(
    task_id='upsert_users',
    source_project_dataset_table='my-project.staging.user_updates',
    destination_table='public.users',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    conflict_columns=['id'],
    replace=False
)
```

### Incremental Daily Load

```python
transfer = BigQueryToPostgresOperator(
    task_id='daily_load',
    source_project_dataset_table='my-project.analytics.events_{{ ds_nodash }}',
    destination_table='staging.daily_events',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    replace=True
)
```

### CSV Format

```python
transfer = BigQueryToPostgresOperator(
    task_id='csv_transfer',
    source_project_dataset_table='my-project.dataset.table',
    destination_table='warehouse.table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    export_format='csv'
)
```

### Create Table If Missing

```python
transfer = BigQueryToPostgresOperator(
    task_id='transfer_with_create',
    source_project_dataset_table='my-project.dataset.new_table',
    destination_table='staging.new_table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    create_if_empty=True
)
```

### Custom GCS Path

```python
transfer = BigQueryToPostgresOperator(
    task_id='transfer_custom_path',
    source_project_dataset_table='my-project.dataset.table',
    destination_table='warehouse.table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    gcs_temp_path='custom/path/data.parquet'
)
```

### Skip Source Validation

```python
transfer = BigQueryToPostgresOperator(
    task_id='transfer_no_check',
    source_project_dataset_table='my-project.dataset.table',
    destination_table='warehouse.table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    check_source_exists=False
)
```

### Dry Run

```python
transfer = BigQueryToPostgresOperator(
    task_id='test_transfer',
    source_project_dataset_table='my-project.dataset.large_table',
    destination_table='warehouse.large_table',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    dry_run=True
)
```

## How It Works

1. Validates source table has data (optional)
2. Detects JSON columns and selects appropriate format
3. Exports BigQuery data to GCS (Parquet/CSV/JSONL)
4. Downloads from GCS and loads into PostgreSQL
5. Cleans up temporary GCS files

## JSON Column Handling

JSON/JSONB columns are automatically detected and handled:

```python
# If BigQuery has JSON columns, format automatically switches to JSONL
transfer = BigQueryToPostgresOperator(
    task_id='json_transfer',
    source_project_dataset_table='project.dataset.events',
    destination_table='warehouse.events',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    auto_detect_json_columns=True
)
```

## Merge/Upsert Mode

Use conflict columns for upsert:

```python
transfer = BigQueryToPostgresOperator(
    task_id='merge_data',
    source_project_dataset_table='project.staging.updates',
    destination_table='public.products',
    postgres_conn_id='postgres_default',
    gcs_bucket='temp-bucket',
    conflict_columns=['sku'],
    replace=False
)
```