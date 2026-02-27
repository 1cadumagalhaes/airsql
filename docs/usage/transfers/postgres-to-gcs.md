# PostgreSQL to GCS

Export data from PostgreSQL directly to Google Cloud Storage in CSV, JSONL, or Parquet format.

## Operator Usage

```python
from airsql.transfers import PostgresToGCSOperator

export = PostgresToGCSOperator(
    task_id='pg_to_gcs_export',
    sql='SELECT * FROM reports.large_dataset',
    postgres_conn_id='postgres_default',
    bucket='backup-bucket',
    filename='exports/{{ ds_nodash }}/report.csv',
    gcp_conn_id='google_cloud_default'
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | `str` | Yes | SQL query to extract data |
| `postgres_conn_id` | `str` | Yes | PostgreSQL connection ID |
| `bucket` | `str` | Yes | GCS bucket name |
| `filename` | `str` | Yes | GCS object path |
| `gcp_conn_id` | `str` | No | GCP connection ID |
| `export_format` | `str` | No | csv, jsonl, parquet (default: csv) |
| `schema_filename` | `str` | No | Path for BigQuery schema JSON |
| `pandas_chunksize` | `int` | No | Rows per chunk for large exports |
| `use_copy` | `bool` | No | Use PostgreSQL COPY for streaming |
| `use_temp_file` | `bool` | No | Use temp file instead of streaming |
| `csv_kwargs` | `dict` | No | Additional pandas CSV options |
| `parquet_kwargs` | `dict` | No | Additional pandas Parquet options |
| `auto_switch_format` | `bool` | No | Auto-switch to JSONL for problematic data |
| `dry_run` | `bool` | No | Simulate without writing |

## Examples

### CSV Export

```python
export = PostgresToGCSOperator(
    task_id='csv_export',
    sql='SELECT * FROM users',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='exports/users/{{ ds }}/users.csv',
    export_format='csv'
)
```

### JSONL Export

```python
export = PostgresToGCSOperator(
    task_id='jsonl_export',
    sql='SELECT * FROM events WHERE date = {{ ds }}',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='events/{{ ds }}.jsonl',
    export_format='jsonl'
)
```

### Large Dataset Streaming

```python
export = PostgresToGCSOperator(
    task_id='streaming_export',
    sql='SELECT * FROM huge_table',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='huge_table/export.parquet',
    export_format='parquet',
    use_copy=True
)
```

### With Schema File

```python
export = PostgresToGCSOperator(
    task_id='export_with_schema',
    sql='SELECT * FROM analytics.metrics',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='metrics/{{ ds }}.jsonl',
    export_format='jsonl',
    schema_filename='schemas/metrics.json'
)
```

### Chunked Export

```python
export = PostgresToGCSOperator(
    task_id='chunked_export',
    sql='SELECT * FROM large_table',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='large_table/data.parquet',
    export_format='parquet',
    pandas_chunksize=50000
)
```

### Custom CSV Options

```python
export = PostgresToGCSOperator(
    task_id='custom_csv',
    sql='SELECT * FROM products',
    postgres_conn_id='postgres_default',
    bucket='exports',
    filename='products.csv',
    export_format='csv',
    csv_kwargs={'sep': '|', 'encoding': 'utf-8'}
)
```

### Dry Run

```python
export = PostgresToGCSOperator(
    task_id='test_export',
    sql='SELECT * FROM large_table LIMIT 1000000',
    postgres_conn_id='postgres_default',
    bucket='test-bucket',
    filename='test/data.csv',
    dry_run=True
)
```

## How It Works

1. Executes SQL query against PostgreSQL
2. Streams results to GCS in chunks
3. Automatically handles JSON columns (switches to JSONL)
4. Generates BigQuery-compatible schema (optional)
5. Cleans up temporary files

## Format Selection

| Format | Use Case | JSON Support | Compression |
|--------|----------|--------------|-------------|
| `csv` | Simple data, smaller files | No | No |
| `jsonl` | JSON columns, nested data | Yes | No |
| `parquet` | Columnar storage, analytics | Limited | Yes |

## JSON Columns

JSON/JSONB columns are automatically detected:

```python
# If table has JSON columns, format automatically switches to JSONL
export = PostgresToGCSOperator(
    task_id='json_export',
    sql='SELECT id, data::jsonb as data FROM events',
    postgres_conn_id='postgres_default',
    bucket='data-lake',
    filename='events.jsonl',
    export_format='csv',  # Will auto-switch to JSONL
    auto_switch_format=True
)
```

## Incremental Exports

```python
export = PostgresToGCSOperator(
    task_id='incremental_export',
    sql="""
        SELECT * FROM orders 
        WHERE created_at >= '{{ ds }}' 
          AND created_at < '{{ tomorrow_ds }}'
    """,
    postgres_conn_id='postgres_default',
    bucket='daily-exports',
    filename='orders/{{ ds }}.csv'
)
```

## Archive Pattern

```python
export = PostgresToGCSOperator(
    task_id='archive_old_data',
    sql="""
        SELECT * FROM transactions 
        WHERE created_at < '{{ macros.ds_add(ds, -365) }}'
    """,
    postgres_conn_id='postgres_default',
    bucket='archive-bucket',
    filename='historical/{{ ds_nodash }}.parquet',
    export_format='parquet'
)
```