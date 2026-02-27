# GCS to PostgreSQL

Import data from Google Cloud Storage files directly into PostgreSQL tables.

## Operator Usage

```python
from airsql.transfers import GCSToPostgresOperator

import_data = GCSToPostgresOperator(
    task_id='gcs_to_pg_import',
    bucket_name='data-bucket',
    object_name='exports/users.jsonl',
    target_table_name='staging.import_table',
    postgres_conn_id='postgres_default',
    gcp_conn_id='google_cloud_default'
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `target_table_name` | `str` | Yes | PostgreSQL table (schema.table) |
| `bucket_name` | `str` | Yes | GCS bucket name |
| `object_name` | `str` | Yes | GCS object path |
| `postgres_conn_id` | `str` | Yes | PostgreSQL connection ID |
| `gcp_conn_id` | `str` | No | GCP connection ID |
| `conflict_columns` | `List[str]` | No | Columns for upsert conflict resolution |
| `replace` | `bool` | No | Replace table content (default: False) |
| `create_if_missing` | `bool` | No | Create table if missing (default: False) |
| `create_if_empty` | `bool` | No | Create empty table if source empty |
| `grant_table_privileges` | `bool` | No | Grant ALL privileges to PUBLIC (default: True) |
| `source_schema` | `dict` | No | Source column types for type coercion |
| `dry_run` | `bool` | No | Simulate without writing |

## Examples

### Basic Import

```python
import_task = GCSToPostgresOperator(
    task_id='import_data',
    bucket_name='data-bucket',
    object_name='exports/users.csv',
    target_table_name='staging.users',
    postgres_conn_id='postgres_default'
)
```

### JSONL Import

```python
import_task = GCSToPostgresOperator(
    task_id='import_jsonl',
    bucket_name='data-bucket',
    object_name='exports/events.jsonl',
    target_table_name='staging.events',
    postgres_conn_id='postgres_default'
)
```

### Parquet Import

```python
import_task = GCSToPostgresOperator(
    task_id='import_parquet',
    bucket_name='data-bucket',
    object_name='exports/data.parquet',
    target_table_name='warehouse.data',
    postgres_conn_id='postgres_default'
)
```

### With Upsert

```python
import_task = GCSToPostgresOperator(
    task_id='upsert_data',
    bucket_name='data-bucket',
    object_name='exports/user_updates.parquet',
    target_table_name='public.users',
    postgres_conn_id='postgres_default',
    conflict_columns=['id']
)
```

### Replace Table

```python
import_task = GCSToPostgresOperator(
    task_id='replace_table',
    bucket_name='data-bucket',
    object_name='exports/full_export.jsonl',
    target_table_name='warehouse.full_data',
    postgres_conn_id='postgres_default',
    replace=True
)
```

### Create Table If Missing

```python
import_task = GCSToPostgresOperator(
    task_id='import_or_create',
    bucket_name='data-bucket',
    object_name='exports/new_table.csv',
    target_table_name='staging.new_table',
    postgres_conn_id='postgres_default',
    create_if_missing=True
)
```

### Create If Empty Source

```python
import_task = GCSToPostgresOperator(
    task_id='import_empty',
    bucket_name='data-bucket',
    object_name='exports/might_be_empty.csv',
    target_table_name='staging.data',
    postgres_conn_id='postgres_default',
    create_if_empty=True
)
```

### With Source Schema

```python
import_task = GCSToPostgresOperator(
    task_id='import_with_schema',
    bucket_name='data-bucket',
    object_name='exports/data.parquet',
    target_table_name='public.data',
    postgres_conn_id='postgres_default',
    source_schema={
        'id': 'INTEGER',
        'amount': 'FLOAT',
        'created_at': 'TIMESTAMP'
    }
)
```

### Dry Run

```python
import_task = GCSToPostgresOperator(
    task_id='test_import',
    bucket_name='data-bucket',
    object_name='exports/large_file.parquet',
    target_table_name='staging.test',
    postgres_conn_id='postgres_default',
    dry_run=True
)
```

## How It Works

1. Downloads file from GCS
2. Detects format from file extension (csv, jsonl, parquet, avro)
3. Reads data into pandas DataFrame
4. Coerces column types to match PostgreSQL schema
5. Handles JSON columns automatically
6. Inserts or upserts data into PostgreSQL

## Supported Formats

| Format | Extension | Use Case |
|--------|-----------|----------|
| CSV | `.csv` | Simple tabular data |
| JSONL | `.jsonl` | Nested data, JSON columns |
| Parquet | `.parquet` | Columnar storage, analytics |
| Avro | `.avro` | Schema evolution support |

Format is auto-detected from filename.

## Upsert Mode

Use conflict columns for merge/upsert:

```python
import_task = GCSToPostgresOperator(
    task_id='merge_updates',
    bucket_name='data-bucket',
    object_name='updates/products.csv',
    target_table_name='public.products',
    postgres_conn_id='postgres_default',
    conflict_columns=['sku'],
    # Only update these columns on conflict
)
```

## Type Coercion

Automatically coerces types between BigQuery and PostgreSQL:

| BigQuery | PostgreSQL |
|----------|------------|
| INTEGER | integer |
| FLOAT | double precision |
| BOOLEAN | boolean |
| STRING | text |
| TIMESTAMP | timestamptz |
| JSON | jsonb |