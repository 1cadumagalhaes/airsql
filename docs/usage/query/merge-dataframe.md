# Merge DataFrame

The `@sql.merge_dataframe` decorator and `DataFrameMergeOperator` merge a pandas DataFrame into a table with upsert logic.

## Operator Usage

```python
import pandas as pd
from airsql.operators import DataFrameMergeOperator
from airsql import Table

df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
})

merge_task = DataFrameMergeOperator(
    task_id="merge_users",
    dataframe=df,
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"],
    update_columns=["name", "email"],
    pre_truncate=False
)

merge_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `dataframe` | `pd.DataFrame` | Yes | Pandas DataFrame to merge |
| `output_table` | `Table` | Yes | Destination table reference |
| `conflict_columns` | `List[str]` | Yes | Columns for conflict detection |
| `update_columns` | `List[str]` | No | Columns to update (default: all non-conflict) |
| `timestamp_column` | `str` | No | Column to populate with timestamp |
| `pre_truncate` | `bool` | No | If True, truncate before merge |
| `dry_run_flag` | `bool` | No | If True, simulate without writing |
| `**kwargs` | `Any` | No | Additional operator kwargs |

## Decorator Usage

### Function Returns DataFrame

```python
from airsql import sql, Table
import pandas as pd

@sql.merge_dataframe(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["id"]
)
def sync_users():
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
    })

merge_task = sync_users()
```

### Direct DataFrame Merging

```python
df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

@sql.merge_dataframe(
    output_table=Table("postgres_conn", "staging.data"),
    conflict_columns=["id"],
    dataframe=df
)
def merge_existing_data():
    pass
```

## Examples

### Single Column Conflict

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "inventory.products"),
    conflict_columns=["sku"]
)
def update_inventory():
    return pd.DataFrame({
        'sku': ['SKU001', 'SKU002', 'SKU003'],
        'name': ['Product A', 'Product B', 'Product C'],
        'quantity': [100, 50, 75],
        'price': [19.99, 29.99, 39.99]
    })
```

### Composite Key Conflict

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "analytics.daily_metrics"),
    conflict_columns=["metric_date", "metric_name"],
    update_columns=["metric_value"]
)
def update_daily_metrics():
    return pd.DataFrame({
        'metric_date': ['2024-01-01', '2024-01-01'],
        'metric_name': ['users', 'revenue'],
        'metric_value': [1000, 50000]
    })
```

### Selective Column Update

Only update specific columns on conflict:

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "public.users"),
    conflict_columns=["user_id"],
    update_columns=["name", "last_login"]
)
def update_user_profiles():
    return pd.DataFrame({
        'user_id': [1, 2],
        'name': ['Updated Alice', 'Updated Bob'],
        'email': ['alice@example.com', 'bob@example.com'],
        'last_login': [pd.Timestamp.now(), pd.Timestamp.now()],
        'created_at': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')]
    })
```

### Pre-Truncate Before Merge

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "reporting.state"),
    conflict_columns=["entity_id"],
    pre_truncate=True
)
def rebuild_state():
    return pd.DataFrame({
        'entity_id': [1, 2, 3],
        'state': ['active', 'inactive', 'pending'],
        'updated_at': [pd.Timestamp.now()] * 3
    })
```

### With Timestamp Column

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "audit.change_log"),
    conflict_columns=["change_id"],
    timestamp_column="loaded_at"
)
def load_changes():
    return pd.DataFrame({
        'change_id': [101, 102],
        'entity': ['user', 'order'],
        'action': ['create', 'update']
    })
```

### Dry Run Mode

```python
@sql.merge_dataframe(
    output_table=Table("postgres_conn", "test.data"),
    conflict_columns=["id"],
    dry_run=True
)
def test_merge():
    return pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
```

## How It Works

1. The DataFrame is processed row by row
2. For each row:
   - Check if the `conflict_columns` match an existing row
   - If match: UPDATE the specified `update_columns`
   - If no match: INSERT the new row
3. If `timestamp_column` is specified, it's populated with the current timestamp

### Generated SQL (PostgreSQL Example)

```sql
INSERT INTO users (id, name, email, last_login)
VALUES (?, ?, ?, ?)
ON CONFLICT (id) 
DO UPDATE SET 
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    last_login = EXCLUDED.last_login
```

## Update Columns Behavior

| `update_columns` | Behavior |
|------------------|----------|
| `None` (default) | Update all columns except conflict columns |
| `['col1', 'col2']` | Update only specified columns |
| `[]` | Insert only (skip updates) |

## Temporary Tables

```python
temp_table = Table("postgres_conn", "temp.staging", temporary=True)

@sql.merge_dataframe(
    output_table=temp_table,
    conflict_columns=["id"]
)
def process_batch():
    return pd.DataFrame({'id': [1, 2, 3]})
```

## Related Operations

- [`@sql.merge`](merge.md) - Merge SQL query results
- [`@sql.load_dataframe`](load-dataframe.md) - Load DataFrame without merge
- [`@sql.extract_and_merge`](extract-and-merge.md) - Extract and merge in one task