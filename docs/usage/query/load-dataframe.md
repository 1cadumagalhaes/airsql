# Load DataFrame

The `@sql.load_dataframe` decorator and `DataFrameLoadOperator` load a pandas DataFrame directly into a database table.

## Operator Usage

```python
import pandas as pd
from airsql.operators import DataFrameLoadOperator
from airsql import Table

df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'value': [100, 200, 300]
})

load_task = DataFrameLoadOperator(
    task_id="load_dataframe",
    dataframe=df,
    output_table=Table("postgres_conn", "staging.loaded_data"),
    if_exists="replace"
)

load_task.execute(context)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_id` | `str` | Yes | Unique task identifier |
| `dataframe` | `pd.DataFrame` | Yes | Pandas DataFrame to load |
| `output_table` | `Table` | Yes | Destination table reference |
| `if_exists` | `str` | No | Behavior: 'append', 'replace', 'fail', 'truncate' |
| `timestamp_column` | `str` | No | Column to populate with current timestamp |
| `dry_run_flag` | `bool` | No | If True, simulate without writing |
| `**kwargs` | `Any` | No | Additional operator kwargs |

## Decorator Usage

### Function Returns DataFrame

```python
from airsql import sql, Table
import pandas as pd

@sql.load_dataframe(
    output_table=Table("postgres_conn", "analytics.user_summary"),
    if_exists="replace"
)
def create_user_summary():
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'total_orders': [10, 5, 8]
    })

load_task = create_user_summary()
```

### Direct DataFrame Loading

```python
import pandas as pd

df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

@sql.load_dataframe(
    output_table=Table("postgres_conn", "staging.data"),
    dataframe=df
)
def load_existing_data():
    pass
```

## Examples

### Replace Table Content

```python
@sql.load_dataframe(
    output_table=Table("postgres_conn", "reports.summary"),
    if_exists="replace"
)
def generate_summary():
    return pd.DataFrame({
        'metric': ['users', 'orders', 'revenue'],
        'value': [1000, 500, 25000]
    })
```

### Append to Existing Table

```python
@sql.load_dataframe(
    output_table=Table("postgres_conn", "staging.events"),
    if_exists="append"
)
def add_events():
    return pd.DataFrame({
        'event_id': [101, 102],
        'event_type': ['click', 'purchase'],
        'timestamp': [pd.Timestamp.now(), pd.Timestamp.now()]
    })
```

### With Timestamp Column

```python
@sql.load_dataframe(
    output_table=Table("postgres_conn", "audit.changes"),
    if_exists="append",
    timestamp_column="loaded_at"
)
def track_changes():
    return pd.DataFrame({
        'entity_id': [1, 2],
        'change_type': ['update', 'create']
    })
```

### Dry Run Mode

```python
@sql.load_dataframe(
    output_table=Table("postgres_conn", "test.data"),
    if_exists="replace",
    dry_run=True
)
def test_load():
    return pd.DataFrame({'col': [1, 2, 3]})
```

## if_exists Options

| Value | Behavior |
|-------|----------|
| `append` | Add data to existing table |
| `replace` | Drop and recreate table |
| `truncate` | Truncate table, preserve structure |
| `fail` | Raise error if table exists |

## Temporary Tables

```python
temp_table = Table("postgres_conn", "temp.staging", temporary=True)

@sql.load_dataframe(output_table=temp_table, if_exists="replace")
def create_temp_data():
    return pd.DataFrame({'id': [1, 2, 3]})
```

## Related Operations

- [`@sql.merge_dataframe`](merge-dataframe.md) - Merge DataFrame with upsert
- [`@sql.extract_and_load`](extract-and-load.md) - Extract via SQL and load
- [`@sql.query`](query.md) - Execute SQL and write to table