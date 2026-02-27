---
icon: lucide/database
---

# Query Guide

AirSQL provides a comprehensive set of operators and decorators for SQL query operations. Each operation type supports both direct operator usage and decorator-based syntax.

## Overview

AirSQL offers two ways to work with SQL operations:

| Approach | Description | Best For |
|----------|-------------|----------|
| **Decorators** | Python-decorated functions that return operators | Clean, Pythonic syntax; most common use |
| **Operators** | Direct class instantiation | Maximum control; dynamic configuration |

## Operation Types

| Operation | Description | Output |
|-----------|-------------|--------|
| [`@sql.dataframe`](dataframe.md) | Execute SQL and return a pandas DataFrame | DataFrame |
| [`@sql.query`](query.md) | Execute SQL and write results to a table | Table reference |
| [`@sql.append`](append.md) | Execute SQL and append to existing table | Table reference |
| [`@sql.replace`](replace.md) | Execute SQL and replace entire table content | Table reference |
| [`@sql.truncate`](truncate.md) | Truncate table and insert new data | Table reference |
| [`@sql.merge`](merge.md) | Execute SQL and merge/upsert into table | Table reference |
| [`@sql.load_dataframe`](load-dataframe.md) | Load a pandas DataFrame into a table | None |
| [`@sql.merge_dataframe`](merge-dataframe.md) | Merge/upsert a DataFrame into a table | None |
| [`@sql.extract_and_load`](extract-and-load.md) | Extract via SQL and load into table | String summary |
| [`@sql.extract_and_merge`](extract-and-merge.md) | Extract via SQL and merge into table | String summary |
| [`@sql.check`](check.md) | Execute data quality checks | None |
| [`@sql.ddl`](ddl.md) | Execute DDL statements | None |

## Common Parameters

Most query operations share these common parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `output_table` | `Table` | Destination table reference |
| `source_conn` | `str` | Connection ID for source database |
| `sql_file` | `str` | Path to SQL file (alternative to function return) |
| `dry_run` | `bool` | If True, simulate without writing data |
| `pre_truncate` | `bool` | If True, truncate before writing (where applicable) |

## Quick Examples

### Using Decorators

```python
from airsql import sql, Table

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

@sql.query(output_table=Table("postgres_conn", "reports.daily_summary"))
def create_report():
    return "SELECT DATE(created_at) as date, COUNT(*) as total FROM orders GROUP BY 1"

@sql.replace(output_table=Table("postgres_conn", "cache.user_cache"))
def refresh_cache():
    return "SELECT * FROM users WHERE last_login > CURRENT_DATE - INTERVAL '30 days'"
```

### Using Operators Directly

```python
from airsql.operators import SQLQueryOperator, SQLDataFrameOperator, SQLReplaceOperator
from airsql import Table

df_task = SQLDataFrameOperator(
    task_id="get_active_users",
    sql="SELECT * FROM users WHERE active = true",
    source_conn="postgres_conn"
)

query_task = SQLQueryOperator(
    task_id="create_report",
    sql="SELECT DATE(created_at) as date, COUNT(*) as total FROM orders GROUP BY 1",
    output_table=Table("postgres_conn", "reports.daily_summary"),
    source_conn="postgres_conn"
)

replace_task = SQLReplaceOperator(
    task_id="refresh_cache",
    sql="SELECT * FROM users WHERE last_login > CURRENT_DATE - INTERVAL '30 days'",
    output_table=Table("postgres_conn", "cache.user_cache"),
    source_conn="postgres_conn"
)
```