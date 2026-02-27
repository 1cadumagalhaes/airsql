---
icon: lucide/book-open
---

# Usage

Detailed guides for using AirSQL operators, sensors, and transfer operators.

## Query Operations

Execute SQL queries with various output strategies.

- [Query Overview](query/index.md) - Introduction to query operations
- [Return DataFrame](query/dataframe.md) - Execute SQL and return pandas DataFrame
- [Query to Table](query/query.md) - Execute SQL and write to table
- [Append to Table](query/append.md) - Execute SQL and append to existing table
- [Replace Table](query/replace.md) - Execute SQL and replace table content
- [Truncate and Reload](query/truncate.md) - Truncate table and insert new data
- [Merge/Upsert](query/merge.md) - Execute SQL and merge with conflict resolution
- [Load DataFrame](query/load-dataframe.md) - Load pandas DataFrame directly
- [Merge DataFrame](query/merge-dataframe.md) - Merge DataFrame with upsert logic
- [Extract and Load](query/extract-and-load.md) - Extract via SQL and load in one task
- [Extract and Merge](query/extract-and-merge.md) - Extract via SQL and merge in one task
- [Data Quality Checks](query/check.md) - Execute data quality validation
- [DDL Operations](query/ddl.md) - Execute DDL statements

## Sensors

Monitor database conditions with SQL-based sensors.

- [Sensors Guide](sensors/index.md) - SQL sensors with retry logic

## Transfers

Move data between different systems.

- [Transfers Guide](transfers/index.md) - Transfer operators for BigQuery, Postgres, GCS
