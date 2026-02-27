---
icon: lucide/database
---

# AirSQL

<p align="center">
  <strong>A decorator-based SQL execution framework for Apache Airflow</strong>
</p>

## What is AirSQL?

AirSQL is a modern Python framework that provides clean, intuitive decorators for SQL operations in Airflow. It simplifies data pipeline development by offering a Python-like syntax for common data operations while maintaining full compatibility with Airflow's ecosystem.

## Features

- Decorator-based syntax for clean, Pythonic SQL operations
- Native Airflow integration using connections and patterns
- Multi-database support: PostgreSQL, BigQuery, and more
- SQL file support with Jinja templating
- Flexible outputs: DataFrames, tables, or files
- Smart operations: replace, merge/upsert, truncate
- SQL sensors with retry logic for BigQuery and PostgreSQL
- Transfer operators for BigQuery, Postgres, and GCS

## Installation

```bash
pip install airsql
```

Or with uv:

```bash
uv add airsql
```

## Quick Start

### Basic DataFrame Query

```python
from airsql import sql

@sql.dataframe(source_conn="postgres_conn")
def get_active_users():
    return "SELECT * FROM users WHERE active = true"

df_task = get_active_users()
```

### Query with Table Output

```python
from airsql import sql, Table

@sql.query(output_table=Table("postgres_conn", "reports.daily_summary"))
def create_daily_report():
    return """
    SELECT DATE(created_at) as date, COUNT(*) as total
    FROM orders
    GROUP BY DATE(created_at)
    """
```

## Next Steps

- [Getting Started](getting-started.md) - Installation and basic concepts
- [Core Concepts](core-concepts.md) - Tables, connections, and decorators
- [Query Guide](usage/query/index.md) - All query operations
- [Sensors Guide](usage/sensors/index.md) - SQL-based sensors
- [Transfers Guide](usage/transfers/index.md) - Data transfer operators
- [API Reference](reference/index.md) - Auto-generated API docs

## Examples

Complete workflow examples demonstrating AirSQL in action:

- Query operations with decorators and operators
- Cross-database data transfers
- Data quality checks with sensors
- Complete DAG examples with TaskFlow API

See [Examples](examples/index.md) for full code examples.

## About

AirSQL is built with a simple philosophy: make SQL operations in Airflow feel natural. Instead of writing verbose operator instantiations, express your data operations using intuitive Python decorators.

Key principles:

- **Decorator-first design**: Express data operations as Python functions
- **Type safety**: Full type hints for IDE support and validation
- **Multi-database support**: Works seamlessly with PostgreSQL, BigQuery, and more
- **Airflow native**: Built specifically for Airflow's TaskFlow API

See [About](about/index.md) for more information.