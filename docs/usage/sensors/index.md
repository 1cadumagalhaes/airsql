---
icon: lucide/radar
---

# Sensors Guide

AirSQL provides SQL-based sensors that extend Airflow's built-in sensors with additional retry logic for BigQuery and PostgreSQL databases.

## Overview

Sensors wait for a condition to be met before proceeding. AirSQL sensors execute SQL queries and succeed when the query returns truthy results.

## Available Sensors

| Sensor | Database | Description |
|--------|----------|-------------|
| [`PostgresSqlSensor`](postgres.md) | PostgreSQL | SQL sensor with retry logic |
| [`BigQuerySqlSensor`](bigquery.md) | BigQuery | SQL sensor for BigQuery tables |

## Quick Example

```python
from airsql.sensors import PostgresSqlSensor

wait_for_data = PostgresSqlSensor(
    task_id='wait_for_data',
    sql="SELECT COUNT(*) > 0 FROM daily_data WHERE date = '{{ ds }}'",
    conn_id='postgres_default'
)
```

## Success Conditions

Sensors succeed when:

1. The query returns at least one row
2. All values in the returned row(s) are truthy (not 0, NULL, or FALSE)

Common SQL patterns:

```sql
SELECT COUNT(*) > 0 FROM table

SELECT COUNT(*) = 0 FROM table WHERE error_condition

SELECT EXISTS (SELECT 1 FROM table WHERE condition)

SELECT COUNT(*) >= threshold FROM table
```

## Retry Logic

Both sensors include built-in retry logic that can skip after exhausting retries:

```python
from datetime import timedelta

sensor = PostgresSqlSensor(
    task_id='wait_with_retries',
    sql="SELECT COUNT(*) > 0 FROM late_arriving_data",
    conn_id='postgres_default',
    retries=3,
    poke_interval=60,
    retry_delay=timedelta(minutes=5)
)
```