---
icon: lucide/list-checks
---

# Release Notes

## 0.18.1

Released: 2026-07-29

### Highlights

- Keeps PostgreSQL partitions in the configured target schema instead of relying on the connection `search_path`.
- Repairs partitions previously created in `public` by `0.18.0` during the next replacement.
- Applies schema-safe partition replacement to both batched and legacy loading paths.
- Handles string `DATE` and `TIMESTAMP` partition values when calculating exclusive upper bounds.

### Notes

- Partition recovery only detaches and removes a relation proven through `pg_inherits` to belong to the target parent table.
- Existing unrelated tables with the same name are not removed.

## 0.18.0

Released: 2026-07-29

### Highlights

- Adds bounded-memory loading for BigQuery-to-PostgreSQL transfers with batch readers for Parquet, CSV, and JSONL.
- Processes GCS objects sequentially and avoids redundant full-object downloads for size reporting.
- Streams PostgreSQL row conversion instead of materializing a complete tuple list.
- Adds PostgreSQL staging for cross-batch upserts and partition exchange.
- Adds chunked PostgreSQL extraction with optional size-limited GCS output shards.
- Adds batch schema-drift validation and safer temporary-file cleanup.

### Compatibility

- Existing append, replace, retry, and transaction semantics remain unchanged.
- Upsert and partition operations retain atomic publication through PostgreSQL staging.
- Non-sharded PostgreSQL-to-GCS exports remain available for compatibility.

## 0.17.0

Released: 2026-07-20

### Highlights

- Supports Airflow 3.3 template rendering in AirSQL operators and transfers.

## 0.16.0

Released: 2026-06-25

### Highlights

- Renders AirSQL templates at runtime for more reliable dynamic task configuration.

## 0.15.0

Released: 2026-05-27

### Highlights

- Uses a staging table for `GCSToPostgresOperator` upserts.
- Deduplicates incoming upsert rows by `conflict_columns`, keeping the last row.
- Loads staged upsert data through the typed row insertion path instead of client-side batched row upserts.
- Preserves scalar types for PostgreSQL loads by unwrapping PyArrow scalar values.
- Avoids `pandas.to_sql` in remaining `GCSToPostgresOperator` append and partition paths that could coerce nullable integer columns to text.
- Skips no-op PostgreSQL updates using `IS DISTINCT FROM` to reduce WAL, dead tuples, and index churn.

### Notes

- The staging-table upsert path keeps existing `conflict_columns` behavior but performs the merge as a set-based `INSERT ... SELECT ... ON CONFLICT` from a temporary table.
- If duplicate conflict keys are present in the incoming DataFrame, AirSQL keeps the last row before staging. This matches the previous practical last-write-wins behavior while avoiding PostgreSQL duplicate-update errors.
- This release supersedes `0.14.4`, which briefly shipped the same staging-table upsert change before the release line was corrected to `0.15.0`.

## 0.14.4

Released: 2026-05-27

- Added staging-table upserts for `GCSToPostgresOperator`.
- Superseded by `0.15.0`.

## 0.14.3

Released: 2026-05-27

- Normalizes PostgreSQL DataFrame merge values before insertion.
- Converts PyArrow scalar values to native Python values before sending rows to PostgreSQL.
- Uses typed row insertion for remaining `GCSToPostgresOperator` append paths instead of `pandas.to_sql`.
- Adds a no-op update guard to PostgreSQL upsert/merge paths with `IS DISTINCT FROM`.

## 0.14.2

Released: 2026-05-27

- Preserves scalar types when loading partitioned PostgreSQL tables.
- Replaces the partition-exchange temp-table `pandas.to_sql` load with the same typed row insertion path used by replace loads.
- Adds regression coverage for nullable integer partition loads.

## 0.14.1

Released: 2026-05-27

- Preserves DataFrame scalar types for PostgreSQL replace and upsert loads.
- Removes blanket `astype(object)` conversion from tuple generation.
- Keeps numeric, boolean, and nullable values as native Python values while still serializing `dict` and `list` values as JSON strings.

## 0.14.0

Released: 2026-05-27

- Adds `schema_overrides` to `PostgresToBigQueryOperator` and `PostgresToGCSOperator`.
- Allows callers to force specific BigQuery field types when schema inference sees a different PostgreSQL expression type.
- Applies schema overrides after inference in both COPY-based and pandas/pyarrow-based schema generation paths.
- Fixes PostgreSQL `timestamptz` mapping for BigQuery loads by mapping it to `TIMESTAMP` instead of invalid `TIMETIME`.
- Aligns Ruff lint configuration with the upgraded toolchain used by CI.

## Links

- [GitHub releases](https://github.com/1cadumagalhaes/airsql/releases)
- [PyPI package](https://pypi.org/project/airsql/)
