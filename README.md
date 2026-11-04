# spark-scd2

[![CI](https://github.com/bhargavdundu90-dev/spark-scd2/actions/workflows/ci.yml/badge.svg)](https://github.com/bhargavdundu90-dev/spark-scd2/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Slowly changing dimension Type 2 loads from full snapshots, in PySpark.

Many source systems cannot give you a change log. Every night they export the whole table,
and it is up to you to work out what changed, keep the history, and not wreck the dimension
when an extract arrives empty, twice, or out of order. `spark-scd2` does that comparison for
you and returns the new dimension as a DataFrame, so you can write it with Parquet, Iceberg,
Delta or Hive tables as you already do.

What it handles:

- **Change detection by hash.** A SHA-256 over the Type 2 columns, with nulls coalesced so
  that `NULL -> value` and `value -> NULL` count as changes.
- **Every case of a full outer join** between the current versions and the snapshot: insert,
  change, delete, reopen (a key that was closed and comes back) and unchanged.
- **Type 1 columns** (corrections such as a misspelt name) overwritten on every version without
  opening a new one.
- **Half-open validity**, `effective_from <= d < effective_to`, with a configurable open end
  (`9999-12-31` by default), so point-in-time joins never match two versions.
- **Guard rails.** Empty snapshots, null keys, duplicate keys, missing columns and loads older
  than the last one are refused before the dimension is touched. Re-running the same snapshot
  for the same day is a no-op.
- **Partial extracts.** `delete_mode="ignore"` keeps keys that are missing from the snapshot
  open instead of closing them.
- **Iceberg SQL.** `iceberg_merge_sql` generates the equivalent `MERGE INTO` / `INSERT`
  statements for a dimension stored in Apache Iceberg, with the same hashes and surrogate keys.

## Install

```bash
pip install spark-scd2        # coming soon to PyPI
```

Until the first PyPI release, install from GitHub:

```bash
pip install "git+https://github.com/bhargavdundu90-dev/spark-scd2.git@v0.1.0"
```

Requires Python 3.9+ and PySpark 3.3+ (with Java 8, 11 or 17 as your Spark version needs).

## Quickstart

```python
from spark_scd2 import SCD2Config, apply_snapshot

config = SCD2Config(
    keys=["account_id"],
    type2_cols=["product_code", "interest_rate", "branch_code", "status"],
    type1_cols=["customer_name", "mobile"],
)
snapshot = spark.read.parquet("s3://raw/loan_accounts/dt=2026-11-03/")
dim = spark.read.parquet("s3://dw/dim_loan_account/")  # or None on the first load
dim = apply_snapshot(dim, snapshot, "2026-11-03", config)
dim.write.mode("overwrite").parquet("s3://dw/dim_loan_account_v2/")
```

`apply_snapshot` is lazy like any other transformation. Write the result to a new location or
a table format that supports atomic overwrite (Iceberg, Delta) rather than overwriting the
path you are reading from. For long chains of loads in one job, call `.localCheckpoint()` on
the result of each load to keep the query plan short.

## Column roles

| Role | Configured by | On change |
| --- | --- | --- |
| Business key | `keys` | Identifies the entity. Must be unique and non-null in every snapshot. |
| Type 2 | `type2_cols` | Closes the current version on the snapshot date and opens a new one. |
| Type 1 | `type1_cols` | Overwritten on every version of the entity; no new version. |
| Validity | `effective_from_col`, `effective_to_col` | Half-open `[from, to)`. The open version ends at `open_end`. |
| Current flag | `is_current_col` | `true` on the one open version of each key. |
| Hash | `hash_col` | SHA-256 of the Type 2 columns, kept so the next load can compare cheaply. |
| Change type | `change_type_col` | `insert`, `update` or `reopen`. Set to `None` to leave it out. |
| Surrogate key | `surrogate_key_col` | Optional. 16 hex characters from the keys and `effective_from`; deterministic across re-runs. |

Columns in the snapshot that have no role are ignored. The dimension's columns are always
`config.dimension_columns`, in that order.

Keys missing from a snapshot are closed (`delete_mode="close"`, for full extracts). If you load
partial or incremental extracts, use `delete_mode="ignore"`; changes and inserts still apply.

## Point-in-time queries

```python
from spark_scd2 import as_of, current

# the rate and status every loan had on the reporting date
as_of(dim, "2026-11-03", config).select("account_id", "interest_rate", "status")

# only the open versions
current(dim, config)
```

For fact tables, join on the key and the validity window:

```sql
SELECT f.txn_id, f.amount, d.branch_code
FROM fact_repayment f
JOIN dim_loan_account d
  ON d.account_id = f.account_id
 AND f.txn_date >= d.effective_from
 AND f.txn_date <  d.effective_to
```

## Compared with a hand-written MERGE

A hand-written SCD2 `MERGE` usually starts small and grows a list of special cases:

```sql
MERGE INTO dim d
USING stg s ON d.account_id = s.account_id AND d.is_current
WHEN MATCHED AND (d.rate <> s.rate OR d.status <> s.status) THEN
  UPDATE SET effective_to = current_date(), is_current = false
WHEN NOT MATCHED THEN
  INSERT (...) VALUES (...);
-- then a second INSERT for the new versions of changed rows,
-- then something for keys that disappeared ...
```

The comparison above silently misses `NULL -> value` changes, never closes deleted keys,
labels a returning key as brand new, and happily applies an empty or duplicated extract.
Each fix is another clause to get right, in every pipeline that needs a dimension.

With `spark-scd2` the rules live in one tested place and the pipeline only says which columns
play which role. If the dimension stays in Iceberg and you prefer to `MERGE` in place, the same
rules are available as SQL:

```python
from spark_scd2 import iceberg_merge_sql, validate_snapshot

validate_snapshot(snapshot, config)  # SQL cannot refuse a bad extract on its own
snapshot.createOrReplaceTempView("stg_loan_account")
statements = iceberg_merge_sql("glue_catalog.dw.dim_loan_account", "stg_loan_account", "2026-11-03", config)
for statement in statements:
    spark.sql(statement)
```

The generated statements are tested against a real Iceberg table to give exactly the same
rows as `apply_snapshot`. They do not check load order; keep one load per date, in date order.

## API

| Function | What it does |
| --- | --- |
| `SCD2Config(...)` | Column roles and options. Immutable; validated on creation. |
| `apply_snapshot(dim_df, snapshot_df, as_of_date, config)` | Full Type 2 + Type 1 load of one snapshot. `dim_df` may be `None`. |
| `apply_type1(dim_df, snapshot_df, config)` | Only the Type 1 overwrite. |
| `validate_snapshot(snapshot_df, config)` | Raises `SnapshotError` for empty, null-key, duplicate-key or incomplete snapshots. |
| `as_of(dim_df, date, config)` / `current(dim_df, config)` | Point-in-time and current views. |
| `empty_dimension(snapshot_df, config)` | Empty dimension with types taken from the snapshot. |
| `row_hash(columns)` | The hash expression, if you need it elsewhere. |
| `iceberg_merge_sql(target, source, as_of_date, config)` | Equivalent Spark SQL for an Iceberg table. |

Dates may be `datetime.date` objects or `YYYY-MM-DD` strings.

## Roadmap

- Delta Lake `MERGE` writer alongside the Iceberg one
- Decimal scale normalisation in the hash, so a schema change from `decimal(5,2)` to
  `decimal(6,3)` does not version every row
- Optional soft-delete flag column instead of only closing missing keys
- Late-arriving snapshot support (rebuild from a given date)
- Examples for AWS Glue and EMR, and a PyPI release

Ideas and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0. Copyright 2026 Dundu Bhargav.
