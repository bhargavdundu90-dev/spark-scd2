"""SCD Type 2 loads from full (or partial) snapshots."""

from __future__ import annotations

import datetime as _dt
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DateLike, SCD2Config, to_date
from .errors import SnapshotError

NULL_TOKEN = "~"
SEPARATOR = "|"

INSERT, UPDATE, REOPEN = "insert", "update", "reopen"

_ACTION = "__scd2_action"
_IN_SNAPSHOT = "__scd2_in_snapshot"
_SEEN = "__scd2_seen"


# --------------------------------------------------------------------------- hashing


def row_hash(columns) -> Column:
    """SHA-256 over ``columns``, with nulls coalesced so that ``NULL -> value`` is a change.

    Each value is cast to string, a null becomes ``"~"`` and the values are joined with
    ``"|"``. Decimals keep their scale when cast, so ``9.50`` and ``9.5`` only hash the same
    when the column type is the same; keep the snapshot schema stable between loads.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in columns]
    return F.sha2(F.concat_ws(SEPARATOR, *parts), 256)


def _surrogate_key(config: SCD2Config, prefix: str, effective_from: Column) -> Column:
    parts = [F.col(f"{prefix}.{k}").cast("string") for k in config.keys]
    return F.sha2(F.concat_ws(":", *parts, effective_from.cast("string")), 256).substr(1, 16)


# --------------------------------------------------------------------------- validation


def _require_columns(df: DataFrame, columns, what: str) -> None:
    present = {c.lower() for c in df.columns}
    missing = [c for c in columns if c.lower() not in present]
    if missing:
        raise SnapshotError(f"{what} is missing column(s): {', '.join(missing)}")


def validate_snapshot(snapshot_df: DataFrame, config: SCD2Config) -> None:
    """Refuse a snapshot that would damage the dimension.

    Raises :class:`SnapshotError` when the snapshot is empty, lacks a configured column, has
    a row with a null key, or has two rows for the same key. An empty full extract would
    otherwise close every entity, and a duplicate would open two current versions.
    """
    _require_columns(snapshot_df, config.data_columns, "snapshot")

    any_null_key = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in config.keys])
    stats = snapshot_df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(any_null_key.cast("int")).alias("null_keys"),
    ).first()
    rows, null_keys = stats["rows"], stats["null_keys"] or 0
    if rows == 0:
        raise SnapshotError("empty snapshot: refusing to load (it would close every current version)")
    if null_keys:
        raise SnapshotError(f"{null_keys} snapshot row(s) have a null in key column(s) {list(config.keys)}")

    dupes = snapshot_df.groupBy(*config.keys).count().where(F.col("count") > 1)
    sample = dupes.limit(5).collect()
    if sample:
        shown = ", ".join(str(tuple(r[k] for k in config.keys)) for r in sample)
        raise SnapshotError(f"duplicate keys in snapshot, for example {shown}")


def _last_load_date(dim_df: DataFrame, config: SCD2Config) -> _dt.date | None:
    closed_to = F.when(~F.col(config.is_current_col), F.col(config.effective_to_col))
    row = dim_df.agg(
        F.max(config.effective_from_col).alias("f"),
        F.max(closed_to).alias("t"),
    ).first()
    dates = [d for d in (row["f"], row["t"]) if d is not None]
    return max(dates) if dates else None


# --------------------------------------------------------------------------- dimension helpers


def empty_dimension(snapshot_df: DataFrame, config: SCD2Config) -> DataFrame:
    """An empty dimension whose schema follows the snapshot's column types."""
    _require_columns(snapshot_df, config.data_columns, "snapshot")
    by_name = {f.name.lower(): f for f in snapshot_df.schema.fields}
    fields: list[T.StructField] = []
    if config.surrogate_key_col:
        fields.append(T.StructField(config.surrogate_key_col, T.StringType(), True))
    for c in config.data_columns:
        src = by_name[c.lower()]
        fields.append(T.StructField(c, src.dataType, True))
    fields += [
        T.StructField(config.effective_from_col, T.DateType(), True),
        T.StructField(config.effective_to_col, T.DateType(), True),
        T.StructField(config.is_current_col, T.BooleanType(), True),
    ]
    if config.change_type_col:
        fields.append(T.StructField(config.change_type_col, T.StringType(), True))
    fields.append(T.StructField(config.hash_col, T.StringType(), True))
    spark = snapshot_df.sparkSession
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), T.StructType(fields))


def as_of(dim_df: DataFrame, date: DateLike, config: SCD2Config) -> DataFrame:
    """Point-in-time view: the version of each entity that was valid on ``date``."""
    d = F.lit(to_date(date)).cast("date")
    return dim_df.where((F.col(config.effective_from_col) <= d) & (d < F.col(config.effective_to_col)))


def current(dim_df: DataFrame, config: SCD2Config) -> DataFrame:
    """Only the open version of each entity."""
    return dim_df.where(F.col(config.is_current_col))


# --------------------------------------------------------------------------- Type 1


def apply_type1(dim_df: DataFrame, snapshot_df: DataFrame, config: SCD2Config) -> DataFrame:
    """Overwrite the Type 1 columns on every version of each entity present in the snapshot.

    Entities missing from the snapshot keep their last known values. A null in the snapshot is
    applied like any other value: clearing a wrong value is a correction too.
    """
    if not config.type1_cols:
        return dim_df.select(*config.dimension_columns)
    _require_columns(snapshot_df, [*config.keys, *config.type1_cols], "snapshot")
    new = snapshot_df.select(
        *config.keys,
        *[F.col(c).alias(f"__new_{c}") for c in config.type1_cols],
        F.lit(True).alias(_IN_SNAPSHOT),
    )
    out = dim_df.join(new, list(config.keys), "left")
    for c in config.type1_cols:
        out = out.withColumn(c, F.when(F.col(_IN_SNAPSHOT), F.col(f"__new_{c}")).otherwise(F.col(c)))
    return out.select(*config.dimension_columns)


# --------------------------------------------------------------------------- Type 2


def apply_snapshot(
    dim_df: DataFrame | None,
    snapshot_df: DataFrame,
    as_of_date: DateLike,
    config: SCD2Config,
) -> DataFrame:
    """Return the dimension after loading one snapshot taken on ``as_of_date``.

    ``dim_df`` may be ``None`` for the first load. The input DataFrames are not modified;
    write the result back with your table format of choice.

    Each current version is compared with the snapshot row of the same key:

    * key only in the snapshot: ``insert`` (or ``reopen`` when the key had history),
    * Type 2 hash differs: the current version is closed and an ``update`` is opened,
    * key only in the dimension: closed when ``delete_mode="close"``, kept otherwise,
    * hash equal: unchanged.

    Type 1 columns are then overwritten on every version of keys in the snapshot.
    """
    d_value = to_date(as_of_date, "as_of_date")
    if d_value >= config.open_end_date:
        raise SnapshotError(f"as_of_date {d_value} must be before open_end {config.open_end_date}")

    validate_snapshot(snapshot_df, config)
    if dim_df is None:
        dim_df = empty_dimension(snapshot_df, config)
    _require_columns(dim_df, config.dimension_columns, "dimension")

    last = _last_load_date(dim_df, config)
    if last is not None and d_value < last:
        raise SnapshotError(
            f"snapshot for {d_value} is older than the last load ({last}); loads must be applied in "
            "date order, so rebuild from that date instead"
        )

    keys = list(config.keys)
    d = F.lit(d_value).cast("date")
    open_end = F.lit(config.open_end_date).cast("date")

    snap = snapshot_df.select(*config.data_columns).withColumn(config.hash_col, row_hash(config.type2_cols))
    dim = dim_df.select(*config.dimension_columns)
    cur = dim.where(F.col(config.is_current_col))
    closed = dim.where(~F.col(config.is_current_col))

    cond = reduce(lambda a, b: a & b, [F.col(f"c.{k}") == F.col(f"s.{k}") for k in keys])
    c_missing = F.col(f"c.{keys[0]}").isNull()
    s_missing = F.col(f"s.{keys[0]}").isNull()
    joined = (
        cur.alias("c")
        .join(snap.alias("s"), cond, "full_outer")
        .withColumn(
            _ACTION,
            F.when(c_missing, F.lit(INSERT))
            .when(s_missing, F.lit("delete"))
            .when(F.col(f"c.{config.hash_col}") != F.col(f"s.{config.hash_col}"), F.lit("change"))
            .otherwise(F.lit("same")),
        )
    )

    ends = ["change", "delete"] if config.delete_mode == "close" else ["change"]
    closing = joined.where(F.col(_ACTION).isin(ends))
    if last is not None and d_value == last:
        # Same-day re-run: an identical snapshot is a no-op, but a different one would close a
        # version on the day it was opened and leave a zero-length interval.
        zero = closing.where(F.col(f"c.{config.effective_from_col}") >= d).limit(1).count()
        if zero:
            raise SnapshotError(
                f"a different snapshot for {d_value} was already loaded; rebuild from that date instead "
                "of loading it twice"
            )

    def keep_current(end: Column | None = None) -> list[Column]:
        out = []
        for c in config.dimension_columns:
            if end is not None and c == config.effective_to_col:
                out.append(end.alias(c))
            elif end is not None and c == config.is_current_col:
                out.append(F.lit(False).alias(c))
            else:
                out.append(F.col(f"c.{c}").alias(c))
        return out

    def new_version() -> list[Column]:
        out = []
        if config.surrogate_key_col:
            out.append(_surrogate_key(config, "s", d).alias(config.surrogate_key_col))
        out += [F.col(f"s.{c}").alias(c) for c in config.data_columns]
        out += [
            d.alias(config.effective_from_col),
            open_end.alias(config.effective_to_col),
            F.lit(True).alias(config.is_current_col),
        ]
        if config.change_type_col:
            kind = F.when(F.col(_ACTION) == "change", F.lit(UPDATE)).otherwise(F.lit(INSERT))
            out.append(kind.alias(config.change_type_col))
        out.append(F.col(f"s.{config.hash_col}").alias(config.hash_col))
        return out

    kept_actions = ["same"] if config.delete_mode == "close" else ["same", "delete"]
    kept = joined.where(F.col(_ACTION).isin(kept_actions)).select(*keep_current())
    ended = closing.select(*keep_current(d))
    opened = joined.where(F.col(_ACTION).isin(INSERT, "change")).select(*new_version())

    if config.change_type_col:
        # A key that was closed earlier and shows up again is a reopen, not a brand-new entity.
        seen = closed.select(*keys).distinct().withColumn(_SEEN, F.lit(True))
        ct = config.change_type_col
        opened = (
            opened.join(seen, keys, "left")
            .withColumn(
                ct,
                F.when((F.col(ct) == INSERT) & F.col(_SEEN).isNotNull(), F.lit(REOPEN)).otherwise(F.col(ct)),
            )
            .select(*config.dimension_columns)
        )

    result = closed.unionByName(kept).unionByName(ended).unionByName(opened.select(*config.dimension_columns))
    return apply_type1(result, snapshot_df, config)
