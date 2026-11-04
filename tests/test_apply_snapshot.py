import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from spark_scd2 import SCD2Config, SnapshotError, apply_snapshot, as_of, current, empty_dimension

from helpers import CONFIG, D1, D2, D3, D4, OPEN, SCHEMA, acc, as_rows, load, random_history, snap, versions


def test_first_load_inserts_every_row_as_current(spark):
    dim = load(spark, (D1, [acc("A"), acc("B")]))
    rows = dim.collect()
    assert len(rows) == 2
    assert all(
        r.is_current and r.effective_from == D1 and r.effective_to == OPEN and r.change_type == "insert"
        for r in rows
    )
    assert list(dim.columns) == list(CONFIG.dimension_columns)


def test_type2_change_closes_old_version_and_opens_new(spark):
    dim = load(spark, (D1, [acc("A", rate="9.00")]), (D2, [acc("A", rate="9.25")]))
    v = versions(dim, "A")
    assert [(x["rate"], x["effective_from"], x["effective_to"], x["is_current"]) for x in v] == [
        (Decimal("9.00"), D1, D2, False),
        (Decimal("9.25"), D2, OPEN, True),
    ]
    assert [x["change_type"] for x in v] == ["insert", "update"]


def test_unchanged_rows_get_no_new_version(spark):
    dim = load(spark, (D1, [acc("A"), acc("B")]), (D2, [acc("A"), acc("B", branch="BLR004")]))
    assert len(versions(dim, "A")) == 1
    assert len(versions(dim, "B")) == 2


def test_type1_correction_overwrites_every_version_without_new_version(spark):
    dim = load(
        spark,
        (D1, [acc("A", rate="9.00")]),
        (D2, [acc("A", rate="9.50")]),
        (D3, [acc("A", rate="9.50", name="CUSTOMER A", mobile="9111111111")]),
    )
    v = versions(dim, "A")
    assert len(v) == 2
    assert {x["customer_name"] for x in v} == {"CUSTOMER A"}
    assert {x["mobile"] for x in v} == {"9111111111"}


def test_missing_key_is_closed_and_comes_back_as_reopen(spark):
    dim = load(spark, (D1, [acc("A"), acc("B")]), (D2, [acc("A")]), (D3, [acc("A"), acc("B")]))
    v = versions(dim, "B")
    assert [(x["effective_from"], x["effective_to"], x["is_current"], x["change_type"]) for x in v] == [
        (D1, D2, False, "insert"),
        (D3, OPEN, True, "reopen"),
    ]
    assert as_of(dim, D2, CONFIG).where("account_id = 'B'").count() == 0


def test_closed_key_keeps_its_last_type1_values(spark):
    dim = load(spark, (D1, [acc("A"), acc("B", mobile="9222222222")]), (D2, [acc("A")]))
    assert versions(dim, "B")[0]["mobile"] == "9222222222"


def test_null_to_value_and_value_to_null_are_changes(spark):
    dim = load(
        spark,
        (D1, [acc("A", branch=None)]),
        (D2, [acc("A", branch="HYD001")]),
        (D3, [acc("A", branch=None)]),
    )
    assert [x["branch"] for x in versions(dim, "A")] == [None, "HYD001", None]


def test_null_and_literal_tilde_hash_differently(spark):
    dim = load(spark, (D1, [acc("A", branch=None)]), (D2, [acc("A", branch="")]))
    assert len(versions(dim, "A")) == 2


def test_rerunning_the_same_snapshot_is_a_no_op(spark):
    rows = [acc("A"), acc("B", status="SMA1")]
    once = load(spark, (D1, [acc("A"), acc("B")]), (D2, rows))
    twice = apply_snapshot(once, snap(spark, *rows), D2, CONFIG)
    assert as_rows(twice) == as_rows(once)


def test_a_different_snapshot_for_an_already_loaded_day_is_refused(spark):
    dim = load(spark, (D1, [acc("A")]), (D2, [acc("A", rate="9.10")]))
    with pytest.raises(SnapshotError, match="already loaded"):
        apply_snapshot(dim, snap(spark, acc("A", rate="9.20")), D2, CONFIG)


def test_older_snapshot_is_refused(spark):
    dim = load(spark, (D1, [acc("A")]), (D2, [acc("A", rate="9.10")]))
    with pytest.raises(SnapshotError, match="older"):
        apply_snapshot(dim, snap(spark, acc("A")), D1, CONFIG)


def test_delete_counts_as_a_load_for_ordering(spark):
    dim = load(spark, (D1, [acc("A"), acc("B")]), (D3, [acc("A")]))
    with pytest.raises(SnapshotError, match="older"):
        apply_snapshot(dim, snap(spark, acc("A")), D2, CONFIG)


@pytest.mark.parametrize(
    "rows, message",
    [
        ([acc("A"), acc("A", rate="9.10")], "duplicate"),
        ([], "empty"),
        ([acc(None)], "null"),
    ],
)
def test_bad_snapshot_is_refused_before_touching_the_dimension(spark, rows, message):
    dim = load(spark, (D1, [acc("A")]))
    with pytest.raises(SnapshotError, match=message):
        apply_snapshot(dim, snap(spark, *rows), D2, CONFIG)


def test_snapshot_missing_a_configured_column_is_refused(spark):
    bad = snap(spark, acc("A")).drop("status")
    with pytest.raises(SnapshotError, match="status"):
        apply_snapshot(None, bad, D1, CONFIG)


def test_as_of_date_on_or_after_open_end_is_refused(spark):
    with pytest.raises(SnapshotError, match="open_end"):
        apply_snapshot(None, snap(spark, acc("A")), "9999-12-31", CONFIG)


def test_partial_extract_with_delete_mode_ignore_keeps_missing_keys_open(spark):
    cfg = SCD2Config(**{**_kwargs(CONFIG), "delete_mode": "ignore"})
    dim = load(
        spark,
        (D1, [acc("A"), acc("B", mobile="9222222222")]),
        (D2, [acc("A", rate="9.40")]),
        config=cfg,
    )
    b = versions(dim, "B")
    assert [(x["effective_to"], x["is_current"], x["mobile"]) for x in b] == [(OPEN, True, "9222222222")]
    assert len(versions(dim, "A")) == 2


def test_composite_keys(spark):
    cfg = SCD2Config(keys=["tenant", "id"], type2_cols=["plan"], type1_cols=["email"])
    schema = "tenant string, id int, plan string, email string"
    s1 = spark.createDataFrame([("t1", 1, "free", "a@x"), ("t2", 1, "free", "b@x")], schema)
    s2 = spark.createDataFrame([("t1", 1, "pro", "a@x"), ("t2", 1, "free", "B@x")], schema)
    dim = apply_snapshot(apply_snapshot(None, s1, D1, cfg), s2, D2, cfg)
    t1 = dim.where("tenant = 't1'").orderBy("effective_from").collect()
    t2 = dim.where("tenant = 't2'").collect()
    assert [r.plan for r in t1] == ["free", "pro"]
    assert len(t2) == 1 and t2[0].email == "B@x"


def test_surrogate_keys_are_unique_and_deterministic(spark):
    days = [(D1, [acc("A"), acc("B")]), (D2, [acc("A", rate="9.30")]), (D3, [acc("A"), acc("B")])]
    first, second = load(spark, *days), load(spark, *days)
    assert first.groupBy("account_sk").count().where("count > 1").count() == 0
    assert as_rows(first.select("account_sk")) == as_rows(second.select("account_sk"))
    assert all(len(r.account_sk) == 16 for r in first.collect())


def test_custom_column_names_and_open_end(spark):
    cfg = SCD2Config(
        keys=["account_id"],
        type2_cols=["rate"],
        effective_from_col="valid_from",
        effective_to_col="valid_to",
        is_current_col="current_flag",
        hash_col="row_hash",
        change_type_col=None,
        open_end="2999-12-31",
    )
    s = spark.createDataFrame([("A", Decimal("1.00"))], "account_id string, rate decimal(5,2)")
    s2 = spark.createDataFrame([("A", Decimal("2.00"))], "account_id string, rate decimal(5,2)")
    dim = apply_snapshot(apply_snapshot(None, s, D1, cfg), s2, D2, cfg)
    assert dim.columns == ["account_id", "rate", "valid_from", "valid_to", "current_flag", "row_hash"]
    v = dim.orderBy("valid_from").collect()
    assert [(r.valid_to, r.current_flag) for r in v] == [(D2, False), (dt.date(2999, 12, 31), True)]


def test_extra_snapshot_columns_are_ignored(spark):
    s = snap(spark, acc("A")).withColumn("ingested_at", F.current_timestamp())
    dim = apply_snapshot(None, s, D1, CONFIG)
    assert "ingested_at" not in dim.columns


def test_as_of_and_current_accept_strings(spark):
    dim = load(spark, (D1, [acc("A", rate="9.00")]), (D2, [acc("A", rate="9.50")]))
    assert as_of(dim, "2026-11-01", CONFIG).first().rate == Decimal("9.00")
    assert as_of(dim, "2026-11-02", CONFIG).first().rate == Decimal("9.50")
    assert as_of(dim, "2026-10-31", CONFIG).count() == 0
    assert current(dim, CONFIG).count() == 1


def test_empty_dimension_follows_snapshot_types(spark):
    empty = empty_dimension(snap(spark, acc("A")), CONFIG)
    types = dict(empty.dtypes)
    assert empty.count() == 0
    assert types["rate"] == "decimal(5,2)"
    assert types["effective_from"] == "date" and types["is_current"] == "boolean"


def test_inputs_are_left_untouched(spark):
    first = load(spark, (D1, [acc("A")]))
    before = as_rows(first)
    load(spark, (D2, [acc("A", rate="9.90")]), dim=first)
    assert as_rows(first) == before


def test_random_history_rebuilds_every_snapshot_and_keeps_invariants(spark):
    history = random_history(days=6, accounts=20, seed=11)
    dim = None
    for day, rows in history:
        dim = apply_snapshot(dim, spark.createDataFrame(rows, SCHEMA), day, CONFIG).localCheckpoint()

    type2 = ["account_id", *CONFIG.type2_cols]
    for day, rows in history:
        got = sorted(tuple(r) for r in as_of(dim, day, CONFIG).select(*type2).collect())
        want = sorted((r[0], r[1], r[2], r[3], r[4]) for r in rows)
        assert got == want, day

    last_day, last_rows = history[-1]
    got_t1 = sorted(
        tuple(r) for r in current(dim, CONFIG).select("account_id", "customer_name", "mobile").collect()
    )
    assert got_t1 == sorted((r[0], r[5], r[6]) for r in last_rows)
    assert dim.where("is_current").groupBy("account_id").count().where("count > 1").count() == 0
    assert dim.where("effective_from >= effective_to").count() == 0
    assert dim.groupBy("account_sk").count().where("count > 1").count() == 0
    assert {r.change_type for r in dim.collect()} >= {"insert", "update", "reopen"}


def test_four_day_story(spark):
    dim = load(
        spark,
        (D1, [acc("A"), acc("B"), acc("C")]),
        (D2, [acc("A", status="SMA1"), acc("C")]),
        (D3, [acc("A", status="SMA1"), acc("B"), acc("C", name="C. Rao")]),
        (D4, [acc("A", status="ACTIVE"), acc("B"), acc("C", name="C. Rao")]),
    )
    counts = {
        (r.change_type, r.is_current): r["count"]
        for r in dim.groupBy("change_type", "is_current").count().collect()
    }
    assert counts == {
        ("insert", False): 2,  # A closed on D2, B closed on D2
        ("insert", True): 1,  # C never changed
        ("update", False): 1,  # A's SMA1 version, closed on D4
        ("update", True): 1,  # A back to ACTIVE
        ("reopen", True): 1,  # B came back on D3
    }


def _kwargs(cfg):
    return dict(
        keys=cfg.keys,
        type2_cols=cfg.type2_cols,
        type1_cols=cfg.type1_cols,
        surrogate_key_col=cfg.surrogate_key_col,
    )
