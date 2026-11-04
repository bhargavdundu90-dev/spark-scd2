import pytest

from spark_scd2 import SCD2Config, SnapshotError, apply_type1, validate_snapshot

from helpers import CONFIG, D1, acc, load, snap


def test_apply_type1_alone_updates_every_version_and_applies_nulls(spark):
    dim = load(spark, (D1, [acc("A", mobile="9000000000"), acc("B")]))
    fixed = apply_type1(dim, snap(spark, acc("A", mobile=None, name="Anil")), CONFIG)
    rows = {r.account_id: r for r in fixed.collect()}
    assert rows["A"].mobile is None and rows["A"].customer_name == "Anil"
    assert rows["B"].customer_name == "Customer B"
    assert fixed.count() == dim.count()


def test_apply_type1_without_type1_columns_is_a_no_op(spark):
    cfg = SCD2Config(keys=["account_id"], type2_cols=["rate"])
    s = snap(spark, acc("A"))
    dim = load(spark, (D1, [acc("A")]), config=cfg)
    assert apply_type1(dim, s, cfg).collect() == dim.collect()


def test_validate_snapshot_accepts_a_clean_snapshot(spark):
    validate_snapshot(snap(spark, acc("A"), acc("B")), CONFIG)


def test_validate_snapshot_reports_example_duplicates(spark):
    with pytest.raises(SnapshotError, match=r"\('A',\)"):
        validate_snapshot(snap(spark, acc("A"), acc("A")), CONFIG)
