import pytest

from spark_scd2 import SCD2Config, apply_snapshot, empty_dimension, iceberg_merge_sql, validate_snapshot

from conftest import ICEBERG_ENABLED
from helpers import CONFIG, SCHEMA, as_rows, random_history


def test_statements_for_full_snapshot():
    close, insert, type1 = iceberg_merge_sql("glue.dw.dim_account", "stg_account", "2026-11-03", CONFIG)
    assert close.startswith("MERGE INTO `glue`.`dw`.`dim_account` d")
    assert "LEFT JOIN (SELECT `account_id`" in close
    assert "x.new_hash IS NULL OR x.new_hash <> d.`scd2_hash`" in close
    assert "`effective_to` = DATE '2026-11-03', `is_current` = false" in close
    assert insert.startswith("INSERT INTO `glue`.`dw`.`dim_account`")
    assert "DATE '9999-12-31'" in insert and "'reopen'" in insert
    assert "NOT (d.`customer_name` <=> s.`customer_name`)" in type1


def test_partial_extract_never_closes_missing_keys():
    cfg = SCD2Config(keys=["id"], type2_cols=["a"], delete_mode="ignore")
    close, insert = iceberg_merge_sql("dim", "stg", "2026-11-03", cfg)
    assert "LEFT JOIN (SELECT" not in close
    assert "IS NULL OR" not in close


def test_no_type1_statement_without_type1_columns():
    cfg = SCD2Config(keys=["id"], type2_cols=["a"], change_type_col=None)
    statements = iceberg_merge_sql("dim", "stg", "2026-11-03", cfg)
    assert len(statements) == 2
    assert "CASE WHEN" not in statements[1]


def test_table_names_are_checked():
    with pytest.raises(ValueError, match="identifiers"):
        iceberg_merge_sql("dim; DROP TABLE x", "stg", "2026-11-03", CONFIG)


def test_statements_parse_in_spark(spark):
    parser = spark._jsparkSession.sessionState().sqlParser()
    for statement in iceberg_merge_sql("cat.db.dim", "stg", "2026-11-03", CONFIG):
        parser.parsePlan(statement)


@pytest.mark.skipif(not ICEBERG_ENABLED, reason="set SPARK_SCD2_ICEBERG_JAR or SPARK_SCD2_ICEBERG_PACKAGES")
@pytest.mark.parametrize("delete_mode", ["close", "ignore"])
def test_iceberg_merge_matches_dataframe_api(spark, delete_mode):
    cfg = SCD2Config(
        keys=CONFIG.keys,
        type2_cols=CONFIG.type2_cols,
        type1_cols=CONFIG.type1_cols,
        surrogate_key_col=CONFIG.surrogate_key_col,
        delete_mode=delete_mode,
    )
    table = f"local.db.dim_account_{delete_mode}"
    history = random_history(days=5, accounts=15, seed=3)
    first = spark.createDataFrame(history[0][1], SCHEMA)
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    empty_dimension(first, cfg).writeTo(table).using("iceberg").create()

    expected = None
    for day, rows in history:
        snapshot = spark.createDataFrame(rows, SCHEMA)
        validate_snapshot(snapshot, cfg)
        snapshot.createOrReplaceTempView("stg_account")
        for statement in iceberg_merge_sql(table, "stg_account", day, cfg):
            spark.sql(statement)
        expected = apply_snapshot(expected, snapshot, day, cfg).localCheckpoint()

    assert as_rows(spark.table(table)) == as_rows(expected)


def test_composite_keys_join_on_every_key():
    cfg = SCD2Config(keys=["branch_id", "account_id"], type2_cols=["status"])
    close, insert = iceberg_merge_sql("dim", "stg", "2026-11-03", cfg)
    assert "ON d.`branch_id` = x.`branch_id` AND d.`account_id` = x.`account_id` AND d.`is_current`" in close
    assert "c.`branch_id` = s.`branch_id` AND c.`account_id` = s.`account_id`" in insert
    assert "WHERE c.`branch_id` IS NULL" in insert


def test_surrogate_key_uses_all_keys_and_load_date():
    cfg = SCD2Config(keys=["branch_id", "account_id"], type2_cols=["status"], surrogate_key_col="sk")
    _, insert = iceberg_merge_sql("dim", "stg", "2026-11-03", cfg)
    assert "CAST(s.`branch_id` AS STRING), CAST(s.`account_id` AS STRING)" in insert
    assert "CAST(DATE '2026-11-03' AS STRING)" in insert


def test_invalid_as_of_date_is_rejected():
    with pytest.raises(Exception):
        iceberg_merge_sql("dim", "stg", "not-a-date", CONFIG)
