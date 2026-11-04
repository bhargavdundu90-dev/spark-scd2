import datetime as dt

import pytest

from spark_scd2 import ConfigError, SCD2Config


def test_defaults_and_column_order():
    cfg = SCD2Config(keys="id", type2_cols=["a", "b"], type1_cols=["c"], surrogate_key_col="sk")
    assert cfg.keys == ("id",)
    assert cfg.open_end_date == dt.date(9999, 12, 31)
    assert cfg.dimension_columns == (
        "sk",
        "id",
        "a",
        "b",
        "c",
        "effective_from",
        "effective_to",
        "is_current",
        "change_type",
        "scd2_hash",
    )


def test_change_type_column_can_be_dropped():
    cfg = SCD2Config(keys=["id"], type2_cols=["a"], change_type_col=None)
    assert "change_type" not in cfg.dimension_columns


def test_open_end_accepts_a_date():
    cfg = SCD2Config(keys=["id"], type2_cols=["a"], open_end=dt.date(2999, 1, 1))
    assert cfg.open_end_date == dt.date(2999, 1, 1)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (dict(keys=[], type2_cols=["a"]), "key column"),
        (dict(keys=["id"], type2_cols=[]), "Type 2 column"),
        (dict(keys=["id"], type2_cols=["a"], type1_cols=["A"]), "one role"),
        (dict(keys=["id"], type2_cols=["id"]), "one role"),
        (dict(keys=["id"], type2_cols=["is_current"]), "one role"),
        (dict(keys=["id"], type2_cols=["a"], delete_mode="drop"), "delete_mode"),
        (dict(keys=["id"], type2_cols=["a"], open_end="31/12/9999"), "open_end"),
        (dict(keys=["id", ""], type2_cols=["a"]), "non-empty"),
    ],
)
def test_inconsistent_config_is_rejected(kwargs, message):
    with pytest.raises(ConfigError, match=message):
        SCD2Config(**kwargs)


def test_config_is_immutable():
    cfg = SCD2Config(keys=["id"], type2_cols=["a"])
    with pytest.raises(AttributeError):
        cfg.keys = ("other",)
