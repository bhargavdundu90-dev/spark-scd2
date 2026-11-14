import datetime as dt

import pytest

from spark_scd2 import ConfigError
from spark_scd2.config import to_date


def test_iso_string_is_parsed():
    assert to_date("2026-11-14") == dt.date(2026, 11, 14)


def test_date_passes_through():
    d = dt.date(2026, 1, 31)
    assert to_date(d) is d


def test_datetime_is_truncated_to_date():
    assert to_date(dt.datetime(2026, 5, 6, 23, 59)) == dt.date(2026, 5, 6)


@pytest.mark.parametrize("bad", ["14/11/2026", "2026-13-01", ""])
def test_bad_strings_raise_config_error(bad):
    with pytest.raises(ConfigError, match="YYYY-MM-DD"):
        to_date(bad, "snapshot_date")


@pytest.mark.parametrize("bad", [20261114, None, 1.5])
def test_non_date_types_raise_config_error(bad):
    with pytest.raises(ConfigError, match="got"):
        to_date(bad)
