"""Exceptions raised by spark-scd2."""


class SCD2Error(ValueError):
    """Base class for every error raised by this library."""


class ConfigError(SCD2Error):
    """The configuration is inconsistent (missing keys, a column with two roles, ...)."""


class SnapshotError(SCD2Error):
    """The snapshot, or the load as a whole, is unsafe to apply to the dimension."""
