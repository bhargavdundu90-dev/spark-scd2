"""Slowly changing dimension Type 2 loads from full snapshots in PySpark."""

from .config import SCD2Config
from .core import apply_snapshot, apply_type1, as_of, current, empty_dimension, row_hash, validate_snapshot
from .errors import ConfigError, SCD2Error, SnapshotError
from .sql import iceberg_merge_sql

__version__ = "0.1.0"

__all__ = [
    "SCD2Config",
    "apply_snapshot",
    "apply_type1",
    "as_of",
    "current",
    "empty_dimension",
    "row_hash",
    "validate_snapshot",
    "iceberg_merge_sql",
    "SCD2Error",
    "ConfigError",
    "SnapshotError",
    "__version__",
]
