"""Configuration for an SCD Type 2 dimension."""

from __future__ import annotations

import datetime as _dt
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Union

from .errors import ConfigError

DateLike = Union[_dt.date, str]

DELETE_MODES = ("close", "ignore")


def to_date(value: DateLike, what: str = "date") -> _dt.date:
    """Accept a ``datetime.date`` or an ISO ``YYYY-MM-DD`` string."""
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ConfigError(f"{what} must be YYYY-MM-DD, got {value!r}") from exc
    raise ConfigError(f"{what} must be a date or a YYYY-MM-DD string, got {type(value).__name__}")


def _as_tuple(value: str | Sequence[str] | None, name: str) -> tuple:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    out = tuple(value)
    for c in out:
        if not isinstance(c, str) or not c:
            raise ConfigError(f"{name} must contain non-empty column names, got {c!r}")
    return out


@dataclass(frozen=True)
class SCD2Config:
    """Describes which column plays which role in the dimension.

    Parameters
    ----------
    keys:
        Business (natural) key columns. Together they identify one entity in the snapshot.
    type2_cols:
        Columns whose changes are tracked as history. A change closes the current version
        and opens a new one.
    type1_cols:
        Columns that are overwritten in place on every version of the entity (corrections,
        such as a misspelt name). They never create a new version.
    effective_from_col, effective_to_col:
        Validity columns. Validity is half-open: a version is valid on ``d`` when
        ``effective_from <= d < effective_to``.
    is_current_col:
        Boolean flag, true on the single open version of each entity.
    open_end:
        ``effective_to`` value of the open version. Defaults to ``9999-12-31``.
    hash_col:
        Column that stores the SHA-256 of the Type 2 columns.
    change_type_col:
        Optional column recording why a version was opened: ``insert``, ``update`` or
        ``reopen``. Set to ``None`` to leave it out.
    surrogate_key_col:
        Optional surrogate key column, a 16-character hex id derived from the keys and the
        version's ``effective_from``. Deterministic, so re-running a load gives the same ids.
    delete_mode:
        What to do with entities that are in the dimension but missing from the snapshot.
        ``"close"`` (default) treats the snapshot as complete and closes them;
        ``"ignore"`` treats it as a partial extract and leaves them open.
    """

    keys: Sequence[str]
    type2_cols: Sequence[str]
    type1_cols: Sequence[str] = ()
    effective_from_col: str = "effective_from"
    effective_to_col: str = "effective_to"
    is_current_col: str = "is_current"
    open_end: DateLike = "9999-12-31"
    hash_col: str = "scd2_hash"
    change_type_col: str | None = "change_type"
    surrogate_key_col: str | None = None
    delete_mode: str = "close"
    _open_end: _dt.date = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        keys = _as_tuple(self.keys, "keys")
        type2 = _as_tuple(self.type2_cols, "type2_cols")
        type1 = _as_tuple(self.type1_cols, "type1_cols")
        object.__setattr__(self, "keys", keys)
        object.__setattr__(self, "type2_cols", type2)
        object.__setattr__(self, "type1_cols", type1)
        object.__setattr__(self, "_open_end", to_date(self.open_end, "open_end"))

        if not keys:
            raise ConfigError("at least one key column is required")
        if not type2:
            raise ConfigError("at least one Type 2 column is required; use apply_type1 for Type 1 only")
        if self.delete_mode not in DELETE_MODES:
            raise ConfigError(f"delete_mode must be one of {DELETE_MODES}, got {self.delete_mode!r}")

        names = [*keys, *type2, *type1, *self.metadata_columns]
        lowered = [n.lower() for n in names]
        dupes = sorted({n for n, low in zip(names, lowered) if lowered.count(low) > 1})
        if dupes:
            raise ConfigError(f"a column can only have one role, repeated: {', '.join(dupes)}")

    @property
    def open_end_date(self) -> _dt.date:
        return self._open_end

    @property
    def data_columns(self) -> tuple:
        """Key, Type 2 and Type 1 columns, in that order."""
        return (*self.keys, *self.type2_cols, *self.type1_cols)

    @property
    def metadata_columns(self) -> tuple:
        """Columns the library maintains, in dimension order."""
        cols = [self.effective_from_col, self.effective_to_col, self.is_current_col]
        if self.change_type_col:
            cols.append(self.change_type_col)
        cols.append(self.hash_col)
        return tuple(cols)

    @property
    def dimension_columns(self) -> tuple:
        """Every dimension column, in the order the library writes them."""
        sk = (self.surrogate_key_col,) if self.surrogate_key_col else ()
        return (*sk, *self.data_columns, *self.metadata_columns)
