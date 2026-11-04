"""Generate the Spark SQL statements for the same load on an Apache Iceberg table.

Useful when the dimension lives in Iceberg and you would rather run ``MERGE INTO`` in place
(Glue, EMR, Databricks or any Spark session with the Iceberg SQL extensions) than rewrite the
table from a DataFrame.

The statements expect:

* ``target``: an existing Iceberg table whose columns are ``config.dimension_columns``, in
  that order (``empty_dimension`` gives a matching schema to create it from);
* ``source``: a table or temporary view holding the snapshot, with at least
  ``config.data_columns``. Validate it first with :func:`spark_scd2.validate_snapshot`, since
  SQL cannot refuse an empty or duplicated snapshot on its own.

Run the statements in order, as one job. Hashes and surrogate keys match the DataFrame API,
so both paths can be mixed on the same table.
"""

from __future__ import annotations

import re

from .config import DateLike, SCD2Config, to_date
from .core import NULL_TOKEN, SEPARATOR

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _q(name: str) -> str:
    """Quote one identifier with backticks."""
    return "`" + name.replace("`", "``") + "`"


def _table(name: str) -> str:
    parts = name.split(".")
    if not all(_IDENT.match(p) for p in parts):
        raise ValueError(f"table name must be dot-separated identifiers, got {name!r}")
    return ".".join(_q(p) for p in parts)


def _hash_expr(columns, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    parts = ", ".join(f"coalesce(CAST({prefix}{_q(c)} AS STRING), '{NULL_TOKEN}')" for c in columns)
    return f"sha2(concat_ws('{SEPARATOR}', {parts}), 256)"


def _on(keys, left: str, right: str) -> str:
    return " AND ".join(f"{left}.{_q(k)} = {right}.{_q(k)}" for k in keys)


def iceberg_merge_sql(target: str, source: str, as_of_date: DateLike, config: SCD2Config) -> list[str]:
    """Return the SQL statements that load ``source`` into ``target`` as of ``as_of_date``.

    1. ``MERGE`` that closes changed versions (and missing keys when ``delete_mode="close"``),
    2. ``INSERT`` of new versions for new, changed and reopened keys,
    3. ``MERGE`` of Type 1 corrections, only when Type 1 columns are configured.
    """
    t, s = _table(target), _table(source)
    d = f"DATE '{to_date(as_of_date, 'as_of_date').isoformat()}'"
    open_end = f"DATE '{config.open_end_date.isoformat()}'"
    keys = list(config.keys)
    k0 = _q(keys[0])
    eff_to = _q(config.effective_to_col)
    is_cur, hash_col = _q(config.is_current_col), _q(config.hash_col)
    key_list = ", ".join(_q(k) for k in keys)
    c_keys = ", ".join(f"c.{_q(k)}" for k in keys)

    if config.delete_mode == "close":
        snap_join = "LEFT JOIN"
        close_when = f"x.new_hash IS NULL OR x.new_hash <> d.{hash_col}"
    else:
        snap_join = "JOIN"
        close_when = f"x.new_hash <> d.{hash_col}"

    close = f"""MERGE INTO {t} d
USING (
    SELECT {c_keys}, s.new_hash
    FROM {t} c
    {snap_join} (SELECT {key_list}, {_hash_expr(config.type2_cols)} AS new_hash FROM {s}) s
      ON {_on(keys, "s", "c")}
    WHERE c.{is_cur}
) x
ON {_on(keys, "d", "x")} AND d.{is_cur}
WHEN MATCHED AND ({close_when}) THEN
    UPDATE SET {eff_to} = {d}, {is_cur} = false"""

    select = []
    if config.surrogate_key_col:
        sk_parts = ", ".join(f"CAST(s.{_q(k)} AS STRING)" for k in keys)
        select.append(f"substr(sha2(concat_ws(':', {sk_parts}, CAST({d} AS STRING)), 256), 1, 16)")
    select += [f"s.{_q(c)}" for c in config.data_columns]
    select += [d, open_end, "true"]
    if config.change_type_col:
        select.append(
            f"CASE WHEN h.{k0} IS NULL THEN 'insert' WHEN h.last_to = {d} THEN 'update' ELSE 'reopen' END"
        )
    select.append(_hash_expr(config.type2_cols, "s"))
    select_sql = ",\n       ".join(select)

    insert = f"""INSERT INTO {t}
SELECT {select_sql}
FROM {s} s
LEFT JOIN {t} c ON {_on(keys, "c", "s")} AND c.{is_cur}
LEFT JOIN (SELECT {key_list}, max({eff_to}) AS last_to FROM {t} GROUP BY {key_list}) h
  ON {_on(keys, "h", "s")}
WHERE c.{k0} IS NULL"""

    statements = [close, insert]

    if config.type1_cols:
        t1 = [_q(c) for c in config.type1_cols]
        differs = " OR ".join(f"NOT (d.{c} <=> s.{c})" for c in t1)
        sets = ", ".join(f"{c} = s.{c}" for c in t1)
        statements.append(
            f"""MERGE INTO {t} d
USING (SELECT {key_list}, {", ".join(t1)} FROM {s}) s
ON {_on(keys, "d", "s")}
WHEN MATCHED AND ({differs}) THEN
    UPDATE SET {sets}"""
        )
    return statements
