"""Small builders shared by the tests."""

import datetime as dt
import random
from decimal import Decimal

from pyspark.sql import functions as F

from spark_scd2 import SCD2Config, apply_snapshot

D1, D2, D3, D4 = (dt.date(2026, 11, d) for d in (1, 2, 3, 4))
OPEN = dt.date(9999, 12, 31)

SCHEMA = (
    "account_id string, product string, rate decimal(5,2), branch string, status string, "
    "customer_name string, mobile string"
)

CONFIG = SCD2Config(
    keys=["account_id"],
    type2_cols=["product", "rate", "branch", "status"],
    type1_cols=["customer_name", "mobile"],
    surrogate_key_col="account_sk",
)


def acc(
    account_id, rate="9.00", branch="HYD001", status="ACTIVE", name=None, mobile="9000000000", product="HL"
):
    return (account_id, product, Decimal(rate), branch, status, name or f"Customer {account_id}", mobile)


def snap(spark, *rows, schema=SCHEMA):
    return spark.createDataFrame(list(rows), schema)


def load(spark, *days, config=CONFIG, dim=None):
    for day, rows in days:
        dim = apply_snapshot(dim, snap(spark, *rows), day, config)
    return dim


def versions(dim, account_id, key="account_id", order="effective_from"):
    return [r.asDict() for r in dim.where(F.col(key) == account_id).orderBy(order).collect()]


def as_rows(df):
    return sorted(tuple(r) for r in df.collect())


def random_history(days=6, accounts=20, seed=7, change_rate=0.3):
    """Daily full snapshots with changes, deletes, reopens and Type 1 corrections."""
    rng = random.Random(seed)
    alive = {f"L{i:03d}": acc(f"L{i:03d}", rate=f"{rng.uniform(7, 12):.2f}") for i in range(accounts)}
    gone = {}
    history = []
    for n in range(days):
        day = dt.date(2026, 10, 1) + dt.timedelta(days=n)
        if n:
            for key in list(gone):  # keys missing since an earlier day may come back
                if rng.random() < 0.4:
                    alive[key] = gone.pop(key)
            for key in list(alive):
                roll = rng.random()
                row = list(alive[key])
                if roll < change_rate / 3:
                    row[2] = Decimal(f"{rng.uniform(7, 12):.2f}")
                elif roll < 2 * change_rate / 3:
                    row[4] = rng.choice(["ACTIVE", "SMA1", "SMA2", "NPA", None])
                elif roll < change_rate:
                    row[6] = f"9{rng.randrange(10**8, 10**9)}"  # Type 1 only
                elif roll < change_rate + 0.1:
                    gone[key] = alive.pop(key)
                    continue
                alive[key] = tuple(row)
            for i in range(rng.randrange(0, 3)):
                key = f"N{n:02d}{i}"
                alive[key] = acc(key)
        history.append((day, sorted(alive.values())))
    return history
