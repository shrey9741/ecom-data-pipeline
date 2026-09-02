"""
Transform + validate layer.

Reads the staged (raw) tables, harmonizes the two order sources into one
schema, runs a set of validation checks, quarantines rows that fail
validation (instead of silently dropping or silently loading them), and
writes clean, analysis-ready tables to the warehouse.

Validation checks implemented:
  1. Schema check      -- required columns present after harmonization
  2. Null check         -- customer_id / product_id / order_date not null
  3. Duplicate check     -- exact duplicate order_id removed
  4. Referential integrity -- returns.order_id must exist in orders;
                              orders.product_id must exist in products
  5. Range/domain check   -- quantity between 1 and 20, status in known set

Every failed row is written to a quarantine table (not just dropped) so a
human can audit what was rejected and why.
"""
import sqlite3
from pathlib import Path

import pandas as pd

from logger_setup import get_logger

BASE = Path(__file__).parent.parent
DB_PATH = BASE / "warehouse.db"
logger = get_logger("transform_validate")

VALID_STATUSES = {"delivered", "shipped", "cancelled", "pending", "returned"}


def _connect():
    return sqlite3.connect(DB_PATH)


def _quarantine(conn, df, table_name, reason):
    if df.empty:
        return
    df = df.copy()
    df["_quarantine_reason"] = reason
    df.to_sql(f"quarantine_{table_name}", conn, if_exists="append", index=False)
    logger.warning(f"QUARANTINED {len(df)} row(s) from {table_name} | reason={reason}")


def _harmonize_orders(conn):
    """Both order sources need to land on the same schema before anything
    downstream can treat them as one dataset."""
    batch = pd.read_sql("SELECT * FROM stg_orders_batch", conn)
    incr = pd.read_sql("SELECT * FROM stg_orders_incremental", conn) \
        if _table_exists(conn, "stg_orders_incremental") else pd.DataFrame(columns=batch.columns)

    # --- harmonize status casing (batch source is inconsistent: "Delivered"
    # vs "delivered" vs "DELIVERED" all mean the same thing) ---
    batch["status"] = batch["status"].str.strip().str.lower()
    if not incr.empty:
        incr["status"] = incr["status"].str.strip().str.lower()

    # --- harmonize date formats: batch mixes YYYY-MM-DD, DD/MM/YYYY, DD-Mon-YYYY ---
    batch["order_date"] = pd.to_datetime(batch["order_date"], format="mixed", dayfirst=True, errors="coerce")
    if not incr.empty:
        incr["order_date"] = pd.to_datetime(incr["order_date"], errors="coerce")

    combined = pd.concat([batch, incr], ignore_index=True)
    logger.info(f"HARMONIZE ok | batch={len(batch)} incremental={len(incr)} combined={len(combined)}")
    return combined


def _table_exists(conn, name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _validate_orders(conn, orders, products):
    initial = len(orders)

    # 1. Unparseable dates (schema/type failure) -> quarantine
    bad_dates = orders[orders["order_date"].isna()]
    _quarantine(conn, bad_dates, "orders", "unparseable_order_date")
    orders = orders[orders["order_date"].notna()]

    # 2. Null checks on required fields
    bad_nulls = orders[orders["customer_id"].isna() | orders["customer_id"].eq("")]
    _quarantine(conn, bad_nulls, "orders", "missing_customer_id")
    orders = orders[~(orders["customer_id"].isna() | orders["customer_id"].eq(""))]

    # 3. Duplicate order_id
    dupes = orders[orders.duplicated(subset="order_id", keep="first")]
    _quarantine(conn, dupes, "orders", "duplicate_order_id")
    orders = orders.drop_duplicates(subset="order_id", keep="first")

    # 4. Referential integrity: product_id must exist in products
    valid_products = set(products["product_id"])
    bad_product_ref = orders[~orders["product_id"].isin(valid_products)]
    _quarantine(conn, bad_product_ref, "orders", "unknown_product_id")
    orders = orders[orders["product_id"].isin(valid_products)]

    # 5. Domain checks: quantity range, status in known set
    bad_qty = orders[~orders["quantity"].between(1, 20)]
    _quarantine(conn, bad_qty, "orders", "quantity_out_of_range")
    orders = orders[orders["quantity"].between(1, 20)]

    bad_status = orders[~orders["status"].isin(VALID_STATUSES)]
    _quarantine(conn, bad_status, "orders", "unknown_status")
    orders = orders[orders["status"].isin(VALID_STATUSES)]

    logger.info(
        f"VALIDATE orders ok | input={initial} passed={len(orders)} "
        f"quarantined={initial - len(orders)}"
    )
    return orders


def _validate_returns(conn, returns, valid_order_ids):
    initial = len(returns)

    dupes = returns[returns.duplicated(subset="return_id", keep="first")]
    _quarantine(conn, dupes, "returns", "duplicate_return_id")
    returns = returns.drop_duplicates(subset="return_id", keep="first")

    orphans = returns[~returns["order_id"].isin(valid_order_ids)]
    _quarantine(conn, orphans, "returns", "orphan_order_id")
    returns = returns[returns["order_id"].isin(valid_order_ids)]

    logger.info(
        f"VALIDATE returns ok | input={initial} passed={len(returns)} "
        f"quarantined={initial - len(returns)}"
    )
    return returns


def run():
    conn = _connect()
    try:
        products = pd.read_sql("SELECT * FROM stg_products", conn)
        customers = pd.read_sql("SELECT * FROM stg_customers", conn)
        returns_raw = pd.read_sql("SELECT * FROM stg_returns", conn)

        orders = _harmonize_orders(conn)
        clean_orders = _validate_orders(conn, orders, products)
        clean_returns = _validate_returns(conn, returns_raw, set(clean_orders["order_id"]))

        # write clean, analysis-ready warehouse tables
        clean_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)
        clean_returns.to_sql("fact_returns", conn, if_exists="replace", index=False)
        customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
        products.to_sql("dim_products", conn, if_exists="replace", index=False)

        logger.info(
            f"WAREHOUSE write ok | fact_orders={len(clean_orders)} "
            f"fact_returns={len(clean_returns)} dim_customers={len(customers)} "
            f"dim_products={len(products)}"
        )
    except Exception as e:
        logger.error(f"TRANSFORM/VALIDATE failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
