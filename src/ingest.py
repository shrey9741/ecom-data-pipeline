"""
Ingestion layer.

- load_batch(): full load of the legacy nightly dump into a staging table.
- load_incremental(): CDC-style incremental load. Reads a watermark (the
  max order_date already ingested) from a small state table, and only
  pulls rows newer than that watermark from the incoming feed -- so
  re-running this script never double-ingests the same incremental rows.

Both paths log row counts and any read failures to logs/pipeline.log.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from logger_setup import get_logger

BASE = Path(__file__).parent.parent
DB_PATH = BASE / "warehouse.db"
logger = get_logger("ingest")


def _connect():
    return sqlite3.connect(DB_PATH)


def _ensure_state_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()


def _get_watermark(conn):
    _ensure_state_table(conn)
    row = conn.execute(
        "SELECT value FROM pipeline_state WHERE key = 'orders_watermark'"
    ).fetchone()
    return row[0] if row else "1970-01-01 00:00:00"


def _set_watermark(conn, value):
    conn.execute(
        "INSERT INTO pipeline_state (key, value) VALUES ('orders_watermark', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (value,),
    )
    conn.commit()


def load_batch():
    conn = _connect()
    try:
        customers = pd.read_csv(BASE / "data" / "raw" / "customers.csv")
        products = pd.read_csv(BASE / "data" / "raw" / "products.csv")
        orders = pd.read_csv(BASE / "data" / "raw" / "orders_batch.csv")
        returns = pd.read_csv(BASE / "data" / "raw" / "returns.csv")

        customers.to_sql("stg_customers", conn, if_exists="replace", index=False)
        products.to_sql("stg_products", conn, if_exists="replace", index=False)
        orders.to_sql("stg_orders_batch", conn, if_exists="replace", index=False)
        returns.to_sql("stg_returns", conn, if_exists="replace", index=False)

        logger.info(
            f"BATCH LOAD ok | customers={len(customers)} products={len(products)} "
            f"orders_batch={len(orders)} returns={len(returns)}"
        )
    except Exception as e:
        logger.error(f"BATCH LOAD failed: {e}")
        raise
    finally:
        conn.close()


def load_incremental():
    conn = _connect()
    try:
        watermark = _get_watermark(conn)
        incoming = pd.read_csv(BASE / "data" / "incoming" / "orders_incremental.csv")
        incoming["order_date"] = pd.to_datetime(incoming["order_date"], errors="coerce")

        new_rows = incoming[incoming["order_date"] > pd.Timestamp(watermark)]

        if new_rows.empty:
            logger.info(f"INCREMENTAL LOAD skipped | no rows newer than watermark={watermark}")
            return

        new_rows.to_sql("stg_orders_incremental", conn, if_exists="append", index=False)

        new_watermark = new_rows["order_date"].max().strftime("%Y-%m-%d %H:%M:%S")
        _set_watermark(conn, new_watermark)

        logger.info(
            f"INCREMENTAL LOAD ok | prev_watermark={watermark} "
            f"new_rows={len(new_rows)} new_watermark={new_watermark}"
        )
    except Exception as e:
        logger.error(f"INCREMENTAL LOAD failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    load_batch()
    load_incremental()
    load_incremental()  # run twice on purpose -- proves the watermark stops re-ingestion
