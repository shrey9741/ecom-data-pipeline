# E-Commerce Data Pipeline

A batch + incremental ingestion pipeline that harmonizes multi-source order
data, validates it against schema/domain/referential rules, quarantines bad
records for audit, and loads clean star-schema tables into a warehouse.

Built to mirror a real data-engineering workflow: **ingest → harmonize →
validate → orchestrate → log**, using Python, pandas, and SQL (SQLite here;
schema is portable to Postgres) rather than a big-data stack, since the
scale doesn't need one.

## Why this exists

Most take-home/portfolio pipelines run one clean CSV through pandas and stop.
This one deliberately uses **messy, multi-source synthetic data** — mixed
date formats, inconsistent status casing, missing keys, duplicates, and
orphan foreign keys — because that's the actual job: data engineers spend
most of their time on harmonization and validation, not on the "happy path"
transform.

## Architecture

```
data/raw/customers.csv        \
data/raw/products.csv          |--> ingest.load_batch() ------\
data/raw/orders_batch.csv      |                                 \
data/raw/returns.csv          /                                    v
                                                              warehouse.db (SQLite)
data/incoming/orders_incremental.csv --> ingest.load_incremental()  |
        (watermarked on order_date,                                 |
         idempotent on re-run)                                      v
                                                        transform_validate.run()
                                                          - harmonize order schema
                                                          - 5 validation checks
                                                          - quarantine failures
                                                                      |
                                                                      v
                                                    fact_orders / fact_returns
                                                    dim_customers / dim_products
                                                    quarantine_orders / quarantine_returns
```

`pipeline.py` orchestrates all stages in sequence, times each one, and logs
to both console and `logs/pipeline.log`. A stage failure aborts the run
instead of letting downstream stages run on partial data.

## Run it

```bash
pip install pandas faker
python src/generate_data.py   # generates synthetic messy source data
python src/pipeline.py        # runs the full pipeline (linear orchestration)
```

### Airflow variant

`dags/ecom_pipeline_dag.py` wraps the same `ingest.py` / `transform_validate.py`
functions in an Airflow DAG instead of the linear script — `ingest_batch`
and `ingest_incremental` run in parallel, `transform_and_validate` depends
on both, with 2 retries and a 5-minute retry delay per task. The business
logic is identical; only the orchestration layer changes, which is the
actual point of adopting Airflow over a cron'd script.

```bash
pip install apache-airflow
# place dags/ under $AIRFLOW_HOME/dags, ensure src/ is on PYTHONPATH
airflow dags trigger ecom_pipeline
```


## Validation checks implemented

1. **Schema/type check** — unparseable dates rejected
2. **Null check** — required fields (customer_id, product_id, order_date) not null
3. **Duplicate check** — exact duplicate order_id / return_id removed, first kept
4. **Referential integrity** — order.product_id must exist in products;
   return.order_id must exist in validated orders
5. **Domain/range check** — quantity in [1, 20], status in known value set

Every rejected row is written to a `quarantine_*` table with a reason code
— nothing is silently dropped.

## Latest run (actual numbers, not illustrative)

| Stage | Result |
|---|---|
| Batch load | 300 customers, 60 products, 1512 orders, 186 returns |
| Incremental load | 200 new orders ingested; watermark = max(order_date) |
| Incremental re-run | 0 rows (watermark correctly prevents double-ingestion) |
| Harmonized orders | 1712 (1512 batch + 200 incremental, unified schema) |
| Orders quarantined | 38 (26 missing customer_id, 12 duplicate order_id) |
| Orders loaded | 1674 → `fact_orders` |
| Returns quarantined | 20 (14 orphan order_id, 6 duplicate return_id) |
| Returns loaded | 166 → `fact_returns` |
| Total pipeline runtime | 0.08s |

## One design decision worth explaining

**Why a watermark instead of a full reload on every run:** a full reload of
`orders_batch` on every run is simple but doesn't scale and risks
re-processing records that already passed validation. Watermarking the
incremental feed on `order_date` and persisting it in a `pipeline_state`
table means each run only processes genuinely new data — this is the same
core idea as CDC-based ingestion, just without a message broker in front of
it. If this needed to scale to a real streaming source, the watermark
column would move from an app-level SQLite table to whatever offset/LSN
mechanism the source system exposes (Kafka offset, DB replication LSN,
etc.) — the incremental logic stays the same.

## What I'd add with more time

- Airflow DAG instead of the linear `pipeline.py` orchestrator, for
  proper scheduling/retries
- Great Expectations instead of hand-rolled validation functions
- Postgres instead of SQLite for concurrent writes
