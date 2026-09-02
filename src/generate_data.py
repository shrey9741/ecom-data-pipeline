"""
Generates synthetic e-commerce source data across 3 systems, with deliberate
schema/format mismatches so the pipeline has real harmonization work to do:

  1. customers.csv     -- CRM export (clean-ish, snake_case)
  2. products.csv       -- Product catalog (clean-ish)
  3. orders_batch.csv    -- Legacy nightly batch dump (messy: inconsistent
                            date formats, mixed-case status, some nulls)
  4. orders_incremental.csv -- "new orders since last run" (simulates a
                            CDC-style incremental feed, timestamp-watermarked)
  5. returns.csv         -- Returns/complaints feed (has orphan order_ids
                            and duplicate rows -- to be caught by validation)

Run: python src/generate_data.py
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
INCOMING_DIR = Path(__file__).parent.parent / "data" / "incoming"
RAW_DIR.mkdir(parents=True, exist_ok=True)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 300
N_PRODUCTS = 60
N_ORDERS_BATCH = 1500
N_ORDERS_INCREMENTAL = 200
N_RETURNS = 180

STATUSES_MESSY = ["Delivered", "delivered", "DELIVERED", "Shipped", "shipped",
                   "Cancelled", "cancelled", "Pending", "Returned"]
CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Beauty", "Sports", "Books"]


def gen_customers():
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        rows.append({
            "customer_id": f"C{i:05d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "signup_date": fake.date_between(start_date="-3y", end_date="-30d").isoformat(),
        })
    with open(RAW_DIR / "customers.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


def gen_products():
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        rows.append({
            "product_id": f"P{i:04d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "unit_price": round(random.uniform(99, 15000), 2),
        })
    with open(RAW_DIR / "products.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _messy_date(dt):
    """Randomly format the same date 3 different ways -- simulates a legacy
    export where the date format was never standardized."""
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y"])
    return dt.strftime(fmt)


def gen_orders_batch(customers, products):
    rows = []
    base_date = datetime.now() - timedelta(days=60)
    for i in range(1, N_ORDERS_BATCH + 1):
        cust = random.choice(customers)
        prod = random.choice(products)
        order_dt = base_date + timedelta(days=random.randint(0, 45),
                                          hours=random.randint(0, 23))
        # inject some intentional messiness
        customer_id = cust["customer_id"] if random.random() > 0.02 else ""  # 2% missing
        qty = random.randint(1, 5)
        rows.append({
            "order_id": f"O{i:06d}",
            "customer_id": customer_id,
            "product_id": prod["product_id"],
            "quantity": qty,
            "order_date": _messy_date(order_dt),
            "status": random.choice(STATUSES_MESSY),
        })
    # inject a handful of exact duplicate rows (simulates a re-run dump)
    rows.extend(random.sample(rows, 12))
    with open(RAW_DIR / "orders_batch.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


def gen_orders_incremental(customers, products, start_id):
    """Simulates new orders arriving after the last pipeline run --
    watermarked by order_date, which the ingestion step will filter on."""
    rows = []
    now = datetime.now()
    for i in range(N_ORDERS_INCREMENTAL):
        cust = random.choice(customers)
        prod = random.choice(products)
        order_dt = now - timedelta(hours=random.randint(0, 36))
        rows.append({
            "order_id": f"O{start_id + i:06d}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": random.randint(1, 5),
            "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "status": random.choice(["Pending", "Shipped", "Delivered"]),
        })
    with open(INCOMING_DIR / "orders_incremental.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


def gen_returns(order_rows):
    rows = []
    valid_order_ids = [r["order_id"] for r in order_rows]
    for i in range(1, N_RETURNS + 1):
        # ~5% reference an order_id that doesn't exist -- orphan record,
        # should be caught by referential-integrity validation
        if random.random() < 0.05:
            order_id = f"O{random.randint(900000, 999999):06d}"
        else:
            order_id = random.choice(valid_order_ids)
        rows.append({
            "return_id": f"R{i:05d}",
            "order_id": order_id,
            "reason": random.choice(["Defective", "Wrong item", "Not as described",
                                      "Changed mind", "Late delivery"]),
            "return_date": fake.date_between(start_date="-30d", end_date="today").isoformat(),
        })
    # inject a few duplicate return rows
    rows.extend(random.sample(rows, 6))
    with open(RAW_DIR / "returns.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return rows


if __name__ == "__main__":
    customers = gen_customers()
    products = gen_products()
    orders_batch = gen_orders_batch(customers, products)
    orders_incremental = gen_orders_incremental(products=products, customers=customers,
                                                 start_id=N_ORDERS_BATCH + 1)
    gen_returns(orders_batch + orders_incremental)
    print(f"Generated: {len(customers)} customers, {len(products)} products, "
          f"{len(orders_batch)} batch orders (incl. dupes), "
          f"{len(orders_incremental)} incremental orders, "
          f"{N_RETURNS} return records (incl. dupes/orphans)")
