"""
E-Commerce Analytics Dashboard

Reads directly from warehouse.db -- the validated, quarantine-filtered
output of the ingest -> harmonize -> validate pipeline (see src/pipeline.py).
This is deliberately NOT reading raw source files: the point is to show
the dashboard as the business-facing layer sitting on top of a pipeline
that already guarantees data quality, not a fresh ad-hoc analysis.

Run: streamlit run dashboard/app.py
"""
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE = Path(__file__).parent.parent
DB_PATH = BASE / "warehouse.db"

st.set_page_config(page_title="E-Commerce Pipeline Dashboard", layout="wide")


@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    orders = pd.read_sql("SELECT * FROM fact_orders", conn)
    returns = pd.read_sql("SELECT * FROM fact_returns", conn)
    products = pd.read_sql("SELECT * FROM dim_products", conn)
    customers = pd.read_sql("SELECT * FROM dim_customers", conn)

    q_orders = pd.read_sql("SELECT * FROM quarantine_orders", conn)
    q_returns = pd.read_sql("SELECT * FROM quarantine_returns", conn)

    conn.close()

    orders["order_date"] = pd.to_datetime(orders["order_date"])
    orders = orders.merge(products, on="product_id", how="left")
    orders["revenue"] = orders["quantity"] * orders["unit_price"]

    returns_full = returns.merge(
        orders[["order_id", "category"]], on="order_id", how="left"
    )

    return orders, returns_full, q_orders, q_returns


orders, returns_full, q_orders, q_returns = load_data()

st.title("E-Commerce Order Pipeline — Analytics Dashboard")
st.caption(
    "Built on top of a batch + incremental ETL pipeline with schema harmonization, "
    "validation, and quarantine handling. Figures below reflect only records that "
    "passed validation."
)

# ---- KPI row ----
total_orders = len(orders)
total_revenue = orders["revenue"].sum()
avg_order_value = orders["revenue"].mean()
return_rate = len(returns_full) / total_orders * 100 if total_orders else 0
quarantine_rate = (
    (len(q_orders) + len(q_returns))
    / (total_orders + len(q_orders) + len(returns_full) + len(q_returns))
    * 100
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Orders", f"{total_orders:,}")
k2.metric("Total Revenue", f"₹{total_revenue:,.0f}")
k3.metric("Avg Order Value", f"₹{avg_order_value:,.0f}")
k4.metric("Return Rate", f"{return_rate:.1f}%")
k5.metric("Records Quarantined", f"{quarantine_rate:.1f}%")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Order Volume")
    daily = orders.set_index("order_date").resample("D").size().reset_index(name="orders")
    fig = px.line(daily, x="order_date", y="orders")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Revenue by Category")
    by_cat = orders.groupby("category")["revenue"].sum().sort_values(ascending=False).reset_index()
    fig = px.bar(by_cat, x="category", y="revenue")
    st.plotly_chart(fig, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Return Reasons")
    by_reason = returns_full["reason"].value_counts().reset_index()
    by_reason.columns = ["reason", "count"]
    fig = px.pie(by_reason, names="reason", values="count", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Order Status Breakdown")
    by_status = orders["status"].value_counts().reset_index()
    by_status.columns = ["status", "count"]
    fig = px.bar(by_status, x="status", y="count")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Data Quality — Why Records Were Quarantined")
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Orders quarantined**")
    if not q_orders.empty:
        st.dataframe(q_orders["_quarantine_reason"].value_counts().reset_index(
            names=["reason", "count"]), use_container_width=True)
    else:
        st.write("None")
with c2:
    st.markdown("**Returns quarantined**")
    if not q_returns.empty:
        st.dataframe(q_returns["_quarantine_reason"].value_counts().reset_index(
            names=["reason", "count"]), use_container_width=True)
    else:
        st.write("None")

st.subheader("Top & Bottom Products by Revenue")
by_product = orders.groupby("product_name")["revenue"].sum().sort_values(ascending=False)
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Top 5**")
    st.dataframe(by_product.head(5).reset_index(), use_container_width=True)
with c2:
    st.markdown("**Bottom 5**")
    st.dataframe(by_product.tail(5).reset_index(), use_container_width=True)
