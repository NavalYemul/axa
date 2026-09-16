# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Create catalog, schema, tables and load demo data
# MAGIC
# MAGIC Creates the "Northwind Retail" star schema for the Genie One + Ontology demo:
# MAGIC - Catalog + schema (with comments)
# MAGIC - 4 dimension tables + 3 fact tables, every table and every column carries a
# MAGIC   `COMMENT` (this is exactly the metadata Genie's *inferred context* layer reads)
# MAGIC - Informational `PRIMARY KEY` / `FOREIGN KEY` constraints (Unity Catalog constraints
# MAGIC   are informational/not-enforced, but they are what lets Genie and BI tools infer
# MAGIC   join paths automatically)
# MAGIC - Synthetic data, seeded for reproducible *values*, but the date range is a
# MAGIC   rolling ~2 years ending **today** - re-running on a later date shifts the
# MAGIC   window forward and regenerates dim_date/facts/inventory to cover up to that day
# MAGIC
# MAGIC Safe to re-run: tables are `CREATE OR REPLACE`, constraints are dropped/re-added,
# MAGIC and data loads use `insertInto(..., overwrite=True)`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Catalog + schema

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG}
COMMENT 'Genie One + Genie Ontology demo catalog (Northwind Retail sales analytics)'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
COMMENT 'Star schema: sales, returns and inventory facts with conformed date/product/customer/store dimensions'
""")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Tables (DDL with table + column comments)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.dim_date (
  date_key      INT     NOT NULL COMMENT 'Surrogate key, YYYYMMDD integer',
  calendar_date DATE    NOT NULL COMMENT 'Calendar date',
  year          INT              COMMENT 'Calendar year',
  quarter       INT              COMMENT 'Calendar quarter (1-4)',
  month         INT              COMMENT 'Calendar month number (1-12)',
  month_name    STRING           COMMENT 'Month name, e.g. January',
  day_of_week   STRING           COMMENT 'Day of week name, e.g. Monday',
  is_weekend    BOOLEAN          COMMENT 'True if the date falls on Saturday or Sunday'
)
COMMENT 'Date dimension, one row per calendar day. Used to conform all fact tables to a shared calendar.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.dim_product (
  product_key  INT            NOT NULL COMMENT 'Surrogate key for product',
  sku          STRING         NOT NULL COMMENT 'Business key / stock keeping unit',
  product_name STRING                  COMMENT 'Product display name',
  category     STRING                  COMMENT 'Merchandising category, e.g. Electronics, Apparel',
  brand        STRING                  COMMENT 'Brand name',
  unit_cost    DECIMAL(10,2)           COMMENT 'Wholesale unit cost in USD'
)
COMMENT 'Product dimension.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.dim_customer (
  customer_key  INT    NOT NULL COMMENT 'Surrogate key for customer',
  customer_name STRING          COMMENT 'Customer display name',
  segment       STRING          COMMENT 'Customer segment: Consumer, Small Business, or Enterprise',
  region        STRING          COMMENT 'Sales region the customer belongs to',
  signup_date   DATE            COMMENT 'Date the customer first registered'
)
COMMENT 'Customer dimension.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.dim_store (
  store_key  INT    NOT NULL COMMENT 'Surrogate key for store / sales channel',
  store_name STRING          COMMENT 'Store or channel display name',
  region     STRING          COMMENT 'Region the store operates in',
  channel    STRING          COMMENT 'Sales channel: Online or In-Store'
)
COMMENT 'Store / sales channel dimension.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.fact_sales (
  order_id     BIGINT        NOT NULL COMMENT 'Order line surrogate key',
  date_key     INT           NOT NULL COMMENT 'FK to dim_date.date_key, date the order was placed',
  product_key  INT           NOT NULL COMMENT 'FK to dim_product.product_key',
  customer_key INT           NOT NULL COMMENT 'FK to dim_customer.customer_key',
  store_key    INT           NOT NULL COMMENT 'FK to dim_store.store_key',
  quantity     INT                    COMMENT 'Units sold on this order line',
  unit_price   DECIMAL(10,2)          COMMENT 'Realized selling price per unit, in USD',
  revenue      DECIMAL(12,2)          COMMENT 'quantity * unit_price - gross revenue in USD for this line'
)
COMMENT 'Sales fact table, one row per order line item. Grain: one order line.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.fact_returns (
  return_id     BIGINT        NOT NULL COMMENT 'Return line surrogate key',
  order_id      BIGINT        NOT NULL COMMENT 'FK to fact_sales.order_id, the original order line being returned',
  date_key      INT           NOT NULL COMMENT 'FK to dim_date.date_key, date of the return',
  product_key   INT           NOT NULL COMMENT 'FK to dim_product.product_key',
  customer_key  INT           NOT NULL COMMENT 'FK to dim_customer.customer_key',
  quantity      INT                    COMMENT 'Units returned',
  return_amount DECIMAL(12,2)          COMMENT 'Refunded amount in USD',
  return_reason STRING                 COMMENT 'Reason code for the return, e.g. Defective, Wrong Item'
)
COMMENT 'Returns fact table, one row per returned order line. Grain: one return line.'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ_SCHEMA}.fact_inventory (
  snapshot_date_key INT NOT NULL COMMENT 'FK to dim_date.date_key, inventory snapshot date (first of month)',
  product_key       INT NOT NULL COMMENT 'FK to dim_product.product_key',
  store_key         INT NOT NULL COMMENT 'FK to dim_store.store_key',
  stock_on_hand     INT          COMMENT 'Units on hand at the snapshot date',
  stock_received    INT          COMMENT 'Units received into stock since the prior snapshot'
)
COMMENT 'Monthly inventory snapshot fact table. Grain: one product/store/month.'
""")

print("Tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Primary key constraints
# MAGIC
# MAGIC Unity Catalog PK/FK constraints are informational (not enforced), but Genie and
# MAGIC BI tools use them to infer valid join paths, so we add them anyway.

# COMMAND ----------

pk_statements = [
    f"ALTER TABLE {FQ_SCHEMA}.dim_date     ADD CONSTRAINT pk_dim_date     PRIMARY KEY (date_key)",
    f"ALTER TABLE {FQ_SCHEMA}.dim_product  ADD CONSTRAINT pk_dim_product  PRIMARY KEY (product_key)",
    f"ALTER TABLE {FQ_SCHEMA}.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)",
    f"ALTER TABLE {FQ_SCHEMA}.dim_store    ADD CONSTRAINT pk_dim_store    PRIMARY KEY (store_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_sales   ADD CONSTRAINT pk_fact_sales   PRIMARY KEY (order_id)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_returns ADD CONSTRAINT pk_fact_returns PRIMARY KEY (return_id)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_inventory ADD CONSTRAINT pk_fact_inventory "
    f"PRIMARY KEY (snapshot_date_key, product_key, store_key)",
]

for stmt in pk_statements:
    try:
        spark.sql(stmt)
        print(f"OK   {stmt}")
    except Exception as e:
        # constraint already exists on a re-run - safe to ignore
        print(f"SKIP {stmt} -> {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Foreign key constraints
# MAGIC
# MAGIC Added after PKs so every referenced key already exists (`fact_returns.order_id`
# MAGIC references `fact_sales.order_id`, so `fact_sales`'s PK must exist first).

# COMMAND ----------

fk_statements = [
    f"ALTER TABLE {FQ_SCHEMA}.fact_sales ADD CONSTRAINT fk_sales_date "
    f"FOREIGN KEY (date_key) REFERENCES {FQ_SCHEMA}.dim_date (date_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_sales ADD CONSTRAINT fk_sales_product "
    f"FOREIGN KEY (product_key) REFERENCES {FQ_SCHEMA}.dim_product (product_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_sales ADD CONSTRAINT fk_sales_customer "
    f"FOREIGN KEY (customer_key) REFERENCES {FQ_SCHEMA}.dim_customer (customer_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_sales ADD CONSTRAINT fk_sales_store "
    f"FOREIGN KEY (store_key) REFERENCES {FQ_SCHEMA}.dim_store (store_key)",

    f"ALTER TABLE {FQ_SCHEMA}.fact_returns ADD CONSTRAINT fk_returns_order "
    f"FOREIGN KEY (order_id) REFERENCES {FQ_SCHEMA}.fact_sales (order_id)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_returns ADD CONSTRAINT fk_returns_date "
    f"FOREIGN KEY (date_key) REFERENCES {FQ_SCHEMA}.dim_date (date_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_returns ADD CONSTRAINT fk_returns_product "
    f"FOREIGN KEY (product_key) REFERENCES {FQ_SCHEMA}.dim_product (product_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_returns ADD CONSTRAINT fk_returns_customer "
    f"FOREIGN KEY (customer_key) REFERENCES {FQ_SCHEMA}.dim_customer (customer_key)",

    f"ALTER TABLE {FQ_SCHEMA}.fact_inventory ADD CONSTRAINT fk_inventory_date "
    f"FOREIGN KEY (snapshot_date_key) REFERENCES {FQ_SCHEMA}.dim_date (date_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_inventory ADD CONSTRAINT fk_inventory_product "
    f"FOREIGN KEY (product_key) REFERENCES {FQ_SCHEMA}.dim_product (product_key)",
    f"ALTER TABLE {FQ_SCHEMA}.fact_inventory ADD CONSTRAINT fk_inventory_store "
    f"FOREIGN KEY (store_key) REFERENCES {FQ_SCHEMA}.dim_store (store_key)",
]

for stmt in fk_statements:
    try:
        spark.sql(stmt)
        print(f"OK   {stmt}")
    except Exception as e:
        print(f"SKIP {stmt} -> {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Generate synthetic data
# MAGIC
# MAGIC Deterministic (seeded) generation with pandas/numpy so re-runs are reproducible.
# MAGIC ~2 years of daily dates, 50 products, 200 customers, 10 stores, 5,000 sales
# MAGIC lines, ~8% of those as returns, and a 6-month inventory snapshot.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date, timedelta

rng = np.random.default_rng(42)

# ---- dim_date : rolling ~2 years of history through today ------------------
end_date = date.today()
start_date = end_date - timedelta(days=730)
n_days = (end_date - start_date).days + 1
all_dates = [start_date + timedelta(days=i) for i in range(n_days)]

pdf_date = pd.DataFrame({
    "date_key": [int(d.strftime("%Y%m%d")) for d in all_dates],
    "calendar_date": all_dates,
    "year": [d.year for d in all_dates],
    "quarter": [(d.month - 1) // 3 + 1 for d in all_dates],
    "month": [d.month for d in all_dates],
    "month_name": [d.strftime("%B") for d in all_dates],
    "day_of_week": [d.strftime("%A") for d in all_dates],
    "is_weekend": [d.weekday() >= 5 for d in all_dates],
})

# ---- dim_product -------------------------------------------------------------
categories = ["Electronics", "Apparel", "Home & Kitchen", "Sports", "Beauty"]
brands = ["Acme", "Globex", "Initech", "Umbrella", "Stark"]
n_products = 50

pdf_product = pd.DataFrame({
    "product_key": np.arange(1, n_products + 1),
    "sku": [f"SKU-{i:04d}" for i in range(1, n_products + 1)],
    "category": rng.choice(categories, n_products),
    "brand": rng.choice(brands, n_products),
    "unit_cost": np.round(rng.uniform(5, 500, n_products), 2),
})
pdf_product["product_name"] = (
    pdf_product["brand"] + " " + pdf_product["category"] + " " + pdf_product["sku"]
)
pdf_product = pdf_product[["product_key", "sku", "product_name", "category", "brand", "unit_cost"]]

# ---- dim_customer --------------------------------------------------------------
segments = ["Consumer", "Small Business", "Enterprise"]
regions = ["North America", "Europe", "APAC", "LATAM"]
n_customers = 200
signup_offsets = rng.integers(0, n_days, n_customers)

pdf_customer = pd.DataFrame({
    "customer_key": np.arange(1, n_customers + 1),
    "customer_name": [f"Customer {i:04d}" for i in range(1, n_customers + 1)],
    "segment": rng.choice(segments, n_customers, p=[0.6, 0.3, 0.1]),
    "region": rng.choice(regions, n_customers),
    "signup_date": [start_date + timedelta(days=int(o)) for o in signup_offsets],
})

# ---- dim_store -------------------------------------------------------------
channels = ["Online", "In-Store"]
n_stores = 10

pdf_store = pd.DataFrame({
    "store_key": np.arange(1, n_stores + 1),
    "store_name": [f"Store {i:02d}" for i in range(1, n_stores + 1)],
    "region": rng.choice(regions, n_stores),
    "channel": rng.choice(channels, n_stores, p=[0.5, 0.5]),
})

# ---- fact_sales -------------------------------------------------------------
n_sales = 5000
sales_date_idx = rng.integers(0, n_days, n_sales)
sales_product_idx = rng.integers(0, n_products, n_sales)
sales_customer_idx = rng.integers(0, n_customers, n_sales)
sales_store_idx = rng.integers(0, n_stores, n_sales)
quantity = rng.integers(1, 6, n_sales)
markup = rng.uniform(1.3, 2.0, n_sales)
unit_cost_arr = pdf_product["unit_cost"].to_numpy()[sales_product_idx]
unit_price = np.round(unit_cost_arr * markup, 2)
revenue = np.round(unit_price * quantity, 2)

pdf_sales = pd.DataFrame({
    "order_id": np.arange(1, n_sales + 1),
    "date_key": pdf_date["date_key"].to_numpy()[sales_date_idx],
    "product_key": pdf_product["product_key"].to_numpy()[sales_product_idx],
    "customer_key": pdf_customer["customer_key"].to_numpy()[sales_customer_idx],
    "store_key": pdf_store["store_key"].to_numpy()[sales_store_idx],
    "quantity": quantity,
    "unit_price": unit_price,
    "revenue": revenue,
})

# ---- fact_returns : ~8% of sales lines --------------------------------------
return_reasons = ["Defective", "Wrong Item", "No Longer Needed", "Better Price Found", "Damaged in Transit"]
n_returns = int(n_sales * 0.08)
returned = pdf_sales.sample(n=n_returns, random_state=42).reset_index(drop=True)
return_quantity = np.minimum(returned["quantity"].to_numpy(), rng.integers(1, 4, n_returns))

pdf_returns = pd.DataFrame({
    "return_id": np.arange(1, n_returns + 1),
    "order_id": returned["order_id"],
    "date_key": returned["date_key"],
    "product_key": returned["product_key"],
    "customer_key": returned["customer_key"],
    "quantity": return_quantity,
    "return_amount": np.round(return_quantity * returned["unit_price"].to_numpy(), 2),
    "return_reason": rng.choice(return_reasons, n_returns),
})

# ---- fact_inventory : monthly snapshot, trailing 6 months through the
# current month, per product/store -------------------------------------------
current_month_start = date(end_date.year, end_date.month, 1)
month_starts = pd.date_range(end=current_month_start, periods=6, freq="MS").date
inv_rows = [
    (int(md.strftime("%Y%m%d")), int(pk), int(sk))
    for md in month_starts
    for pk in pdf_product["product_key"]
    for sk in pdf_store["store_key"]
]
pdf_inventory = pd.DataFrame(inv_rows, columns=["snapshot_date_key", "product_key", "store_key"])
n_inv = len(pdf_inventory)
pdf_inventory["stock_on_hand"] = rng.integers(0, 500, n_inv)
pdf_inventory["stock_received"] = rng.integers(0, 200, n_inv)

print("Generated:",
      f"{len(pdf_date)} dates,", f"{len(pdf_product)} products,", f"{len(pdf_customer)} customers,",
      f"{len(pdf_store)} stores,", f"{len(pdf_sales)} sales lines,", f"{len(pdf_returns)} returns,",
      f"{len(pdf_inventory)} inventory snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Load into Delta tables
# MAGIC
# MAGIC Uses `insertInto(..., overwrite=True)` (not `saveAsTable`) so the table
# MAGIC definition - comments and constraints - is preserved; only the data is replaced.

# COMMAND ----------

def load_table(pdf: "pd.DataFrame", table_name: str, column_types: dict):
    sdf = spark.createDataFrame(pdf)
    for col, dtype in column_types.items():
        sdf = sdf.withColumn(col, sdf[col].cast(dtype))
    sdf = sdf.select(*column_types.keys())
    sdf.write.insertInto(f"{FQ_SCHEMA}.{table_name}", overwrite=True)
    print(f"Loaded {sdf.count()} rows into {FQ_SCHEMA}.{table_name}")


load_table(pdf_date, "dim_date", {
    "date_key": "int", "calendar_date": "date", "year": "int", "quarter": "int",
    "month": "int", "month_name": "string", "day_of_week": "string", "is_weekend": "boolean",
})

load_table(pdf_product, "dim_product", {
    "product_key": "int", "sku": "string", "product_name": "string",
    "category": "string", "brand": "string", "unit_cost": "decimal(10,2)",
})

load_table(pdf_customer, "dim_customer", {
    "customer_key": "int", "customer_name": "string", "segment": "string",
    "region": "string", "signup_date": "date",
})

load_table(pdf_store, "dim_store", {
    "store_key": "int", "store_name": "string", "region": "string", "channel": "string",
})

load_table(pdf_sales, "fact_sales", {
    "order_id": "bigint", "date_key": "int", "product_key": "int", "customer_key": "int",
    "store_key": "int", "quantity": "int", "unit_price": "decimal(10,2)", "revenue": "decimal(12,2)",
})

load_table(pdf_returns, "fact_returns", {
    "return_id": "bigint", "order_id": "bigint", "date_key": "int", "product_key": "int",
    "customer_key": "int", "quantity": "int", "return_amount": "decimal(12,2)", "return_reason": "string",
})

load_table(pdf_inventory, "fact_inventory", {
    "snapshot_date_key": "int", "product_key": "int", "store_key": "int",
    "stock_on_hand": "int", "stock_received": "int",
})

print("\nDone. Next: run 02_create_metric_views.")