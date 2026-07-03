# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Raw Data Generator

# COMMAND ----------

# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run date (YYYY-MM-DD, blank=today)")
dbutils.widgets.dropdown("generation_mode", "initial_seed", ["initial_seed","daily_incremental"], "Generation mode")
dbutils.widgets.text("num_customers_seed",            "500",  "Customers to seed")
dbutils.widgets.text("num_new_customers_incremental", "15",   "New customers (incremental)")
dbutils.widgets.text("num_customers_to_update",       "25",   "Customers to update (incremental)")
dbutils.widgets.text("num_products",      "150",  "Products")
dbutils.widgets.dropdown("refresh_products", "no", ["yes","no"], "Force product refresh")
dbutils.widgets.text("num_orders",        "400",  "Orders for run_date")
dbutils.widgets.text("dirty_data_ratio",  "0.06", "Dirty-data injection ratio (0-1)")
dbutils.widgets.text("random_seed",       "42",   "Random seed")

# COMMAND ----------

import random, json, math
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import Row
import pyspark.sql.functions as F

RUN_DATE               = dbutils.widgets.get("run_date").strip() or datetime.utcnow().strftime("%Y-%m-%d")
GENERATION_MODE        = dbutils.widgets.get("generation_mode").strip()
NUM_CUSTOMERS_SEED     = int(dbutils.widgets.get("num_customers_seed")            or "500")
NUM_NEW_CUSTOMERS_INCR = int(dbutils.widgets.get("num_new_customers_incremental") or "15")
NUM_CUSTOMERS_TO_UPDATE= int(dbutils.widgets.get("num_customers_to_update")       or "25")
NUM_PRODUCTS           = int(dbutils.widgets.get("num_products")                  or "150")
REFRESH_PRODUCTS       = dbutils.widgets.get("refresh_products").strip() == "yes"
NUM_ORDERS             = int(dbutils.widgets.get("num_orders")                    or "400")
DIRTY_RATIO            = float(dbutils.widgets.get("dirty_data_ratio")            or "0.06")
RANDOM_SEED            = int(dbutils.widgets.get("random_seed")                   or "42")

random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker()

RUN_TS = RUN_DATE.replace("-", "")
print(f"run_date={RUN_DATE}  mode={GENERATION_MODE}  dirty_ratio={DIRTY_RATIO}")
print(f"BASE_DATA_PATH={BASE_DATA_PATH}")

# COMMAND ----------

# MAGIC %md ## Reference Data

# COMMAND ----------

CATEGORY_TREE = {
    "Electronics":    ["Smartphones","Laptops","Headphones","Televisions","Cameras"],
    "Apparel":        ["Mens Wear","Womens Wear","Footwear","Accessories"],
    "Home & Kitchen": ["Cookware","Furniture","Decor","Appliances"],
    "Sports":         ["Fitness","Outdoor","Team Sports"],
    "Grocery":        ["Beverages","Snacks","Staples"],
}
BRANDS      = ["Zentra","Northwind","Bluepeak","Veloria","Crestline","Pixelhive","Marsh & Co","Aurex"]
STORE_CODES = [f"ST{n:03d}" for n in range(1, 11)]

def maybe(prob):
    return random.random() < prob

def seasonal_multiplier(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    month_factor   = 1 + 0.3 * math.sin((d.month - 1) / 12 * 2 * math.pi)
    weekday_factor = 1.2 if d.weekday() in (4,5) else (0.85 if d.weekday() == 6 else 1.0)
    return round(month_factor * weekday_factor, 3)

# COMMAND ----------

# MAGIC %md ## Write Helpers — Spark-native (no pandas string conversion)
# MAGIC
# MAGIC All writes use Spark `coalesce(1).write.csv / .text` directly into DBFS.
# MAGIC This avoids `dbutils.fs.put()` which silently hangs on large files in Free Edition.

# COMMAND ----------

def write_spark_df_as_csv(sdf, dataset, filename, sep=","):
    """Write a Spark DataFrame as a single CSV file into raw/<dataset>/."""
    raw_dir  = f"{BASE_DATA_PATH}/raw/{dataset}"
    tmp_dir  = f"{raw_dir}/_tmp_{filename}"
    out_path = f"{raw_dir}/{filename}"

    dbutils.fs.mkdirs(raw_dir)

    # Write to a temp folder (Spark always writes part files)
    (sdf.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", sep)
        .csv(tmp_dir))

    # Find the single part file and rename it to the target filename
    part_files = [f for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part file found in {tmp_dir}")
    dbutils.fs.mv(part_files[0].path, out_path)
    dbutils.fs.rm(tmp_dir, recurse=True)

    row_count = sdf.count()
    print(f"  wrote {row_count:>6} rows -> {out_path}")
    return out_path


def write_spark_df_as_json(sdf, dataset, filename):
    """Write a single-row Spark DataFrame as a JSON text file."""
    raw_dir  = f"{BASE_DATA_PATH}/raw/{dataset}"
    tmp_dir  = f"{raw_dir}/_tmp_{filename}"
    out_path = f"{raw_dir}/{filename}"

    dbutils.fs.mkdirs(raw_dir)

    (sdf.coalesce(1)
        .write.mode("overwrite")
        .text(tmp_dir))

    part_files = [f for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part file found in {tmp_dir}")
    dbutils.fs.mv(part_files[0].path, out_path)
    dbutils.fs.rm(tmp_dir, recurse=True)

    print(f"  wrote payload  -> {out_path}")
    return out_path


def read_existing_ids(dataset, id_column):
    raw_dir = f"{BASE_DATA_PATH}/raw/{dataset}"
    try:
        files = [f for f in dbutils.fs.ls(raw_dir) if not f.name.startswith("_")]
    except Exception:
        return []
    if not files:
        return []
    try:
        df = spark.read.option("header", True).csv(raw_dir)
        if id_column not in df.columns:
            return []
        return sorted(
            {r[id_column] for r in df.select(id_column).distinct().collect()},
            key=lambda x: int(x) if str(x).lstrip("-").isdigit() else 0
        )
    except Exception as e:
        print(f"  [warn] read_existing_ids({dataset}): {e}")
        return []


def read_existing_customers_spark():
    raw_dir = f"{BASE_DATA_PATH}/raw/customers"
    try:
        files = [f for f in dbutils.fs.ls(raw_dir) if not f.name.startswith("_")]
    except Exception:
        return None
    if not files:
        return None
    return spark.read.option("header", True).csv(raw_dir)

# COMMAND ----------

# MAGIC %md ## Row Generators — return lists of dicts (converted to Spark DataFrames)

# COMMAND ----------

def generate_products_rows(num_products, start_id=1):
    rows = []
    for pid in range(start_id, start_id + num_products):
        category    = random.choice(list(CATEGORY_TREE))
        subcategory = random.choice(CATEGORY_TREE[category])
        brand       = random.choice(BRANDS)
        cost_price  = round(random.uniform(5, 1500), 2)
        if maybe(DIRTY_RATIO): category    = category.lower() if maybe(0.5) else category.upper()
        if maybe(DIRTY_RATIO): subcategory = None
        if maybe(DIRTY_RATIO): brand       = None
        cost_str = f"${cost_price:.2f}" if maybe(DIRTY_RATIO * 0.5) else f"{cost_price:.2f}"
        rows.append(Row(
            ProductID   = str(pid),
            ProductName = f"{brand or 'Generic'} {subcategory or category} {pid}",
            Category    = category,
            SubCategory = subcategory,
            Brand       = brand,
            CostPrice   = cost_str,
        ))
    dup_count = max(1, int(num_products * 0.02))
    rows.extend(random.sample(rows, dup_count))
    return rows


def _make_customer_row(cid, as_of):
    first, last = fake.first_name(), fake.last_name()
    return Row(
        CustomerID  = str(cid),
        FirstName   = first,
        LastName    = last,
        Email       = None if maybe(DIRTY_RATIO) else f"{first.lower()}.{last.lower()}{cid}@example.com",
        Phone       = "" if maybe(DIRTY_RATIO) else fake.phone_number(),
        City        = None if maybe(DIRTY_RATIO) else fake.city(),
        State       = fake.state_abbr().lower() if maybe(DIRTY_RATIO) else fake.state_abbr(),
        LastUpdated = (as_of - timedelta(minutes=random.randint(0,60))).strftime("%Y-%m-%d %H:%M:%S"),
    )


def generate_customers_full_rows(num_customers):
    as_of = datetime.strptime(RUN_DATE, "%Y-%m-%d")
    rows  = [_make_customer_row(cid, as_of) for cid in range(1, num_customers + 1)]
    dup_count = max(1, int(num_customers * 0.01))
    rows.extend(random.sample(rows, dup_count))
    return rows


def generate_customers_incremental_rows(existing_sdf, num_new, num_updates):
    as_of = datetime.strptime(RUN_DATE, "%Y-%m-%d")
    rows  = []

    if existing_sdf is not None:
        existing_ids = [r["CustomerID"] for r in
                        existing_sdf.select("CustomerID").distinct().collect()]
        update_ids   = random.sample(existing_ids, min(num_updates, len(existing_ids)))
        existing_map = {r["CustomerID"]: r.asDict()
                        for r in existing_sdf.filter(
                            F.col("CustomerID").isin(update_ids)).collect()}
        for cid in update_ids:
            base = existing_map.get(cid, {})
            rows.append(Row(
                CustomerID  = cid,
                FirstName   = base.get("FirstName", fake.first_name()),
                LastName    = base.get("LastName",  fake.last_name()),
                Email       = base.get("Email"),
                Phone       = fake.phone_number(),
                City        = fake.city(),
                State       = fake.state_abbr(),
                LastUpdated = (as_of - timedelta(minutes=random.randint(0,30))).strftime("%Y-%m-%d %H:%M:%S"),
            ))
        max_id = max(int(i) for i in existing_ids if str(i).lstrip("-").isdigit())
    else:
        max_id = 0

    for i in range(num_new):
        rows.append(_make_customer_row(max_id + i + 1, as_of))
    return rows


def generate_orders_rows(num_orders, order_date, customer_id_pool,
                          product_id_pool, store_codes, start_order_id=1):
    rows   = []
    s_mult = seasonal_multiplier(order_date)
    for i in range(num_orders):
        oid        = start_order_id + i
        cust       = random.choice(customer_id_pool)
        prod       = random.choice(product_id_pool)
        qty        = max(1, int(random.randint(1, 8) * s_mult))
        unit_price = round(random.uniform(5, 1500) * s_mult, 2)
        store      = random.choice(store_codes)
        od         = order_date

        if maybe(DIRTY_RATIO): cust = str(int(max(customer_id_pool, key=int)) + random.randint(1000,9999))
        if maybe(DIRTY_RATIO): prod = str(int(max(product_id_pool, key=int)) + random.randint(1000,9999))
        if maybe(DIRTY_RATIO): qty  = random.choice([0, -1, qty])
        if maybe(DIRTY_RATIO * 0.3): od = "2024-13-45"
        if maybe(DIRTY_RATIO * 0.2): unit_price = -unit_price

        up_str = "" if maybe(DIRTY_RATIO) else f"{unit_price:.2f}"
        rows.append(Row(
            OrderID    = str(oid),
            CustomerID = cust,
            ProductID  = prod,
            OrderDate  = od,
            Quantity   = str(qty),
            UnitPrice  = up_str,
            StoreCode  = None if maybe(DIRTY_RATIO) else store,
        ))
    dup_count = max(1, int(num_orders * 0.015))
    rows.extend(random.sample(rows, dup_count))
    return rows


def generate_inventory_rows(product_id_pool, store_codes, snapshot_date):
    rows = []
    for store in store_codes:
        for pid in random.sample(product_id_pool, min(len(product_id_pool), 50)):
            stock   = random.randint(0, 5000)
            reorder = random.randint(10, 500)
            if maybe(DIRTY_RATIO * 0.5): stock = -stock
            rows.append(Row(
                StoreCode    = store,
                ProductID    = str(pid),
                StockLevel   = str(stock),
                ReorderPoint = str(reorder),
                SnapshotDate = snapshot_date,
            ))
    return rows

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

print(f"=== Generating raw data for {RUN_DATE} (mode={GENERATION_MODE}) ===\n")

# ---- Products ----
print("[products]")
if GENERATION_MODE == "initial_seed" or REFRESH_PRODUCTS:
    sdf = spark.createDataFrame(generate_products_rows(NUM_PRODUCTS))
    write_spark_df_as_csv(sdf, "products", f"products_{RUN_TS}.csv")

product_id_pool = read_existing_ids("products", "ProductID")
if not product_id_pool:
    print("  no products found — generating now")
    sdf = spark.createDataFrame(generate_products_rows(NUM_PRODUCTS))
    write_spark_df_as_csv(sdf, "products", f"products_{RUN_TS}.csv")
    product_id_pool = read_existing_ids("products", "ProductID")

print(f"  product pool: {len(product_id_pool)} IDs")

# ---- Customers ----
print("\n[customers]")
if GENERATION_MODE == "initial_seed":
    sdf = spark.createDataFrame(generate_customers_full_rows(NUM_CUSTOMERS_SEED))
    write_spark_df_as_csv(sdf, "customers", f"customers_full_{RUN_TS}.csv")
else:
    existing_sdf = read_existing_customers_spark()
    sdf = spark.createDataFrame(
        generate_customers_incremental_rows(existing_sdf, NUM_NEW_CUSTOMERS_INCR, NUM_CUSTOMERS_TO_UPDATE)
    )
    write_spark_df_as_csv(sdf, "customers",
                           f"customers_incremental_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")

customer_id_pool = read_existing_ids("customers", "CustomerID")
print(f"  customer pool: {len(customer_id_pool)} IDs")
if not customer_id_pool:
    raise RuntimeError("Customer pool is empty after write — check DBFS permissions.")

# ---- Orders ----
print("\n[orders]")
existing_order_ids = read_existing_ids("orders", "OrderID")
start_oid = (max(int(x) for x in existing_order_ids if str(x).lstrip("-").isdigit()) + 1) \
            if existing_order_ids else 100000
sdf = spark.createDataFrame(
    generate_orders_rows(NUM_ORDERS, RUN_DATE, customer_id_pool,
                          product_id_pool, STORE_CODES, start_order_id=start_oid)
)
write_spark_df_as_csv(sdf, "orders", f"orders_{RUN_TS}.csv")

# ---- Exchange Rates ----
print("\n[exchange_rates]")
region_currencies = {
    "Europe":   ["EUR","GBP","CHF"],
    "Asia":     ["INR","JPY","CNY","SGD"],
    "Americas": ["CAD","BRL"],
    "Oceania":  ["AUD"],
}
rate_groups = []
for region, currencies in region_currencies.items():
    rates = [{"targetCurrency": ccy,
              "rate": round(random.uniform(0.5, 90), 4) if not maybe(0.05) else None,
              "meta": {"source": random.choice(["ECB","RBI","OpenFX","Reuters"]),
                       "confidence": random.choice(["HIGH","MEDIUM"])}}
             for ccy in currencies]
    rate_groups.append({"region": region, "rates": rates})

payload = {"asOfDate": RUN_DATE,
           "base": {"currencyCode": "USD", "region": "Global"},
           "rateGroups": rate_groups}
json_str = json.dumps(payload, indent=2, default=str)
fx_sdf   = spark.createDataFrame([Row(value=json_str)])
write_spark_df_as_json(fx_sdf, "exchange_rates", f"exchange_rates_{RUN_TS}.json")

# ---- Store Inventory (TSV) ----
print("\n[store_inventory]")
sdf = spark.createDataFrame(
    generate_inventory_rows(product_id_pool, STORE_CODES, RUN_DATE)
)
write_spark_df_as_csv(sdf, "store_inventory", f"store_inventory_{RUN_TS}.tsv", sep="\t")

# ---- Summary ----
print("\n=== Generation complete ===")
for ds in ["products","customers","orders","exchange_rates","store_inventory"]:
    try:
        raw_dir = f"{BASE_DATA_PATH}/raw/{ds}"
        files   = [f for f in dbutils.fs.ls(raw_dir) if not f.name.startswith("_")]
        print(f"  {ds:>20}: {len(files)} file(s)")
    except Exception:
        print(f"  {ds:>20}: (no files yet)")

dbutils.notebook.exit("01_data_generator: OK")