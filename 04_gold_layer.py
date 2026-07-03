# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold Layer  *(Enhanced v2.0)*
# MAGIC **Enterprise Retail Analytics Platform**
# MAGIC
# MAGIC ### Enhancements over v1
# MAGIC
# MAGIC | New Gold Table | Purpose |
# MAGIC |---|---|
# MAGIC | `gold.agg_rfm_customer_segments` | RFM (Recency / Frequency / Monetary) scores + segment labels per customer |
# MAGIC | `gold.agg_store_inventory_health` | In-stock rate, stock-out count, below-reorder-point count per store |
# MAGIC | `gold.agg_weekly_forecast_input`  | 13-week rolling revenue totals ready for forecasting models |
# MAGIC | `audit.data_lineage` | Lineage logged on every Gold write |
# MAGIC
# MAGIC All original v1 tables (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`,
# MAGIC `agg_daily_sales_by_store`, `agg_sales_by_category`) are retained unchanged.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# Pre-create all schemas before any table operation
for _schema in ["audit", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{_schema}")
print("All schemas ready.")



dbutils.widgets.dropdown(
    "gold_object_filter", "ALL",
    ["ALL","dim_customer","dim_product","dim_date","fact_sales",
     "aggregates","rfm_segments","inventory_health","forecast_input"],
    "Gold object to (re)build"
)

# COMMAND ----------

from pyspark.sql.window import Window

OBJECT_FILTER = dbutils.widgets.get("gold_object_filter")

def should_build(name: str) -> bool:
    return OBJECT_FILTER in ("ALL", name)

# COMMAND ----------

# MAGIC %md ## Store → Currency Map (unchanged assumption from v1)

# COMMAND ----------

STORE_CURRENCY_MAP = {
    "ST001":"USD","ST002":"USD","ST003":"EUR","ST004":"GBP","ST005":"INR",
    "ST006":"INR","ST007":"JPY","ST008":"CAD","ST009":"AUD","ST010":"CNY",
}
BASE_CURRENCY = "USD"
store_currency_df = spark.createDataFrame(
    [(k, v) for k, v in STORE_CURRENCY_MAP.items()], ["StoreCode","LocalCurrency"]
)

# COMMAND ----------

# MAGIC %md ## dim_customer — SCD2

# COMMAND ----------

if should_build("dim_customer"):
    log_pipeline_event("gold", "dim_customer", "STARTED")
    try:
        silver_customers = read_delta_table("silver","customers").filter("_IsRejected = false")
        window           = Window.orderBy("CustomerID","_SCD_EffectiveStartDate")
        dim_customer = (silver_customers
            .withColumn("CustomerSK", F.row_number().over(window))
            .select("CustomerSK","CustomerID","FirstName","LastName","Email","Phone",
                    "City","State","_SCD_EffectiveStartDate","_SCD_EffectiveEndDate","_SCD_IsCurrent"))
        row_count = dim_customer.count()
        write_delta_table(dim_customer, "gold", "dim_customer", mode="overwrite",
                          zorder_cols=["CustomerID"])
        log_lineage("silver","customers","gold","dim_customer","SCD2_FORWARD",row_count)
        log_pipeline_event("gold","dim_customer","SUCCESS",records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold","dim_customer","FAILED",error_message=str(e))
        alert_failure("gold","dim_customer",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## dim_product — Type 1

# COMMAND ----------

if should_build("dim_product"):
    log_pipeline_event("gold","dim_product","STARTED")
    try:
        silver_products = read_delta_table("silver","products").filter("_IsRejected = false")
        prod_window     = Window.orderBy("ProductID")
        dim_product = (silver_products.dropDuplicates(["ProductID"])
            .withColumn("ProductSK", F.row_number().over(prod_window))
            .select("ProductSK","ProductID","ProductName","Category","SubCategory","Brand","CostPrice"))
        row_count = dim_product.count()
        write_delta_table(dim_product,"gold","dim_product",mode="overwrite",
                          zorder_cols=["ProductID"])
        log_lineage("silver","products","gold","dim_product","SNAPSHOT",row_count)
        log_pipeline_event("gold","dim_product","SUCCESS",records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold","dim_product","FAILED",error_message=str(e))
        alert_failure("gold","dim_product",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## dim_date — Generated

# COMMAND ----------

if should_build("dim_date"):
    log_pipeline_event("gold","dim_date","STARTED")
    try:
        bounds = (read_delta_table("silver","orders").filter("_IsRejected = false")
                  .agg(F.min("OrderDate").alias("min_d"), F.max("OrderDate").alias("max_d")).first())
        if bounds["min_d"] is None:
            log_pipeline_event("gold","dim_date","SUCCESS",records_out=0)
        else:
            import pandas as pd
            date_range = pd.date_range(start=bounds["min_d"],
                                        end=bounds["max_d"] + pd.Timedelta(days=1), freq="D")
            date_rows = [(
                int(d.strftime("%Y%m%d")), d.date().isoformat(),
                int(d.year), int((d.month-1)//3+1), int(d.month), d.strftime("%B"),
                int(d.day), int(d.isoweekday()), d.strftime("%A"),
                bool(d.isoweekday() in (6,7)), int(d.isocalendar()[1]),
            ) for d in date_range]
            dim_date = spark.createDataFrame(date_rows, [
                "DateSK","CalendarDate","Year","Quarter","Month","MonthName",
                "Day","DayOfWeek","DayName","IsWeekend","WeekOfYear",
            ])
            row_count = dim_date.count()
            write_delta_table(dim_date,"gold","dim_date",mode="overwrite",
                              zorder_cols=["CalendarDate"])
            log_lineage("silver","orders","gold","dim_date","GENERATE",row_count)
            log_pipeline_event("gold","dim_date","SUCCESS",records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold","dim_date","FAILED",error_message=str(e))
        alert_failure("gold","dim_date",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## fact_sales — Point-in-time join + currency conversion

# COMMAND ----------

if should_build("fact_sales"):
    log_pipeline_event("gold","fact_sales","STARTED")
    try:
        orders      = read_delta_table("silver","orders").filter("_IsRejected = false")
        dim_customer= read_delta_table("gold","dim_customer")
        dim_product = read_delta_table("gold","dim_product")
        fx_rates    = read_delta_table("silver","exchange_rates").filter("_IsRejected = false")

        orders_c = orders.alias("o").join(
            dim_customer.alias("c"),
            (F.col("o.CustomerID") == F.col("c.CustomerID"))
            & (F.col("o.OrderDate") >= F.to_date("c._SCD_EffectiveStartDate"))
            & (F.col("c._SCD_EffectiveEndDate").isNull()
               | (F.col("o.OrderDate") < F.to_date("c._SCD_EffectiveEndDate"))),
            "left"
        ).select("o.*", F.col("c.CustomerSK"))

        orders_p = orders_c.join(
            dim_product.select("ProductID","ProductSK","CostPrice"), on="ProductID", how="left"
        )
        orders_fx = (orders_p.join(store_currency_df, on="StoreCode", how="left")
                     .withColumn("LocalCurrency",
                                 F.coalesce(F.col("LocalCurrency"), F.lit(BASE_CURRENCY))))

        fx_latest = (fx_rates
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("BaseCurrency","TargetCurrency")
                      .orderBy(F.col("RateDate").desc())))
            .filter("_rn = 1")
            .select("BaseCurrency","TargetCurrency","ExchangeRate"))

        fact_sales = (orders_fx
            .join(fx_latest,
                  (orders_fx.LocalCurrency == fx_latest.TargetCurrency)
                  & (fx_latest.BaseCurrency == BASE_CURRENCY), "left")
            .withColumn("ExchangeRate", F.coalesce(F.col("ExchangeRate"), F.lit(1.0)))
            .withColumn("DateSK",                F.date_format("OrderDate","yyyyMMdd").cast("int"))
            .withColumn("LineTotalLocal",        F.col("Quantity") * F.col("UnitPrice"))
            .withColumn("LineTotalBaseCurrency", F.round(F.col("LineTotalLocal") / F.col("ExchangeRate"), 2))
            .withColumn("LineCostBaseCurrency",  F.round((F.col("Quantity") * F.col("CostPrice")) / F.col("ExchangeRate"), 2))
            .withColumn("GrossMarginBaseCurrency",F.round(F.col("LineTotalBaseCurrency") - F.col("LineCostBaseCurrency"), 2))
            .select("OrderID","DateSK","CustomerSK","ProductSK","StoreCode","LocalCurrency",
                    "Quantity","UnitPrice","ExchangeRate",
                    "LineTotalLocal","LineTotalBaseCurrency",
                    "LineCostBaseCurrency","GrossMarginBaseCurrency"))

        row_count = fact_sales.count()
        write_delta_table(fact_sales,"gold","fact_sales",mode="overwrite",
                          zorder_cols=["DateSK","StoreCode"])
        log_lineage("silver","orders","gold","fact_sales","STAR_LOAD",row_count)
        log_pipeline_event("gold","fact_sales","SUCCESS",records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold","fact_sales","FAILED",error_message=str(e))
        alert_failure("gold","fact_sales",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## Standard Aggregates (v1 retained)

# COMMAND ----------

if should_build("aggregates"):
    log_pipeline_event("gold","aggregates","STARTED")
    try:
        fact_sales  = read_delta_table("gold","fact_sales")
        dim_date    = read_delta_table("gold","dim_date")
        dim_product = read_delta_table("gold","dim_product")

        daily_by_store = (fact_sales.join(dim_date, on="DateSK", how="left")
            .groupBy("CalendarDate","StoreCode")
            .agg(F.countDistinct("OrderID").alias("TotalOrders"),
                 F.sum("Quantity").alias("TotalQuantity"),
                 F.sum("LineTotalBaseCurrency").alias(f"TotalRevenue{BASE_CURRENCY}"),
                 F.sum("GrossMarginBaseCurrency").alias(f"TotalMargin{BASE_CURRENCY}")))
        write_delta_table(daily_by_store,"gold","agg_daily_sales_by_store",mode="overwrite")

        by_category = (fact_sales.join(dim_product, on="ProductSK", how="left")
            .groupBy("Category","SubCategory")
            .agg(F.sum("Quantity").alias("TotalQuantity"),
                 F.sum("LineTotalBaseCurrency").alias(f"TotalRevenue{BASE_CURRENCY}"),
                 F.sum("GrossMarginBaseCurrency").alias(f"TotalMargin{BASE_CURRENCY}")))
        write_delta_table(by_category,"gold","agg_sales_by_category",mode="overwrite")

        log_lineage("gold","fact_sales","gold","agg_daily_sales_by_store","AGGREGATE",daily_by_store.count())
        log_lineage("gold","fact_sales","gold","agg_sales_by_category","AGGREGATE",by_category.count())
        log_pipeline_event("gold","aggregates","SUCCESS",
                           records_out=daily_by_store.count()+by_category.count())
    except Exception as e:
        log_pipeline_event("gold","aggregates","FAILED",error_message=str(e))
        alert_failure("gold","aggregates",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## NEW — RFM Customer Segmentation
# MAGIC
# MAGIC Computes per-customer RFM (Recency / Frequency / Monetary) scores using
# MAGIC quintile-based scoring (1–5 for each dimension), then assigns a business
# MAGIC segment label based on the combined score.
# MAGIC
# MAGIC | Segment | Logic |
# MAGIC |---|---|
# MAGIC | Champions | R=5, F≥4, M≥4 |
# MAGIC | Loyal | F≥4 |
# MAGIC | At Risk | R≤2, F≥3 |
# MAGIC | Lost | R=1, F≤2 |
# MAGIC | New Customers | F=1 |
# MAGIC | Potential | all others |

# COMMAND ----------

if should_build("rfm_segments"):
    log_pipeline_event("gold","rfm_segments","STARTED")
    try:
        fact_sales   = read_delta_table("gold","fact_sales")
        dim_date     = read_delta_table("gold","dim_date")
        dim_customer = read_delta_table("gold","dim_customer").filter("_SCD_IsCurrent = true")

        # Reference date = latest order date in the dataset
        ref_date = fact_sales.join(dim_date, on="DateSK").agg(F.max("CalendarDate")).first()[0]

        rfm_raw = (fact_sales
            .join(dim_date, on="DateSK", how="left")
            .groupBy("CustomerSK")
            .agg(
                F.datediff(F.lit(ref_date), F.max("CalendarDate")).alias("Recency"),
                F.countDistinct("OrderID").alias("Frequency"),
                F.sum("LineTotalBaseCurrency").alias("MonetaryValue"),
            ))

        # Quintile scoring: lower recency = better (score 5); higher F/M = better
        r_window = Window.orderBy(F.col("Recency").desc())
        f_window = Window.orderBy(F.col("Frequency"))
        m_window = Window.orderBy(F.col("MonetaryValue"))
        total    = rfm_raw.count()

        rfm_scored = (rfm_raw
            .withColumn("R_Score", F.ntile(5).over(r_window))
            .withColumn("F_Score", F.ntile(5).over(f_window))
            .withColumn("M_Score", F.ntile(5).over(m_window))
            .withColumn("RFM_Score", F.col("R_Score") + F.col("F_Score") + F.col("M_Score")))

        rfm_segmented = rfm_scored.withColumn("Segment",
            F.when((F.col("R_Score")==5) & (F.col("F_Score")>=4) & (F.col("M_Score")>=4), "Champions")
             .when(F.col("F_Score")>=4, "Loyal")
             .when((F.col("R_Score")<=2) & (F.col("F_Score")>=3), "At Risk")
             .when((F.col("R_Score")==1) & (F.col("F_Score")<=2), "Lost")
             .when(F.col("F_Score")==1, "New Customer")
             .otherwise("Potential"))

        # Join back to get CustomerID
        rfm_final = rfm_segmented.join(
            dim_customer.select("CustomerSK","CustomerID","FirstName","LastName","City","State"),
            on="CustomerSK", how="left"
        ).select("CustomerSK","CustomerID","FirstName","LastName","City","State",
                 "Recency","Frequency","MonetaryValue",
                 "R_Score","F_Score","M_Score","RFM_Score","Segment")

        row_count = rfm_final.count()
        write_delta_table(rfm_final,"gold","agg_rfm_customer_segments",mode="overwrite")
        log_lineage("gold","fact_sales","gold","agg_rfm_customer_segments","RFM_SCORE",row_count)
        log_pipeline_event("gold","rfm_segments","SUCCESS",records_out=row_count)
        print(f"gold.agg_rfm_customer_segments: {row_count:,} customers scored")

        # Segment distribution summary
        display(rfm_final.groupBy("Segment").count().orderBy("count", ascending=False))
    except Exception as e:
        log_pipeline_event("gold","rfm_segments","FAILED",error_message=str(e))
        alert_failure("gold","rfm_segments",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## NEW — Store Inventory Health
# MAGIC
# MAGIC Joins `silver.store_inventory` (from the new TSV source) to `gold.dim_product`
# MAGIC to produce per-store metrics:
# MAGIC - `InStockRate`         — % of SKUs with StockLevel > 0
# MAGIC - `StockOutCount`       — SKUs with StockLevel = 0
# MAGIC - `BelowReorderCount`   — SKUs where StockLevel < ReorderPoint
# MAGIC - `TotalStockValueUSD`  — StockLevel × CostPrice summed across all SKUs

# COMMAND ----------

if should_build("inventory_health"):
    log_pipeline_event("gold","inventory_health","STARTED")
    try:
        if not table_exists("silver","store_inventory"):
            print("[skip] store_inventory: no Silver table yet — run data generator with TSV source")
            log_pipeline_event("gold","inventory_health","SUCCESS",records_out=0)
        else:
            inventory   = read_delta_table("silver","store_inventory").filter("_IsRejected = false")
            dim_product = read_delta_table("gold","dim_product")

            inv_with_cost = inventory.join(
                dim_product.select("ProductID","CostPrice"), on="ProductID", how="left"
            )

            store_health = (inv_with_cost
                .withColumn("IsInStock",        (F.col("StockLevel") > 0).cast("int"))
                .withColumn("IsBelowReorder",   (F.col("StockLevel") < F.col("ReorderPoint")).cast("int"))
                .withColumn("StockValue",       F.col("StockLevel") * F.col("CostPrice"))
                .groupBy("StoreCode","SnapshotDate")
                .agg(
                    F.count("ProductID").alias("TotalSKUs"),
                    F.sum("IsInStock").alias("InStockSKUs"),
                    F.sum(F.when(F.col("StockLevel")==0, 1).otherwise(0)).alias("StockOutCount"),
                    F.sum("IsBelowReorder").alias("BelowReorderCount"),
                    F.round(F.sum("StockValue"),2).alias("TotalStockValueUSD"),
                )
                .withColumn("InStockRate",
                    F.round(F.col("InStockSKUs") / F.col("TotalSKUs") * 100, 2))
            )

            row_count = store_health.count()
            write_delta_table(store_health,"gold","agg_store_inventory_health",mode="overwrite",
                              zorder_cols=["StoreCode"])
            log_lineage("silver","store_inventory","gold","agg_store_inventory_health",
                        "AGGREGATE",row_count)
            log_pipeline_event("gold","inventory_health","SUCCESS",records_out=row_count)
            print(f"gold.agg_store_inventory_health: {row_count:,} rows")
            display(store_health.orderBy("StoreCode").limit(20))
    except Exception as e:
        log_pipeline_event("gold","inventory_health","FAILED",error_message=str(e))
        alert_failure("gold","inventory_health",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## NEW — Weekly Forecast Input
# MAGIC
# MAGIC Builds a 13-week rolling window of revenue per store, structured for
# MAGIC ingestion into Azure ML forecasting models or Power BI Analytics.
# MAGIC Each row = (StoreCode, WeekStart, TotalRevenueUSD, lag_1w, lag_2w, … lag_12w).

# COMMAND ----------

if should_build("forecast_input"):
    log_pipeline_event("gold","forecast_input","STARTED")
    try:
        fact_sales = read_delta_table("gold","fact_sales")
        dim_date   = read_delta_table("gold","dim_date")

        weekly = (fact_sales.join(dim_date, on="DateSK", how="left")
            .withColumn("WeekStart",
                F.date_trunc("week", F.col("CalendarDate").cast("date")))
            .groupBy("StoreCode","WeekStart")
            .agg(F.sum("LineTotalBaseCurrency").alias("TotalRevenueUSD")))

        store_week_window = Window.partitionBy("StoreCode").orderBy("WeekStart")

        # Add 12 weekly lag features
        forecast_df = weekly
        for lag in range(1, 13):
            forecast_df = forecast_df.withColumn(
                f"Lag_{lag}w",
                F.lag("TotalRevenueUSD", lag).over(store_week_window)
            )

        # Rolling 4-week and 13-week averages
        rolling_4  = Window.partitionBy("StoreCode").orderBy("WeekStart").rowsBetween(-3, 0)
        rolling_13 = Window.partitionBy("StoreCode").orderBy("WeekStart").rowsBetween(-12, 0)
        forecast_df = (forecast_df
            .withColumn("Rolling4wAvgRevenue",  F.round(F.avg("TotalRevenueUSD").over(rolling_4), 2))
            .withColumn("Rolling13wAvgRevenue", F.round(F.avg("TotalRevenueUSD").over(rolling_13), 2))
            .withColumn("WoW_GrowthPct",
                F.round((F.col("TotalRevenueUSD") - F.col("Lag_1w")) / F.col("Lag_1w") * 100, 2))
        )

        row_count = forecast_df.count()
        write_delta_table(forecast_df,"gold","agg_weekly_forecast_input",mode="overwrite",
                          zorder_cols=["StoreCode","WeekStart"])
        log_lineage("gold","fact_sales","gold","agg_weekly_forecast_input",
                    "FORECAST_PREP",row_count)
        log_pipeline_event("gold","forecast_input","SUCCESS",records_out=row_count)
        print(f"gold.agg_weekly_forecast_input: {row_count:,} rows")
        display(forecast_df.orderBy("StoreCode","WeekStart").limit(20))
    except Exception as e:
        log_pipeline_event("gold","forecast_input","FAILED",error_message=str(e))
        alert_failure("gold","forecast_input",str(e))
        raise

# COMMAND ----------

# MAGIC %md ## Sanity Check

# COMMAND ----------

gold_tables = [
    "dim_customer","dim_product","dim_date","fact_sales",
    "agg_daily_sales_by_store","agg_sales_by_category",
    "agg_rfm_customer_segments","agg_store_inventory_health","agg_weekly_forecast_input",
]
for tbl in gold_tables:
    if table_exists("gold", tbl):
        print(f"gold.{tbl}: {read_delta_table('gold', tbl).count():,} rows")

if table_exists("audit","data_lineage"):
    print("\n--- Lineage for this run ---")
    display(read_delta_table("audit","data_lineage")
            .filter(F.col("pipeline_run_id")==PIPELINE_RUN_ID)
            .orderBy("recorded_at"))