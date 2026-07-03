# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config & Utilities *(Enhanced v2.0)*

# COMMAND ----------

# MAGIC %pip install requests --quiet

# COMMAND ----------

dbutils.widgets.dropdown("environment", "free_edition", ["free_edition", "community", "azure"], "Environment")
dbutils.widgets.text("base_data_path",    "",         "Base data path (blank=auto)")
dbutils.widgets.text("catalog_name",      "capstone", "Unity Catalog catalog name")
dbutils.widgets.text("pipeline_run_id",   "",         "ADF Pipeline Run Id (blank=auto)")
dbutils.widgets.text("alert_email",       "",         "Alert email (blank=disabled)")
dbutils.widgets.text("teams_webhook_url", "",         "MS Teams webhook URL (blank=disabled)")

# COMMAND ----------

import uuid, json, requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

ENVIRONMENT   = dbutils.widgets.get("environment").strip().lower()
CATALOG_NAME  = dbutils.widgets.get("catalog_name").strip() or "capstone"
ALERT_EMAIL   = dbutils.widgets.get("alert_email").strip()
TEAMS_WEBHOOK = dbutils.widgets.get("teams_webhook_url").strip()

USE_UNITY_CATALOG = ENVIRONMENT in ("free_edition", "azure", "community")

_base_override = dbutils.widgets.get("base_data_path").strip()
if _base_override:
    BASE_DATA_PATH = _base_override.rstrip("/")
elif ENVIRONMENT in ("free_edition", "community"):
    BASE_DATA_PATH = "/Volumes/capstone/storage/raw_files"
else:
    BASE_DATA_PATH = "abfss://datalake@CHANGE_ME.dfs.core.windows.net/capstone_project"

print(f"Environment    : {ENVIRONMENT}")
print(f"Unity Catalog  : {USE_UNITY_CATALOG}  (catalog='{CATALOG_NAME}')")
print(f"Base data path : {BASE_DATA_PATH}")
print(f"Alert email    : {ALERT_EMAIL or '(disabled)'}")
print(f"Teams webhook  : {'configured' if TEAMS_WEBHOOK else '(disabled)'}")

# COMMAND ----------

# MAGIC %md ## Path & Catalog Helpers

# COMMAND ----------

def get_layer_path(layer, dataset=None):
    path = f"{BASE_DATA_PATH}/{layer}"
    return f"{path}/{dataset}" if dataset else path

def ensure_path(path):
    dbutils.fs.mkdirs(path)

def qualify_table(schema_name, table_name):
    if USE_UNITY_CATALOG:
        return f"{CATALOG_NAME}.{schema_name}.{table_name}"
    return f"{schema_name}.{table_name}"

def ensure_schema(schema_name):
    if USE_UNITY_CATALOG:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema_name}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")

def utc_now():
    return datetime.now(timezone.utc)

PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
print(f"Pipeline Run Id: {PIPELINE_RUN_ID}")

# COMMAND ----------

# MAGIC %md ## Audit Columns

# COMMAND ----------

def add_bronze_audit_columns(df, run_id=None):
    run_id = run_id or PIPELINE_RUN_ID
    return (df
        .withColumn("_AdfPipelineRunId",   F.lit(run_id).cast("string"))
        .withColumn("_IngestionTimestamp",  F.current_timestamp()))

def add_silver_audit_columns(df, run_id=None):
    run_id = run_id or PIPELINE_RUN_ID
    return (df
        .withColumn("_AdfPipelineRunId",   F.lit(run_id).cast("string"))
        .withColumn("_ProcessedTimestamp", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Delta Read / Write / Optimise

# COMMAND ----------

def table_exists(schema_name, table_name):
    try:
        spark.table(qualify_table(schema_name, table_name))
        return True
    except Exception:
        return False

def read_delta_table(schema_name, table_name):
    return spark.table(qualify_table(schema_name, table_name))

def write_delta_table(df, schema_name, table_name,
                      mode="append", zorder_cols=None, vacuum_hours=None):
    ensure_schema(schema_name)
    full_name = qualify_table(schema_name, table_name)
    (df.write.format("delta")
       .mode(mode)
       .option("mergeSchema", "true")
       .saveAsTable(full_name))
    if zorder_cols:
        cols_sql = ", ".join(zorder_cols)
        try:
            spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({cols_sql})")
            print(f"  [optimize] ZORDER {full_name} by {zorder_cols}")
        except Exception as e:
            print(f"  [optimize] skipped: {e}")
    if vacuum_hours is not None:
        try:
            spark.sql(f"VACUUM {full_name} RETAIN {vacuum_hours} HOURS")
        except Exception as e:
            print(f"  [vacuum] skipped: {e}")

# COMMAND ----------

# MAGIC %md ## Raw File Registry & Watermarks

# COMMAND ----------

def list_raw_files(dataset):
    path = get_layer_path("raw", dataset)
    try:
        return [f for f in dbutils.fs.ls(path) if not f.name.startswith("_")]
    except Exception:
        return []

def get_already_ingested_files(dataset):
    if not table_exists("audit", "bronze_file_registry"):
        return set()
    rows = (read_delta_table("audit", "bronze_file_registry")
            .filter(F.col("dataset") == dataset)
            .select("file_path").collect())
    return {r["file_path"] for r in rows}

def register_ingested_files(dataset, files):
    ensure_schema("audit")
    rows = [(dataset, f.path, f.size, datetime.now(timezone.utc)) for f in files]
    schema = T.StructType([
        T.StructField("dataset",         T.StringType(),    False),
        T.StructField("file_path",       T.StringType(),    False),
        T.StructField("file_size_bytes", T.LongType(),      True),
        T.StructField("registered_at",   T.TimestampType(), False),
    ])
    write_delta_table(spark.createDataFrame(rows, schema),
                      "audit", "bronze_file_registry", mode="append")

def log_pipeline_event(layer, dataset, status,
                       records_in=None, records_out=None,
                       records_rejected=None, error_message=None):
    ensure_schema("audit")
    row = [(PIPELINE_RUN_ID, layer, dataset, status,
            records_in, records_out, records_rejected,
            error_message, datetime.now(timezone.utc))]
    schema = T.StructType([
        T.StructField("pipeline_run_id",  T.StringType(),    True),
        T.StructField("layer",            T.StringType(),    True),
        T.StructField("dataset",          T.StringType(),    True),
        T.StructField("status",           T.StringType(),    True),
        T.StructField("records_in",       T.LongType(),      True),
        T.StructField("records_out",      T.LongType(),      True),
        T.StructField("records_rejected", T.LongType(),      True),
        T.StructField("error_message",    T.StringType(),    True),
        T.StructField("log_timestamp",    T.TimestampType(), False),
    ])
    write_delta_table(spark.createDataFrame(row, schema),
                      "audit", "pipeline_execution_log", mode="append")

def get_silver_watermark(dataset):
    if not table_exists("audit", "silver_watermark"):
        return None
    row = read_delta_table("audit", "silver_watermark").filter(F.col("dataset") == dataset).first()
    return row["last_processed_ts"] if row else None

def set_silver_watermark(dataset, watermark_ts):
    ensure_schema("audit")
    full_name = qualify_table("audit", "silver_watermark")
    if table_exists("audit", "silver_watermark"):
        spark.sql(f"DELETE FROM {full_name} WHERE dataset = '{dataset}'")
    write_delta_table(
        spark.createDataFrame([(dataset, watermark_ts)], ["dataset", "last_processed_ts"]),
        "audit", "silver_watermark", mode="append")

# COMMAND ----------

# MAGIC %md ## Lineage Logger

# COMMAND ----------

def log_lineage(source_layer, source_table, target_layer, target_table,
                operation, row_count=None):
    try:
        ensure_schema("audit")
        src_full = (qualify_table(source_layer, source_table)
                    if source_layer and "." not in source_table else source_table)
        row = [(PIPELINE_RUN_ID, source_layer, src_full,
                target_layer, qualify_table(target_layer, target_table),
                operation, row_count, datetime.now(timezone.utc))]
        schema = T.StructType([
            T.StructField("pipeline_run_id",  T.StringType(),    True),
            T.StructField("source_layer",     T.StringType(),    True),
            T.StructField("source_full_name", T.StringType(),    True),
            T.StructField("target_layer",     T.StringType(),    True),
            T.StructField("target_full_name", T.StringType(),    True),
            T.StructField("operation",        T.StringType(),    True),
            T.StructField("row_count",        T.LongType(),      True),
            T.StructField("recorded_at",      T.TimestampType(), False),
        ])
        write_delta_table(spark.createDataFrame(row, schema),
                          "audit", "data_lineage", mode="append")
    except Exception as e:
        print(f"  [lineage] skipped (non-fatal): {e}")

# COMMAND ----------

# MAGIC %md ## Alerting

# COMMAND ----------

def send_teams_alert(title, message, color="FF0000"):
    if not TEAMS_WEBHOOK:
        return
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{"activityTitle": f"**{title}**", "activityText": message}],
    }
    try:
        requests.post(TEAMS_WEBHOOK, json=payload, timeout=10).raise_for_status()
        print(f"  [teams] sent: {title}")
    except Exception as e:
        print(f"  [teams] failed (non-fatal): {e}")

def send_email_alert(subject, body):
    if not ALERT_EMAIL:
        return
    try:
        dbutils.notification.email(to=[ALERT_EMAIL], subject=subject, body=body)
        print(f"  [email] sent: {subject}")
    except Exception as e:
        print(f"  [email] unavailable (non-fatal): {e}")

def alert_failure(layer, dataset, error):
    msg = f"Run ID: {PIPELINE_RUN_ID}\nLayer: {layer}\nDataset: {dataset}\nError: {error}"
    send_teams_alert(f"Pipeline FAILED — {layer}.{dataset}", msg, color="FF0000")
    send_email_alert(f"[ALERT] Pipeline FAILED — {layer}.{dataset}", msg)

def alert_success_summary(layer, summary):
    send_teams_alert(f"Pipeline SUCCESS — {layer}", summary, color="00C851")

# COMMAND ----------

# MAGIC %md ## Dataset Registry

# COMMAND ----------

DATASET_REGISTRY = {

    "products": {
        "raw_format": "csv", "separator": ",",
        "bronze_schema": None, "is_json": False, "needs_flatten": False,
        "load_pattern": "overwrite_latest", "is_scd2": False,
        "primary_keys": ["ProductID"], "dedupe_keys": ["ProductID"],
        "not_null_columns": ["ProductID", "ProductName"],
        "dq_rules": ["CostPrice IS NOT NULL AND CostPrice <= 0"],
        "dq_profile_rules": {
            "CostPrice":   {"min": 0.01, "max": 50000},
            "Category":    {"allowed_values": ["Electronics","Apparel","Home & Kitchen","Sports","Grocery"]},
            "ProductName": {"max_length": 200},
        },
        "referential_checks": [], "date_columns": {},
        "titlecase_columns": ["Category"],
        "zorder_columns": ["ProductID"],
        "silver_schema": {
            "ProductID": "int", "ProductName": "string",
            "Category": "string", "SubCategory": "string",
            "Brand": "string", "CostPrice": "decimal(10,2)",
        },
    },

    "customers": {
        "raw_format": "csv", "separator": ",",
        "bronze_schema": {
            "CustomerID": "int", "FirstName": "string", "LastName": "string",
            "Email": "string", "Phone": "string",
            "City": "string", "State": "string", "LastUpdated": "timestamp",
        },
        "is_json": False, "needs_flatten": False,
        "load_pattern": "append", "is_scd2": True,
        "scd2_business_key": "CustomerID",
        "scd2_tracked_columns": ["FirstName","LastName","Email","Phone","City","State"],
        "watermark_column": "LastUpdated",
        "primary_keys": ["CustomerID"], "dedupe_keys": ["CustomerID","LastUpdated"],
        "not_null_columns": ["CustomerID","FirstName","LastName"],
        "dq_rules": [],
        "dq_profile_rules": {
            "Email": {"regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"},
            "State": {"max_length": 2},
        },
        "referential_checks": [], "date_columns": {},
        "uppercase_columns": ["State"],
        "zorder_columns": ["CustomerID"],
        "silver_schema": {
            "CustomerID": "int", "FirstName": "string", "LastName": "string",
            "Email": "string", "Phone": "string",
            "City": "string", "State": "string", "LastUpdated": "timestamp",
        },
    },

    "exchange_rates": {
        "raw_format": "json", "separator": None,
        "bronze_schema": None, "is_json": True, "needs_flatten": True,
        "json_flatten_rename_map": {
            "asOfDate":                        "RateDate",
            "base_currencyCode":               "BaseCurrency",
            "rateGroups_rates_targetCurrency": "TargetCurrency",
            "rateGroups_rates_rate":           "ExchangeRate",
        },
        "load_pattern": "append", "is_scd2": False,
        "primary_keys": ["BaseCurrency","TargetCurrency","RateDate"],
        "dedupe_keys":  ["BaseCurrency","TargetCurrency","RateDate"],
        "not_null_columns": ["BaseCurrency","TargetCurrency","ExchangeRate","RateDate"],
        "dq_rules": ["ExchangeRate IS NOT NULL AND ExchangeRate <= 0"],
        "dq_profile_rules": {"ExchangeRate": {"min": 0.001, "max": 500}},
        "referential_checks": [], "date_columns": {"RateDate": "yyyy-MM-dd"},
        "zorder_columns": ["RateDate","BaseCurrency"],
        "silver_schema": {
            "BaseCurrency": "string", "TargetCurrency": "string",
            "ExchangeRate": "decimal(12,6)", "RateDate": "date",
        },
    },

    "orders": {
        "raw_format": "csv", "separator": ",",
        "bronze_schema": None, "is_json": False, "needs_flatten": False,
        "load_pattern": "append", "is_scd2": False,
        "primary_keys": ["OrderID"], "dedupe_keys": ["OrderID"],
        "not_null_columns": ["OrderID","CustomerID","ProductID","OrderDate","Quantity","UnitPrice"],
        "dq_rules": [
            "Quantity IS NOT NULL AND Quantity <= 0",
            "UnitPrice IS NOT NULL AND UnitPrice <= 0",
        ],
        "dq_profile_rules": {
            "Quantity":  {"min": 1, "max": 500},
            "UnitPrice": {"min": 0.01, "max": 100000},
            "StoreCode": {"allowed_values": [f"ST{n:03d}" for n in range(1, 11)]},
        },
        "referential_checks": [
            {"column": "CustomerID", "ref_schema": "silver", "ref_table": "customers", "ref_column": "CustomerID"},
            {"column": "ProductID",  "ref_schema": "silver", "ref_table": "products",  "ref_column": "ProductID"},
        ],
        "date_columns": {"OrderDate": "yyyy-MM-dd"},
        "zorder_columns": ["OrderDate","StoreCode"],
        "silver_schema": {
            "OrderID": "bigint", "CustomerID": "int", "ProductID": "int",
            "OrderDate": "date", "Quantity": "int",
            "UnitPrice": "decimal(10,2)", "StoreCode": "string",
        },
    },

    "store_inventory": {
        "raw_format": "csv", "separator": "\t",
        "bronze_schema": None, "is_json": False, "needs_flatten": False,
        "load_pattern": "overwrite_latest", "is_scd2": False,
        "primary_keys": ["StoreCode","ProductID"],
        "dedupe_keys":  ["StoreCode","ProductID","SnapshotDate"],
        "not_null_columns": ["StoreCode","ProductID","StockLevel","SnapshotDate"],
        "dq_rules": ["StockLevel IS NOT NULL AND StockLevel < 0"],
        "dq_profile_rules": {
            "StockLevel":   {"min": 0, "max": 100000},
            "ReorderPoint": {"min": 0, "max": 10000},
        },
        "referential_checks": [],
        "date_columns": {"SnapshotDate": "yyyy-MM-dd"},
        "zorder_columns": ["StoreCode","ProductID"],
        "silver_schema": {
            "StoreCode":    "string", "ProductID":    "int",
            "StockLevel":   "int",    "ReorderPoint": "int",
            "SnapshotDate": "date",
        },
    },
}

print(f"Registered datasets: {list(DATASET_REGISTRY.keys())}")
