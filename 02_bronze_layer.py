# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze Layer  *(Enhanced v2.0)*
# MAGIC **Enterprise Retail Analytics Platform**
# MAGIC
# MAGIC ### Enhancements over v1
# MAGIC | Enhancement | Detail |
# MAGIC |---|---|
# MAGIC | **TSV support** | Reads `separator` from registry — same code path handles CSV, TSV, pipe-delimited |
# MAGIC | **REST API ingestion** | `fetch_api_source()` calls any `raw_format='api'` endpoint and stores raw JSON as `raw_payload` |
# MAGIC | **Lineage logging** | Every Bronze write records a lineage event |
# MAGIC | **ZORDER optimisation** | Calls `write_delta_table(..., zorder_cols=...)` from registry config |
# MAGIC | **Teams/email alert on failure** | `alert_failure()` fires on any dataset exception |

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# Pre-create all schemas so Unity Catalog never throws TABLE_OR_VIEW_NOT_FOUND
# on first run. Safe to run multiple times (IF NOT EXISTS).
for _schema in ["audit", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{_schema}")
    print(f"  schema ready: {CATALOG_NAME}.{_schema}")

# COMMAND ----------

dbutils.widgets.dropdown(
    "dataset_filter", "ALL", ["ALL"] + list(DATASET_REGISTRY.keys()),
    "Dataset to process"
)

# COMMAND ----------

DATASET_FILTER = dbutils.widgets.get("dataset_filter")
datasets_to_process = (
    DATASET_REGISTRY if DATASET_FILTER == "ALL"
    else {DATASET_FILTER: DATASET_REGISTRY[DATASET_FILTER]}
)
print(f"Processing: {list(datasets_to_process.keys())}")

# COMMAND ----------

# MAGIC %md ## Ingestion Functions

# COMMAND ----------

def pick_new_files(dataset: str):
    all_files    = list_raw_files(dataset)
    already_done = get_already_ingested_files(dataset)
    return [f for f in all_files if f.path not in already_done]


def ingest_csv_dataset(dataset: str, cfg: dict, files):
    """
    Handles CSV and TSV (and any other delimiter) via the registry `separator` key.
    All-STRING by default; casts to native types if `bronze_schema` is set.
    """
    paths = [f.path for f in files]
    sep   = cfg.get("separator", ",") or ","
    df    = spark.read.option("header", True).option("sep", sep).csv(paths)

    if cfg.get("bronze_schema"):
        for col_name, target_type in cfg["bronze_schema"].items():
            if col_name in df.columns:
                if target_type == "timestamp":
                    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
                else:
                    df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    else:
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

    df = df.withColumn("_SourceFileName", F.col("_metadata.file_path"))
    return add_bronze_audit_columns(df)


def ingest_json_dataset(dataset: str, cfg: dict, files):
    """Stores the entire JSON file as a single raw_payload STRING row."""
    paths = [f.path for f in files]
    df = (spark.read.text(paths, wholetext=True)
          .withColumnRenamed("value", "raw_payload")
          .withColumn("_SourceFileName", F.col("_metadata.file_path")))
    return add_bronze_audit_columns(df)


def fetch_api_source(dataset: str, cfg: dict):
    """
    NEW: REST API ingestion.
    Calls the configured endpoint, stores the full JSON response as `raw_payload`.
    Supports auth_type: none | bearer | api_key.
    Returns a single-row DataFrame or None on error.
    """
    endpoint    = cfg.get("api_endpoint", "")
    auth_type   = cfg.get("api_auth_type", "none").lower()
    headers     = dict(cfg.get("api_headers", {}))
    headers["Accept"] = "application/json"

    if auth_type == "bearer":
        token = dbutils.secrets.get(scope="pipeline_secrets", key=f"{dataset}_bearer_token")
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "api_key":
        key_name  = cfg.get("api_key_header", "X-Api-Key")
        key_value = dbutils.secrets.get(scope="pipeline_secrets", key=f"{dataset}_api_key")
        headers[key_name] = key_value

    try:
        import requests as _req
        resp = _req.get(endpoint, headers=headers, timeout=30)
        resp.raise_for_status()
        raw_json = resp.text
    except Exception as e:
        print(f"  [api] {dataset}: request failed — {e}")
        return None

    # Wrap as a single-row DataFrame matching the JSON Bronze pattern
    row_df = spark.createDataFrame([(raw_json,)], ["raw_payload"])
    row_df = row_df.withColumn("_SourceFileName", F.lit(endpoint))
    return add_bronze_audit_columns(row_df)

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

failures = []

for dataset, cfg in datasets_to_process.items():
    log_pipeline_event("bronze", dataset, "STARTED")
    try:
        raw_format = cfg.get("raw_format", "csv")

        # ── API sources: always re-fetch, no file registry
        if raw_format == "api":
            bronze_df = fetch_api_source(dataset, cfg)
            if bronze_df is None:
                log_pipeline_event("bronze", dataset, "SUCCESS", records_in=0, records_out=0)
                continue
            row_count = bronze_df.count()
            write_delta_table(bronze_df, "bronze", dataset,
                              mode="append",
                              zorder_cols=cfg.get("zorder_columns"))
            log_lineage("raw", f"API:{cfg.get('api_endpoint','?')}",
                        "bronze", dataset, "API_INGEST", row_count)
            log_pipeline_event("bronze", dataset, "SUCCESS",
                               records_in=row_count, records_out=row_count)
            continue

        # ── File-based sources (CSV / TSV / JSON)
        new_files = pick_new_files(dataset)
        if not new_files:
            log_pipeline_event("bronze", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no new raw files")
            continue

        write_mode = "overwrite" if cfg["load_pattern"] == "overwrite_latest" else "append"
        files_to_load = (
            [max(new_files, key=lambda f: f.modificationTime)]
            if cfg["load_pattern"] == "overwrite_latest"
            else new_files
        )

        bronze_df = (ingest_json_dataset(dataset, cfg, files_to_load)
                     if cfg["is_json"]
                     else ingest_csv_dataset(dataset, cfg, files_to_load))

        row_count = bronze_df.count()
        write_delta_table(bronze_df, "bronze", dataset,
                          mode=write_mode,
                          zorder_cols=cfg.get("zorder_columns"))
        register_ingested_files(dataset, new_files)

        # Lineage: raw file → bronze table
        log_lineage(f"raw/{dataset}", ",".join(f.path for f in files_to_load[:3]),
                    "bronze", dataset, write_mode.upper(), row_count)
        log_pipeline_event("bronze", dataset, "SUCCESS",
                           records_in=row_count, records_out=row_count)

    except Exception as e:
        log_pipeline_event("bronze", dataset, "FAILED", error_message=str(e))
        alert_failure("bronze", dataset, str(e))          # NEW: Teams + Email alert
        failures.append((dataset, str(e)))

# COMMAND ----------

if failures:
    summary = "; ".join(f"{d}: {e}" for d, e in failures)
    raise RuntimeError(f"Bronze failed for {len(failures)} dataset(s): {summary}")
else:
    alert_success_summary("bronze",
        f"All {len(datasets_to_process)} Bronze datasets ingested. Run: {PIPELINE_RUN_ID}")
    print("Bronze layer completed successfully.")

# COMMAND ----------

# MAGIC %md ## Sanity Check

# COMMAND ----------

for dataset in datasets_to_process:
    if table_exists("bronze", dataset):
        cnt = read_delta_table("bronze", dataset).count()
        print(f"bronze.{dataset}: {cnt:,} rows")