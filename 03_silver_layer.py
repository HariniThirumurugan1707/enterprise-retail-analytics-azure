# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver Layer  *(Enhanced v2.0)*
# MAGIC **Enterprise Retail Analytics Platform**
# MAGIC
# MAGIC ### Enhancements over v1
# MAGIC | Enhancement | Detail |
# MAGIC |---|---|
# MAGIC | **DQ Profiling Report** | After every dataset, `run_dq_profile()` computes null %, range violations, regex mismatches, and allowed-value violations; writes results to `audit.dq_profile_report` |
# MAGIC | **Lineage logging** | Bronze table → Silver table recorded per dataset |
# MAGIC | **ZORDER on write** | `zorder_columns` from registry passed to `write_delta_table` |
# MAGIC | **Teams/email alert on failure** | `alert_failure()` fires on any dataset exception |
# MAGIC | **TSV / API datasets handled automatically** | Registry-driven — no extra code paths |

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# Pre-create all schemas before any table operation
for _schema in ["audit", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{_schema}")
print("All schemas ready.")



dbutils.widgets.dropdown(
    "dataset_filter", "ALL", ["ALL"] + list(DATASET_REGISTRY.keys()),
    "Dataset to process"
)

# COMMAND ----------

from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

DATASET_FILTER = dbutils.widgets.get("dataset_filter")
datasets_to_process = (
    DATASET_REGISTRY if DATASET_FILTER == "ALL"
    else {DATASET_FILTER: DATASET_REGISTRY[DATASET_FILTER]}
)
print(f"Processing: {list(datasets_to_process.keys())}")

# COMMAND ----------

# MAGIC %md ## JSON Flatten

# COMMAND ----------

def flatten_complete(df):
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        if not struct_cols and not array_cols:
            break
        for c in array_cols:
            df = df.withColumn(c, explode_outer(col(c)))
        current_schema = df.schema
        for c in struct_cols:
            if c not in current_schema.names:
                continue
            expanded = [col(f"{c}.{field.name}").alias(f"{c}_{field.name}")
                        for field in current_schema[c].dataType.fields]
            df = df.select("*", *expanded).drop(c)
    return df


def parse_and_flatten_json_bronze(bronze_df, rename_map: dict):
    sample = bronze_df.filter(F.col("raw_payload").isNotNull()).first()
    if not sample:
        return None
    json_schema = spark.range(1).select(F.schema_of_json(F.lit(sample["raw_payload"]))).first()[0]
    parsed = bronze_df.select(F.from_json("raw_payload", json_schema).alias("data")).select("data.*")
    flat   = flatten_complete(parsed)
    for old, new in rename_map.items():
        if old in flat.columns:
            flat = flat.withColumnRenamed(old, new)
    return flat

# COMMAND ----------

# MAGIC %md ## Schema Evolution

# COMMAND ----------

def evolve_schema_and_write(df, schema_name: str, table_name: str,
                             mode: str = "append", zorder_cols: list = None) -> str:
    ensure_schema(schema_name)
    full_name = qualify_table(schema_name, table_name)
    if not table_exists(schema_name, table_name):
        write_delta_table(df, schema_name, table_name, mode="overwrite", zorder_cols=zorder_cols)
        return full_name

    catalog_for_lookup = CATALOG_NAME if USE_UNITY_CATALOG else "hive_metastore"
    try:
        table_cols_df = spark.sql(f"""
            SELECT column_name, data_type
            FROM system.information_schema.columns
            WHERE table_catalog = '{catalog_for_lookup}'
              AND table_schema  = '{schema_name}'
              AND table_name    = '{table_name}'
        """)
        table_schema = {r["column_name"]: r["data_type"] for r in table_cols_df.collect()}
    except Exception:
        existing     = spark.table(full_name)
        table_schema = {f.name: f.dataType.simpleString() for f in existing.schema.fields}

    incoming_schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    new_cols        = {c: t for c, t in incoming_schema.items() if c not in table_schema}
    if new_cols:
        col_defs = ", ".join(f"`{c}` {t}" for c, t in new_cols.items())
        spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS ({col_defs})")
        print(f"  [schema evolution] {full_name} += {list(new_cols.keys())}")

    write_delta_table(df, schema_name, table_name, mode=mode, zorder_cols=zorder_cols)
    return full_name

# COMMAND ----------

# MAGIC %md ## Clean, Cast, Validate

# COMMAND ----------

def clean_and_cast(df, cfg: dict):
    before        = df.count()
    df            = df.dropDuplicates(cfg["dedupe_keys"]) if cfg["dedupe_keys"] else df.dropDuplicates()
    after_dedupe  = df.count()

    for col_name, target_type in cfg["silver_schema"].items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(target_type))
            continue
        if col_name in cfg.get("date_columns", {}):
            fmt = cfg["date_columns"][col_name]
            df = df.withColumn(col_name,
                F.expr(f"try_to_timestamp(trim({col_name}), '{fmt}')").cast("date"))
        elif target_type == "timestamp":
            df = df.withColumn(col_name,
                F.expr(f"try_to_timestamp(trim({col_name}))"))
        elif target_type.startswith("decimal") or target_type in ("int","bigint","double","float"):
            cleaned = F.regexp_replace(F.trim(F.col(col_name)), r"[^0-9.\-]", "")
            cleaned = F.when(cleaned == "", None).otherwise(cleaned)
            df = df.withColumn(col_name, cleaned.cast(target_type))
        else:
            df = df.withColumn(col_name, F.trim(F.col(col_name)).cast(target_type))

    for c in cfg.get("uppercase_columns", []):
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.col(c)))
    for c in cfg.get("titlecase_columns", []):
        if c in df.columns:
            df = df.withColumn(c, F.initcap(F.col(c)))

    reject_exprs   = []
    aliases_to_drop = []
    for c in cfg.get("not_null_columns", []):
        if c in df.columns:
            reject_exprs.append(F.col(c).isNull())
    for rule in cfg.get("dq_rules", []):
        reject_exprs.append(F.expr(rule))
    for rc in cfg.get("referential_checks", []):
        col_name   = rc["column"]
        ref_alias  = f"_ref_{col_name}"
        aliases_to_drop.append(ref_alias)
        if table_exists(rc["ref_schema"], rc["ref_table"]):
            ref_vals = (read_delta_table(rc["ref_schema"], rc["ref_table"])
                        .select(F.col(rc["ref_column"]).cast("string").alias(ref_alias))
                        .distinct())
            df = df.join(ref_vals, df[col_name].cast("string") == ref_vals[ref_alias], "left")
            reject_exprs.append(F.col(ref_alias).isNull())

    is_rejected = reduce(lambda a, b: a | b, reject_exprs) if reject_exprs else F.lit(False)
    df = df.withColumn("_IsRejected", F.coalesce(is_rejected, F.lit(True)))
    for alias in aliases_to_drop:
        df = df.drop(alias)
    return df, before, after_dedupe

# COMMAND ----------

# MAGIC %md ## NEW — DQ Profiling Report
# MAGIC
# MAGIC Computes column-level statistics for each Silver dataset and writes them to
# MAGIC `audit.dq_profile_report`.  Power BI can connect to this table for a live
# MAGIC data-quality dashboard.
# MAGIC
# MAGIC Profiled metrics per column:
# MAGIC - `null_pct` — percentage of null values
# MAGIC - `min_violation_pct` / `max_violation_pct` — values outside configured range
# MAGIC - `regex_fail_pct` — values not matching the configured regex
# MAGIC - `invalid_value_pct` — values not in an allowed-values list

# COMMAND ----------

def run_dq_profile(df, dataset: str, cfg: dict, total_rows: int) -> None:
    """Runs DQ profile assertions and appends results to audit.dq_profile_report."""
    profile_rules = cfg.get("dq_profile_rules", {})
    if not profile_rules or total_rows == 0:
        return

    rows = []
    run_ts = F.current_timestamp()

    for col_name, rules in profile_rules.items():
        if col_name not in df.columns:
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct   = round(null_count / total_rows * 100, 2)

        min_violation_pct = max_violation_pct = regex_fail_pct = invalid_value_pct = None

        if "min" in rules:
            viol = df.filter(F.col(col_name).cast("double") < rules["min"]).count()
            min_violation_pct = round(viol / total_rows * 100, 2)

        if "max" in rules:
            viol = df.filter(F.col(col_name).cast("double") > rules["max"]).count()
            max_violation_pct = round(viol / total_rows * 100, 2)

        if "regex" in rules:
            viol = df.filter(
                F.col(col_name).isNotNull() &
                (~F.col(col_name).rlike(rules["regex"]))
            ).count()
            regex_fail_pct = round(viol / total_rows * 100, 2)

        if "max_length" in rules:
            viol = df.filter(F.length(F.col(col_name)) > rules["max_length"]).count()
            max_violation_pct = round(viol / total_rows * 100, 2)

        if "allowed_values" in rules:
            allowed = [str(v) for v in rules["allowed_values"]]
            viol = df.filter(
                F.col(col_name).isNotNull() &
                (~F.col(col_name).cast("string").isin(allowed))
            ).count()
            invalid_value_pct = round(viol / total_rows * 100, 2)

        rows.append((
            PIPELINE_RUN_ID, dataset, col_name, total_rows, null_pct,
            min_violation_pct, max_violation_pct,
            regex_fail_pct, invalid_value_pct,
            datetime.now(timezone.utc),
        ))

    if not rows:
        return

    schema = T.StructType([
        T.StructField("pipeline_run_id",     T.StringType(),  True),
        T.StructField("dataset",             T.StringType(),  True),
        T.StructField("column_name",         T.StringType(),  True),
        T.StructField("total_rows",          T.LongType(),    True),
        T.StructField("null_pct",            T.DoubleType(),  True),
        T.StructField("min_violation_pct",   T.DoubleType(),  True),
        T.StructField("max_violation_pct",   T.DoubleType(),  True),
        T.StructField("regex_fail_pct",      T.DoubleType(),  True),
        T.StructField("invalid_value_pct",   T.DoubleType(),  True),
        T.StructField("profiled_at",         T.TimestampType(), False),
    ])
    write_delta_table(spark.createDataFrame(rows, schema),
                      "audit", "dq_profile_report", mode="append")
    print(f"  [dq-profile] {dataset}: {len(rows)} column(s) profiled")

# COMMAND ----------

# MAGIC %md ## SCD Type 2

# COMMAND ----------

def upsert_scd2(df_incoming, schema_name: str, table_name: str, cfg: dict, run_id: str = None):
    business_key  = cfg["scd2_business_key"]
    tracked_cols  = cfg["scd2_tracked_columns"]
    effective_col = cfg["watermark_column"]
    run_id        = run_id or PIPELINE_RUN_ID

    df_incoming = df_incoming.withColumn(
        "_SCD_RecordHash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]), 256),
    )
    ensure_schema(schema_name)
    full_name = qualify_table(schema_name, table_name)

    if not table_exists(schema_name, table_name):
        initial = (df_incoming
                   .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
                   .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
                   .withColumn("_SCD_IsCurrent",          F.lit(True)))
        initial = add_silver_audit_columns(initial, run_id)
        write_delta_table(initial, schema_name, table_name, mode="overwrite",
                          zorder_cols=cfg.get("zorder_columns"))
        return full_name, initial.count(), 0

    target_current = (read_delta_table(schema_name, table_name)
                      .filter("_SCD_IsCurrent = true")
                      .select(business_key, F.col("_SCD_RecordHash").alias("_target_hash")))
    compared       = df_incoming.join(target_current, on=business_key, how="left")
    changed_or_new = compared.filter(
        F.col("_target_hash").isNull() | (F.col("_SCD_RecordHash") != F.col("_target_hash"))
    ).drop("_target_hash")

    changed_keys = [r[business_key] for r in changed_or_new.select(business_key).distinct().collect()]
    if not changed_keys:
        return full_name, 0, df_incoming.count()

    (DeltaTable.forName(spark, full_name).alias("t")
     .merge(changed_or_new.alias("s"),
            f"t.{business_key} = s.{business_key} AND t._SCD_IsCurrent = true")
     .whenMatchedUpdate(set={"_SCD_IsCurrent": "false",
                              "_SCD_EffectiveEndDate": f"s.{effective_col}"})
     .execute())

    new_versions = (changed_or_new
                    .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
                    .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
                    .withColumn("_SCD_IsCurrent",          F.lit(True)))
    new_versions = add_silver_audit_columns(new_versions, run_id)
    evolve_schema_and_write(new_versions, schema_name, table_name,
                             mode="append", zorder_cols=cfg.get("zorder_columns"))
    return full_name, len(changed_keys), df_incoming.count() - len(changed_keys)

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

failures = []

for dataset, cfg in datasets_to_process.items():
    log_pipeline_event("silver", dataset, "STARTED")
    try:
        if not table_exists("bronze", dataset):
            log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no Bronze table yet")
            continue

        bronze_df_full = read_delta_table("bronze", dataset)
        if cfg["load_pattern"] == "append":
            last_watermark = get_silver_watermark(dataset)
            bronze_df = (bronze_df_full.filter(F.col("_IngestionTimestamp") > F.lit(last_watermark))
                         if last_watermark is not None else bronze_df_full)
        else:
            bronze_df = bronze_df_full

        records_in = bronze_df.count()
        if records_in == 0:
            log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no new Bronze rows")
            continue

        if cfg["is_json"] and cfg["needs_flatten"]:
            working_df = parse_and_flatten_json_bronze(bronze_df, cfg["json_flatten_rename_map"])
            if working_df is None:
                log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
                continue
        else:
            keep_cols  = [c for c in bronze_df.columns if not c.startswith("_")]
            working_df = bronze_df.select(*keep_cols)

        clean_df, before, after_dedupe = clean_and_cast(working_df, cfg)
        print(f"  {dataset}: {before} → {after_dedupe} rows after dedupe")

        # NEW: DQ Profiling
        run_dq_profile(clean_df, dataset, cfg, after_dedupe)

        good_df      = clean_df.filter("_IsRejected = false")
        rejected_df  = clean_df.filter("_IsRejected = true")
        rejected_count = rejected_df.count()

        if rejected_count > 0:
            rejected_audited = add_silver_audit_columns(rejected_df)
            evolve_schema_and_write(rejected_audited, "silver", f"{dataset}_rejected_records",
                                     mode="append")

        if cfg["is_scd2"]:
            full_name, changed_ct, unchanged_ct = upsert_scd2(good_df, "silver", dataset, cfg)
            records_out = changed_ct
        else:
            good_audited = add_silver_audit_columns(good_df)
            write_mode   = "append" if cfg["load_pattern"] == "append" else "overwrite"
            evolve_schema_and_write(good_audited, "silver", dataset,
                                     mode=write_mode, zorder_cols=cfg.get("zorder_columns"))
            records_out = good_df.count()

        # Lineage: bronze → silver
        log_lineage("bronze", dataset, "silver", dataset, "TRANSFORM", records_out)

        log_pipeline_event("silver", dataset, "SUCCESS",
                           records_in=records_in, records_out=records_out,
                           records_rejected=rejected_count)

        if cfg["load_pattern"] == "append":
            new_watermark = bronze_df.agg(F.max("_IngestionTimestamp")).first()[0]
            if new_watermark is not None:
                set_silver_watermark(dataset, new_watermark)

    except Exception as e:
        log_pipeline_event("silver", dataset, "FAILED", error_message=str(e))
        alert_failure("silver", dataset, str(e))          # NEW: Teams + Email alert
        failures.append((dataset, str(e)))

# COMMAND ----------

if failures:
    summary = "; ".join(f"{d}: {e}" for d, e in failures)
    raise RuntimeError(f"Silver failed for {len(failures)} dataset(s): {summary}")
else:
    alert_success_summary("silver",
        f"All {len(datasets_to_process)} Silver datasets processed. Run: {PIPELINE_RUN_ID}")
    print("Silver layer completed successfully.")

# COMMAND ----------

# MAGIC %md ## Sanity Check

# COMMAND ----------

for dataset in datasets_to_process:
    if table_exists("silver", dataset):
        cnt  = read_delta_table("silver", dataset).count()
        print(f"silver.{dataset}: {cnt:,} rows")
    if table_exists("silver", f"{dataset}_rejected_records"):
        rcnt = read_delta_table("silver", f"{dataset}_rejected_records").count()
        print(f"silver.{dataset}_rejected_records: {rcnt:,} rows")

if table_exists("audit", "dq_profile_report"):
    display(read_delta_table("audit", "dq_profile_report")
            .orderBy("profiled_at", ascending=False).limit(30))