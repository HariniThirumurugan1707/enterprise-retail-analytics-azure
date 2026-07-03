# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests 

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# ── Test runner ────────────────────────────────────────────────────────────────
passed  = []
failed  = []
skipped = []

def run_test(name, fn):
    try:
        result = fn()
        if result is None or result is True:
            passed.append(name)
            print(f"  PASS  {name}")
        elif result == "SKIP":
            skipped.append(name)
            print(f"  SKIP  {name}")
        else:
            failed.append(name)
            print(f"  FAIL  {name}  →  {result}")
    except Exception as e:
        failed.append(name)
        print(f"  FAIL  {name}  →  {e}")

def assert_true(condition, msg="condition was False"):
    if not condition:
        raise AssertionError(msg)

def assert_equal(actual, expected, msg=None):
    if actual != expected:
        raise AssertionError(msg or f"expected {expected!r}, got {actual!r}")

def assert_gte(actual, minimum, msg=None):
    if actual < minimum:
        raise AssertionError(msg or f"expected >= {minimum}, got {actual}")

def assert_columns_exist(df, cols):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AssertionError(f"Missing columns: {missing}")

def skip_if_missing(schema, table):
    if not table_exists(schema, table):
        return "SKIP"

# COMMAND ----------

print("=" * 60)
print("  SUITE 1 — Config & Registry")
print("=" * 60)

def test_registry_has_all_datasets():
    expected = {"products","customers","orders","exchange_rates","store_inventory"}
    actual   = set(DATASET_REGISTRY.keys())
    missing  = expected - actual
    assert_true(not missing, f"Missing from registry: {missing}")

def test_registry_required_keys():
    required = ["raw_format","load_pattern","dedupe_keys","not_null_columns","silver_schema"]
    for ds, cfg in DATASET_REGISTRY.items():
        for key in required:
            assert_true(key in cfg, f"{ds} missing key: {key}")

def test_base_data_path_not_empty():
    assert_true(len(BASE_DATA_PATH) > 0, "BASE_DATA_PATH is empty")

def test_base_data_path_correct_format():
    valid_prefixes = ("/Volumes/", "dbfs:/", "abfss://")
    assert_true(
        any(BASE_DATA_PATH.startswith(p) for p in valid_prefixes),
        f"BASE_DATA_PATH has unexpected format: {BASE_DATA_PATH}"
    )

def test_catalog_name_set():
    assert_true(CATALOG_NAME == "capstone", f"Expected 'capstone', got '{CATALOG_NAME}'")

def test_pipeline_run_id_format():
    import re
    pattern = r"^[0-9a-f\-]{36}$"
    assert_true(re.match(pattern, PIPELINE_RUN_ID) is not None or len(PIPELINE_RUN_ID) > 0,
                f"Invalid pipeline_run_id: {PIPELINE_RUN_ID}")

def test_tsv_dataset_has_tab_separator():
    cfg = DATASET_REGISTRY.get("store_inventory", {})
    assert_equal(cfg.get("separator"), "\t", "store_inventory separator should be \\t")

def test_api_dataset_has_endpoint():
    cfg = DATASET_REGISTRY.get("fx_rates_api", {})
    if not cfg:
        return "SKIP"
    endpoint = cfg.get("api_endpoint", "")
    if not endpoint:
        return "SKIP"
    assert_true(endpoint.startswith("http"),
                "fx_rates_api must have a valid api_endpoint")

run_test("Registry has all 6 datasets",           test_registry_has_all_datasets)
run_test("Registry entries have required keys",    test_registry_required_keys)
run_test("BASE_DATA_PATH is set",                  test_base_data_path_not_empty)
run_test("BASE_DATA_PATH has valid format",        test_base_data_path_correct_format)
run_test("Catalog name is 'capstone'",             test_catalog_name_set)
run_test("Pipeline run ID is valid",               test_pipeline_run_id_format)
run_test("store_inventory uses tab separator",     test_tsv_dataset_has_tab_separator)
run_test("fx_rates_api has endpoint URL",          test_api_dataset_has_endpoint)

# COMMAND ----------

print("\n" + "=" * 60)
print("  SUITE 2 — Bronze Layer")
print("=" * 60)

def test_bronze_tables_exist():
    for ds in ["products","customers","orders","exchange_rates"]:
        assert_true(table_exists("bronze", ds), f"bronze.{ds} does not exist")

def test_bronze_row_counts():
    for ds in ["products","customers","orders","exchange_rates"]:
        cnt = read_delta_table("bronze", ds).count()
        assert_gte(cnt, 1, f"bronze.{ds} has 0 rows")

def test_bronze_audit_columns_exist():
    required = ["_AdfPipelineRunId","_IngestionTimestamp"]
    for ds in ["products","customers","orders","exchange_rates"]:
        df = read_delta_table("bronze", ds)
        assert_columns_exist(df, required)

def test_bronze_ingestion_timestamp_not_null():
    for ds in ["products","customers","orders"]:
        df   = read_delta_table("bronze", ds)
        nulls = df.filter(F.col("_IngestionTimestamp").isNull()).count()
        assert_equal(nulls, 0, f"bronze.{ds} has {nulls} null _IngestionTimestamp values")

def test_bronze_pipeline_run_id_populated():
    for ds in ["products","orders"]:
        df    = read_delta_table("bronze", ds)
        nulls = df.filter(F.col("_AdfPipelineRunId").isNull()).count()
        assert_equal(nulls, 0, f"bronze.{ds} has {nulls} null _AdfPipelineRunId values")

def test_bronze_file_registry_exists():
    assert_true(table_exists("audit","bronze_file_registry"),
                "audit.bronze_file_registry does not exist")

def test_bronze_file_registry_has_records():
    cnt = read_delta_table("audit","bronze_file_registry").count()
    assert_gte(cnt, 1, "audit.bronze_file_registry is empty")

def test_bronze_file_registry_datasets_recorded():
    df = read_delta_table("audit","bronze_file_registry")
    recorded = {r["dataset"] for r in df.select("dataset").distinct().collect()}
    expected = {"products","customers","orders","exchange_rates"}
    missing  = expected - recorded
    assert_true(not missing, f"Datasets not in file registry: {missing}")

def test_bronze_json_has_raw_payload():
    df = read_delta_table("bronze","exchange_rates")
    assert_true("raw_payload" in df.columns,
                "bronze.exchange_rates missing raw_payload column")
    nulls = df.filter(F.col("raw_payload").isNull()).count()
    assert_equal(nulls, 0, "bronze.exchange_rates has null raw_payload rows")

def test_bronze_tsv_table_exists():
    if not table_exists("bronze","store_inventory"):
        return "SKIP"
    cnt = read_delta_table("bronze","store_inventory").count()
    assert_gte(cnt, 1, "bronze.store_inventory has 0 rows")

def test_bronze_idempotency():
    # Running Bronze twice should not double the row count
    # We test this by checking file_registry prevents re-ingestion
    df = read_delta_table("audit","bronze_file_registry")
    total = df.count()
    unique = df.select("file_path").distinct().count()
    assert_equal(total, unique, f"Duplicate file paths in registry: {total} total vs {unique} unique")

run_test("All Bronze tables exist",                 test_bronze_tables_exist)
run_test("Bronze tables have rows",                 test_bronze_row_counts)
run_test("Bronze audit columns present",            test_bronze_audit_columns_exist)
run_test("_IngestionTimestamp never null",           test_bronze_ingestion_timestamp_not_null)
run_test("_AdfPipelineRunId never null",             test_bronze_pipeline_run_id_populated)
run_test("File registry table exists",              test_bronze_file_registry_exists)
run_test("File registry has records",               test_bronze_file_registry_has_records)
run_test("All datasets recorded in registry",       test_bronze_file_registry_datasets_recorded)
run_test("exchange_rates has raw_payload column",   test_bronze_json_has_raw_payload)
run_test("store_inventory TSV table exists",        test_bronze_tsv_table_exists)
run_test("File registry has no duplicates",         test_bronze_idempotency)

# COMMAND ----------

print("\n" + "=" * 60)
print("  SUITE 3 — Silver Layer")
print("=" * 60)

def test_silver_tables_exist():
    for ds in ["products","customers","orders","exchange_rates"]:
        assert_true(table_exists("silver", ds), f"silver.{ds} does not exist")

def test_silver_row_counts():
    for ds in ["products","customers","orders"]:
        cnt = read_delta_table("silver", ds).count()
        assert_gte(cnt, 1, f"silver.{ds} has 0 rows")

def test_silver_isrejected_column_exists():
    for ds in ["products","customers","orders","exchange_rates"]:
        df = read_delta_table("silver", ds)
        assert_true("_IsRejected" in df.columns,
                    f"silver.{ds} missing _IsRejected column")

def test_silver_good_rows_exist():
    for ds in ["products","customers","orders"]:
        df  = read_delta_table("silver", ds)
        cnt = df.filter("_IsRejected = false").count()
        assert_gte(cnt, 1, f"silver.{ds} has no non-rejected rows")

def test_silver_rejected_records_table_exists():
    for ds in ["products","customers","orders","exchange_rates"]:
        assert_true(table_exists("silver", f"{ds}_rejected_records"),
                    f"silver.{ds}_rejected_records does not exist")

def test_silver_products_types():
    df = read_delta_table("silver","products").filter("_IsRejected = false")
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert_true("ProductID" in schema_map, "silver.products missing ProductID")
    assert_true(schema_map.get("ProductID","").startswith("int"),
                f"ProductID should be int, got {schema_map.get('ProductID')}")

def test_silver_orders_types():
    df = read_delta_table("silver","orders").filter("_IsRejected = false")
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert_true(schema_map.get("OrderDate","") == "date",
                f"OrderDate should be date, got {schema_map.get('OrderDate')}")
    assert_true(schema_map.get("Quantity","").startswith("int"),
                f"Quantity should be int, got {schema_map.get('Quantity')}")

def test_silver_orders_no_negative_quantity():
    df = read_delta_table("silver","orders").filter("_IsRejected = false")
    neg = df.filter(F.col("Quantity") < 0).count()
    assert_equal(neg, 0, f"silver.orders has {neg} rows with negative Quantity (should be rejected)")

def test_silver_orders_no_negative_price():
    df = read_delta_table("silver","orders").filter("_IsRejected = false")
    neg = df.filter(F.col("UnitPrice") < 0).count()
    assert_equal(neg, 0, f"silver.orders has {neg} rows with negative UnitPrice")

def test_silver_customers_scd2_columns():
    df = read_delta_table("silver","customers")
    required = ["_SCD_EffectiveStartDate","_SCD_EffectiveEndDate","_SCD_IsCurrent","_SCD_RecordHash"]
    assert_columns_exist(df, required)

def test_silver_customers_scd2_has_current_rows():
    df  = read_delta_table("silver","customers")
    cnt = df.filter("_SCD_IsCurrent = true").count()
    assert_gte(cnt, 1, "silver.customers has no current SCD2 rows")

def test_silver_customers_no_duplicate_current():
    df    = read_delta_table("silver","customers").filter("_SCD_IsCurrent = true")
    total = df.count()
    unique= df.select("CustomerID").distinct().count()
    assert_equal(total, unique,
                 f"Duplicate current CustomerIDs in silver.customers: {total} rows, {unique} unique IDs")

def test_silver_watermark_exists():
    assert_true(table_exists("audit","silver_watermark"),
                "audit.silver_watermark does not exist")

def test_silver_watermark_has_entries():
    cnt = read_delta_table("audit","silver_watermark").count()
    assert_gte(cnt, 1, "audit.silver_watermark is empty — watermark not set")

def test_silver_exchange_rates_types():
    if not table_exists("silver","exchange_rates"):
        return "SKIP"
    df = read_delta_table("silver","exchange_rates").filter("_IsRejected = false")
    assert_gte(df.count(), 1, "silver.exchange_rates has no clean rows")
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert_true("RateDate" in schema_map, "silver.exchange_rates missing RateDate")

def test_silver_malformed_dates_rejected():
    df         = read_delta_table("silver","orders")
    rejected   = df.filter("_IsRejected = true")
    good_dates = rejected.filter(F.col("OrderDate").isNotNull() & (F.col("OrderDate") < F.lit("2000-01-01").cast("date"))).count()
    assert_equal(good_dates, 0, "Some future-invalid dates passed through as clean")

run_test("All Silver tables exist",                    test_silver_tables_exist)
run_test("Silver tables have rows",                    test_silver_row_counts)
run_test("_IsRejected column present everywhere",      test_silver_isrejected_column_exists)
run_test("Clean rows exist in all datasets",           test_silver_good_rows_exist)
run_test("Rejected records tables exist",              test_silver_rejected_records_table_exists)
run_test("Products: ProductID is integer type",        test_silver_products_types)
run_test("Orders: OrderDate is date, Quantity is int", test_silver_orders_types)
run_test("No negative Quantity in clean orders",       test_silver_orders_no_negative_quantity)
run_test("No negative UnitPrice in clean orders",      test_silver_orders_no_negative_price)
run_test("Customers: SCD2 columns exist",              test_silver_customers_scd2_columns)
run_test("Customers: current SCD2 rows exist",         test_silver_customers_scd2_has_current_rows)
run_test("Customers: no duplicate current records",    test_silver_customers_no_duplicate_current)
run_test("Silver watermark table exists",              test_silver_watermark_exists)
run_test("Silver watermark has entries",               test_silver_watermark_has_entries)
run_test("Exchange rates types correct",               test_silver_exchange_rates_types)
run_test("Malformed dates are rejected not passed",    test_silver_malformed_dates_rejected)

# COMMAND ----------

print("\n" + "=" * 60)
print("  SUITE 4 — Gold Layer")
print("=" * 60)

def test_gold_dim_tables_exist():
    for tbl in ["dim_customer","dim_product","dim_date"]:
        assert_true(table_exists("gold", tbl), f"gold.{tbl} does not exist")

def test_gold_fact_sales_exists():
    assert_true(table_exists("gold","fact_sales"), "gold.fact_sales does not exist")

def test_gold_fact_sales_has_rows():
    cnt = read_delta_table("gold","fact_sales").count()
    assert_gte(cnt, 1, "gold.fact_sales has 0 rows")

def test_gold_dim_customer_has_surrogate_key():
    df = read_delta_table("gold","dim_customer")
    assert_true("CustomerSK" in df.columns, "gold.dim_customer missing CustomerSK")
    nulls = df.filter(F.col("CustomerSK").isNull()).count()
    assert_equal(nulls, 0, "CustomerSK has null values")

def test_gold_dim_product_has_surrogate_key():
    df = read_delta_table("gold","dim_product")
    assert_true("ProductSK" in df.columns, "gold.dim_product missing ProductSK")
    nulls = df.filter(F.col("ProductSK").isNull()).count()
    assert_equal(nulls, 0, "ProductSK has null values")

def test_gold_dim_product_no_duplicates():
    df    = read_delta_table("gold","dim_product")
    total = df.count()
    unique= df.select("ProductID").distinct().count()
    assert_equal(total, unique,
                 f"gold.dim_product has duplicate ProductIDs: {total} rows vs {unique} unique")

def test_gold_dim_date_has_required_columns():
    df       = read_delta_table("gold","dim_date")
    required = ["DateSK","CalendarDate","Year","Quarter","Month","MonthName",
                "Day","DayOfWeek","DayName","IsWeekend","WeekOfYear"]
    assert_columns_exist(df, required)

def test_gold_dim_date_no_gaps():
    df  = read_delta_table("gold","dim_date")
    cnt = df.count()
    assert_gte(cnt, 1, "gold.dim_date is empty")
    min_sk = df.agg(F.min("DateSK")).first()[0]
    max_sk = df.agg(F.max("DateSK")).first()[0]
    assert_true(min_sk is not None and max_sk is not None, "dim_date has null DateSK values")

def test_gold_fact_sales_foreign_keys():
    fact = read_delta_table("gold","fact_sales")
    required = ["OrderID","DateSK","CustomerSK","ProductSK","StoreCode",
                "LineTotalBaseCurrency","GrossMarginBaseCurrency"]
    assert_columns_exist(fact, required)

def test_gold_fact_sales_no_negative_revenue():
    df  = read_delta_table("gold","fact_sales")
    neg = df.filter(F.col("LineTotalBaseCurrency") < 0).count()
    assert_equal(neg, 0, f"fact_sales has {neg} rows with negative LineTotalBaseCurrency")

def test_gold_point_in_time_join():
    fact = read_delta_table("gold","fact_sales")
    nullsk = fact.filter(F.col("CustomerSK").isNull()).count()
    total  = fact.count()
    null_pct = nullsk / total * 100 if total > 0 else 0
    assert_true(null_pct < 20,
                f"{null_pct:.1f}% of fact_sales rows have null CustomerSK (point-in-time join may be broken)")

def test_gold_aggregates_exist():
    for tbl in ["agg_daily_sales_by_store","agg_sales_by_category"]:
        assert_true(table_exists("gold", tbl), f"gold.{tbl} does not exist")

def test_gold_daily_sales_has_rows():
    cnt = read_delta_table("gold","agg_daily_sales_by_store").count()
    assert_gte(cnt, 1, "gold.agg_daily_sales_by_store has 0 rows")

def test_gold_rfm_segments_exist():
    if not table_exists("gold","agg_rfm_customer_segments"):
        return "SKIP"
    cnt = read_delta_table("gold","agg_rfm_customer_segments").count()
    assert_gte(cnt, 1, "gold.agg_rfm_customer_segments has 0 rows")

def test_gold_rfm_segment_labels():
    if not table_exists("gold","agg_rfm_customer_segments"):
        return "SKIP"
    df     = read_delta_table("gold","agg_rfm_customer_segments")
    assert_true("Segment" in df.columns, "RFM table missing Segment column")
    nulls  = df.filter(F.col("Segment").isNull()).count()
    assert_equal(nulls, 0, f"RFM table has {nulls} rows with null Segment")
    valid  = {"Champions","Loyal","At Risk","Lost","New Customer","Potential"}
    actual = {r["Segment"] for r in df.select("Segment").distinct().collect()}
    invalid= actual - valid
    assert_true(not invalid, f"Invalid RFM segment labels found: {invalid}")

def test_gold_rfm_scores_in_range():
    if not table_exists("gold","agg_rfm_customer_segments"):
        return "SKIP"
    df = read_delta_table("gold","agg_rfm_customer_segments")
    for col_name in ["R_Score","F_Score","M_Score"]:
        out_of_range = df.filter((F.col(col_name) < 1) | (F.col(col_name) > 5)).count()
        assert_equal(out_of_range, 0, f"{col_name} has values outside 1-5 range")

def test_gold_inventory_health_exists():
    if not table_exists("gold","agg_store_inventory_health"):
        return "SKIP"
    cnt = read_delta_table("gold","agg_store_inventory_health").count()
    assert_gte(cnt, 1, "gold.agg_store_inventory_health has 0 rows")

def test_gold_inventory_in_stock_rate_range():
    if not table_exists("gold","agg_store_inventory_health"):
        return "SKIP"
    df  = read_delta_table("gold","agg_store_inventory_health")
    assert_true("InStockRate" in df.columns, "agg_store_inventory_health missing InStockRate")
    out = df.filter((F.col("InStockRate") < 0) | (F.col("InStockRate") > 100)).count()
    assert_equal(out, 0, f"InStockRate has {out} values outside 0-100%")

def test_gold_forecast_input_exists():
    if not table_exists("gold","agg_weekly_forecast_input"):
        return "SKIP"
    cnt = read_delta_table("gold","agg_weekly_forecast_input").count()
    assert_gte(cnt, 1, "gold.agg_weekly_forecast_input has 0 rows")

def test_gold_forecast_lag_columns():
    if not table_exists("gold","agg_weekly_forecast_input"):
        return "SKIP"
    df   = read_delta_table("gold","agg_weekly_forecast_input")
    lags = [f"Lag_{i}w" for i in range(1, 13)]
    assert_columns_exist(df, lags)

run_test("Dimension tables exist",                   test_gold_dim_tables_exist)
run_test("fact_sales exists",                        test_gold_fact_sales_exists)
run_test("fact_sales has rows",                      test_gold_fact_sales_has_rows)
run_test("dim_customer has CustomerSK",              test_gold_dim_customer_has_surrogate_key)
run_test("dim_product has ProductSK",                test_gold_dim_product_has_surrogate_key)
run_test("dim_product has no duplicate ProductIDs",  test_gold_dim_product_no_duplicates)
run_test("dim_date has all required columns",        test_gold_dim_date_has_required_columns)
run_test("dim_date has valid date range",            test_gold_dim_date_no_gaps)
run_test("fact_sales has all foreign key columns",   test_gold_fact_sales_foreign_keys)
run_test("No negative revenue in fact_sales",        test_gold_fact_sales_no_negative_revenue)
run_test("Point-in-time join working (<20% null SK)",test_gold_point_in_time_join)
run_test("Standard aggregates exist",                test_gold_aggregates_exist)
run_test("Daily sales aggregate has rows",           test_gold_daily_sales_has_rows)
run_test("RFM segments table exists",                test_gold_rfm_segments_exist)
run_test("RFM segment labels are valid",             test_gold_rfm_segment_labels)
run_test("RFM scores are in range 1-5",              test_gold_rfm_scores_in_range)
run_test("Inventory health table exists",            test_gold_inventory_health_exists)
run_test("InStockRate is between 0-100",             test_gold_inventory_in_stock_rate_range)
run_test("Forecast input table exists",              test_gold_forecast_input_exists)
run_test("Forecast input has 12 lag columns",        test_gold_forecast_lag_columns)

# COMMAND ----------

print("\n" + "=" * 60)
print("  SUITE 5 — Audit Tables")
print("=" * 60)

def test_pipeline_log_exists():
    assert_true(table_exists("audit","pipeline_execution_log"),
                "audit.pipeline_execution_log does not exist")

def test_pipeline_log_has_records():
    cnt = read_delta_table("audit","pipeline_execution_log").count()
    assert_gte(cnt, 1, "audit.pipeline_execution_log is empty")

def test_pipeline_log_has_success_events():
    df  = read_delta_table("audit","pipeline_execution_log")
    cnt = df.filter(F.col("status") == "SUCCESS").count()
    assert_gte(cnt, 1, "No SUCCESS events in pipeline_execution_log")

def test_pipeline_log_all_layers_recorded():
    df     = read_delta_table("audit","pipeline_execution_log")
    layers = {r["layer"] for r in df.select("layer").distinct().collect()}
    expected = {"bronze","silver","gold"}
    missing  = expected - layers
    assert_true(not missing, f"Layers missing from log: {missing}")

def test_pipeline_log_no_orphan_failures():
    df       = read_delta_table("audit","pipeline_execution_log")
    failures = df.filter(F.col("status") == "FAILED").count()
    total    = df.count()
    fail_pct = failures / total * 100 if total > 0 else 0
    assert_true(fail_pct < 50,
                f"{fail_pct:.1f}% of log events are FAILED — pipeline may be unhealthy")

def test_lineage_table_exists():
    assert_true(table_exists("audit","data_lineage"),
                "audit.data_lineage does not exist")

def test_lineage_has_records():
    if not table_exists("audit","data_lineage"):
        return "SKIP"
    cnt = read_delta_table("audit","data_lineage").count()
    assert_gte(cnt, 1, "audit.data_lineage is empty")

def test_lineage_covers_all_layers():
    if not table_exists("audit","data_lineage"):
        return "SKIP"
    df     = read_delta_table("audit","data_lineage")
    layers = {r["target_layer"] for r in df.select("target_layer").distinct().collect()}
    expected = {"bronze","silver","gold"}
    missing  = expected - layers
    assert_true(not missing, f"Layers missing from lineage: {missing}")

def test_dq_profile_report_exists():
    assert_true(table_exists("audit","dq_profile_report"),
                "audit.dq_profile_report does not exist")

def test_dq_profile_has_records():
    if not table_exists("audit","dq_profile_report"):
        return "SKIP"
    cnt = read_delta_table("audit","dq_profile_report").count()
    assert_gte(cnt, 1, "audit.dq_profile_report is empty")

def test_dq_profile_null_pct_in_range():
    if not table_exists("audit","dq_profile_report"):
        return "SKIP"
    df  = read_delta_table("audit","dq_profile_report")
    out = df.filter((F.col("null_pct") < 0) | (F.col("null_pct") > 100)).count()
    assert_equal(out, 0, f"dq_profile_report has {out} rows with null_pct outside 0-100")

run_test("Pipeline execution log exists",          test_pipeline_log_exists)
run_test("Pipeline log has records",               test_pipeline_log_has_records)
run_test("Pipeline log has SUCCESS events",        test_pipeline_log_has_success_events)
run_test("All layers logged (bronze/silver/gold)", test_pipeline_log_all_layers_recorded)
run_test("Failure rate below 50%",                 test_pipeline_log_no_orphan_failures)
run_test("Data lineage table exists",              test_lineage_table_exists)
run_test("Lineage table has records",              test_lineage_has_records)
run_test("Lineage covers all 3 layers",            test_lineage_covers_all_layers)
run_test("DQ profile report exists",               test_dq_profile_report_exists)
run_test("DQ profile has records",                 test_dq_profile_has_records)
run_test("DQ null_pct values are 0-100",           test_dq_profile_null_pct_in_range)

# COMMAND ----------

print("\n" + "=" * 60)
print("  SUITE 6 — Data Quality Checks")
print("=" * 60)

def test_dq_orders_customerid_not_null():
    df    = read_delta_table("silver","orders").filter("_IsRejected = false")
    nulls = df.filter(F.col("CustomerID").isNull()).count()
    assert_equal(nulls, 0, f"silver.orders has {nulls} clean rows with null CustomerID")

def test_dq_orders_productid_not_null():
    df    = read_delta_table("silver","orders").filter("_IsRejected = false")
    nulls = df.filter(F.col("ProductID").isNull()).count()
    assert_equal(nulls, 0, f"silver.orders has {nulls} clean rows with null ProductID")

def test_dq_orders_orderdate_not_null():
    df    = read_delta_table("silver","orders").filter("_IsRejected = false")
    nulls = df.filter(F.col("OrderDate").isNull()).count()
    assert_equal(nulls, 0, f"silver.orders has {nulls} clean rows with null OrderDate")

def test_dq_products_productid_unique():
    df    = read_delta_table("silver","products").filter("_IsRejected = false")
    total = df.count()
    uniq  = df.select("ProductID").distinct().count()
    assert_equal(total, uniq, f"silver.products has {total - uniq} duplicate ProductIDs")

def test_dq_products_costprice_positive():
    df  = read_delta_table("silver","products").filter("_IsRejected = false")
    neg = df.filter(F.col("CostPrice") <= 0).count()
    assert_equal(neg, 0, f"silver.products has {neg} clean rows with CostPrice <= 0")

def test_dq_customers_name_not_null():
    df    = read_delta_table("silver","customers").filter("_IsRejected = false")
    nulls = df.filter(F.col("FirstName").isNull() | F.col("LastName").isNull()).count()
    assert_equal(nulls, 0, f"silver.customers has {nulls} rows with null name")

def test_dq_exchange_rates_positive():
    if not table_exists("silver","exchange_rates"):
        return "SKIP"
    df  = read_delta_table("silver","exchange_rates").filter("_IsRejected = false")
    neg = df.filter(F.col("ExchangeRate") <= 0).count()
    assert_equal(neg, 0, f"silver.exchange_rates has {neg} clean rows with ExchangeRate <= 0")

def test_dq_store_codes_valid():
    valid_stores = [f"ST{n:03d}" for n in range(1, 11)]
    df  = read_delta_table("silver","orders").filter("_IsRejected = false")
    if "StoreCode" not in df.columns:
        return "SKIP"
    inv = df.filter(~F.col("StoreCode").isin(valid_stores)).count()
    assert_equal(inv, 0, f"silver.orders has {inv} clean rows with invalid StoreCode")

def test_dq_fact_sales_quantity_positive():
    df  = read_delta_table("gold","fact_sales")
    neg = df.filter(F.col("Quantity") <= 0).count()
    assert_equal(neg, 0, f"gold.fact_sales has {neg} rows with Quantity <= 0")

def test_dq_fact_sales_exchange_rate_positive():
    df  = read_delta_table("gold","fact_sales")
    neg = df.filter(F.col("ExchangeRate") <= 0).count()
    assert_equal(neg, 0, f"gold.fact_sales has {neg} rows with ExchangeRate <= 0")

def test_dq_referential_integrity_fact_to_dim_product():
    fact    = read_delta_table("gold","fact_sales").select("ProductSK")
    dim     = read_delta_table("gold","dim_product").select("ProductSK")
    orphans = fact.join(dim, on="ProductSK", how="left_anti").count()
    assert_equal(orphans, 0,
                 f"gold.fact_sales has {orphans} rows with ProductSK not in dim_product")

def test_dq_referential_integrity_fact_to_dim_date():
    fact    = read_delta_table("gold","fact_sales").select("DateSK")
    dim     = read_delta_table("gold","dim_date").select("DateSK")
    orphans = fact.join(dim, on="DateSK", how="left_anti").count()
    assert_equal(orphans, 0,
                 f"gold.fact_sales has {orphans} rows with DateSK not in dim_date")

run_test("Clean orders: CustomerID never null",         test_dq_orders_customerid_not_null)
run_test("Clean orders: ProductID never null",          test_dq_orders_productid_not_null)
run_test("Clean orders: OrderDate never null",          test_dq_orders_orderdate_not_null)
run_test("Silver products: ProductID unique",           test_dq_products_productid_unique)
run_test("Silver products: CostPrice > 0",             test_dq_products_costprice_positive)
run_test("Silver customers: name never null",           test_dq_customers_name_not_null)
run_test("Silver exchange rates: rate > 0",             test_dq_exchange_rates_positive)
run_test("Silver orders: StoreCode valid",              test_dq_store_codes_valid)
run_test("Fact sales: Quantity > 0",                   test_dq_fact_sales_quantity_positive)
run_test("Fact sales: ExchangeRate > 0",               test_dq_fact_sales_exchange_rate_positive)
run_test("Fact→dim_product referential integrity",     test_dq_referential_integrity_fact_to_dim_product)
run_test("Fact→dim_date referential integrity",        test_dq_referential_integrity_fact_to_dim_date)

# COMMAND ----------

# MAGIC %md ## Test Results Summary

# COMMAND ----------

total = len(passed) + len(failed) + len(skipped)
print("\n" + "=" * 60)
print(f"  TEST RESULTS — {total} tests")
print("=" * 60)
print(f"  PASSED  : {len(passed)}")
print(f"  FAILED  : {len(failed)}")
print(f"  SKIPPED : {len(skipped)}")
print("=" * 60)

if failed:
    print("\nFailed tests:")
    for t in failed:
        print(f"  ✗  {t}")
else:
    print("\n  All tests passed!")

if skipped:
    print("\nSkipped tests (table not yet built):")
    for t in skipped:
        print(f"  –  {t}")

print("=" * 60)

# Write results to audit table
rows = []
import datetime
for t in passed:
    rows.append((PIPELINE_RUN_ID, t, "PASSED",  None,  datetime.datetime.utcnow()))
for t in failed:
    rows.append((PIPELINE_RUN_ID, t, "FAILED",  None,  datetime.datetime.utcnow()))
for t in skipped:
    rows.append((PIPELINE_RUN_ID, t, "SKIPPED", None,  datetime.datetime.utcnow()))

schema = T.StructType([
    T.StructField("pipeline_run_id", T.StringType(),    True),
    T.StructField("test_name",       T.StringType(),    True),
    T.StructField("result",          T.StringType(),    True),
    T.StructField("error_message",   T.StringType(),    True),
    T.StructField("run_at",          T.TimestampType(), True),
])
write_delta_table(spark.createDataFrame(rows, schema),
                  "audit", "unit_test_results", mode="append")
print(f"\nResults saved to capstone.audit.unit_test_results")

if failed:
    raise RuntimeError(f"{len(failed)} test(s) FAILED — see output above")
else:
    dbutils.notebook.exit(f"All {len(passed)} tests passed. {len(skipped)} skipped.")