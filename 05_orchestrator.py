# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline Orchestrator *(Enhanced v2.0)*

# COMMAND ----------

dbutils.widgets.dropdown(
    "orchestration_mode", "daily_incremental",
    ["initial_seed","daily_incremental","bronze_to_gold_only","dq_report_only"],
    "Orchestration mode"
)
dbutils.widgets.text("run_date",          "",             "Run date (YYYY-MM-DD, blank=today)")
dbutils.widgets.dropdown("environment",   "free_edition", ["free_edition","community","azure"], "Environment")
dbutils.widgets.text("base_data_path",    "",             "Base data path (blank=auto)")
dbutils.widgets.text("catalog_name",      "capstone",     "Unity Catalog catalog name")
dbutils.widgets.text("pipeline_run_id",   "",             "ADF Pipeline Run Id (blank=auto)")
dbutils.widgets.text("timeout_seconds",   "3600",         "Per-notebook timeout (seconds)")
dbutils.widgets.text("dirty_data_ratio",  "0.06",         "Dirty-data injection ratio")
dbutils.widgets.text("alert_email",       "",             "Alert email (blank=disabled)")
dbutils.widgets.text("teams_webhook_url", "",             "MS Teams webhook URL (blank=disabled)")
dbutils.widgets.text("max_retries",       "1",            "Max retries per step on failure")

# COMMAND ----------

import uuid, time
from datetime import datetime

ORCHESTRATION_MODE = dbutils.widgets.get("orchestration_mode").strip()
RUN_DATE           = dbutils.widgets.get("run_date").strip() or datetime.utcnow().strftime("%Y-%m-%d")
ENVIRONMENT        = dbutils.widgets.get("environment").strip()
BASE_DATA_PATH     = dbutils.widgets.get("base_data_path").strip()
CATALOG_NAME       = dbutils.widgets.get("catalog_name").strip() or "capstone"
PIPELINE_RUN_ID    = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
TIMEOUT            = int(dbutils.widgets.get("timeout_seconds").strip() or "3600")
DIRTY_RATIO        = dbutils.widgets.get("dirty_data_ratio").strip() or "0.06"
ALERT_EMAIL        = dbutils.widgets.get("alert_email").strip()
TEAMS_WEBHOOK      = dbutils.widgets.get("teams_webhook_url").strip()
MAX_RETRIES        = int(dbutils.widgets.get("max_retries").strip() or "1")

# ── Auto-detect notebook folder from this notebook's path ──────────────────
# This makes the relative paths work regardless of what the user named the folder.
ctx            = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
THIS_PATH      = ctx.notebookPath().get()                    # e.g. /Users/x@y.com/capstone/05_orchestrator
NOTEBOOK_DIR   = "/".join(THIS_PATH.split("/")[:-1])         # e.g. /Users/x@y.com/capstone
print(f"Notebook folder detected: {NOTEBOOK_DIR}")

SHARED_PARAMS = {
    "environment":        ENVIRONMENT,
    "base_data_path":     BASE_DATA_PATH,
    "catalog_name":       CATALOG_NAME,
    "pipeline_run_id":    PIPELINE_RUN_ID,
    "alert_email":        ALERT_EMAIL,
    "teams_webhook_url":  TEAMS_WEBHOOK,
}

print(f"=== Orchestrator v2.0 ===")
print(f"mode         : {ORCHESTRATION_MODE}")
print(f"run_date     : {RUN_DATE}")
print(f"environment  : {ENVIRONMENT}")
print(f"pipeline_run : {PIPELINE_RUN_ID}")
print(f"timeout/nb   : {TIMEOUT}s  |  max_retries: {MAX_RETRIES}")

# COMMAND ----------

# MAGIC %md ## Step Runner with Auto-Path Detection and Retry

# COMMAND ----------

def run_step(notebook_name, extra_params=None, label=None, max_retries=None):
    """
    Runs a sibling notebook by name only — the folder is auto-detected.
    Retries up to max_retries times with 30s back-off.
    """
    if max_retries is None:
        max_retries = MAX_RETRIES
    label         = label or notebook_name
    notebook_path = f"{NOTEBOOK_DIR}/{notebook_name}"
    params        = {**SHARED_PARAMS, **(extra_params or {})}
    last_error    = None

    for attempt in range(1, max_retries + 2):
        start = datetime.utcnow()
        print(f"\n→ [{label}] attempt {attempt}/{max_retries+1} at {start.strftime('%H:%M:%S')} UTC")
        print(f"  path: {notebook_path}")
        try:
            result  = dbutils.notebook.run(notebook_path, TIMEOUT, params)
            elapsed = (datetime.utcnow() - start).total_seconds()
            print(f"✓ [{label}] completed in {elapsed:.1f}s  result={result!r}")
            return result or "OK"
        except Exception as e:
            elapsed    = (datetime.utcnow() - start).total_seconds()
            last_error = e
            print(f"✗ [{label}] FAILED after {elapsed:.1f}s: {e}")
            if attempt <= max_retries:
                wait = 30 * attempt
                print(f"  retrying in {wait}s …")
                time.sleep(wait)

    raise RuntimeError(
        f"[{label}] failed after {max_retries+1} attempt(s): {last_error}"
    ) from last_error

# COMMAND ----------

# MAGIC %md ## Step 1 — Data Generator

# COMMAND ----------

if ORCHESTRATION_MODE in ("initial_seed", "daily_incremental"):
    gen_mode = "initial_seed" if ORCHESTRATION_MODE == "initial_seed" else "daily_incremental"
    run_step("01_data_generator",
             extra_params={
                 "generation_mode":   gen_mode,
                 "run_date":          RUN_DATE,
                 "dirty_data_ratio":  DIRTY_RATIO,
                 "num_customers_seed":"500",
                 "num_products":      "150",
                 "num_orders":        "400",
                 "refresh_products":  "no",
             },
             label="01_data_generator")
else:
    print(f"\n→ [01_data_generator] skipped (mode={ORCHESTRATION_MODE})")

# COMMAND ----------

# MAGIC %md ## Step 2 — Bronze

# COMMAND ----------

run_step("02_bronze_layer",
         extra_params={"dataset_filter": "ALL"},
         label="02_bronze_layer")

# COMMAND ----------

# MAGIC %md ## Step 3 — Silver

# COMMAND ----------

run_step("03_silver_layer",
         extra_params={"dataset_filter": "ALL"},
         label="03_silver_layer")

# COMMAND ----------

# MAGIC %md ## Step 4 — Gold

# COMMAND ----------

if ORCHESTRATION_MODE != "dq_report_only":
    run_step("04_gold_layer",
             extra_params={"gold_object_filter": "ALL"},
             label="04_gold_layer")
else:
    print("\n→ [04_gold_layer] skipped (mode=dq_report_only)")

# COMMAND ----------

# MAGIC %md ## Done

# COMMAND ----------

summary = (
    f"Pipeline v2.0 complete | run_date={RUN_DATE} | mode={ORCHESTRATION_MODE} "
    f"| pipeline_run_id={PIPELINE_RUN_ID}"
)
print(f"\n=== {summary} ===")
dbutils.notebook.exit(summary)