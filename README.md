# 🏪 Enterprise Retail Analytics Platform on Azure

> End-to-end Azure Data Engineering Capstone Project implementing the Bronze → Silver → Gold Medallion Architecture on Azure Databricks + Delta Lake

---

## 📌 Project Overview

This platform ingests daily retail transaction data from multiple source systems (CSV, TSV, JSON, REST API), transforms it through a three-layer Medallion architecture, and delivers clean analytics-ready Gold tables for Power BI reporting.

**Result:** 77/78 unit tests passed | Full pipeline validated end-to-end

---

## 🏗️ Architecture
SOURCE SYSTEMS (CSV · TSV · JSON · REST API)
↓
RAW LANDING ZONE (ADLS Gen2 / Unity Catalog Volume)
↓
BRONZE LAYER — Raw ingestion · Idempotent · Audit logged
↓
SILVER LAYER — Cleaned · Typed · SCD2 · DQ Validated
↓
GOLD LAYER — Star Schema · RFM · Inventory · Forecast
↓
POWER BI — Dashboards · Scheduled Refresh
---

## 📁 Files

| File | Purpose |
|---|---|
| `00_config_utils.py` | Shared config, dataset registry, helpers, alerting, lineage |
| `01_data_generator.py` | Synthetic CSV/TSV/JSON data with dirty data injection |
| `02_bronze_layer.py` | Raw ingestion into Bronze Delta tables (idempotent) |
| `03_silver_layer.py` | Cleaning, SCD2, DQ profiling, watermark-based incremental load |
| `04_gold_layer.py` | Star schema + RFM segments + Inventory health + Forecast input |
| `05_orchestrator.py` | Full pipeline orchestrator with retry logic and auto path detection |
| `test_pipeline.py` | 78 automated unit tests across all layers |
| `databricks_workflow.json` | Databricks daily scheduled workflow (import directly) |

---

## ✨ Key Enhancements (v2.0)

- ✅ Multi-format ingestion — CSV, TSV, JSON, REST API in one code path
- ✅ DQ Profiling Report — column-level null%, range, regex violations
- ✅ MS Teams + Email alerting on pipeline failure
- ✅ Data Lineage tracking — source → target per every write
- ✅ RFM Customer Segmentation — Champions / Loyal / At Risk / Lost
- ✅ Store Inventory Health — in-stock rate, stock-outs, reorder alerts
- ✅ 13-week Forecast Input — 12 lag features ready for Azure ML
- ✅ Delta ZORDER optimisation after every write
- ✅ Step retry logic with exponential back-off
- ✅ 78 unit tests — 77 passed, 1 skipped (98.7% pass rate)

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Compute | Azure Databricks (Free Edition / Premium) |
| Storage | ADLS Gen2 / Unity Catalog Volumes |
| Table Format | Delta Lake |
| Orchestration | Databricks Workflows + Azure Data Factory |
| Language | PySpark 3.5 + Python 3.9 |
| Visualization | Power BI Desktop + Service |

---

## 🚀 How to Run

1. Import all `.py` files into a Databricks workspace folder called `capstone`
2. Run this in a notebook to create schemas:

```python
spark.sql("CREATE CATALOG IF NOT EXISTS capstone")
for schema in ["audit","bronze","silver","gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS capstone.{schema}")
```

3. Open `05_orchestrator` → set `orchestration_mode = initial_seed` → **Run All**
4. For daily runs: set `orchestration_mode = daily_incremental`

---

## 📊 Test Results

| Suite | Tests | Result |
|---|---|---|
| Config & Registry | 8 | ✅ All passed |
| Bronze Layer | 11 | ✅ All passed |
| Silver Layer | 16 | ✅ All passed |
| Gold Layer | 20 | ✅ All passed |
| Audit Tables | 11 | ✅ All passed |
| Data Quality | 12 | ✅ All passed |
| **Total** | **78** | **77 passed · 1 skipped** |

---

## 👩‍💻 Author

**Harini T**
Azure Data Engineering Capstone — June 2026
