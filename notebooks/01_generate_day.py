# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Land one business day of data
# MAGIC Writes `customers`, `accounts` and `transactions` CSVs for day N into the landing volume (deterministic: rerunning a day writes identical files).
# MAGIC Day 0 = full initial load. Day 1, 2, ... = incremental changes (new/changed/deleted customers, late + replayed + dirty transactions).

# COMMAND ----------

import json, os, sys

# make `src/bank_lakehouse` importable when this notebook runs from a Git folder / bundle deployment
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from bank_lakehouse.generator import generate_day

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bank_lakehouse")
dbutils.widgets.text("day", "0")  # integer day offset

catalog, schema, day = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), int(dbutils.widgets.get("day"))
root = f"/Volumes/{catalog}/{schema}/landing"

manifest = generate_day(day, root)
print(json.dumps(manifest, indent=2))  # planted-fault counts: use them to check the quarantine tables later

# COMMAND ----------

# Persist the manifest so the DQ gate can compare quarantine counts with what was planted.
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ops_manifest (day INT, manifest STRING)")
spark.sql(f"DELETE FROM {catalog}.{schema}.ops_manifest WHERE day = {day}")  # idempotent
spark.createDataFrame([(day, json.dumps(manifest))], "day INT, manifest STRING").write.mode("append").saveAsTable(f"{catalog}.{schema}.ops_manifest")
