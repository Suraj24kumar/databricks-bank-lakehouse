# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (run once)
# MAGIC Creates the schema and the managed landing volume. On Databricks Free Edition the catalog is `workspace`.
# MAGIC Skip this notebook if you deploy with the bundle (`databricks bundle deploy`), which creates them for you.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bank_lakehouse")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
print(f"Landing path: /Volumes/{catalog}/{schema}/landing")
