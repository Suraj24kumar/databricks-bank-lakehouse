# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · The same SCD2, written by hand with `MERGE INTO` (interview deep-dive)
# MAGIC The pipeline uses declarative `AUTO CDC`. Interviewers love asking "how would you do SCD2 manually?".
# MAGIC This notebook builds `dim_customer_manual` from `silver_customers`' source feed using the classic technique:
# MAGIC *hash-diff change detection* + *staged MERGE* (a NULL merge key forces the INSERT of the new version) and an explicit `is_current` Y/N flag.
# MAGIC It is a learning notebook: it reads the **bronze** customers feed (all days landed so far), keeps the **latest row per customer per update time**, and replays changes in order.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bank_lakehouse")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
TARGET = f"{catalog}.{schema}.dim_customer_manual"
TRACKED = ["city", "segment", "kyc_status", "risk_band"]

src = (
    spark.table(f"{catalog}.{schema}.bronze_customers")
    .withColumn("updated_at", F.try_to_timestamp("updated_at"))
    .where("customer_id IS NOT NULL AND updated_at IS NOT NULL AND op = 'UPSERT'")
    .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in TRACKED]), 256))
)

# COMMAND ----------

# Replay one *update time* at a time, oldest first, so history is built in the right order (idempotent: full rebuild).
spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
spark.sql(f"""
CREATE TABLE {TARGET} (
  customer_id STRING, name STRING, email STRING, city STRING, segment STRING, kyc_status STRING, risk_band STRING,
  row_hash STRING, effective_from TIMESTAMP, effective_to TIMESTAMP, is_current STRING)""")

target = DeltaTable.forName(spark, TARGET)
batches = [r[0] for r in src.select("updated_at").distinct().orderBy("updated_at").collect()]  # fine for demo data volumes

for ts in batches:
    # one row per customer for this timestamp (dedupe)
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    batch = src.where(F.col("updated_at") == F.lit(ts)).withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

    # 1) keep only rows that are NEW customers or whose tracked attributes CHANGED
    cur = spark.table(TARGET).where("is_current = 'Y'").select("customer_id", F.col("row_hash").alias("cur_hash"))
    changed = batch.join(cur, "customer_id", "left").where("cur_hash IS NULL OR cur_hash <> row_hash")

    # 2) staged source: copy A closes the old version (real key), copy B inserts the new one (NULL key never matches)
    staged = changed.withColumn("merge_key", F.col("customer_id")).unionByName(changed.withColumn("merge_key", F.lit(None).cast("string")))

    (target.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = 'Y'")
        .whenMatchedUpdate(set={"is_current": F.lit("N"), "effective_to": F.col("s.updated_at")})
        .whenNotMatchedInsert(
            condition="s.merge_key IS NULL",
            values={"customer_id": "s.customer_id", "name": "s.name", "email": "s.email", "city": "s.city", "segment": "s.segment",
                    "kyc_status": "s.kyc_status", "risk_band": "s.risk_band", "row_hash": "s.row_hash",
                    "effective_from": "s.updated_at", "effective_to": "CAST(NULL AS TIMESTAMP)", "is_current": "'Y'"})
        .execute())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers with history: old version N, current version Y
# MAGIC SELECT customer_id, city, segment, kyc_status, risk_band, effective_from, effective_to, is_current
# MAGIC FROM workspace.bank_lakehouse.dim_customer_manual
# MAGIC WHERE customer_id IN (SELECT customer_id FROM workspace.bank_lakehouse.dim_customer_manual GROUP BY customer_id HAVING COUNT(*) > 1)
# MAGIC ORDER BY customer_id, effective_from LIMIT 30
