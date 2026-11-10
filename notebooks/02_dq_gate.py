# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Data-quality gate (fails the job if an invariant is broken)
# MAGIC Checks the SCD2 invariants, de-duplication and the quarantine reconciliation. Run after the pipeline refresh.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bank_lakehouse")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

CHECKS = {
    # SCD2: exactly one open (current) version per customer
    "one_current_version_per_customer": """
        SELECT customer_id FROM silver_customers WHERE __END_AT IS NULL GROUP BY customer_id HAVING COUNT(*) > 1""",
    # SCD2: a version never starts after it ends
    "no_negative_validity_interval": """
        SELECT customer_id FROM silver_customers WHERE __END_AT IS NOT NULL AND __START_AT >= __END_AT""",
    # SCD2: versions of one customer must not overlap
    "no_overlapping_versions": """
        SELECT a.customer_id FROM silver_customers a JOIN silver_customers b
          ON a.customer_id = b.customer_id AND a.__START_AT < b.__START_AT
         AND (a.__END_AT IS NULL OR a.__END_AT > b.__START_AT)""",
    # facts: transaction ids are unique after dedupe
    "unique_transaction_ids": "SELECT txn_id FROM silver_transactions GROUP BY txn_id HAVING COUNT(*) > 1",
    # quarantine rows always carry a reason
    "quarantine_has_reason": "SELECT * FROM quarantine_transactions WHERE _failed_rules IS NULL OR size(_failed_rules) = 0",
    # point-in-time join: no transaction should be left without a customer version
    "every_fact_has_customer_version": "SELECT txn_id FROM fct_transactions WHERE customer_sk_start IS NULL",
}

failures = {}
for name, sql in CHECKS.items():
    n = spark.sql(sql).count()
    print(f"{'PASS' if n == 0 else 'FAIL'}  {name}  (violations: {n})")
    if n:
        failures[name] = n

# Reconciliation: every bronze transaction is exactly one of  valid | quarantined | duplicate (a replay of a valid one).
bronze = spark.table("bronze_transactions").count()
quarantined = spark.table("quarantine_transactions").count()
valid_distinct = spark.table("silver_transactions").count()
valid_bronze_rows = bronze - quarantined
duplicates_dropped = valid_bronze_rows - valid_distinct
print(f"bronze={bronze} quarantined={quarantined} silver(distinct valid)={valid_distinct} duplicates_dropped={duplicates_dropped}")
if duplicates_dropped < 0:
    failures["reconciliation_negative_duplicates"] = duplicates_dropped

# COMMAND ----------

if failures:
    raise Exception(f"Data-quality gate FAILED: {failures}")
print("All data-quality checks passed.")
