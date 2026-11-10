"""Gold: materialized views. The key piece is the POINT-IN-TIME join of facts to the SCD2 customer history."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_customer", comment="SCD2 customer dimension with an explicit Y/N current flag.", table_properties={"quality": "gold"})
def dim_customer():
    c = spark.read.table("silver_customers")
    return (
        c.withColumn("valid_from", F.col("__START_AT"))
        .withColumn("valid_to", F.coalesce(F.col("__END_AT"), F.lit("9999-12-31 00:00:00").cast("timestamp")))
        .withColumn("is_current", F.when(F.col("__END_AT").isNull(), F.lit("Y")).otherwise(F.lit("N")))
    )


@dp.materialized_view(
    name="fct_transactions",
    comment="Each transaction tagged with the customer attributes that were valid AT the transaction time.",
    table_properties={"quality": "gold"},
    cluster_by=["txn_date", "segment"],
)
@dp.expect("customer_version_found", "customer_sk_start IS NOT NULL")
def fct_transactions():
    t = spark.read.table("silver_transactions").alias("t")
    c = spark.read.table("silver_customers").alias("c")
    cond = (
        (F.col("t.customer_id") == F.col("c.customer_id"))
        & (F.col("t.txn_ts") >= F.col("c.__START_AT"))
        & (F.col("c.__END_AT").isNull() | (F.col("t.txn_ts") < F.col("c.__END_AT")))
    )
    return (
        t.join(c, cond, "left")
        .select(
            "t.txn_id", "t.account_id", "t.customer_id", "t.txn_ts", F.to_date("t.txn_ts").alias("txn_date"),
            "t.amount", "t.channel", "t.txn_type", "t.txn_city",
            F.col("c.city").alias("city"), F.col("c.segment").alias("segment"),
            (F.col("t.txn_city") != F.col("c.city")).alias("is_away"),
            F.col("c.kyc_status").alias("kyc_status"), F.col("c.__START_AT").alias("customer_sk_start"),
        )
    )


@dp.materialized_view(name="gold_daily_by_segment", comment="Daily transaction KPIs per customer segment (as of transaction time).", table_properties={"quality": "gold"})
def gold_daily_by_segment():
    f = spark.read.table("fct_transactions")
    return f.groupBy("txn_date", "segment").agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("active_customers"),
    )


@dp.materialized_view(name="gold_data_quality", comment="Rows loaded vs rejected per feed.", table_properties={"quality": "gold"})
def gold_data_quality():
    def one(feed):
        b = spark.read.table(f"bronze_{feed}").agg(F.count("*").alias("bronze_rows"))
        q = spark.read.table(f"quarantine_{feed}").agg(F.count("*").alias("quarantined_rows"))
        return b.crossJoin(q).select(F.lit(feed).alias("feed"), "bronze_rows", "quarantined_rows")

    df = one("customers").unionByName(one("accounts")).unionByName(one("transactions"))
    return df.withColumn("quarantine_rate", F.round(F.col("quarantined_rows") / F.col("bronze_rows"), 4))
