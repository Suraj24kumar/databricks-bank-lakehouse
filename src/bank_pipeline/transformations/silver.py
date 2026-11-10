"""Silver: typed + validated + de-duplicated, with AUTO CDC doing the incremental apply.

  silver_customers     SCD Type 2  (history: __START_AT / __END_AT; delete closes the current version)
  silver_accounts      SCD Type 1  (latest wins, no history)
  silver_transactions  SCD Type 1 on txn_id -> acts as the de-duplicator for replayed deliveries
  quarantine_*         rows that broke a rule, with the names of the broken rules
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from bank_lakehouse.rules import (
    ACCOUNT_RULES, CUSTOMER_RULES, CUSTOMER_TRACKED, TRANSACTION_RULES, quarantine,
)

LINEAGE = ["_rescued_data", "_source_file", "_file_modified_at", "_ingested_at"]


def _typed_customers(df):
    return df.withColumn("updated_at", F.try_to_timestamp("updated_at"))


def _typed_accounts(df):
    return df.withColumn("updated_at", F.try_to_timestamp("updated_at"))


def _typed_transactions(df):
    return (
        df.withColumn("txn_ts", F.try_to_timestamp("txn_ts"))
        .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(18,2))"))
    )


def _conformed_view(feed: str, rules: dict, typed):
    """Temp view = bronze + typing + `drop` expectations (bad rows never reach the AUTO CDC target)."""

    def conformed():
        return typed(spark.readStream.table(f"bronze_{feed}"))

    view = dp.expect_all_or_drop(rules)(conformed)
    dp.temporary_view(name=f"v_{feed}_conformed")(view)

    @dp.table(name=f"quarantine_{feed}", comment=f"{feed} rows rejected by a rule, with `_failed_rules`.", table_properties={"quality": "quarantine"})
    def quarantined():
        # rules run on the RAW strings (so "try_cast(amount...)" etc. see the original text)
        return quarantine(spark.readStream.table(f"bronze_{feed}"), rules)


# ---- customers: SCD2 --------------------------------------------------------------------------------
_conformed_view("customers", CUSTOMER_RULES, _typed_customers)
dp.create_streaming_table(name="silver_customers", comment="Customer dimension, SCD2: one row per version.", table_properties={"quality": "silver"})
dp.create_auto_cdc_flow(
    target="silver_customers",
    source="v_customers_conformed",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", *LINEAGE],
    stored_as_scd_type=2,
    track_history_column_list=CUSTOMER_TRACKED,  # email is untracked -> in-place update, no new version
    name="auto_cdc_customers",
)

# ---- accounts: SCD1 ---------------------------------------------------------------------------------
_conformed_view("accounts", ACCOUNT_RULES, _typed_accounts)
dp.create_streaming_table(name="silver_accounts", comment="Accounts, SCD1: current state only.", table_properties={"quality": "silver"})
dp.create_auto_cdc_flow(
    target="silver_accounts",
    source="v_accounts_conformed",
    keys=["account_id"],
    sequence_by=F.col("updated_at"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", *LINEAGE],
    stored_as_scd_type=1,
    name="auto_cdc_accounts",
)

# ---- transactions: append-only facts, deduplicated by key ------------------------------------------
_conformed_view("transactions", TRANSACTION_RULES, _typed_transactions)
dp.create_streaming_table(name="silver_transactions", comment="Valid, de-duplicated transactions.", table_properties={"quality": "silver"})
dp.create_auto_cdc_flow(
    target="silver_transactions",
    source="v_transactions_conformed",
    keys=["txn_id"],
    # A replay arrives in a LATER file than the original, so (file time, path) is a unique, increasing tiebreaker.
    sequence_by=F.struct(F.col("_file_modified_at"), F.col("_source_file")),
    except_column_list=LINEAGE,
    stored_as_scd_type=1,
    name="auto_cdc_transactions",
)
