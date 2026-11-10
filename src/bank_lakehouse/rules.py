"""Schemas and data-quality rules shared by the Lakeflow pipeline, the DQ gate and the tests."""

from __future__ import annotations

# Everything lands as STRING in bronze: a malformed value is quarantined later with a reason instead of silently becoming NULL.
RAW_COLUMNS = {
    "customers": ["customer_id", "name", "email", "city", "segment", "kyc_status", "risk_band", "op", "updated_at"],
    "accounts": ["account_id", "customer_id", "account_type", "status", "op", "updated_at"],
    "transactions": ["txn_id", "account_id", "customer_id", "txn_ts", "amount", "currency", "channel", "txn_type"],
}

# SCD2 tracks these; `email` is deliberately NOT tracked, so an email change updates the current row in place.
CUSTOMER_TRACKED = ["city", "segment", "kyc_status", "risk_band"]

# name -> SQL expression that must be TRUE for the row to be kept. Rows failing any rule go to quarantine_*.
TRANSACTION_RULES = {
    "account_present": "account_id IS NOT NULL AND trim(account_id) <> ''",
    "amount_positive": "try_cast(amount AS DECIMAL(18,2)) > 0",
    "timestamp_parsed": "try_to_timestamp(txn_ts) IS NOT NULL",
    "channel_known": "channel IN ('ATM','POS','ONLINE','BRANCH','UPI')",
}
CUSTOMER_RULES = {
    "customer_id_present": "customer_id IS NOT NULL AND trim(customer_id) <> ''",
    "updated_at_parsed": "try_to_timestamp(updated_at) IS NOT NULL",
    "op_known": "op IN ('UPSERT','DELETE')",
}
ACCOUNT_RULES = {
    "account_id_present": "account_id IS NOT NULL AND trim(account_id) <> ''",
    "updated_at_parsed": "try_to_timestamp(updated_at) IS NOT NULL",
    "op_known": "op IN ('UPSERT','DELETE')",
}


def quarantine(df, rules: dict):
    """Rows that fail at least one rule, annotated with the names of the failed rules."""
    from pyspark.sql import functions as F

    failed = F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(f"({e})"), F.lit(False)), F.lit(n)) for n, e in rules.items()]),
        lambda x: x.isNotNull(),
    )
    return df.withColumn("_failed_rules", failed).where(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())
