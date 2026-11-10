"""Explore the Kaggle bank-transactions CSV before designing the loader.

Run from the project root:
    python scripts/explore_data.py
    python scripts/explore_data.py path/to/other.csv
"""
import sys

import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/bank_transactions.csv"

# dtype=str: read everything as text first, so we never lose leading zeros in
# TransactionTime (HHMMSS) and we control date parsing ourselves.
df = pd.read_csv(path, dtype=str)

# Dates are day/month/2-digit-year, e.g. 2/8/16 = 2 Aug 2016.
df["txn_date"] = pd.to_datetime(df["TransactionDate"], format="%d/%m/%y", errors="coerce")
bad_dates = df["txn_date"].isna().sum()

print(f"Row count:                 {len(df):,}")
print(f"Unparseable dates:         {bad_dates:,}")
print(f"Min TransactionDate:       {df['txn_date'].min().date()}")
print(f"Max TransactionDate:       {df['txn_date'].max().date()}")
print(f"Distinct dates:            {df['txn_date'].nunique():,}")

print("\nRows per date (first 10 dates):")
print(df.groupby("txn_date").size().sort_index().head(10).to_string())

print(f"\nDistinct CustomerID:       {df['CustomerID'].nunique():,}")

# SCD2 candidate: does a customer's location differ across their rows?
locs = df.groupby("CustomerID")["CustLocation"].nunique(dropna=True)
print(f"CustomerIDs with >1 distinct CustLocation: {(locs > 1).sum():,}")
