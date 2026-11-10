"""Are the CustLocation changes in the Kaggle CSV real, or just spelling noise?

Run from the project root:
    python scripts/inspect_location_changes.py
"""
import sys

import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/bank_transactions.csv"

df = pd.read_csv(path, dtype=str)
df["txn_date"] = pd.to_datetime(df["TransactionDate"], format="%d/%m/%y")
# Sort key inside a day: HHMMSS as text needs left-padding to 6 digits.
df["txn_time"] = df["TransactionTime"].str.zfill(6)
df = df.dropna(subset=["CustLocation"]).sort_values(["CustomerID", "txn_date", "txn_time"])

# --- Which customers have more than one location? ---
n_loc = df.groupby("CustomerID")["CustLocation"].nunique()
multi_ids = n_loc[n_loc > 1].index
multi = df[df["CustomerID"].isin(multi_ids)].copy()
print(f"Multi-location customers: {len(multi_ids):,}")

# --- 2) How many of them have transactions on >= 3 different dates? ---
n_dates = multi.groupby("CustomerID")["txn_date"].nunique()
print(f"...with transactions on >= 3 distinct dates: {(n_dates >= 3).sum():,}")
print(f"...with transactions on exactly 2 distinct dates: {(n_dates == 2).sum():,}")
print(f"...with all rows on a single date: {(n_dates == 1).sum():,}")

# --- 3) Days between consecutive transactions where the location changed ---
multi["prev_loc"] = multi.groupby("CustomerID")["CustLocation"].shift()
multi["prev_date"] = multi.groupby("CustomerID")["txn_date"].shift()
changes = multi[multi["prev_loc"].notna() & (multi["CustLocation"] != multi["prev_loc"])]
gap = (changes["txn_date"] - changes["prev_date"]).dt.days
print(f"\nLocation-change events: {len(changes):,}")
print(f"Days between the old-location txn and the new-location txn:")
print(f"  min={gap.min()}  median={gap.median()}  max={gap.max()}")
print(f"  same-day changes (gap = 0): {(gap == 0).sum():,}")
print(f"  gap >= 1 day: {(gap >= 1).sum():,}")

# --- 1) 15 example customers, with per-transaction rows ---
# Prefer customers with >= 3 dates so the history is readable; fixed seed = repeatable.
pool = n_dates[n_dates >= 3].index
sample_ids = pd.Series(pool).sample(15, random_state=42).tolist()
print("\n=== 15 example customers (date | time | location) ===")
for cid in sample_ids:
    rows = multi[multi["CustomerID"] == cid]
    print(f"\n{cid}")
    for r in rows.head(8).itertuples():
        print(f"  {r.txn_date.date()} {r.txn_time}  {r.CustLocation}")
    if len(rows) > 8:
        print(f"  ... {len(rows) - 8} more rows")
