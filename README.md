# Bank Lakehouse: incremental + SCD2 on Databricks Free Edition

One project that combines the best idea from three open-source repos you shortlisted, rebuilt as small, readable code you can fully explain in an interview:

| Idea | Taken from | In this project |
|---|---|---|
| Manual SCD2 with `MERGE INTO` (flag Y/N, `effective_from/to`) | [VenkatagiriRamesh/databricks-scd2-pipeline](https://github.com/VenkatagiriRamesh/databricks-scd2-pipeline) | `notebooks/03_manual_scd2_merge_demo.py` (rewritten with hash-diff, `is_current` Y/N) |
| Declarative `AUTO CDC` (SCD1/SCD2), expectations, append/streaming tables | [Vivekbhand09/Spark-Declarative-Pipeline](https://github.com/Vivekbhand09/Spark-Declarative-Pipeline) (MIT) | `src/bank_pipeline/transformations/silver.py` |
| Auto Loader, quarantine, Lakeflow Job, Asset Bundle, DQ gate, point-in-time joins, Free Edition constraints | [rohit91jacob/food-delivery-databricks-lakehouse](https://github.com/rohit91jacob/food-delivery-databricks-lakehouse) (MIT) | `bronze.py`, `gold.py`, `databricks.yml`, `02_dq_gate.py` |

All code here is newly written; the three repos were read for design ideas and API usage only. Use the domain/names freely, but credit them in your README if you keep the structure close.

> **Honest status:** the generator is tested locally (`pytest`, 4 passing). The Databricks pieces (pipeline, bundle, notebooks) were **not run on a live workspace**. They follow the API usage of repos that are reported to run on Free Edition, but expect to fix small errors on first run. That debugging is part of making it yours.

## Architecture

```
Lakeflow Job  bank_daily   (param: day)
 ├─ land_day          notebook 01: write day-N CSVs into the UC volume        (idempotent, deterministic)
 ├─ refresh_pipeline  Lakeflow Declarative Pipeline (serverless)
 │     bronze_*  (Auto Loader, all STRING, lineage cols)
 │       ├─ quarantine_*   rows that break a rule + names of broken rules
 │       └─ silver_*       AUTO CDC:  customers = SCD2 | accounts = SCD1 | transactions = SCD1 on txn_id (dedupe)
 │     gold: dim_customer (is_current Y/N) · fct_transactions (point-in-time join) · gold_daily_by_segment · gold_data_quality
 └─ dq_gate           notebook 02: SCD2 invariants + dedupe + quarantine reconciliation; raises -> job fails
```

## Which concept is where (interview map)

| Concept | Where | One-line answer |
|---|---|---|
| Incremental load | Auto Loader (`bronze.py`), day N files | Only new files are read; checkpoints are managed by the pipeline |
| CDC feed | `customers` has `op` = UPSERT/DELETE + `updated_at` | `apply_as_deletes` closes the current SCD2 version |
| SCD2 | `silver_customers` (`__START_AT`, `__END_AT`) + `dim_customer.is_current` Y/N | Tracked columns: city, segment, kyc_status, risk_band |
| Untracked change | `email` | Updates the current row in place (no new version): `track_history_column_list` |
| SCD1 | `silver_accounts` | Latest wins, no history |
| Dedupe / replays | `silver_transactions` AUTO CDC keyed on `txn_id` | Replay lands in a later file, so (file time, path) breaks ties |
| Late-arriving data | 2% of transactions have yesterday's event time | Point-in-time join still picks the right customer version |
| Point-in-time join | `fct_transactions` | `txn_ts >= __START_AT AND (__END_AT IS NULL OR txn_ts < __END_AT)` |
| Data quality | expectations (`drop`) + `quarantine_*` + `02_dq_gate.py` | Bad rows are kept with reasons, never silently dropped |
| Idempotency | deterministic generator, AUTO CDC keys, rerun a day safely | Rerunning day N rewrites identical files |
| Orchestration | Lakeflow Job DAG (land → pipeline → DQ) | Airflow is optional; see below |
| Manual SCD2 | notebook 03 | hash-diff + staged `MERGE` (NULL merge key forces the INSERT) |

## Free Edition fit (checked against Databricks' limitations page + the reference repos' READMEs)

- Serverless only: every task here is serverless (`serverless: true`, notebook tasks without a cluster). ✅
- Max 5 concurrent job tasks, 1 active pipeline update: the DAG is linear and `max_concurrent_runs: 1`. ✅
- No custom storage: managed schema + managed volume. ✅
- Restricted internet: data is generated inside Databricks, no downloads. ✅
- Fair-use quota: keep runs small (300 customers, 1,500 txns/day) and don't schedule it. ⚠️
- Not guaranteed: Auto Loader on Free Edition. The reference repo reports it working *inside a pipeline*; a different repo reported it failing in a plain notebook stream. If bronze fails, switch `bronze.py` to batch `spark.read` + `COPY INTO`-style reads of new files.
- Not verified here: CLI/Asset Bundles on your Free Edition account. If `bundle deploy` fails, import the repo as a Git folder, create the pipeline in the UI (point it at `src/bank_pipeline/transformations`, add config `bank.landing_path`, `root_path` = `src`), and run the notebooks manually.

## Setup (laptop + Databricks Free Edition)

```bash
git clone <your repo> && cd bank-lakehouse
pip install pytest && pytest -q                       # generator tests, no Spark needed
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run bank_daily -t dev --params day=0     # full load
databricks bundle run bank_daily -t dev --params day=1     # incremental
databricks bundle run bank_daily -t dev --params day=2
```

Then in a SQL editor / notebook:

```sql
-- SCD2 proof: old version N, current version Y (AUTO CDC output, shown with the Y/N flag)
SELECT customer_id, city, segment, __START_AT, __END_AT, is_current
FROM workspace.bank_lakehouse.dim_customer
WHERE customer_id IN (SELECT customer_id FROM workspace.bank_lakehouse.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
ORDER BY customer_id, __START_AT;

-- Quarantine reasons (compare with the manifest printed by notebook 01)
SELECT explode(_failed_rules) AS rule, count(*) FROM workspace.bank_lakehouse.quarantine_transactions GROUP BY 1;
```

Note: in `development` mode the bundle prefixes names (schema becomes `dev_<you>_bank_lakehouse`), so adjust the SQL above, or deploy with `-t prod` after changing the `prod` target.

## Make it yours (do at least 3)

1. Swap the domain to your own (cards, loans, insurance claims) by editing `generator.py` and `rules.py`.
2. Add a `loans` feed with SCD2 on interest rate; join facts point-in-time.
3. Add a data-contract check for schema drift (new column in a landing file).
4. Add an AI/BI dashboard on `gold_daily_by_segment` and `gold_data_quality`.
5. Add a GitHub Actions workflow running `pytest` and `databricks bundle validate`.
6. Add a monthly `full_refresh` task and write down why you'd (not) need it.

## Airflow (optional, later)

Airflow is an external scheduler. It cannot run inside Free Edition, but it can run on your laptop in Docker and trigger the job through the Databricks API (`DatabricksRunNowOperator`) with a personal access token. Lakeflow Jobs already does scheduling here, so start without Airflow and add it last if you want that line on your CV.

## Resume bullets (fill the numbers from YOUR run; don't invent any)

- Built an incremental lakehouse on Databricks (Auto Loader, Lakeflow Declarative Pipelines, Delta Lake, Unity Catalog) with bronze/silver/gold layers, processing [N] daily CDC and transaction records orchestrated by a Lakeflow Job.
- Implemented SCD Type 2 customer history with AUTO CDC (tracked vs untracked attributes, delete handling) and rebuilt it manually with hash-diff `MERGE INTO` to validate results.
- Designed point-in-time joins so transactions (including late-arriving ones) are attributed to the customer segment valid at transaction time.
- Added data-quality gates: expectations, quarantine tables with failed-rule reasons, SCD2 invariant checks and bronze-to-silver reconciliation that fails the job on violation.
- Deployed the pipeline as a Databricks Asset Bundle with [N] pytest tests for the data generator, within Databricks Free Edition quotas.

## Likely interview questions

- SCD1 vs SCD2: when do you choose which? What did you track and what not (email), and why?
- How does AUTO CDC order changes? What if two changes for a key have the same sequence value?
- How do you handle a late-arriving customer update after facts are loaded?
- Why does a replay arrive as a duplicate, and why does it not break idempotency?
- Walk me through the manual MERGE: why a NULL merge key? what if the same key changes twice in a batch?
- What happens if the DQ gate fails at 2 a.m.? How would you alert and backfill?
