# Pipeline Overview

This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to ensure data is available for downstream analytics and reporting. If it stops, critical business metrics and reports will be unavailable.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up required tables using `setup_tables()`.
3. Load merchant data using `load_merchants()`.
4. Load transactions into the bronze table using `load_bronze()`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver()`.
6. Load transformed data into the silver table using `load_silver()`.
7. Compute merchant performance metrics using `compute_merchant_performance()`.
8. Compute daily summary metrics using `compute_daily_summary()`.
9. Load performance and summary metrics into gold tables using `load_gold()`.

## Schedule / Trigger

This pipeline runs every night at 2 AM UTC via a scheduled cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection()` fails.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables()` throws an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants()` fails to insert records.
4. **Bronze Table Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze()` fails to insert records.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Missing merchant mapping.
   - **Symptom:** `transform_bronze_to_silver()` produces incomplete data.

## Recovery Actions

1. **Database Connection Failure**
   - Notify Arjun Mehta immediately.
   - Check DuckDB service status.
   - Restart the service if necessary.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables()`.
   - Correct the syntax and rerun the pipeline.
3. **Merchant Data Load Failure**
   - Inspect `MERCHANTS` data for corruption.
   - Clean the data and rerun `load_merchants()`.
4. **Bronze Table Load Failure**
   - Validate `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY` data.
   - Correct invalid records and rerun `load_bronze()`.
5. **Silver Table Transformation Failure**
   - Ensure all merchants in transactions are in `MERCHANTS`.
   - Update `MERCHANTS` and rerun `transform_bronze_to_silver()`.

## Known Bugs

- Hardcoded AWS credentials in the script.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. **Priya Nair** (On-call DE) - priya.nair@sigmadatatech.in, +91-98400-11111
2. **Arjun Mehta** (Tech Lead) - arjun.mehta@sigmadatatech.in
3. **Kavya Reddy** (Platform Manager) - kavya.reddy@sigmadatatech.in

## Data Quality Checks

After a successful run, verify:

- All transactions are in the bronze table.
- Silver table has the correct number of transformed records.
- Gold tables have up-to-date merchant performance and daily summary metrics.