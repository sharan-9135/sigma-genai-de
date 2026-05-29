<<<<<<< HEAD
# Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it through bronze, silver, and gold layers, and computes merchant performance and daily transaction summaries.

## Data Flow Diagram
```
+---------------------+       +---------------------+       +---------------------+       +---------------------+
|     Source          |       |     Bronze Layer    |       |     Silver Layer    |       |     Gold Layer      |
| (TRANSACTIONS_CLEAN |       | (bronze_transactions|       | (silver_transactions|       | (gold_merchant_perf  |
|   & TRANSACTIONS_DIRTY) |       |  +---------------+ |       |  +---------------+ |       |  & gold_daily_summary)|
+---------------------+       | | load_bronze() |       | | transform_bronze()|       | | load_gold()         |
                            +---------------+       +---------------+       +---------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a bronze-silver-gold model to ensure data quality and transformation are modular and maintainable.
- **Quality Flags**: Introduced quality flags in the silver layer to distinguish between clean and dirty data.
- **Aggregations in Gold**: Computed merchant performance and daily summaries in the gold layer for easy reporting and analysis.
- **DuckDB for Storage**: Utilized DuckDB for its lightweight and fast in-memory database capabilities.

## Known Limitations
- **No Error Handling**: The pipeline lacks robust error handling, which could lead to data loss in case of failures.
- **Single-threaded**: The pipeline runs sequentially, which may not be optimal for large datasets.
- **Static Merchant Data**: Merchant data is loaded once and not updated unless the pipeline is rerun.
- **Limited Data Validation**: Only basic validation is performed on incoming transaction data.

## Dependencies
- **DuckDB**: For database operations.
- **MERCHANTS**: A predefined list of merchant data.
- **TRANSACTIONS_CLEAN & TRANSACTIONS_DIRTY**: Source data files containing transaction records.
=======
# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it, and stores it in three layers: Bronze, Silver, and Gold. The Bronze layer stores raw data, the Silver layer stores cleaned and enriched data, and the Gold layer stores aggregated metrics.

## Data Flow Diagram

```
+----------------+      +--------------------+      +--------------------+      +--------------------+
| TRANSACTIONS   | ---> | bronze_transactions| ---> | silver_transactions| ---> | gold_merchant_perf |
| (Clean & Dirty)|      |                    |      |                    |      |                    |
+----------------+      +--------------------+      +--------------------+      +--------------------+
                                                                                     |
                                                                                 +--------------------+
                                                                                 | gold_daily_summary  |
                                                                                 +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a three-tier architecture (Bronze, Silver, Gold) to separate raw data, cleaned data, and aggregated metrics.
- **Data Enrichment**: The Silver layer enriches transaction data by joining it with merchant information, making it more useful for analysis.
- **Aggregation**: The Gold layer computes metrics like merchant performance and daily summaries, providing valuable insights.
- **Data Quality Flags**: The Silver layer includes quality flags to distinguish between clean and potentially problematic data.

## Known Limitations
- **Data Duplication**: The pipeline does not handle duplicate transactions within a single run.
- **Limited Error Handling**: The pipeline has minimal error handling, which could be improved for robustness.
- **Single-Run Processing**: The pipeline processes all transactions in a single run, which may not be suitable for very large datasets.
- **Static Merchant Data**: Merchant data is loaded once and not updated unless the pipeline is rerun.

## Dependencies
- **DuckDB**: The pipeline uses DuckDB for data storage and processing.
- **MERCHANTS**: A list of merchant data used for enriching transactions.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Lists of clean and dirty transaction data, respectively.
>>>>>>> upstream/main
