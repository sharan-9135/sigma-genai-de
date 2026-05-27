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