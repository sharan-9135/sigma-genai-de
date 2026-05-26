import logging
import shutil
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, broadcast, when, sum, count, avg, min, mode, countDistinct
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        # Read raw CSV data
        raw_df = spark.read.csv(input_path, header=True, inferSchema=False)
        
        # Add metadata columns
        raw_df = raw_df.withColumn("ingestion_timestamp", lit(datetime.datetime.now()))
        raw_df = raw_df.withColumn("source_file", lit(input_path))
        raw_df = raw_df.withColumn("pipeline_run_id", lit(run_id))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write to Delta Lake partitioned by load_date
        raw_df.write.mode("overwrite").parquet(partition_path)
        
        logging.info(f"[Stage: Ingest Bronze] Ingested {raw_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}, Row count: {raw_df.count()}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        # Read Bronze Parquet with partition pruning
        bronze_df = spark.read.parquet(bronze_path).filter(col("load_date") == run_date)
        logging.info(f"[Stage: Transform Silver] Input count: {bronze_df.count():,} rows")
        
        # Cast columns to correct types
        bronze_df = bronze_df.withColumn("amount", col("amount").cast(FloatType()))
        bronze_df = bronze_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
        bronze_df = bronze_df.withColumn("customer_id", col("customer_id").cast(StringType()))
        bronze_df = bronze_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        # Filter NULL transaction_id and negative amounts
        bronze_df = bronze_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        logging.info(f"[Stage: Transform Silver] After filter count: {bronze_df.count():,} rows")
        
        # Deduplicate on transaction_id keeping latest ingestion_timestamp
        window = Window.partitionBy("transaction_id")
        bronze_df = bronze_df.withColumn("rank", 
                                        when(col("ingestion_timestamp") == 
                                             max("ingestion_timestamp").over(window), 1).otherwise(0))
        bronze_df = bronze_df.filter(col("rank") == 1).drop("rank")
        logging.info(f"[Stage: Transform Silver] After dedup count: {bronze_df.count():,} rows")
        
        # Read merchants data and broadcast hint, cache merchants
        merchants_df = spark.read.parquet(merchants_path)
        merchants_df = broadcast(merchants_df)
        merchants_df.cache()
        
        # Join with merchants
        joined_df = bronze_df.join(merchants_df, bronze_df.merchant_id == merchants_df.merchant_id, "left")
        
        # Add quality_flag column
        joined_df = joined_df.withColumn("quality_flag", 
                                         when(col("merchant_id").isNotNull(), "CLEAN").otherwise("UNMATCHED"))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write to Silver layer partitioned by date
        joined_df.write.mode("overwrite").parquet(partition_path)
        
        logging.info(f"[Stage: Transform Silver] Output count: {joined_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}, Row count: {joined_df.count()}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting aggregation")
        
        # Read the Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("date") == run_date)
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate metrics
        merchant_performance_df = completed_txns.groupBy("merchant_id", "merchant_name", "category", "city", "date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(col("status").isin("FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write the output
        merchant_performance_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}, Row count: {merchant_performance_df.count()}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting aggregation")
        
        # Read the Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate metrics
        customer_ltv_df = completed_txns.groupBy("customer_id") \
           .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("date").alias("first_txn_date"),
                max("date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            )
        
        # Delete existing partition before writing
        shutil.rmtree(output_path, ignore_errors=True)
        
        # Write the output
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}, Row count: {customer_ltv_df.count()}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting aggregation")
        
        # Read the Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("date") == run_date)
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate metrics
        daily_summary_df = completed_txns.groupBy("date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("total_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants"),
                (count(col("status").isin("FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/{run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write the output
        daily_summary_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}, Row count: {daily_summary_df.count()}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Run Gold] Starting gold layer aggregations")
        
        # Define output paths
        merchant_performance_path = f"{gold_output_dir}/merchant_performance"
        customer_ltv_path = f"{gold_output_dir}/customer_ltv"
        daily_summary_path = f"{gold_output_dir}/daily_summary"
        
        # Run each pipeline stage
        build_merchant_performance(spark, silver_path, merchant_performance_path, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_path)
        build_daily_summary(spark, silver_path, daily_summary_path, run_date)
        
        # Write run metadata
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": "run_id_12345",
            "run_status": "SUCCESS",
            "started_at": datetime.datetime.now().isoformat(),
            "completed_at": datetime.datetime.now().isoformat(),
            "output_paths": {
                "merchant_performance": merchant_performance_path,
                "customer_ltv": customer_ltv_path,
                "daily_summary": daily_summary_path
            }
        }
        
        spark.sparkContext.parallelize([run_metadata]) \
            .write.mode("overwrite").json(f"{gold_output_dir}/run_metadata_{run_date}.json")
        
        logging.info("[Stage: Run Gold] Gold layer aggregations completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        spark.sparkContext.parallelize([run_metadata]) \
           .write.mode("overwrite").json(f"{gold_output_dir}/run_metadata_{run_date}.json")
        raise

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("Customer Segmentation Pipeline").getOrCreate()
    
    # Define paths and run details
    input_path = "s3://path/to/bronze/transactions_raw"
    output_bronze_path = "s3://path/to/bronze/transactions_bronze"
    output_silver_path = "s3://path/to/silver/transactions_silver"
    merchants_path = "s3://path/to/reference/merchants_dim"
    gold_output_dir = "s3://path/to/gold/output"
    run_date = datetime.datetime.now().strftime("%Y%m%d")
    run_id = "run_id_12345"
    
    # Ingest Bronze layer
    ingest_bronze(spark, input_path, output_bronze_path, run_date, run_id)
    
    # Transform Silver layer
    transform_silver(spark, output_bronze_path, merchants_path, output_silver_path, run_date)
    
    # Run Gold layer
    run_gold(spark, output_silver_path, gold_output_dir, run_date)

if __name__ == "__main__":
    main()
