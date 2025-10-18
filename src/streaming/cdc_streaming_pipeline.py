# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Streaming Pipeline - Real-Time Change Data Capture
# MAGIC 
# MAGIC Implements real-time CDC using Delta Change Data Feed to:
# MAGIC - Capture INSERT, UPDATE, DELETE operations
# MAGIC - Stream changes from Bronze to Silver layer
# MAGIC - Process incrementally with low latency
# MAGIC - Maintain exactly-once semantics

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
BRONZE_SCHEMA = "banking_bronze"
SILVER_SCHEMA = "banking_silver"
CHECKPOINT_BASE = "/tmp/streaming_checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Streaming: Customers

# COMMAND ----------

def stream_customers_cdc():
    """
    Stream customer changes using Change Data Feed
    """
    
    print("🔄 Starting CDC stream for customers...")
    
    # Read streaming changes from bronze customers table
    customer_stream = (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")  # Start from latest or specify version
            .option("maxFilesPerTrigger", 100)    # Rate limiting
            .table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    )
    
    # Transform based on change type
    # CDF provides: _change_type (insert, update_preimage, update_postimage, delete)
    processed_stream = (
        customer_stream
        # Filter out preimage records (we only need postimage for updates)
        .filter(col("_change_type").isin(["insert", "update_postimage", "delete"]))
        
        # Add metadata
        .withColumn("cdc_operation", col("_change_type"))
        .withColumn("cdc_timestamp", col("_commit_timestamp"))
        .withColumn("cdc_version", col("_commit_version"))
        
        # Apply transformations (same as batch silver layer)
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("customer_tenure_days", 
            datediff(current_timestamp(), col("account_open_date")))
        .withColumn("is_high_value", 
            when((col("credit_score") >= 750) & (col("annual_income") >= 100000), True)
            .otherwise(False))
        .withColumn("stream_processed_at", current_timestamp())
    )
    
    # Write to silver layer (streaming)
    query = (
        processed_stream.writeStream
            .format("delta")
            .outputMode("append")  # Append mode for CDC
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/customers_cdc")
            .option("mergeSchema", "true")
            .trigger(processingTime="30 seconds")  # Micro-batch every 30 seconds
            .table(f"{CATALOG}.{SILVER_SCHEMA}.customers_cdc_stream")
    )
    
    print(f"✓ Customer CDC stream started: {query.name}")
    return query

# Start the stream
customer_query = stream_customers_cdc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Streaming: Transactions (High Volume)

# COMMAND ----------

def stream_transactions_cdc():
    """
    Stream transaction changes with optimizations for high volume
    """
    
    print("🔄 Starting CDC stream for transactions...")
    
    transaction_stream = (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .option("maxFilesPerTrigger", 1000)  # Higher throughput for transactions
            .option("maxBytesPerTrigger", "1g")   # Rate limit by size
            .table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions")
    )
    
    # Real-time enrichment and transformations
    processed_txn = (
        transaction_stream
        .filter(col("_change_type").isin(["insert", "update_postimage"]))
        
        # CDC metadata
        .withColumn("cdc_operation", col("_change_type"))
        .withColumn("cdc_timestamp", col("_commit_timestamp"))
        
        # Real-time feature engineering
        .withColumn("transaction_hour", hour(col("transaction_date")))
        .withColumn("is_weekend", 
            when(dayofweek(col("transaction_date")).isin([1, 7]), True).otherwise(False))
        .withColumn("is_night_transaction",
            when(col("transaction_hour").between(0, 5), True).otherwise(False))
        
        # Real-time risk scoring
        .withColumn("realtime_risk_score",
            when((col("is_international") == True) & (col("amount") > 1000), 90)
            .when((col("is_night_transaction") == True) & (col("amount") > 500), 70)
            .when(col("fraud_score") > 0.7, 80)
            .otherwise(20))
        
        # High-risk flag for immediate alerts
        .withColumn("requires_immediate_review", col("realtime_risk_score") >= 80)
        
        .withColumn("stream_processed_at", current_timestamp())
    )
    
    # Write to silver layer with partitioning
    query = (
        processed_txn.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/transactions_cdc")
            .partitionBy("transaction_date")
            .trigger(processingTime="10 seconds")  # Faster trigger for transactions
            .table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_cdc_stream")
    )
    
    print(f"✓ Transaction CDC stream started: {query.name}")
    return query

# Start the stream
transaction_query = stream_transactions_cdc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Streaming: Accounts

# COMMAND ----------

def stream_accounts_cdc():
    """
    Stream account changes to detect balance changes in real-time
    """
    
    print("🔄 Starting CDC stream for accounts...")
    
    account_stream = (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .option("maxFilesPerTrigger", 200)
            .table(f"{CATALOG}.{BRONZE_SCHEMA}.accounts")
    )
    
    # Detect significant balance changes
    processed_accounts = (
        account_stream
        .filter(col("_change_type").isin(["insert", "update_postimage"]))
        
        .withColumn("cdc_operation", col("_change_type"))
        .withColumn("cdc_timestamp", col("_commit_timestamp"))
        
        # Account health metrics
        .withColumn("account_age_days",
            datediff(current_timestamp(), col("account_open_date")))
        .withColumn("is_low_balance",
            col("balance") < col("minimum_balance"))
        .withColumn("is_overdrawn",
            col("balance") < 0)
        .withColumn("balance_risk_level",
            when(col("is_overdrawn"), "Critical")
            .when(col("is_low_balance"), "High")
            .when(col("balance") < col("minimum_balance") * 2, "Medium")
            .otherwise("Low"))
        
        # Alert triggers
        .withColumn("requires_alert",
            col("is_overdrawn") | (col("balance") < 100))
        
        .withColumn("stream_processed_at", current_timestamp())
    )
    
    query = (
        processed_accounts.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/accounts_cdc")
            .trigger(processingTime="30 seconds")
            .table(f"{CATALOG}.{SILVER_SCHEMA}.accounts_cdc_stream")
    )
    
    print(f"✓ Account CDC stream started: {query.name}")
    return query

# Start the stream
account_query = stream_accounts_cdc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Streaming: Loans

# COMMAND ----------

def stream_loans_cdc():
    """
    Stream loan changes to monitor payment status in real-time
    """
    
    print("🔄 Starting CDC stream for loans...")
    
    loan_stream = (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .table(f"{CATALOG}.{BRONZE_SCHEMA}.loans")
    )
    
    processed_loans = (
        loan_stream
        .filter(col("_change_type").isin(["insert", "update_postimage"]))
        
        .withColumn("cdc_operation", col("_change_type"))
        .withColumn("cdc_timestamp", col("_commit_timestamp"))
        
        # Delinquency detection
        .withColumn("delinquency_severity",
            when(col("days_past_due") >= 90, "Severe")
            .when(col("days_past_due") >= 60, "Serious")
            .when(col("days_past_due") >= 30, "Moderate")
            .when(col("days_past_due") > 0, "Early")
            .otherwise("Current"))
        
        # Default risk indicators
        .withColumn("default_risk_flag",
            (col("days_past_due") > 90) | 
            (col("missed_payments") >= 3) |
            col("is_default"))
        
        # Collections priority
        .withColumn("collections_priority",
            when(col("default_risk_flag"), "High")
            .when(col("days_past_due") > 30, "Medium")
            .otherwise("Low"))
        
        .withColumn("stream_processed_at", current_timestamp())
    )
    
    query = (
        processed_loans.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/loans_cdc")
            .trigger(processingTime="60 seconds")
            .table(f"{CATALOG}.{SILVER_SCHEMA}.loans_cdc_stream")
    )
    
    print(f"✓ Loan CDC stream started: {query.name}")
    return query

# Start the stream
loan_query = stream_loans_cdc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor All CDC Streams

# COMMAND ----------

def monitor_streams():
    """
    Monitor all active streaming queries
    """
    
    active_streams = spark.streams.active
    
    print("\n📊 Active CDC Streaming Queries:")
    print("=" * 80)
    
    for stream in active_streams:
        status = stream.status
        print(f"\n🔄 Stream: {stream.name}")
        print(f"   Status: {status['message']}")
        print(f"   Is Active: {stream.isActive}")
        
        if 'numInputRows' in status:
            print(f"   Input Rows: {status.get('numInputRows', 0):,}")
            print(f"   Processing Rate: {status.get('processedRowsPerSecond', 0):.2f} rows/sec")
        
        print(f"   Checkpoint: {stream.lastProgress.get('sources', [{}])[0].get('startOffset', 'N/A') if stream.lastProgress else 'Starting...'}")
    
    print("=" * 80)
    
    return active_streams

# Monitor streams
monitor_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Management Commands

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stop All Streams
# MAGIC ```python
# MAGIC for stream in spark.streams.active:
# MAGIC     print(f"Stopping {stream.name}...")
# MAGIC     stream.stop()
# MAGIC ```
# MAGIC 
# MAGIC ### Check Stream Status
# MAGIC ```python
# MAGIC for stream in spark.streams.active:
# MAGIC     print(f"{stream.name}: {stream.status}")
# MAGIC     if stream.lastProgress:
# MAGIC         print(f"  Latest progress: {stream.lastProgress}")
# MAGIC ```
# MAGIC 
# MAGIC ### Restart Stream from Specific Version
# MAGIC ```python
# MAGIC spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", 100)  # Specific version \
# MAGIC     .table("banking_catalog.banking_bronze.customers")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benefits of CDC Streaming
# MAGIC 
# MAGIC ✅ **Low Latency**: Changes processed within seconds  
# MAGIC ✅ **Exactly-Once**: Guaranteed processing semantics  
# MAGIC ✅ **Incremental**: Only processes changed data  
# MAGIC ✅ **Scalable**: Handles high-volume changes  
# MAGIC ✅ **Auditable**: Full change history tracked  
# MAGIC ✅ **Cost-Effective**: Processes only deltas  

# COMMAND ----------

print("✓ All CDC streaming pipelines configured and running!")
print("⚡ Real-time change data capture is now active for all tables")

