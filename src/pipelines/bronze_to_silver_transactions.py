# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Transactions Pipeline
# MAGIC 
# MAGIC Transforms and cleanses transaction data from bronze to silver layer with:
# MAGIC - Transaction categorization
# MAGIC - Fraud score enrichment
# MAGIC - Amount validations
# MAGIC - Velocity checks

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="transactions_clean",
    comment="Cleaned and validated transaction data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "transaction_date,account_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "transaction_date IS NOT NULL")
@dlt.expect("valid_fraud_score", "fraud_score BETWEEN 0 AND 1")
def transactions_clean():
    """
    Transform bronze transactions to silver with data quality rules
    """
    # Window for calculating running totals and velocity
    window_customer = Window.partitionBy("customer_id").orderBy("transaction_date")
    window_customer_24h = Window.partitionBy("customer_id").orderBy("transaction_date").rangeBetween(-86400, 0)
    
    return (
        dlt.read("banking_bronze.transactions")
        .withColumn("transaction_hour", hour(col("transaction_date")))
        .withColumn("transaction_day_of_week", dayofweek(col("transaction_date")))
        .withColumn("transaction_month", month(col("transaction_date")))
        .withColumn("is_weekend", 
            when(col("transaction_day_of_week").isin([1, 7]), True).otherwise(False))
        .withColumn("is_night_transaction",
            when(col("transaction_hour").between(0, 5), True).otherwise(False))
        .withColumn("amount_category",
            when(col("amount") < 50, "Small")
            .when(col("amount") < 500, "Medium")
            .when(col("amount") < 2000, "Large")
            .otherwise("Very Large"))
        .withColumn("transaction_risk_flag",
            when((col("is_international") == True) & (col("amount") > 1000), "High")
            .when((col("is_night_transaction") == True) & (col("amount") > 500), "Medium")
            .when(col("fraud_score") > 0.5, "Medium")
            .otherwise("Low"))
        # Calculate transaction velocity (count in last 24 hours)
        .withColumn("transactions_24h",
            count("*").over(window_customer_24h))
        .withColumn("amount_24h",
            sum("amount").over(window_customer_24h))
        .withColumn("has_high_velocity",
            when(col("transactions_24h") > 20, True).otherwise(False))
        # Merchant standardization
        .withColumn("merchant_name_clean",
            when(col("merchant_name").isNotNull(), 
                 upper(trim(regexp_replace(col("merchant_name"), "[^a-zA-Z0-9 ]", ""))))
            .otherwise(None))
        .withColumn("silver_updated_at", current_timestamp())
        .dropDuplicates(["transaction_id"])
    )

# COMMAND ----------

@dlt.table(
    name="transactions_fraud_flagged",
    comment="Transactions flagged for potential fraud"
)
def transactions_fraud_flagged():
    """
    High-risk transactions requiring review
    """
    return (
        dlt.read("transactions_clean")
        .filter(
            (col("is_fraud") == True) |
            (col("fraud_score") > 0.7) |
            (col("has_high_velocity") == True) |
            ((col("is_international") == True) & (col("amount") > 2000))
        )
        .withColumn("fraud_reason",
            when(col("is_fraud"), "Confirmed Fraud")
            .when(col("fraud_score") > 0.7, "High Fraud Score")
            .when(col("has_high_velocity"), "High Transaction Velocity")
            .when((col("is_international") == True) & (col("amount") > 2000), "Large International Transaction")
            .otherwise("Under Review"))
    )

# COMMAND ----------

@dlt.table(
    name="transactions_completed",
    comment="Successfully completed transactions only"
)
def transactions_completed():
    """
    Filter for completed transactions
    """
    return (
        dlt.read("transactions_clean")
        .filter(col("status") == "Completed")
        .filter(col("is_fraud") == False)
    )

# COMMAND ----------

@dlt.table(
    name="transactions_summary_daily",
    comment="Daily transaction summary by customer"
)
def transactions_summary_daily():
    """
    Aggregate daily transaction metrics
    """
    return (
        dlt.read("transactions_completed")
        .withColumn("transaction_date_only", to_date(col("transaction_date")))
        .groupBy("customer_id", "account_id", "transaction_date_only")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            max("amount").alias("max_amount"),
            sum(when(col("is_debit"), col("amount")).otherwise(0)).alias("total_debits"),
            sum(when(~col("is_debit"), col("amount")).otherwise(0)).alias("total_credits"),
            sum(when(col("is_international"), 1).otherwise(0)).alias("international_count")
        )
        .withColumn("net_flow", col("total_credits") - col("total_debits"))
    )

