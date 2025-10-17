# Databricks notebook source
# MAGIC %md
# MAGIC # Build Fraud Analytics
# MAGIC 
# MAGIC Creates fraud detection and analytics tables including:
# MAGIC - Real-time fraud alerts
# MAGIC - Pattern detection
# MAGIC - Customer fraud risk profiles
# MAGIC - Historical fraud trends

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"

# Ensure gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------

transactions_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean")
customers_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean")
accounts_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.accounts_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fraud Alerts Table

# COMMAND ----------

# High-risk transactions requiring immediate attention
fraud_alerts = transactions_df \
    .filter(
        (col("is_fraud") == True) |
        (col("fraud_score") > 0.7) |
        (col("has_high_velocity") == True) |
        ((col("is_international") == True) & (col("amount") > 2000)) |
        ((col("is_night_transaction") == True) & (col("amount") > 1000))
    ) \
    .join(customers_df.select("customer_id", "full_name", "email", "phone", "risk_rating"),
          "customer_id", "left") \
    .join(accounts_df.select("account_id", "account_type", "balance"),
          "account_id", "left") \
    .withColumn("alert_severity",
        case(
            when(col("is_fraud") == True, "Critical"),
            when(col("fraud_score") > 0.85, "High"),
            when(col("fraud_score") > 0.7, "Medium")
        ).otherwise("Low")) \
    .withColumn("alert_reason",
        array_join(
            array_remove(
                array(
                    when(col("is_fraud"), "Confirmed Fraud"),
                    when(col("fraud_score") > 0.7, f"High Fraud Score ({col('fraud_score')})"),
                    when(col("has_high_velocity"), "Unusual Transaction Velocity"),
                    when((col("is_international") == True) & (col("amount") > 2000), 
                         "Large International Transaction"),
                    when((col("is_night_transaction") == True) & (col("amount") > 1000),
                         "Large Night Transaction")
                ),
                None
            ),
            "; "
        )) \
    .withColumn("recommended_action",
        case(
            when(col("alert_severity") == "Critical", "Block Card & Contact Customer"),
            when(col("alert_severity") == "High", "Hold Transaction & Verify"),
            when(col("alert_severity") == "Medium", "Alert Customer")
        ).otherwise("Monitor")) \
    .withColumn("alert_generated_at", current_timestamp())

# Write fraud alerts
fraud_alerts_table = f"{CATALOG}.{GOLD_SCHEMA}.fraud_alerts"
fraud_alerts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(fraud_alerts_table)

print(f"✓ Created {fraud_alerts_table}")
print(f"✓ Total alerts: {fraud_alerts.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer Fraud Risk Profiles

# COMMAND ----------

# Aggregate fraud metrics per customer
customer_fraud_profile = transactions_df \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        count(when(col("is_fraud"), 1)).alias("fraud_count"),
        count(when(col("fraud_score") > 0.7, 1)).alias("high_risk_count"),
        avg("fraud_score").alias("avg_fraud_score"),
        max("fraud_score").alias("max_fraud_score"),
        sum("amount").alias("total_transaction_volume"),
        sum(when(col("is_fraud"), col("amount")).otherwise(0)).alias("fraud_loss_amount"),
        count(when(col("is_international"), 1)).alias("international_count"),
        countDistinct("location_country").alias("unique_countries"),
        countDistinct("device_id").alias("unique_devices"),
        max("transactions_24h").alias("max_velocity_24h")
    ) \
    .join(customers_df.select("customer_id", "full_name", "credit_score", 
                               "risk_rating", "customer_segment"),
          "customer_id", "left") \
    .withColumn("fraud_rate",
        when(col("total_transactions") > 0,
             round((col("fraud_count") / col("total_transactions")) * 100, 2))
        .otherwise(0)) \
    .withColumn("fraud_loss_rate",
        when(col("total_transaction_volume") > 0,
             round((col("fraud_loss_amount") / col("total_transaction_volume")) * 100, 2))
        .otherwise(0)) \
    .withColumn("fraud_risk_category",
        case(
            when((col("fraud_count") >= 3) | (col("fraud_rate") > 2), "High Risk"),
            when((col("fraud_count") >= 1) | (col("high_risk_count") >= 5), "Medium Risk"),
            when(col("avg_fraud_score") > 0.5, "Moderate Risk")
        ).otherwise("Low Risk")) \
    .withColumn("behavior_flags",
        array_join(
            array_remove(
                array(
                    when(col("unique_countries") > 10, "Frequent International Traveler"),
                    when(col("max_velocity_24h") > 30, "High Transaction Velocity"),
                    when(col("unique_devices") > 5, "Multiple Devices"),
                    when(col("fraud_count") > 0, "Prior Fraud History")
                ),
                None
            ),
            ", "
        )) \
    .withColumn("risk_score_composite",
        round(
            (coalesce(col("avg_fraud_score"), lit(0)) * 40) +
            (coalesce(col("fraud_rate"), lit(0)) * 0.3) +
            (when(col("fraud_count") > 0, 30).otherwise(0)) +
            (when(col("max_velocity_24h") > 20, 20).otherwise(0)) -
            (when(col("credit_score") >= 750, 10).otherwise(0)),
            2
        ))

# Write customer fraud profiles
customer_fraud_table = f"{CATALOG}.{GOLD_SCHEMA}.customer_fraud_profiles"
customer_fraud_profile.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(customer_fraud_table)

print(f"✓ Created {customer_fraud_table}")
print(f"✓ Total profiles: {customer_fraud_profile.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fraud Analytics Summary

# COMMAND ----------

# Daily fraud statistics
fraud_analytics = transactions_df \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .groupBy("transaction_date") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        count(when(col("is_fraud"), 1)).alias("fraud_transactions"),
        count(when(col("fraud_score") > 0.7, 1)).alias("high_risk_transactions"),
        sum("amount").alias("total_volume"),
        sum(when(col("is_fraud"), col("amount")).otherwise(0)).alias("fraud_volume"),
        avg("fraud_score").alias("avg_fraud_score"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct(when(col("is_fraud"), col("customer_id"))).alias("fraud_customers"),
        count(when(col("is_international"), 1)).alias("international_transactions"),
        count(when(col("is_night_transaction"), 1)).alias("night_transactions")
    ) \
    .withColumn("fraud_rate",
        when(col("total_transactions") > 0,
             round((col("fraud_transactions") / col("total_transactions")) * 100, 4))
        .otherwise(0)) \
    .withColumn("fraud_loss_rate",
        when(col("total_volume") > 0,
             round((col("fraud_volume") / col("total_volume")) * 100, 4))
        .otherwise(0)) \
    .withColumn("fraud_customer_rate",
        when(col("unique_customers") > 0,
             round((col("fraud_customers") / col("unique_customers")) * 100, 4))
        .otherwise(0)) \
    .orderBy(desc("transaction_date"))

# Write fraud analytics
fraud_analytics_table = f"{CATALOG}.{GOLD_SCHEMA}.fraud_analytics"
fraud_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(fraud_analytics_table)

print(f"✓ Created {fraud_analytics_table}")
print(f"✓ Total days: {fraud_analytics.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Merchant Fraud Analysis

# COMMAND ----------

# Identify high-risk merchants
merchant_fraud = transactions_df \
    .filter(col("merchant_name").isNotNull()) \
    .groupBy("merchant_name", "merchant_category", "merchant_id") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        count(when(col("is_fraud"), 1)).alias("fraud_count"),
        sum("amount").alias("total_amount"),
        sum(when(col("is_fraud"), col("amount")).otherwise(0)).alias("fraud_amount"),
        avg("fraud_score").alias("avg_fraud_score"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct(when(col("is_fraud"), col("customer_id"))).alias("fraud_customers")
    ) \
    .withColumn("fraud_rate",
        when(col("transaction_count") >= 10,
             round((col("fraud_count") / col("transaction_count")) * 100, 2))
        .otherwise(0)) \
    .withColumn("merchant_risk_level",
        case(
            when((col("fraud_rate") > 5) & (col("transaction_count") >= 50), "High Risk"),
            when((col("fraud_rate") > 3) & (col("transaction_count") >= 20), "Medium Risk"),
            when(col("fraud_rate") > 1, "Low Risk")
        ).otherwise("Normal")) \
    .filter(col("transaction_count") >= 10) \
    .orderBy(desc("fraud_rate"))

# Write merchant fraud analysis
merchant_fraud_table = f"{CATALOG}.{GOLD_SCHEMA}.merchant_fraud_analysis"
merchant_fraud.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(merchant_fraud_table)

print(f"✓ Created {merchant_fraud_table}")
print(f"✓ Total merchants: {merchant_fraud.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== FRAUD ANALYTICS SUMMARY ===\n")
print(f"Fraud Alerts: {fraud_alerts.count()}")
display(fraud_alerts.groupBy("alert_severity").count().orderBy("alert_severity"))

print(f"\nCustomer Risk Distribution:")
display(customer_fraud_profile.groupBy("fraud_risk_category").count().orderBy(desc("count")))

print(f"\nRecent Fraud Trends (Last 7 Days):")
display(fraud_analytics.orderBy(desc("transaction_date")).limit(7))

print(f"\nTop High-Risk Merchants:")
display(merchant_fraud.filter(col("merchant_risk_level") == "High Risk").limit(10))

