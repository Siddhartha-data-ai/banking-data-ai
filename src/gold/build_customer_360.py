# Databricks notebook source
# MAGIC %md
# MAGIC # Build Customer 360 View
# MAGIC 
# MAGIC Creates a comprehensive 360-degree view of each customer combining:
# MAGIC - Customer demographics and profile
# MAGIC - All account information
# MAGIC - Transaction patterns and behavior
# MAGIC - Loan and credit card portfolio
# MAGIC - Risk metrics and lifetime value

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

customers_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean")
accounts_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.accounts_clean")
transactions_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean")
loans_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loans_clean")
credit_cards_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.credit_cards_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Account Metrics

# COMMAND ----------

# Aggregate account information per customer
account_metrics = accounts_df.groupBy("customer_id").agg(
    count("account_id").alias("total_accounts"),
    sum("balance").alias("total_account_balance"),
    sum("available_balance").alias("total_available_balance"),
    count(when(col("account_status") == "Active", 1)).alias("active_accounts"),
    count(when(col("is_dormant"), 1)).alias("dormant_accounts"),
    collect_list("account_type").alias("account_types_list"),
    max("account_age_years").alias("longest_account_tenure"),
    sum("transaction_count_30d").alias("total_transactions_30d"),
    avg("account_health_score").alias("avg_account_health")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Transaction Behavior

# COMMAND ----------

# Recent transaction patterns (last 90 days)
recent_transactions = transactions_df.filter(
    col("transaction_date") >= date_sub(current_date(), 90)
)

transaction_metrics = recent_transactions.groupBy("customer_id").agg(
    count("transaction_id").alias("transactions_90d"),
    sum("amount").alias("transaction_volume_90d"),
    avg("amount").alias("avg_transaction_amount"),
    max("amount").alias("max_transaction_amount"),
    count(when(col("is_international"), 1)).alias("international_transactions"),
    count(when(col("is_fraud"), 1)).alias("fraud_incidents"),
    avg("fraud_score").alias("avg_fraud_score"),
    countDistinct("merchant_category").alias("unique_merchant_categories"),
    sum(when(col("is_debit"), col("amount")).otherwise(0)).alias("total_debits_90d"),
    sum(when(~col("is_debit"), col("amount")).otherwise(0)).alias("total_credits_90d")
).withColumn("net_flow_90d", col("total_credits_90d") - col("total_debits_90d"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Loan Information

# COMMAND ----------

# Loan portfolio per customer
loan_metrics = loans_df.groupBy("customer_id").agg(
    count("loan_id").alias("total_loans"),
    sum("loan_amount").alias("total_loan_amount"),
    sum("remaining_balance").alias("total_loan_balance"),
    sum("monthly_payment").alias("total_monthly_loan_payment"),
    count(when(col("loan_status") == "Active", 1)).alias("active_loans"),
    count(when(col("is_delinquent"), 1)).alias("delinquent_loans"),
    count(when(col("is_default"), 1)).alias("defaulted_loans"),
    avg("risk_score").alias("avg_loan_risk_score"),
    max("days_past_due").alias("max_days_past_due"),
    collect_list("loan_type").alias("loan_types_list")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Credit Card Information

# COMMAND ----------

# Credit card portfolio per customer
credit_card_metrics = credit_cards_df.groupBy("customer_id").agg(
    count("card_id").alias("total_credit_cards"),
    sum("credit_limit").alias("total_credit_limit"),
    sum("current_balance").alias("total_credit_card_balance"),
    avg("utilization_percent").alias("avg_credit_utilization"),
    sum("rewards_balance").alias("total_rewards_balance"),
    count(when(col("card_status") == "Active", 1)).alias("active_credit_cards"),
    count(when(col("is_delinquent"), 1)).alias("delinquent_credit_cards"),
    sum("minimum_payment").alias("total_minimum_payment"),
    collect_list("card_type").alias("card_types_list"),
    max("days_past_due").alias("max_card_days_past_due")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer 360

# COMMAND ----------

# Start with customer base
customer_360 = customers_df

# Join all metrics
customer_360 = customer_360 \
    .join(account_metrics, "customer_id", "left") \
    .join(transaction_metrics, "customer_id", "left") \
    .join(loan_metrics, "customer_id", "left") \
    .join(credit_card_metrics, "customer_id", "left")

# Fill nulls for customers with no products
customer_360 = customer_360.fillna(0, [
    "total_accounts", "total_account_balance", "active_accounts", 
    "transactions_90d", "transaction_volume_90d", 
    "total_loans", "total_loan_balance", "active_loans",
    "total_credit_cards", "total_credit_limit", "total_credit_card_balance"
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Derived Metrics

# COMMAND ----------

customer_360 = customer_360 \
    .withColumn("total_assets", 
        coalesce(col("total_account_balance"), lit(0))) \
    .withColumn("total_liabilities",
        coalesce(col("total_loan_balance"), lit(0)) + 
        coalesce(col("total_credit_card_balance"), lit(0))) \
    .withColumn("net_worth",
        col("total_assets") - col("total_liabilities")) \
    .withColumn("total_monthly_obligations",
        coalesce(col("total_monthly_loan_payment"), lit(0)) + 
        coalesce(col("total_minimum_payment"), lit(0))) \
    .withColumn("debt_to_income_ratio",
        when(col("annual_income") > 0,
             round((col("total_monthly_obligations") * 12) / col("annual_income"), 4))
        .otherwise(0)) \
    .withColumn("product_count",
        coalesce(col("total_accounts"), lit(0)) + 
        coalesce(col("total_loans"), lit(0)) + 
        coalesce(col("total_credit_cards"), lit(0))) \
    .withColumn("is_multi_product",
        when(col("product_count") >= 3, True).otherwise(False)) \
    .withColumn("estimated_lifetime_value",
        round((col("transaction_volume_90d") / 90 * 365 * 3) + 
              (col("total_credit_card_balance") * 0.02), 2)) \
    .withColumn("risk_composite_score",
        round((coalesce(col("credit_score"), lit(500)) + 
               coalesce(col("avg_loan_risk_score"), lit(500)) +
               (100 - coalesce(col("avg_fraud_score"), lit(0)) * 100)) / 3, 0)) \
    .withColumn("customer_health_score",
        case(
            when((col("credit_score") >= 750) & 
                 (col("delinquent_loans").isNull() | (col("delinquent_loans") == 0)) &
                 (col("delinquent_credit_cards").isNull() | (col("delinquent_credit_cards") == 0)) &
                 (col("debt_to_income_ratio") < 0.36), 100),
            when((col("credit_score") >= 700) & 
                 (col("debt_to_income_ratio") < 0.43), 75),
            when((col("credit_score") >= 650), 50)
        ).otherwise(25)) \
    .withColumn("churn_risk_score",
        case(
            when((col("transactions_90d") == 0) | (col("dormant_accounts") > 0), 80),
            when((col("delinquent_loans") > 0) | (col("delinquent_credit_cards") > 0), 70),
            when((col("transactions_90d") < 5) & (col("customer_tenure_years") < 1), 60),
            when(col("net_flow_90d") < 0, 40)
        ).otherwise(20)) \
    .withColumn("customer_value_tier",
        case(
            when((col("estimated_lifetime_value") > 50000) & 
                 (col("is_multi_product") == True) & 
                 (col("credit_score") >= 750), "Platinum"),
            when((col("estimated_lifetime_value") > 25000) & 
                 (col("product_count") >= 2), "Gold"),
            when(col("estimated_lifetime_value") > 10000, "Silver"),
            when(col("estimated_lifetime_value") > 5000, "Bronze")
        ).otherwise("Basic")) \
    .withColumn("engagement_level",
        case(
            when(col("transactions_90d") >= 50, "High"),
            when(col("transactions_90d") >= 20, "Medium"),
            when(col("transactions_90d") >= 5, "Low")
        ).otherwise("Inactive")) \
    .withColumn("relationship_strength",
        case(
            when((col("is_multi_product") == True) & 
                 (col("customer_tenure_years") >= 5) &
                 (col("customer_health_score") >= 75), "Strong"),
            when((col("product_count") >= 2) & 
                 (col("customer_tenure_years") >= 2), "Moderate")
        ).otherwise("Weak"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Customer 360

# COMMAND ----------

# Write to gold table
output_table = f"{CATALOG}.{GOLD_SCHEMA}.customer_360"

customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

print(f"✓ Successfully created {output_table}")
print(f"✓ Total customers: {customer_360.count()}")

# COMMAND ----------

# Show sample
display(customer_360.select(
    "customer_id", "full_name", "customer_segment", 
    "total_assets", "total_liabilities", "net_worth",
    "product_count", "customer_value_tier", 
    "customer_health_score", "churn_risk_score",
    "engagement_level", "relationship_strength"
).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("Customer 360 Summary:")
print(f"Total Customers: {customer_360.count()}")
print("\nValue Tier Distribution:")
customer_360.groupBy("customer_value_tier").count().orderBy(desc("count")).show()
print("\nEngagement Levels:")
customer_360.groupBy("engagement_level").count().orderBy(desc("count")).show()
print("\nRelationship Strength:")
customer_360.groupBy("relationship_strength").count().orderBy(desc("count")).show()
print("\nChurn Risk Distribution:")
customer_360.groupBy(
    when(col("churn_risk_score") >= 70, "High Risk")
    .when(col("churn_risk_score") >= 40, "Medium Risk")
    .otherwise("Low Risk").alias("churn_category")
).count().show()

