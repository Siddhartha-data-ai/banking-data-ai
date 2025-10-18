# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Credit Cards Pipeline
# MAGIC 
# MAGIC Transforms and cleanses credit card data from bronze to silver layer with:
# MAGIC - Utilization analysis
# MAGIC - Payment behavior tracking
# MAGIC - Rewards calculation
# MAGIC - Risk assessment

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name="credit_cards_clean",
    comment="Cleaned and validated credit card data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "card_id,customer_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_card_id", "card_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_utilization", "utilization_percent >= 0 AND utilization_percent <= 100")
@dlt.expect("valid_credit_limit", "credit_limit > 0")
def credit_cards_clean():
    """
    Transform bronze credit cards to silver with data quality rules
    """
    return (
        dlt.read("banking_bronze.credit_cards")
        .withColumn("card_age_months",
            months_between(current_timestamp(), col("issue_date")))
        .withColumn("months_until_expiry",
            months_between(col("expiry_date"), current_timestamp()))
        .withColumn("is_expiring_soon",
            when(col("months_until_expiry") <= 3, True).otherwise(False))
        .withColumn("is_expired",
            when(col("expiry_date") < current_timestamp(), True).otherwise(False))
        .withColumn("utilization_category",
            when(col("utilization_percent") < 30, "Low")
            .when(col("utilization_percent") < 50, "Medium")
            .when(col("utilization_percent") < 75, "High")
            .otherwise("Very High"))
        .withColumn("payment_status",
            when(col("days_past_due") == 0, "Current")
            .when(col("days_past_due") <= 30, "Late")
            .when(col("days_past_due") <= 60, "Delinquent")
            .otherwise("Seriously Delinquent"))
        .withColumn("payment_behavior",
            when(col("late_payments") + col("missed_payments") == 0, "Excellent")
            .when(col("late_payments") <= 1, "Good")
            .when(col("late_payments") <= 3, "Fair")
            .otherwise("Poor"))
        .withColumn("card_health_score",
            case(
                when((col("utilization_percent") < 30) & 
                     (col("late_payments") == 0) & 
                     (col("days_past_due") == 0), 100),
                when((col("utilization_percent") < 50) & 
                     (col("late_payments") <= 1), 75),
                when(col("utilization_percent") < 75, 50)
            ).otherwise(25))
        .withColumn("credit_limit_category",
            when(col("credit_limit") < 5000, "Basic")
            .when(col("credit_limit") < 15000, "Standard")
            .when(col("credit_limit") < 30000, "Premium")
            .otherwise("Elite"))
        .withColumn("monthly_spend_estimate",
            when(col("card_age_months") > 0,
                 round(col("current_balance") / least(col("card_age_months"), lit(12)), 2))
            .otherwise(0))
        .withColumn("rewards_earn_rate_monthly",
            when(col("card_age_months") > 0,
                 round(col("rewards_earned_ytd") / least(col("card_age_months"), lit(12)), 0))
            .otherwise(0))
        .withColumn("is_at_risk",
            when((col("utilization_percent") > 80) |
                 (col("is_delinquent") == True) |
                 (col("missed_payments") >= 2), True)
            .otherwise(False))
        .withColumn("min_payment_to_balance_ratio",
            when(col("statement_balance") > 0,
                 round((col("minimum_payment") / col("statement_balance")) * 100, 2))
            .otherwise(0))
        .withColumn("paying_minimum_only",
            when((col("last_payment_amount") > 0) & 
                 (col("last_payment_amount") <= col("minimum_payment") * 1.1), True)
            .otherwise(False))
        .withColumn("silver_updated_at", current_timestamp())
        .dropDuplicates(["card_id"])
    )

# COMMAND ----------

@dlt.table(
    name="credit_cards_active",
    comment="Active credit cards only"
)
def credit_cards_active():
    """
    Filter for active cards
    """
    return (
        dlt.read("credit_cards_clean")
        .filter(col("card_status") == "Active")
        .filter(col("is_expired") == False)
    )

# COMMAND ----------

@dlt.table(
    name="credit_cards_high_risk",
    comment="High-risk credit cards requiring attention"
)
def credit_cards_high_risk():
    """
    Cards with high utilization or payment issues
    """
    return (
        dlt.read("credit_cards_clean")
        .filter(
            (col("is_at_risk") == True) |
            (col("utilization_percent") > 90) |
            (col("is_delinquent") == True) |
            (col("paying_minimum_only") == True)
        )
        .withColumn("risk_factors",
            array_join(
                array_remove(
                    array(
                        when(col("utilization_percent") > 90, "High Utilization"),
                        when(col("is_delinquent"), "Delinquent"),
                        when(col("paying_minimum_only"), "Minimum Payments Only"),
                        when(col("missed_payments") >= 2, "Multiple Missed Payments")
                    ),
                    None
                ),
                ", "
            ))
    )

# COMMAND ----------

@dlt.table(
    name="credit_cards_portfolio_summary",
    comment="Credit card portfolio metrics"
)
def credit_cards_portfolio_summary():
    """
    Aggregate portfolio statistics by card type
    """
    return (
        dlt.read("credit_cards_clean")
        .groupBy("card_type", "card_status")
        .agg(
            count("*").alias("card_count"),
            sum("credit_limit").alias("total_credit_extended"),
            sum("current_balance").alias("total_outstanding"),
            avg("utilization_percent").alias("avg_utilization"),
            sum("rewards_earned_ytd").alias("total_rewards_earned"),
            sum(when(col("is_delinquent"), 1).otherwise(0)).alias("delinquent_count"),
            sum(when(col("autopay_enabled"), 1).otherwise(0)).alias("autopay_count"),
            avg("apr").alias("avg_apr")
        )
        .withColumn("total_utilization",
            when(col("total_credit_extended") > 0,
                 round((col("total_outstanding") / col("total_credit_extended")) * 100, 2))
            .otherwise(0))
        .withColumn("delinquency_rate",
            round((col("delinquent_count") / col("card_count")) * 100, 2))
        .withColumn("autopay_adoption",
            round((col("autopay_count") / col("card_count")) * 100, 2))
    )

