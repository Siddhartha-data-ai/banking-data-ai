# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Loans Pipeline
# MAGIC 
# MAGIC Transforms and cleanses loan data from bronze to silver layer with:
# MAGIC - Payment performance metrics
# MAGIC - Risk categorization
# MAGIC - Delinquency tracking
# MAGIC - Portfolio analysis

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name="loans_clean",
    comment="Cleaned and validated loan data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "loan_id,customer_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_loan_id", "loan_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_interest_rate", "interest_rate >= 0 AND interest_rate <= 30")
@dlt.expect("valid_amounts", "loan_amount > 0 AND remaining_balance >= 0")
def loans_clean():
    """
    Transform bronze loans to silver with data quality rules
    """
    return (
        dlt.read("banking_bronze.loans")
        .withColumn("loan_age_months",
            when(col("origination_date").isNotNull(),
                 months_between(current_timestamp(), col("origination_date")))
            .otherwise(0))
        .withColumn("months_remaining",
            when(col("maturity_date").isNotNull(),
                 months_between(col("maturity_date"), current_timestamp()))
            .otherwise(0))
        .withColumn("percent_paid",
            when(col("loan_amount") > 0,
                 round((col("total_paid") / col("loan_amount")) * 100, 2))
            .otherwise(0))
        .withColumn("payment_performance",
            when(col("missed_payments") == 0, "Excellent")
            .when(col("missed_payments") <= 1, "Good")
            .when(col("missed_payments") <= 3, "Fair")
            .otherwise("Poor"))
        .withColumn("delinquency_status",
            when(col("days_past_due") == 0, "Current")
            .when(col("days_past_due") <= 30, "30 Days")
            .when(col("days_past_due") <= 60, "60 Days")
            .when(col("days_past_due") <= 90, "90 Days")
            .when(col("days_past_due") > 90, "90+ Days")
            .otherwise("Current"))
        .withColumn("default_risk_category",
            when((col("is_default") == True) | (col("days_past_due") > 120), "High")
            .when((col("is_delinquent") == True) | (col("missed_payments") >= 2), "Medium")
            .when(col("risk_score") < 600, "Medium")
            .otherwise("Low"))
        .withColumn("loan_status_clean",
            when(col("loan_status") == "In Default", "Default")
            .when(col("days_past_due") > 90, "Serious Delinquency")
            .when(col("is_delinquent"), "Delinquent")
            .otherwise(col("loan_status")))
        .withColumn("estimated_payoff_date",
            when((col("remaining_balance") > 0) & (col("monthly_payment") > 0),
                 add_months(current_timestamp(), 
                           cast(col("remaining_balance") / col("monthly_payment") as IntegerType())))
            .otherwise(None))
        .withColumn("is_at_risk",
            when((col("missed_payments") >= 2) | 
                 (col("days_past_due") > 30) | 
                 (col("debt_to_income_ratio") > 0.43), True)
            .otherwise(False))
        .withColumn("ltv_category",
            when(col("loan_to_value_ratio") > 0.9, "High")
            .when(col("loan_to_value_ratio") > 0.8, "Medium")
            .otherwise("Low"))
        .withColumn("silver_updated_at", current_timestamp())
        .dropDuplicates(["loan_id"])
    )

# COMMAND ----------

@dlt.table(
    name="loans_active",
    comment="Active loans only"
)
def loans_active():
    """
    Filter for active loans
    """
    return (
        dlt.read("loans_clean")
        .filter(col("loan_status_clean") == "Active")
        .filter(col("remaining_balance") > 0)
    )

# COMMAND ----------

@dlt.table(
    name="loans_delinquent",
    comment="Delinquent loans requiring attention"
)
def loans_delinquent():
    """
    Loans with payment issues
    """
    return (
        dlt.read("loans_clean")
        .filter(
            (col("is_delinquent") == True) |
            (col("is_default") == True) |
            (col("days_past_due") > 0)
        )
        .withColumn("severity",
            when(col("days_past_due") > 90, "Critical")
            .when(col("days_past_due") > 60, "Severe")
            .when(col("days_past_due") > 30, "Moderate")
            .otherwise("Minor"))
    )

# COMMAND ----------

@dlt.table(
    name="loans_portfolio_summary",
    comment="Loan portfolio metrics by type"
)
def loans_portfolio_summary():
    """
    Aggregate portfolio statistics
    """
    return (
        dlt.read("loans_clean")
        .groupBy("loan_type", "loan_status_clean")
        .agg(
            count("*").alias("loan_count"),
            sum("loan_amount").alias("total_originated"),
            sum("remaining_balance").alias("total_outstanding"),
            avg("interest_rate").alias("avg_interest_rate"),
            sum("total_paid").alias("total_collected"),
            sum(when(col("is_delinquent"), 1).otherwise(0)).alias("delinquent_count"),
            sum(when(col("is_default"), 1).otherwise(0)).alias("default_count"),
            avg("risk_score").alias("avg_risk_score")
        )
        .withColumn("delinquency_rate", 
            round((col("delinquent_count") / col("loan_count")) * 100, 2))
        .withColumn("default_rate",
            round((col("default_count") / col("loan_count")) * 100, 2))
    )

