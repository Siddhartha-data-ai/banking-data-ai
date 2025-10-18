# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Accounts Pipeline
# MAGIC 
# MAGIC Transforms and cleanses account data from bronze to silver layer with:
# MAGIC - Balance validations
# MAGIC - Account health metrics
# MAGIC - Activity categorization
# MAGIC - Risk indicators

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name="accounts_clean",
    comment="Cleaned and validated account data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "account_id,customer_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("non_negative_balance", "balance >= 0 OR account_status = 'Closed'")
@dlt.expect("valid_interest_rate", "interest_rate >= 0 AND interest_rate <= 20")
def accounts_clean():
    """
    Transform bronze accounts to silver with data quality rules
    """
    return (
        dlt.read("banking_bronze.accounts")
        .withColumn("account_age_days",
            datediff(current_timestamp(), col("account_open_date")))
        .withColumn("account_age_years",
            round(col("account_age_days") / 365, 2))
        .withColumn("days_since_last_transaction",
            datediff(current_timestamp(), col("last_transaction_date")))
        .withColumn("is_dormant",
            when(col("days_since_last_transaction") > 180, True)
            .otherwise(False))
        .withColumn("balance_category",
            when(col("balance") < 1000, "Low")
            .when(col("balance") < 10000, "Medium")
            .when(col("balance") < 50000, "High")
            .otherwise("Very High"))
        .withColumn("has_overdraft_risk",
            when((col("overdraft_protection") == False) & 
                 (col("available_balance") < col("minimum_balance")), True)
            .otherwise(False))
        .withColumn("account_health_score",
            when(col("balance") >= col("minimum_balance") * 2, 100)
            .when(col("balance") >= col("minimum_balance"), 75)
            .when(col("balance") >= col("minimum_balance") * 0.5, 50)
            .otherwise(25))
        .withColumn("monthly_activity_level",
            when(col("transaction_count_30d") > 50, "High")
            .when(col("transaction_count_30d") > 20, "Medium")
            .when(col("transaction_count_30d") > 5, "Low")
            .otherwise("Inactive"))
        .withColumn("silver_updated_at", current_timestamp())
        .dropDuplicates(["account_id"])
    )

# COMMAND ----------

@dlt.table(
    name="accounts_active",
    comment="Active accounts only"
)
def accounts_active():
    """
    Filter for active accounts
    """
    return (
        dlt.read("accounts_clean")
        .filter(col("account_status") == "Active")
        .filter(col("is_dormant") == False)
    )

# COMMAND ----------

@dlt.table(
    name="accounts_at_risk",
    comment="Accounts requiring attention"
)
def accounts_at_risk():
    """
    Identify accounts at risk of closure or overdraft
    """
    return (
        dlt.read("accounts_clean")
        .filter(
            (col("has_overdraft_risk") == True) |
            (col("is_dormant") == True) |
            (col("balance") < col("minimum_balance"))
        )
        .withColumn("risk_reasons",
            when(col("has_overdraft_risk"), "Overdraft Risk")
            .when(col("is_dormant"), "Dormant Account")
            .when(col("balance") < col("minimum_balance"), "Below Minimum Balance")
            .otherwise("Other"))
    )

