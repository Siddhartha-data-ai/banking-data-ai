# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Customers Pipeline
# MAGIC 
# MAGIC Transforms and cleanses customer data from bronze to silver layer with:
# MAGIC - Data quality validations
# MAGIC - Standardization and formatting
# MAGIC - Deduplication
# MAGIC - PII masking options

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
BRONZE_SCHEMA = "banking_bronze"
SILVER_SCHEMA = "banking_silver"

# COMMAND ----------

@dlt.table(
    name="customers_clean",
    comment="Cleaned and validated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id,customer_segment",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect_or_drop("valid_age", "age >= 18 AND age <= 100")
@dlt.expect("valid_credit_score", "credit_score BETWEEN 300 AND 850")
def customers_clean():
    """
    Transform bronze customers to silver with data quality rules
    """
    return (
        dlt.read("banking_bronze.customers")
        .withColumn("full_name", 
            concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9+]", ""))
        .withColumn("ssn_last_4_masked", concat(lit("XXX-XX-"), col("ssn_last_4")))
        .withColumn("customer_tenure_days", 
            datediff(current_timestamp(), col("account_open_date")))
        .withColumn("customer_tenure_years", 
            round(col("customer_tenure_days") / 365, 2))
        .withColumn("is_high_value", 
            when((col("credit_score") >= 750) & (col("annual_income") >= 100000), True)
            .otherwise(False))
        .withColumn("risk_category",
            when(col("risk_rating") == "Low", 1)
            .when(col("risk_rating") == "Medium", 2)
            .when(col("risk_rating") == "High", 3)
            .otherwise(4))
        .withColumn("age_group",
            when(col("age") < 25, "18-24")
            .when(col("age") < 35, "25-34")
            .when(col("age") < 45, "35-44")
            .when(col("age") < 55, "45-54")
            .when(col("age") < 65, "55-64")
            .otherwise("65+"))
        .withColumn("income_bracket",
            when(col("annual_income") < 30000, "Low")
            .when(col("annual_income") < 75000, "Medium")
            .when(col("annual_income") < 150000, "High")
            .otherwise("Very High"))
        .withColumn("silver_updated_at", current_timestamp())
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

@dlt.table(
    name="customers_kyc_validated",
    comment="KYC validated customers only"
)
def customers_kyc_validated():
    """
    Filter for KYC verified customers
    """
    return (
        dlt.read("customers_clean")
        .filter(col("kyc_verified") == True)
        .filter(col("account_status") == "Active")
    )

# COMMAND ----------

@dlt.table(
    name="customers_premium",
    comment="Premium tier customers"
)
def customers_premium():
    """
    Premium segment customers for targeted services
    """
    return (
        dlt.read("customers_clean")
        .filter(col("customer_segment").isin(["Premium", "Gold"]))
        .filter(col("credit_score") >= 700)
    )

