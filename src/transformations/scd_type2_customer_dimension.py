# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 - Customer Dimension
# MAGIC 
# MAGIC Implements Slowly Changing Dimension Type 2 for customer data to track historical changes.
# MAGIC 
# MAGIC **SCD Type 2 Features:**
# MAGIC - Tracks full history of customer changes
# MAGIC - Maintains `effective_from` and `effective_to` dates
# MAGIC - Uses `is_current` flag for active records
# MAGIC - Generates surrogate keys for versioning
# MAGIC - Preserves all historical attribute values

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "banking_catalog"
BRONZE_SCHEMA = "banking_bronze"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"

# Source and target tables
SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.customers"
TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.dim_customer"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SCD Type 2 Customer Dimension Table

# COMMAND ----------

# Create the dimension table with SCD Type 2 columns
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    -- Surrogate key (unique for each version)
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Natural key (business key - not unique due to history)
    customer_id STRING NOT NULL,
    
    -- Customer attributes (SCD Type 2 - tracked changes)
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email STRING,
    phone STRING,
    date_of_birth TIMESTAMP,
    age INT,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING,
    ssn_last_4 STRING,
    credit_score INT,
    risk_rating STRING,
    customer_segment STRING,
    employment_status STRING,
    occupation STRING,
    annual_income INT,
    account_status STRING,
    
    -- SCD Type 2 metadata columns
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    
    -- Audit columns
    record_created_at TIMESTAMP NOT NULL,
    record_updated_at TIMESTAMP NOT NULL,
    
    -- Additional flags
    kyc_verified BOOLEAN,
    kyc_verification_date TIMESTAMP,
    preferred_contact STRING,
    marketing_opt_in BOOLEAN,
    online_banking_enabled BOOLEAN,
    mobile_app_user BOOLEAN,
    
    -- Source tracking
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Customer Dimension - SCD Type 2 with full history tracking';
""")

print(f"✓ Created SCD Type 2 dimension table: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_source_data():
    """
    Read source customer data from bronze layer
    """
    return spark.table(SOURCE_TABLE).withColumn("source_load_timestamp", current_timestamp())

def get_current_dimension():
    """
    Read current (active) records from dimension table
    """
    try:
        return spark.table(TARGET_TABLE).filter(col("is_current") == True)
    except:
        # Table is empty or doesn't exist
        return spark.createDataFrame([], schema=StructType([]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Merge Logic

# COMMAND ----------

def apply_scd_type2():
    """
    Apply SCD Type 2 logic:
    1. Identify new customers (INSERT)
    2. Identify changed customers (UPDATE old + INSERT new)
    3. Identify unchanged customers (no action)
    """
    
    # Get source and target data
    source_df = get_source_data()
    current_dim_df = get_current_dimension()
    
    # If dimension is empty, do initial load
    if current_dim_df.count() == 0:
        print("📦 Initial load - inserting all customers as current records")
        initial_load = source_df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("email"),
            col("phone"),
            col("date_of_birth"),
            col("age"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("country"),
            col("ssn_last_4"),
            col("credit_score"),
            col("risk_rating"),
            col("customer_segment"),
            col("employment_status"),
            col("occupation"),
            col("annual_income"),
            col("account_status"),
            col("updated_at").alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("kyc_verified"),
            col("kyc_verification_date"),
            col("preferred_contact"),
            col("marketing_opt_in"),
            col("online_banking_enabled"),
            col("mobile_app_user"),
            col("created_at").alias("source_created_at"),
            col("updated_at").alias("source_updated_at")
        )
        
        initial_load.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Loaded {initial_load.count()} customers into dimension")
        return
    
    # Join source with current dimension
    joined_df = source_df.alias("src").join(
        current_dim_df.alias("dim"),
        col("src.customer_id") == col("dim.customer_id"),
        "full_outer"
    )
    
    # Identify changes (compare key attributes that should trigger SCD Type 2)
    changed_customers = joined_df.filter(
        # Customer exists in both but has changes
        col("src.customer_id").isNotNull() & col("dim.customer_id").isNotNull() & (
            (col("src.email") != col("dim.email")) |
            (col("src.phone") != col("dim.phone")) |
            (col("src.address") != col("dim.address")) |
            (col("src.city") != col("dim.city")) |
            (col("src.state") != col("dim.state")) |
            (col("src.zip_code") != col("dim.zip_code")) |
            (col("src.credit_score") != col("dim.credit_score")) |
            (col("src.customer_segment") != col("dim.customer_segment")) |
            (col("src.employment_status") != col("dim.employment_status")) |
            (col("src.annual_income") != col("dim.annual_income")) |
            (col("src.account_status") != col("dim.account_status"))
        )
    )
    
    # Identify new customers (not in dimension)
    new_customers = joined_df.filter(
        col("src.customer_id").isNotNull() & col("dim.customer_id").isNull()
    )
    
    num_changed = changed_customers.count()
    num_new = new_customers.count()
    
    print(f"📊 SCD Type 2 Analysis:")
    print(f"  - New customers: {num_new}")
    print(f"  - Changed customers: {num_changed}")
    
    # Process changed customers: Close old records
    if num_changed > 0:
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forName(spark, TARGET_TABLE)
        
        # Get list of changed customer IDs
        changed_ids = [row.customer_id for row in changed_customers.select("dim.customer_id").distinct().collect()]
        
        # Update old records: set effective_to and is_current = False
        delta_table.update(
            condition = (col("customer_id").isin(changed_ids)) & (col("is_current") == True),
            set = {
                "effective_to": current_timestamp(),
                "is_current": lit(False),
                "record_updated_at": current_timestamp()
            }
        )
        print(f"✓ Closed {num_changed} old records")
        
        # Insert new versions of changed customers
        changed_records = changed_customers.select(
            col("src.customer_id").alias("customer_id"),
            col("src.first_name").alias("first_name"),
            col("src.last_name").alias("last_name"),
            col("src.full_name").alias("full_name"),
            col("src.email").alias("email"),
            col("src.phone").alias("phone"),
            col("src.date_of_birth").alias("date_of_birth"),
            col("src.age").alias("age"),
            col("src.address").alias("address"),
            col("src.city").alias("city"),
            col("src.state").alias("state"),
            col("src.zip_code").alias("zip_code"),
            col("src.country").alias("country"),
            col("src.ssn_last_4").alias("ssn_last_4"),
            col("src.credit_score").alias("credit_score"),
            col("src.risk_rating").alias("risk_rating"),
            col("src.customer_segment").alias("customer_segment"),
            col("src.employment_status").alias("employment_status"),
            col("src.occupation").alias("occupation"),
            col("src.annual_income").alias("annual_income"),
            col("src.account_status").alias("account_status"),
            current_timestamp().alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("src.kyc_verified").alias("kyc_verified"),
            col("src.kyc_verification_date").alias("kyc_verification_date"),
            col("src.preferred_contact").alias("preferred_contact"),
            col("src.marketing_opt_in").alias("marketing_opt_in"),
            col("src.online_banking_enabled").alias("online_banking_enabled"),
            col("src.mobile_app_user").alias("mobile_app_user"),
            col("src.created_at").alias("source_created_at"),
            col("src.updated_at").alias("source_updated_at")
        )
        
        changed_records.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Inserted {num_changed} new versions")
    
    # Process new customers: Insert as current records
    if num_new > 0:
        new_records = new_customers.select(
            col("src.customer_id").alias("customer_id"),
            col("src.first_name").alias("first_name"),
            col("src.last_name").alias("last_name"),
            col("src.full_name").alias("full_name"),
            col("src.email").alias("email"),
            col("src.phone").alias("phone"),
            col("src.date_of_birth").alias("date_of_birth"),
            col("src.age").alias("age"),
            col("src.address").alias("address"),
            col("src.city").alias("city"),
            col("src.state").alias("state"),
            col("src.zip_code").alias("zip_code"),
            col("src.country").alias("country"),
            col("src.ssn_last_4").alias("ssn_last_4"),
            col("src.credit_score").alias("credit_score"),
            col("src.risk_rating").alias("risk_rating"),
            col("src.customer_segment").alias("customer_segment"),
            col("src.employment_status").alias("employment_status"),
            col("src.occupation").alias("occupation"),
            col("src.annual_income").alias("annual_income"),
            col("src.account_status").alias("account_status"),
            col("src.updated_at").alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("src.kyc_verified").alias("kyc_verified"),
            col("src.kyc_verification_date").alias("kyc_verification_date"),
            col("src.preferred_contact").alias("preferred_contact"),
            col("src.marketing_opt_in").alias("marketing_opt_in"),
            col("src.online_banking_enabled").alias("online_banking_enabled"),
            col("src.mobile_app_user").alias("mobile_app_user"),
            col("src.created_at").alias("source_created_at"),
            col("src.updated_at").alias("source_updated_at")
        )
        
        new_records.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Inserted {num_new} new customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute SCD Type 2 Process

# COMMAND ----------

print("🔄 Starting SCD Type 2 process for customer dimension...")
print("=" * 70)

apply_scd_type2()

print("=" * 70)
print("✓ SCD Type 2 process completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Show statistics
result_df = spark.table(TARGET_TABLE)

print("\n📊 Dimension Statistics:")
print(f"Total records (all versions): {result_df.count()}")
print(f"Current records: {result_df.filter(col('is_current') == True).count()}")
print(f"Historical records: {result_df.filter(col('is_current') == False).count()}")

# Show customers with multiple versions (history)
print("\n📜 Customers with History (multiple versions):")
customers_with_history = (
    result_df.groupBy("customer_id")
    .agg(count("*").alias("num_versions"))
    .filter(col("num_versions") > 1)
    .orderBy(desc("num_versions"))
)

customers_with_history.show(10)

# COMMAND ----------

# Show sample of current records
print("\n📋 Sample of Current Customer Records:")
display(
    spark.table(TARGET_TABLE)
    .filter(col("is_current") == True)
    .select(
        "customer_sk",
        "customer_id",
        "full_name",
        "email",
        "credit_score",
        "customer_segment",
        "effective_from",
        "effective_to",
        "is_current"
    )
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Current Customer View
# MAGIC ```sql
# MAGIC SELECT * FROM banking_catalog.banking_gold.dim_customer
# MAGIC WHERE is_current = TRUE;
# MAGIC ```
# MAGIC 
# MAGIC ### Get Customer History
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     customer_sk,
# MAGIC     customer_id,
# MAGIC     full_name,
# MAGIC     credit_score,
# MAGIC     customer_segment,
# MAGIC     effective_from,
# MAGIC     effective_to,
# MAGIC     is_current
# MAGIC FROM banking_catalog.banking_gold.dim_customer
# MAGIC WHERE customer_id = 'CUST-12345'
# MAGIC ORDER BY effective_from DESC;
# MAGIC ```
# MAGIC 
# MAGIC ### Point-in-Time Query (as of specific date)
# MAGIC ```sql
# MAGIC SELECT * FROM banking_catalog.banking_gold.dim_customer
# MAGIC WHERE customer_id = 'CUST-12345'
# MAGIC   AND effective_from <= '2024-01-01'
# MAGIC   AND (effective_to IS NULL OR effective_to > '2024-01-01');
# MAGIC ```

# COMMAND ----------

SELECT '✓ SCD Type 2 Customer Dimension created successfully!' as status;

