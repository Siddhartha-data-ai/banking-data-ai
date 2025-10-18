# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 - Account Dimension
# MAGIC 
# MAGIC Implements Slowly Changing Dimension Type 2 for account data to track historical changes.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

CATALOG = "banking_catalog"
BRONZE_SCHEMA = "banking_bronze"
GOLD_SCHEMA = "banking_gold"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.accounts"
TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.dim_account"

# COMMAND ----------

# Create Account Dimension with SCD Type 2
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    -- Surrogate key
    account_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Natural key
    account_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    
    -- Account attributes
    account_type STRING,
    account_number STRING,
    routing_number STRING,
    account_status STRING,
    balance DOUBLE,
    available_balance DOUBLE,
    currency STRING,
    interest_rate DOUBLE,
    minimum_balance DOUBLE,
    overdraft_protection BOOLEAN,
    overdraft_limit DOUBLE,
    monthly_fee DOUBLE,
    branch_id STRING,
    account_open_date TIMESTAMP,
    
    -- SCD Type 2 metadata
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    
    -- Audit
    record_created_at TIMESTAMP NOT NULL,
    record_updated_at TIMESTAMP NOT NULL,
    
    -- Additional attributes
    online_access_enabled BOOLEAN,
    paper_statements BOOLEAN,
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Account Dimension - SCD Type 2';
""")

print(f"✓ Created SCD Type 2 account dimension: {TARGET_TABLE}")

# COMMAND ----------

def apply_account_scd_type2():
    """Apply SCD Type 2 for accounts"""
    source_df = spark.table(SOURCE_TABLE)
    
    try:
        current_dim_df = spark.table(TARGET_TABLE).filter(col("is_current") == True)
    except:
        current_dim_df = spark.createDataFrame([], schema=StructType([]))
    
    # Initial load
    if current_dim_df.count() == 0:
        print("📦 Initial load for account dimension")
        initial = source_df.select(
            col("account_id"),
            col("customer_id"),
            col("account_type"),
            col("account_number"),
            col("routing_number"),
            col("account_status"),
            col("balance"),
            col("available_balance"),
            col("currency"),
            col("interest_rate"),
            col("minimum_balance"),
            col("overdraft_protection"),
            col("overdraft_limit"),
            col("monthly_fee"),
            col("branch_id"),
            col("account_open_date"),
            col("updated_at").alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("online_access_enabled"),
            col("paper_statements"),
            col("created_at").alias("source_created_at"),
            col("updated_at").alias("source_updated_at")
        )
        initial.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Loaded {initial.count()} accounts")
        return
    
    # Detect changes
    joined = source_df.alias("src").join(
        current_dim_df.alias("dim"),
        col("src.account_id") == col("dim.account_id"),
        "full_outer"
    )
    
    changed = joined.filter(
        col("src.account_id").isNotNull() & col("dim.account_id").isNotNull() & (
            (col("src.account_status") != col("dim.account_status")) |
            (col("src.balance") != col("dim.balance")) |
            (col("src.interest_rate") != col("dim.interest_rate")) |
            (col("src.overdraft_protection") != col("dim.overdraft_protection"))
        )
    )
    
    new = joined.filter(
        col("src.account_id").isNotNull() & col("dim.account_id").isNull()
    )
    
    num_changed = changed.count()
    num_new = new.count()
    
    print(f"📊 Changed: {num_changed}, New: {num_new}")
    
    if num_changed > 0:
        delta_table = DeltaTable.forName(spark, TARGET_TABLE)
        changed_ids = [row.account_id for row in changed.select("dim.account_id").distinct().collect()]
        
        delta_table.update(
            condition = (col("account_id").isin(changed_ids)) & (col("is_current") == True),
            set = {
                "effective_to": current_timestamp(),
                "is_current": lit(False),
                "record_updated_at": current_timestamp()
            }
        )
        
        changed_records = changed.select(
            col("src.account_id").alias("account_id"),
            col("src.customer_id").alias("customer_id"),
            col("src.account_type").alias("account_type"),
            col("src.account_number").alias("account_number"),
            col("src.routing_number").alias("routing_number"),
            col("src.account_status").alias("account_status"),
            col("src.balance").alias("balance"),
            col("src.available_balance").alias("available_balance"),
            col("src.currency").alias("currency"),
            col("src.interest_rate").alias("interest_rate"),
            col("src.minimum_balance").alias("minimum_balance"),
            col("src.overdraft_protection").alias("overdraft_protection"),
            col("src.overdraft_limit").alias("overdraft_limit"),
            col("src.monthly_fee").alias("monthly_fee"),
            col("src.branch_id").alias("branch_id"),
            col("src.account_open_date").alias("account_open_date"),
            current_timestamp().alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("src.online_access_enabled").alias("online_access_enabled"),
            col("src.paper_statements").alias("paper_statements"),
            col("src.created_at").alias("source_created_at"),
            col("src.updated_at").alias("source_updated_at")
        )
        changed_records.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Updated {num_changed} accounts")
    
    if num_new > 0:
        new_records = new.select(
            col("src.account_id").alias("account_id"),
            col("src.customer_id").alias("customer_id"),
            col("src.account_type").alias("account_type"),
            col("src.account_number").alias("account_number"),
            col("src.routing_number").alias("routing_number"),
            col("src.account_status").alias("account_status"),
            col("src.balance").alias("balance"),
            col("src.available_balance").alias("available_balance"),
            col("src.currency").alias("currency"),
            col("src.interest_rate").alias("interest_rate"),
            col("src.minimum_balance").alias("minimum_balance"),
            col("src.overdraft_protection").alias("overdraft_protection"),
            col("src.overdraft_limit").alias("overdraft_limit"),
            col("src.monthly_fee").alias("monthly_fee"),
            col("src.branch_id").alias("branch_id"),
            col("src.account_open_date").alias("account_open_date"),
            col("src.updated_at").alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            current_timestamp().alias("record_created_at"),
            current_timestamp().alias("record_updated_at"),
            col("src.online_access_enabled").alias("online_access_enabled"),
            col("src.paper_statements").alias("paper_statements"),
            col("src.created_at").alias("source_created_at"),
            col("src.updated_at").alias("source_updated_at")
        )
        new_records.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"✓ Inserted {num_new} new accounts")

# COMMAND ----------

print("🔄 Applying SCD Type 2 for Account Dimension...")
apply_account_scd_type2()
print("✓ Account dimension updated successfully!")

# COMMAND ----------

# Show results
result = spark.table(TARGET_TABLE)
print(f"\nTotal records: {result.count()}")
print(f"Current records: {result.filter(col('is_current') == True).count()}")
display(result.filter(col("is_current") == True).limit(10))

