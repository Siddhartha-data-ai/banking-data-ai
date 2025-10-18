# Databricks notebook source
# MAGIC %md
# MAGIC # Build Star Schema Dimensions
# MAGIC 
# MAGIC Populates dimension tables for the star schema

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

# COMMAND ----------

CATALOG = "banking_catalog"
SILVER = "banking_silver"
GOLD = "banking_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Date Dimension

# COMMAND ----------

def build_date_dimension(start_date='2020-01-01', end_date='2030-12-31'):
    """Generate date dimension for analytics"""
    
    # Generate date range
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    date_list = []
    current = start
    
    while current <= end:
        date_key = int(current.strftime('%Y%m%d'))
        
        date_list.append({
            'date_key': date_key,
            'date_value': current,
            'year': current.year,
            'quarter': (current.month - 1) // 3 + 1,
            'month': current.month,
            'month_name': current.strftime('%B'),
            'week': current.isocalendar()[1],
            'day_of_month': current.day,
            'day_of_week': current.isoweekday(),
            'day_name': current.strftime('%A'),
            'is_weekend': current.isoweekday() in [6, 7],
            'is_holiday': False,  # Can be enhanced with holiday calendar
            'fiscal_year': current.year if current.month >= 7 else current.year - 1,
            'fiscal_quarter': ((current.month - 7) % 12) // 3 + 1
        })
        
        current += timedelta(days=1)
    
    df = spark.createDataFrame(date_list)
    
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.dim_date")
    
    print(f"✓ Created date dimension with {df.count()} dates")
    return df

# Execute
build_date_dimension()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Merchant Dimension

# COMMAND ----------

def build_merchant_dimension():
    """Extract unique merchants from transactions"""
    
    transactions = spark.table(f"{CATALOG}.{SILVER}.transactions_clean")
    
    merchants = transactions.select(
        col("merchant_id"),
        col("merchant_name"),
        col("merchant_category")
    ).distinct()
    
    merchant_dim = merchants.select(
        col("merchant_id"),
        col("merchant_name"),
        col("merchant_category"),
        when(col("merchant_category").isin(['Gambling', 'Adult Entertainment', 'Crypto']), 'High Risk')
            .when(col("merchant_category").isin(['Gas Station', 'Grocery', 'Pharmacy']), 'Low Risk')
            .otherwise('Medium Risk').alias("merchant_type"),
        when(col("merchant_category").isin(['Gambling', 'Adult Entertainment', 'Crypto']), 'High')
            .otherwise('Low').alias("risk_level"),
        when(col("merchant_category").isin(['Gambling', 'Adult Entertainment', 'Crypto']), True)
            .otherwise(False).alias("is_high_risk"),
        current_timestamp().alias("effective_from"),
        lit(None).cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current")
    )
    
    merchant_dim.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.dim_merchant")
    
    print(f"✓ Created merchant dimension with {merchant_dim.count()} merchants")

build_merchant_dimension()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Product Dimension

# COMMAND ----------

def build_product_dimension():
    """Create banking product dimension"""
    
    products_data = [
        {
            'product_id': 'PROD-001',
            'product_name': 'Basic Checking',
            'product_type': 'Checking',
            'product_category': 'Deposit Account',
            'interest_rate_min': 0.0,
            'interest_rate_max': 0.1,
            'minimum_balance': 0.0,
            'monthly_fee': 5.0
        },
        {
            'product_id': 'PROD-002',
            'product_name': 'Premium Checking',
            'product_type': 'Checking',
            'product_category': 'Deposit Account',
            'interest_rate_min': 0.1,
            'interest_rate_max': 0.5,
            'minimum_balance': 1000.0,
            'monthly_fee': 0.0
        },
        {
            'product_id': 'PROD-003',
            'product_name': 'Savings Account',
            'product_type': 'Savings',
            'product_category': 'Deposit Account',
            'interest_rate_min': 0.5,
            'interest_rate_max': 2.0,
            'minimum_balance': 100.0,
            'monthly_fee': 0.0
        },
        {
            'product_id': 'PROD-004',
            'product_name': 'Personal Loan',
            'product_type': 'Personal Loan',
            'product_category': 'Lending',
            'interest_rate_min': 5.0,
            'interest_rate_max': 18.0,
            'minimum_balance': 0.0,
            'monthly_fee': 0.0
        },
        {
            'product_id': 'PROD-005',
            'product_name': 'Credit Card - Standard',
            'product_type': 'Credit Card',
            'product_category': 'Credit',
            'interest_rate_min': 15.0,
            'interest_rate_max': 25.0,
            'minimum_balance': 0.0,
            'monthly_fee': 0.0
        }
    ]
    
    products_df = spark.createDataFrame(products_data)
    
    products_dim = products_df.withColumn("effective_from", current_timestamp()) \
        .withColumn("effective_to", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True))
    
    products_dim.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.dim_product")
    
    print(f"✓ Created product dimension with {products_dim.count()} products")

build_product_dimension()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Branch Dimension

# COMMAND ----------

def build_branch_dimension():
    """Extract unique branches from accounts"""
    
    accounts = spark.table(f"{CATALOG}.{SILVER}.accounts_clean")
    
    branches = accounts.select("branch_id").distinct().filter(col("branch_id").isNotNull())
    
    branch_dim = branches.select(
        col("branch_id"),
        concat(lit("Branch "), col("branch_id")).alias("branch_name"),
        lit("Full Service").alias("branch_type"),
        lit("123 Main St").alias("address"),
        lit("New York").alias("city"),
        lit("NY").alias("state"),
        lit("10001").alias("zip_code"),
        lit("Northeast").alias("region"),
        lit("Manager Name").alias("manager_name"),
        lit("+1-555-0100").alias("phone"),
        current_timestamp().alias("effective_from"),
        lit(None).cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current")
    )
    
    branch_dim.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.dim_branch")
    
    print(f"✓ Created branch dimension with {branch_dim.count()} branches")

build_branch_dimension()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Dimensions

# COMMAND ----------

print("\n📊 Dimension Table Summary:")
print("=" * 70)

for table in ['dim_date', 'dim_customer', 'dim_account', 'dim_merchant', 'dim_product', 'dim_branch']:
    try:
        count = spark.table(f"{CATALOG}.{GOLD}.{table}").count()
        print(f"✓ {table}: {count:,} records")
    except Exception as e:
        print(f"⚠ {table}: Not yet created (will be created by SCD Type 2 scripts)")

print("=" * 70)
print("✓ Star schema dimensions built successfully!")

