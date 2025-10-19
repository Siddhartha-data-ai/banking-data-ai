# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Banking Accounts Data
# MAGIC 
# MAGIC Generates synthetic account data for the banking platform including:
# MAGIC - Checking and savings accounts
# MAGIC - Account balances and status
# MAGIC - Interest rates and fees
# MAGIC - Account activity metrics

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
SCHEMA = "banking_bronze"
TABLE = "accounts"
CUSTOMER_TABLE = f"{CATALOG}.{SCHEMA}.customers"

# COMMAND ----------

# Configuration for target account count
TARGET_ACCOUNTS = 500_000  # Target: 500K accounts as per enterprise requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer Data

# COMMAND ----------

# Load existing customers
try:
    customers_df = spark.table(CUSTOMER_TABLE)
    customer_ids = [row.customer_id for row in customers_df.select("customer_id").collect()]
    print(f"Loaded {len(customer_ids)} customers")
    
    # Calculate how many customers need accounts to reach target
    # Average 1.65 accounts per customer (based on weights)
    customers_needed = int(TARGET_ACCOUNTS / 1.65)
    if customers_needed < len(customer_ids):
        customer_ids = random.sample(customer_ids, customers_needed)
        print(f"Selected {len(customer_ids)} customers to generate ~{TARGET_ACCOUNTS:,} accounts")
except Exception as e:
    print(f"Error loading customers: {e}")
    print("Please run generate_customers_data.py first!")
    dbutils.notebook.exit("Missing customer data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Account Data

# COMMAND ----------

account_types = ["Checking", "Savings", "Money Market", "Certificate of Deposit"]
account_status = ["Active", "Active", "Active", "Active", "Active", "Dormant", "Frozen", "Closed"]
branch_ids = [f"BR{str(i).zfill(4)}" for i in range(1, 51)]  # 50 branches

def generate_account_for_customer(customer_id):
    """Generate 1-3 accounts per customer"""
    accounts = []
    num_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    
    for i in range(num_accounts):
        account_id = f"ACC{random.randint(1000000000, 9999999999)}"
        acc_type = random.choice(account_types)
        
        # Account open date (within customer tenure)
        account_open_date = datetime.now() - timedelta(days=random.randint(30, 3650))
        
        # Balance based on account type
        if acc_type == "Checking":
            balance = round(random.uniform(100, 50000), 2)
            interest_rate = 0.01
            min_balance = 500
        elif acc_type == "Savings":
            balance = round(random.uniform(500, 100000), 2)
            interest_rate = round(random.uniform(0.5, 2.5), 2)
            min_balance = 1000
        elif acc_type == "Money Market":
            balance = round(random.uniform(5000, 250000), 2)
            interest_rate = round(random.uniform(1.5, 4.0), 2)
            min_balance = 5000
        else:  # CD
            balance = round(random.uniform(10000, 500000), 2)
            interest_rate = round(random.uniform(2.5, 5.5), 2)
            min_balance = 10000
        
        # Calculate available balance (some on hold)
        hold_amount = round(random.uniform(0, balance * 0.1), 2) if random.random() > 0.7 else 0
        available_balance = balance - hold_amount
        
        # Status (most active)
        status = random.choice(account_status)
        if status == "Closed":
            balance = 0
            available_balance = 0
        
        # Overdraft settings for checking
        overdraft_protection = acc_type == "Checking" and random.random() > 0.3
        overdraft_limit = random.choice([500, 1000, 2500, 5000]) if overdraft_protection else 0
        
        accounts.append({
            "account_id": account_id,
            "customer_id": customer_id,
            "account_type": acc_type,
            "account_number": f"{random.randint(100000, 999999)}{random.randint(1000, 9999)}",
            "routing_number": f"0{random.randint(10000000, 99999999)}",
            "account_status": status,
            "balance": balance,
            "available_balance": available_balance,
            "hold_amount": hold_amount,
            "currency": "USD",
            "interest_rate": interest_rate,
            "minimum_balance": min_balance,
            "overdraft_protection": overdraft_protection,
            "overdraft_limit": overdraft_limit,
            "monthly_fee": random.choice([0, 5, 10, 15, 25]) if acc_type == "Checking" else 0,
            "branch_id": random.choice(branch_ids),
            "account_open_date": account_open_date,
            "last_transaction_date": datetime.now() - timedelta(days=random.randint(0, 90)),
            "transaction_count_30d": random.randint(0, 100) if status == "Active" else 0,
            "average_daily_balance": round(balance * random.uniform(0.7, 1.3), 2),
            "year_to_date_interest": round(balance * interest_rate * 0.01 * (datetime.now().month / 12), 2),
            "is_joint_account": random.random() > 0.85,
            "online_access_enabled": random.random() > 0.1,
            "paper_statements": random.random() > 0.7,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    return accounts

# COMMAND ----------

# Generate accounts for all customers
print(f"Generating accounts for {len(customer_ids)} customers...")
all_accounts = []
for customer_id in customer_ids:
    all_accounts.extend(generate_account_for_customer(customer_id))

print(f"Generated {len(all_accounts)} total accounts")

# COMMAND ----------

# Create DataFrame
schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("routing_number", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("available_balance", DoubleType(), True),
    StructField("hold_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("minimum_balance", DoubleType(), True),
    StructField("overdraft_protection", BooleanType(), True),
    StructField("overdraft_limit", DoubleType(), True),
    StructField("monthly_fee", DoubleType(), True),
    StructField("branch_id", StringType(), True),
    StructField("account_open_date", TimestampType(), True),
    StructField("last_transaction_date", TimestampType(), True),
    StructField("transaction_count_30d", IntegerType(), True),
    StructField("average_daily_balance", DoubleType(), True),
    StructField("year_to_date_interest", DoubleType(), True),
    StructField("is_joint_account", BooleanType(), True),
    StructField("online_access_enabled", BooleanType(), True),
    StructField("paper_statements", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.createDataFrame(all_accounts, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Bronze Layer

# COMMAND ----------

# Write to Delta table
table_path = f"{CATALOG}.{SCHEMA}.{TABLE}"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_path)

print(f"✓ Successfully created {table_path}")
print(f"✓ Total records: {df.count()}")

# COMMAND ----------

# Show sample data
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("Data Quality Summary:")
print(f"Total Accounts: {df.count()}")
print(f"Unique Account IDs: {df.select('account_id').distinct().count()}")
print(f"Unique Customers: {df.select('customer_id').distinct().count()}")
print(f"Total Balance: ${df.agg(sum('balance')).collect()[0][0]:,.2f}")
print(f"Average Balance: ${df.agg(avg('balance')).collect()[0][0]:,.2f}")
print("\nAccount Type Distribution:")
df.groupBy("account_type").agg(
    count("*").alias("count"),
    sum("balance").alias("total_balance")
).orderBy(desc("count")).show()
print("\nAccount Status:")
df.groupBy("account_status").count().orderBy(desc("count")).show()

