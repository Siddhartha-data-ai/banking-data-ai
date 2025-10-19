# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Banking Transactions Data
# MAGIC 
# MAGIC Generates synthetic transaction data for the banking platform including:
# MAGIC - Deposits, withdrawals, transfers
# MAGIC - Card purchases and ATM transactions
# MAGIC - Bill payments and online transfers
# MAGIC - Fraudulent transaction patterns

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
TABLE = "transactions"
ACCOUNT_TABLE = f"{CATALOG}.{SCHEMA}.accounts"
TARGET_TRANSACTIONS = 5_000_000  # Target: 5M transactions as per enterprise requirements
TRANSACTIONS_PER_ACCOUNT = 10  # Average transactions per account (5M / 500K accounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Account Data

# COMMAND ----------

# Load existing accounts
try:
    accounts_df = spark.table(ACCOUNT_TABLE)
    accounts = [(row.account_id, row.customer_id, row.account_type) 
                for row in accounts_df.select("account_id", "customer_id", "account_type").collect()]
    print(f"Loaded {len(accounts)} accounts")
except Exception as e:
    print(f"Error loading accounts: {e}")
    print("Please run generate_accounts_data.py first!")
    dbutils.notebook.exit("Missing account data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Transaction Data

# COMMAND ----------

transaction_types = [
    "Purchase", "ATM Withdrawal", "Deposit", "Transfer", "Bill Payment",
    "Direct Deposit", "Check Deposit", "Wire Transfer", "Online Payment",
    "Refund", "Fee", "Interest Credit", "Card Payment"
]

merchant_categories = [
    "Grocery", "Restaurant", "Gas Station", "Department Store", "Online Retail",
    "Pharmacy", "Electronics", "Clothing", "Entertainment", "Travel",
    "Insurance", "Utilities", "Healthcare", "Education", "Subscription"
]

merchants = {
    "Grocery": ["Walmart", "Target", "Whole Foods", "Kroger", "Safeway"],
    "Restaurant": ["McDonald's", "Starbucks", "Chipotle", "Subway", "Olive Garden"],
    "Gas Station": ["Shell", "BP", "Chevron", "Exxon", "Mobil"],
    "Online Retail": ["Amazon", "eBay", "Etsy", "Wayfair", "Overstock"],
    "Department Store": ["Macy's", "Nordstrom", "Kohl's", "JCPenney", "Dillard's"]
}

channels = ["ATM", "Online", "Mobile App", "Branch", "Phone", "Card"]
statuses = ["Completed", "Completed", "Completed", "Completed", "Pending", "Failed"]

def generate_transaction_for_account(account_id, customer_id, account_type):
    """Generate multiple transactions for an account"""
    transactions = []
    num_transactions = random.randint(20, 100)
    
    for _ in range(num_transactions):
        transaction_id = str(uuid.uuid4())
        trans_type = random.choice(transaction_types)
        
        # Transaction date (last 180 days)
        trans_date = datetime.now() - timedelta(
            days=random.randint(0, 180),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Amount based on transaction type
        if trans_type in ["Purchase", "Online Payment"]:
            amount = round(random.uniform(5, 500), 2)
            is_debit = True
        elif trans_type == "ATM Withdrawal":
            amount = round(random.choice([20, 40, 60, 80, 100, 200]), 2)
            is_debit = True
        elif trans_type in ["Deposit", "Direct Deposit", "Check Deposit"]:
            amount = round(random.uniform(100, 5000), 2)
            is_debit = False
        elif trans_type == "Transfer":
            amount = round(random.uniform(50, 2000), 2)
            is_debit = random.choice([True, False])
        elif trans_type == "Bill Payment":
            amount = round(random.uniform(50, 500), 2)
            is_debit = True
        elif trans_type == "Wire Transfer":
            amount = round(random.uniform(1000, 10000), 2)
            is_debit = random.choice([True, False])
        elif trans_type == "Fee":
            amount = round(random.choice([5, 10, 15, 25, 35]), 2)
            is_debit = True
        elif trans_type == "Interest Credit":
            amount = round(random.uniform(1, 50), 2)
            is_debit = False
        else:
            amount = round(random.uniform(10, 1000), 2)
            is_debit = random.choice([True, False])
        
        # Merchant info
        category = random.choice(merchant_categories)
        if category in merchants:
            merchant_name = random.choice(merchants[category])
        else:
            merchant_name = f"{category} Merchant"
        
        # Channel
        if trans_type == "ATM Withdrawal":
            channel = "ATM"
        elif trans_type in ["Online Payment", "Bill Payment"]:
            channel = random.choice(["Online", "Mobile App"])
        elif trans_type in ["Purchase", "Card Payment"]:
            channel = "Card"
        else:
            channel = random.choice(channels)
        
        # Status
        status = random.choice(statuses)
        
        # Fraud indicators (2% fraudulent)
        is_fraud = random.random() < 0.02
        if is_fraud:
            # Fraudulent transactions have certain patterns
            amount = round(random.uniform(500, 5000), 2)  # Higher amounts
            fraud_score = round(random.uniform(0.7, 1.0), 3)
            # Unusual times (late night)
            trans_date = trans_date.replace(hour=random.randint(0, 4))
        else:
            fraud_score = round(random.uniform(0.0, 0.3), 3)
        
        # Location
        is_international = random.random() < 0.05
        if is_international:
            country = random.choice(["Canada", "Mexico", "UK", "France", "Germany", "Japan"])
        else:
            country = "USA"
        
        transactions.append({
            "transaction_id": transaction_id,
            "account_id": account_id,
            "customer_id": customer_id,
            "transaction_date": trans_date,
            "transaction_type": trans_type,
            "amount": amount,
            "is_debit": is_debit,
            "balance_after": 0.0,  # Will be calculated in silver layer
            "merchant_name": merchant_name if trans_type in ["Purchase", "Online Payment"] else None,
            "merchant_category": category if trans_type in ["Purchase", "Online Payment"] else None,
            "merchant_id": f"MER{random.randint(100000, 999999)}" if trans_type in ["Purchase", "Online Payment"] else None,
            "channel": channel,
            "status": status,
            "description": f"{trans_type} - {merchant_name}" if merchant_name else trans_type,
            "reference_number": f"REF{random.randint(1000000000, 9999999999)}",
            "authorization_code": f"AUTH{random.randint(100000, 999999)}" if channel == "Card" else None,
            "card_last_4": f"{random.randint(1000, 9999)}" if channel == "Card" else None,
            "location_city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
            "location_state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
            "location_country": country,
            "is_international": is_international,
            "is_fraud": is_fraud,
            "fraud_score": fraud_score,
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "device_id": f"DEV{uuid.uuid4().hex[:12]}" if channel in ["Online", "Mobile App"] else None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    return transactions

# COMMAND ----------

# Generate transactions for all accounts
print(f"Generating transactions for {len(accounts)} accounts...")
print(f"Target: {TARGET_TRANSACTIONS:,} transactions (~{TRANSACTIONS_PER_ACCOUNT} per account)")

all_transactions = []
for i, (account_id, customer_id, account_type) in enumerate(accounts):
    if i % 10000 == 0 and i > 0:
        print(f"  Processed {i:,}/{len(accounts):,} accounts, generated {len(all_transactions):,} transactions...")
    all_transactions.extend(generate_transaction_for_account(account_id, customer_id, account_type))

print(f"Generated {len(all_transactions):,} total transactions")

# COMMAND ----------

# Create DataFrame
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_date", TimestampType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("is_debit", BooleanType(), True),
    StructField("balance_after", DoubleType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("authorization_code", StringType(), True),
    StructField("card_last_4", StringType(), True),
    StructField("location_city", StringType(), True),
    StructField("location_state", StringType(), True),
    StructField("location_country", StringType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.createDataFrame(all_transactions, schema)

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
print(f"Total Transactions: {df.count()}")
print(f"Unique Transaction IDs: {df.select('transaction_id').distinct().count()}")
print(f"Date Range: {df.agg(min('transaction_date'), max('transaction_date')).collect()[0]}")
print(f"Total Volume: ${df.agg(sum('amount')).collect()[0][0]:,.2f}")
print(f"Fraudulent Transactions: {df.filter(col('is_fraud') == True).count()}")
print("\nTransaction Types:")
df.groupBy("transaction_type").agg(
    count("*").alias("count"),
    sum("amount").alias("total_amount")
).orderBy(desc("count")).show()
print("\nChannel Distribution:")
df.groupBy("channel").count().orderBy(desc("count")).show()
print("\nFraud Detection:")
df.groupBy("is_fraud").count().show()

