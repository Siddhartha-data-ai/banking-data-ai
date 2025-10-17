# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Credit Cards Data
# MAGIC 
# MAGIC Generates synthetic credit card data for the banking platform including:
# MAGIC - Card types (Platinum, Gold, Silver, Basic)
# MAGIC - Credit limits and utilization
# MAGIC - Rewards programs and benefits
# MAGIC - Card status and payment information

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
TABLE = "credit_cards"
CUSTOMER_TABLE = f"{CATALOG}.{SCHEMA}.customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer Data

# COMMAND ----------

# Load existing customers
try:
    customers_df = spark.table(CUSTOMER_TABLE)
    customers = [(row.customer_id, row.credit_score, row.annual_income) 
                 for row in customers_df.select("customer_id", "credit_score", "annual_income").collect()]
    print(f"Loaded {len(customers)} customers")
except Exception as e:
    print(f"Error loading customers: {e}")
    print("Please run generate_customers_data.py first!")
    dbutils.notebook.exit("Missing customer data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Credit Card Data

# COMMAND ----------

card_types = ["Platinum", "Gold", "Silver", "Basic"]
card_networks = ["Visa", "Mastercard", "American Express", "Discover"]
card_statuses = ["Active", "Active", "Active", "Active", "Blocked", "Expired", "Closed"]
rewards_programs = ["Cash Back", "Travel Points", "Airline Miles", "Hotel Points", "No Rewards"]

def get_card_tier_details(credit_score, annual_income):
    """Determine card tier based on credit score and income"""
    if credit_score >= 750 and annual_income >= 100000:
        return "Platinum", random.uniform(15000, 50000), 0.0, "Travel Points", 500
    elif credit_score >= 700 and annual_income >= 75000:
        return "Gold", random.uniform(10000, 25000), 0.0, "Cash Back", 300
    elif credit_score >= 650 and annual_income >= 50000:
        return "Silver", random.uniform(5000, 15000), 0.0, "Cash Back", 100
    else:
        return "Basic", random.uniform(1000, 8000), random.uniform(18.0, 24.99), "No Rewards", 0

def generate_card_number(network):
    """Generate realistic card number based on network"""
    if network == "Visa":
        prefix = "4"
    elif network == "Mastercard":
        prefix = "5"
    elif network == "American Express":
        prefix = "3"
    else:  # Discover
        prefix = "6"
    
    # Generate rest of digits
    remaining = ''.join([str(random.randint(0, 9)) for _ in range(15)])
    return prefix + remaining

def generate_credit_card_for_customer(customer_id, credit_score, annual_income):
    """Generate 0-3 credit cards per customer"""
    cards = []
    
    # 60% of customers have at least one credit card
    if random.random() > 0.6:
        return cards
    
    num_cards = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
    
    for _ in range(num_cards):
        card_id = f"CC{random.randint(1000000000, 9999999999)}"
        
        # Card tier and details
        card_tier, credit_limit, apr, rewards_program, annual_fee = get_card_tier_details(credit_score, annual_income)
        
        # Network
        network = random.choice(card_networks)
        
        # Card number
        card_number = generate_card_number(network)
        card_last_4 = card_number[-4:]
        
        # Dates
        issue_date = datetime.now() - timedelta(days=random.randint(30, 1825))
        expiry_month = random.randint(1, 12)
        expiry_year = datetime.now().year + random.randint(1, 5)
        expiry_date = datetime(expiry_year, expiry_month, 28)
        
        # Balance and utilization
        current_balance = round(random.uniform(0, credit_limit * 0.8), 2)
        available_credit = credit_limit - current_balance
        utilization = round((current_balance / credit_limit * 100), 2)
        
        # Status
        status = random.choice(card_statuses)
        if expiry_date < datetime.now():
            status = "Expired"
        if status in ["Blocked", "Expired", "Closed"]:
            current_balance = 0
            available_credit = 0 if status != "Closed" else credit_limit
        
        # Payment information
        statement_balance = round(random.uniform(0, current_balance), 2)
        minimum_payment = max(25, round(statement_balance * 0.02, 2))
        
        # Last statement date and due date
        last_statement_date = datetime.now() - timedelta(days=random.randint(1, 30))
        payment_due_date = last_statement_date + timedelta(days=random.choice([21, 25, 28]))
        
        # Payment history
        payments_made = random.randint(1, 60)
        late_payments = random.randint(0, 3) if status == "Active" else 0
        missed_payments = 0 if status == "Active" else random.randint(0, 2)
        
        # Calculate days past due
        if payment_due_date < datetime.now() and statement_balance > 0:
            days_past_due = (datetime.now() - payment_due_date).days
        else:
            days_past_due = 0
        
        # Rewards
        if rewards_program != "No Rewards":
            rewards_balance = round(random.uniform(0, 50000), 0)
            rewards_earned_ytd = round(random.uniform(0, 20000), 0)
        else:
            rewards_balance = 0
            rewards_earned_ytd = 0
        
        # Foreign transaction fee
        foreign_transaction_fee = 0.0 if card_tier in ["Platinum", "Gold"] else 3.0
        
        # Cash advance details
        cash_advance_limit = round(credit_limit * random.uniform(0.2, 0.4), 2)
        cash_advance_fee = random.choice([3.0, 5.0])
        cash_advance_apr = round(random.uniform(20.0, 29.99), 2)
        
        cards.append({
            "card_id": card_id,
            "customer_id": customer_id,
            "card_type": card_tier,
            "card_network": network,
            "card_number": card_number,
            "card_last_4": card_last_4,
            "card_status": status,
            "issue_date": issue_date,
            "expiry_date": expiry_date,
            "cvv": f"{random.randint(100, 999)}",
            "credit_limit": credit_limit,
            "current_balance": current_balance,
            "available_credit": available_credit,
            "utilization_percent": utilization,
            "apr": apr if apr > 0 else round(random.uniform(12.99, 24.99), 2),
            "annual_fee": annual_fee,
            "rewards_program": rewards_program,
            "rewards_balance": rewards_balance,
            "rewards_earned_ytd": rewards_earned_ytd,
            "cash_back_rate": round(random.uniform(1.0, 3.0), 1) if rewards_program == "Cash Back" else 0,
            "statement_balance": statement_balance,
            "minimum_payment": minimum_payment,
            "last_statement_date": last_statement_date,
            "payment_due_date": payment_due_date,
            "last_payment_date": datetime.now() - timedelta(days=random.randint(1, 45)) if status == "Active" else None,
            "last_payment_amount": round(random.uniform(minimum_payment, statement_balance), 2) if status == "Active" else 0,
            "payments_made": payments_made,
            "late_payments": late_payments,
            "missed_payments": missed_payments,
            "days_past_due": days_past_due,
            "is_delinquent": days_past_due > 30,
            "autopay_enabled": random.random() > 0.5,
            "autopay_amount": random.choice(["Minimum", "Statement Balance", "Fixed Amount"]) if random.random() > 0.5 else None,
            "paperless_statements": random.random() > 0.3,
            "foreign_transaction_fee": foreign_transaction_fee,
            "balance_transfer_apr": round(random.uniform(0, 15.99), 2),
            "balance_transfer_fee": random.choice([3.0, 5.0]),
            "cash_advance_limit": cash_advance_limit,
            "cash_advance_fee": cash_advance_fee,
            "cash_advance_apr": cash_advance_apr,
            "over_limit_fee": 35 if card_tier == "Basic" else 0,
            "late_payment_fee": random.choice([25, 35, 40]),
            "card_replacement_fee": 0 if card_tier in ["Platinum", "Gold"] else 15,
            "fraud_alerts_enabled": random.random() > 0.2,
            "travel_notification": random.random() > 0.6,
            "contactless_enabled": random.random() > 0.3,
            "virtual_card_enabled": random.random() > 0.5 if card_tier in ["Platinum", "Gold"] else False,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    return cards

# COMMAND ----------

# Generate credit cards for eligible customers
print(f"Generating credit cards for {len(customers)} customers...")
all_cards = []
for customer_id, credit_score, annual_income in customers:
    all_cards.extend(generate_credit_card_for_customer(customer_id, credit_score, annual_income))

print(f"Generated {len(all_cards)} total credit cards")

# COMMAND ----------

# Create DataFrame
schema = StructType([
    StructField("card_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_type", StringType(), True),
    StructField("card_network", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("card_last_4", StringType(), True),
    StructField("card_status", StringType(), True),
    StructField("issue_date", TimestampType(), True),
    StructField("expiry_date", TimestampType(), True),
    StructField("cvv", StringType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("current_balance", DoubleType(), True),
    StructField("available_credit", DoubleType(), True),
    StructField("utilization_percent", DoubleType(), True),
    StructField("apr", DoubleType(), True),
    StructField("annual_fee", DoubleType(), True),
    StructField("rewards_program", StringType(), True),
    StructField("rewards_balance", DoubleType(), True),
    StructField("rewards_earned_ytd", DoubleType(), True),
    StructField("cash_back_rate", DoubleType(), True),
    StructField("statement_balance", DoubleType(), True),
    StructField("minimum_payment", DoubleType(), True),
    StructField("last_statement_date", TimestampType(), True),
    StructField("payment_due_date", TimestampType(), True),
    StructField("last_payment_date", TimestampType(), True),
    StructField("last_payment_amount", DoubleType(), True),
    StructField("payments_made", IntegerType(), True),
    StructField("late_payments", IntegerType(), True),
    StructField("missed_payments", IntegerType(), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("is_delinquent", BooleanType(), True),
    StructField("autopay_enabled", BooleanType(), True),
    StructField("autopay_amount", StringType(), True),
    StructField("paperless_statements", BooleanType(), True),
    StructField("foreign_transaction_fee", DoubleType(), True),
    StructField("balance_transfer_apr", DoubleType(), True),
    StructField("balance_transfer_fee", DoubleType(), True),
    StructField("cash_advance_limit", DoubleType(), True),
    StructField("cash_advance_fee", DoubleType(), True),
    StructField("cash_advance_apr", DoubleType(), True),
    StructField("over_limit_fee", DoubleType(), True),
    StructField("late_payment_fee", DoubleType(), True),
    StructField("card_replacement_fee", DoubleType(), True),
    StructField("fraud_alerts_enabled", BooleanType(), True),
    StructField("travel_notification", BooleanType(), True),
    StructField("contactless_enabled", BooleanType(), True),
    StructField("virtual_card_enabled", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.createDataFrame(all_cards, schema)

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
print(f"Total Credit Cards: {df.count()}")
print(f"Unique Card IDs: {df.select('card_id').distinct().count()}")
print(f"Total Credit Extended: ${df.agg(sum('credit_limit')).collect()[0][0]:,.2f}")
print(f"Total Outstanding Balance: ${df.agg(sum('current_balance')).collect()[0][0]:,.2f}")
print(f"Average Utilization: {df.agg(avg('utilization_percent')).collect()[0][0]:.2f}%")
print(f"Delinquent Cards: {df.filter(col('is_delinquent') == True).count()}")
print("\nCard Type Distribution:")
df.groupBy("card_type").agg(
    count("*").alias("count"),
    avg("credit_limit").alias("avg_limit"),
    avg("utilization_percent").alias("avg_utilization")
).orderBy(desc("count")).show()
print("\nCard Status:")
df.groupBy("card_status").count().orderBy(desc("count")).show()
print("\nRewards Programs:")
df.groupBy("rewards_program").count().orderBy(desc("count")).show()

