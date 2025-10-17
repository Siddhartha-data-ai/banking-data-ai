# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Banking Customers Data
# MAGIC 
# MAGIC Generates synthetic customer data for the banking platform including:
# MAGIC - Personal information (name, contact, demographics)
# MAGIC - Credit scores and risk ratings
# MAGIC - Customer segments and status
# MAGIC - KYC (Know Your Customer) information

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
TABLE = "customers"
NUM_CUSTOMERS = 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Data

# COMMAND ----------

# Sample data for realistic generation
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
               "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
               "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
               "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White",
              "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young"]

cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", 
          "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
          "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis", "Seattle",
          "Denver", "Washington", "Boston", "Nashville", "Detroit", "Portland", "Las Vegas"]

states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL",
          "TX", "OH", "CA", "NC", "IN", "WA", "CO", "DC", "MA", "TN", "MI", "OR", "NV"]

occupations = ["Software Engineer", "Teacher", "Nurse", "Sales Manager", "Accountant",
               "Marketing Manager", "Business Analyst", "Project Manager", "Consultant",
               "Attorney", "Doctor", "Engineer", "Designer", "Data Analyst", "Administrator"]

customer_segments = ["Premium", "Gold", "Silver", "Bronze", "Basic"]
employment_status = ["Employed", "Self-Employed", "Retired", "Student", "Unemployed"]

# COMMAND ----------

def generate_customer():
    """Generate a single customer record"""
    customer_id = str(uuid.uuid4())
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    
    # Generate dates
    account_open_date = datetime.now() - timedelta(days=random.randint(30, 3650))
    dob = datetime.now() - timedelta(days=random.randint(18*365, 80*365))
    
    # Credit score (300-850 range)
    credit_score = random.randint(300, 850)
    
    # Risk rating based on credit score
    if credit_score >= 750:
        risk_rating = "Low"
    elif credit_score >= 650:
        risk_rating = "Medium"
    else:
        risk_rating = "High"
    
    # Customer segment based on tenure and credit
    tenure_years = (datetime.now() - account_open_date).days / 365
    if credit_score >= 750 and tenure_years >= 5:
        segment = "Premium"
    elif credit_score >= 700 and tenure_years >= 3:
        segment = "Gold"
    elif credit_score >= 650:
        segment = "Silver"
    elif credit_score >= 600:
        segment = "Bronze"
    else:
        segment = "Basic"
    
    city_idx = random.randint(0, len(cities) - 1)
    
    return {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": f"{first_name} {last_name}",
        "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@email.com",
        "phone": f"+1-{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
        "date_of_birth": dob,
        "age": int((datetime.now() - dob).days / 365),
        "address": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Park', 'Lake'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr', 'Way'])}",
        "city": cities[city_idx],
        "state": states[city_idx],
        "zip_code": f"{random.randint(10000, 99999)}",
        "country": "USA",
        "ssn_last_4": f"{random.randint(1000, 9999)}",
        "credit_score": credit_score,
        "risk_rating": risk_rating,
        "customer_segment": segment,
        "account_open_date": account_open_date,
        "kyc_verified": random.choice([True, True, True, False]),  # 75% verified
        "kyc_verification_date": account_open_date + timedelta(days=random.randint(0, 30)) if random.random() > 0.25 else None,
        "employment_status": random.choice(employment_status),
        "occupation": random.choice(occupations),
        "annual_income": random.randint(25000, 250000),
        "account_status": random.choice(["Active", "Active", "Active", "Active", "Active", "Dormant", "Suspended"]),
        "preferred_contact": random.choice(["Email", "Phone", "SMS", "Mobile App"]),
        "marketing_opt_in": random.choice([True, False]),
        "online_banking_enabled": random.choice([True, True, True, False]),  # 75% enabled
        "mobile_app_user": random.choice([True, True, False]),  # 66% users
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }

# COMMAND ----------

# Generate customer data
print(f"Generating {NUM_CUSTOMERS} customer records...")
customers_data = [generate_customer() for _ in range(NUM_CUSTOMERS)]

# COMMAND ----------

# Create DataFrame
schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ssn_last_4", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("risk_rating", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("account_open_date", TimestampType(), True),
    StructField("kyc_verified", BooleanType(), True),
    StructField("kyc_verification_date", TimestampType(), True),
    StructField("employment_status", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("annual_income", IntegerType(), True),
    StructField("account_status", StringType(), True),
    StructField("preferred_contact", StringType(), True),
    StructField("marketing_opt_in", BooleanType(), True),
    StructField("online_banking_enabled", BooleanType(), True),
    StructField("mobile_app_user", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.createDataFrame(customers_data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Bronze Layer

# COMMAND ----------

# Create catalog and schema if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

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
print(f"Total Customers: {df.count()}")
print(f"Unique Customer IDs: {df.select('customer_id').distinct().count()}")
print(f"KYC Verified: {df.filter(col('kyc_verified') == True).count()}")
print(f"Active Accounts: {df.filter(col('account_status') == 'Active').count()}")
print("\nCustomer Segments:")
df.groupBy("customer_segment").count().orderBy(desc("count")).show()
print("\nRisk Distribution:")
df.groupBy("risk_rating").count().orderBy(desc("count")).show()

