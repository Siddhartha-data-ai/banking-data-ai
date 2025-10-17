# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Banking Loans Data
# MAGIC 
# MAGIC Generates synthetic loan data for the banking platform including:
# MAGIC - Personal loans, auto loans, mortgages, business loans
# MAGIC - Loan status and repayment information
# MAGIC - Risk scoring and approval workflow
# MAGIC - Payment history and default indicators

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
TABLE = "loans"
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
# MAGIC ## Generate Loan Data

# COMMAND ----------

loan_types = ["Personal Loan", "Auto Loan", "Mortgage", "Home Equity", "Business Loan", "Student Loan"]
loan_purposes = {
    "Personal Loan": ["Debt Consolidation", "Home Improvement", "Medical", "Vacation", "Wedding"],
    "Auto Loan": ["New Car", "Used Car", "Refinance"],
    "Mortgage": ["Home Purchase", "Refinance", "Construction"],
    "Home Equity": ["Home Improvement", "Debt Consolidation", "Education"],
    "Business Loan": ["Equipment", "Working Capital", "Expansion", "Inventory"],
    "Student Loan": ["Undergraduate", "Graduate", "Refinance"]
}

loan_statuses = ["Active", "Active", "Active", "Paid Off", "In Default", "In Review", "Denied", "Closed"]
payment_frequencies = ["Monthly", "Bi-Weekly", "Weekly"]
collateral_types = ["Vehicle", "Property", "None", "Equipment", "Securities"]

def calculate_monthly_payment(principal, annual_rate, term_months):
    """Calculate monthly payment using standard loan formula"""
    if annual_rate == 0:
        return principal / term_months
    monthly_rate = annual_rate / 12 / 100
    payment = principal * (monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1)
    return round(payment, 2)

def generate_loan_for_customer(customer_id, credit_score, annual_income):
    """Generate 0-2 loans per customer (not all customers have loans)"""
    loans = []
    
    # Only 40% of customers have loans
    if random.random() > 0.4:
        return loans
    
    num_loans = random.choices([1, 2], weights=[0.8, 0.2])[0]
    
    for _ in range(num_loans):
        loan_id = f"LOAN{random.randint(1000000000, 9999999999)}"
        loan_type = random.choice(loan_types)
        purpose = random.choice(loan_purposes[loan_type])
        
        # Loan amount based on type and income
        if loan_type == "Personal Loan":
            loan_amount = round(random.uniform(5000, min(50000, annual_income * 0.5)), 2)
            term_months = random.choice([12, 24, 36, 48, 60])
            interest_rate = round(random.uniform(5.0, 15.0), 2)
            collateral = "None"
        elif loan_type == "Auto Loan":
            loan_amount = round(random.uniform(15000, 60000), 2)
            term_months = random.choice([36, 48, 60, 72])
            interest_rate = round(random.uniform(3.0, 8.0), 2)
            collateral = "Vehicle"
        elif loan_type == "Mortgage":
            loan_amount = round(random.uniform(150000, min(800000, annual_income * 5)), 2)
            term_months = random.choice([180, 240, 300, 360])  # 15, 20, 25, 30 years
            interest_rate = round(random.uniform(3.0, 6.5), 2)
            collateral = "Property"
        elif loan_type == "Home Equity":
            loan_amount = round(random.uniform(25000, 200000), 2)
            term_months = random.choice([60, 120, 180, 240])
            interest_rate = round(random.uniform(4.0, 8.0), 2)
            collateral = "Property"
        elif loan_type == "Business Loan":
            loan_amount = round(random.uniform(50000, 500000), 2)
            term_months = random.choice([36, 60, 84, 120])
            interest_rate = round(random.uniform(5.0, 12.0), 2)
            collateral = random.choice(["Equipment", "Property", "None"])
        else:  # Student Loan
            loan_amount = round(random.uniform(10000, 100000), 2)
            term_months = random.choice([120, 180, 240, 300])
            interest_rate = round(random.uniform(3.5, 7.0), 2)
            collateral = "None"
        
        # Application and origination dates
        application_date = datetime.now() - timedelta(days=random.randint(60, 1825))
        origination_date = application_date + timedelta(days=random.randint(7, 45))
        maturity_date = origination_date + timedelta(days=term_months * 30)
        
        # Monthly payment
        monthly_payment = calculate_monthly_payment(loan_amount, interest_rate, term_months)
        
        # Payment history (months since origination)
        months_elapsed = min(term_months, int((datetime.now() - origination_date).days / 30))
        payments_made = months_elapsed
        
        # Calculate current balance
        # Simple approach: reduce by payment amount
        total_paid = payments_made * monthly_payment
        remaining_balance = max(0, loan_amount - total_paid + (loan_amount * interest_rate / 100 * months_elapsed / 12))
        remaining_balance = round(remaining_balance, 2)
        
        # Loan status based on payment behavior
        status = random.choice(loan_statuses)
        if remaining_balance <= 0:
            status = "Paid Off"
        elif months_elapsed < 1:
            status = "Active"
        
        # Risk score (300-850, similar to credit score)
        # Based on customer credit score and payment history
        base_risk = 850 - credit_score
        risk_adjustment = random.randint(-50, 50)
        risk_score = max(300, min(850, 850 - base_risk + risk_adjustment))
        
        # Default indicators
        missed_payments = random.randint(0, 3) if status == "Active" else 0
        if status == "In Default":
            missed_payments = random.randint(3, 12)
            days_past_due = random.randint(90, 365)
        else:
            days_past_due = 0
        
        # Approval workflow
        if status == "Denied":
            approval_status = "Denied"
            denial_reason = random.choice([
                "Insufficient Income",
                "Poor Credit History",
                "High Debt-to-Income Ratio",
                "Incomplete Documentation"
            ])
        elif status == "In Review":
            approval_status = "Pending"
            denial_reason = None
        else:
            approval_status = "Approved"
            denial_reason = None
        
        # Underwriter info
        underwriter_id = f"UW{random.randint(1000, 9999)}" if approval_status in ["Approved", "Denied"] else None
        
        loans.append({
            "loan_id": loan_id,
            "customer_id": customer_id,
            "loan_type": loan_type,
            "loan_purpose": purpose,
            "loan_amount": loan_amount,
            "interest_rate": interest_rate,
            "term_months": term_months,
            "monthly_payment": monthly_payment,
            "remaining_balance": remaining_balance,
            "total_paid": round(total_paid, 2),
            "application_date": application_date,
            "approval_date": origination_date - timedelta(days=random.randint(1, 7)) if approval_status == "Approved" else None,
            "origination_date": origination_date if status not in ["Denied", "In Review"] else None,
            "maturity_date": maturity_date if status not in ["Denied", "In Review"] else None,
            "first_payment_date": origination_date + timedelta(days=30) if status not in ["Denied", "In Review"] else None,
            "next_payment_date": datetime.now() + timedelta(days=random.randint(1, 30)) if status == "Active" else None,
            "last_payment_date": datetime.now() - timedelta(days=random.randint(1, 45)) if status == "Active" else None,
            "loan_status": status,
            "approval_status": approval_status,
            "denial_reason": denial_reason,
            "risk_score": risk_score,
            "collateral_type": collateral,
            "collateral_value": round(loan_amount * random.uniform(1.0, 1.5), 2) if collateral != "None" else 0,
            "loan_to_value_ratio": round(random.uniform(0.7, 0.95), 2) if collateral != "None" else 0,
            "debt_to_income_ratio": round((monthly_payment * 12) / annual_income, 2),
            "payments_made": payments_made,
            "missed_payments": missed_payments,
            "days_past_due": days_past_due,
            "is_delinquent": days_past_due > 30,
            "is_default": status == "In Default",
            "payment_frequency": random.choice(payment_frequencies),
            "auto_pay_enabled": random.random() > 0.4,
            "underwriter_id": underwriter_id,
            "branch_id": f"BR{str(random.randint(1, 50)).zfill(4)}",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    return loans

# COMMAND ----------

# Generate loans for eligible customers
print(f"Generating loans for {len(customers)} customers...")
all_loans = []
for customer_id, credit_score, annual_income in customers:
    all_loans.extend(generate_loan_for_customer(customer_id, credit_score, annual_income))

print(f"Generated {len(all_loans)} total loans")

# COMMAND ----------

# Create DataFrame
schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("loan_type", StringType(), True),
    StructField("loan_purpose", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("term_months", IntegerType(), True),
    StructField("monthly_payment", DoubleType(), True),
    StructField("remaining_balance", DoubleType(), True),
    StructField("total_paid", DoubleType(), True),
    StructField("application_date", TimestampType(), True),
    StructField("approval_date", TimestampType(), True),
    StructField("origination_date", TimestampType(), True),
    StructField("maturity_date", TimestampType(), True),
    StructField("first_payment_date", TimestampType(), True),
    StructField("next_payment_date", TimestampType(), True),
    StructField("last_payment_date", TimestampType(), True),
    StructField("loan_status", StringType(), True),
    StructField("approval_status", StringType(), True),
    StructField("denial_reason", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("collateral_type", StringType(), True),
    StructField("collateral_value", DoubleType(), True),
    StructField("loan_to_value_ratio", DoubleType(), True),
    StructField("debt_to_income_ratio", DoubleType(), True),
    StructField("payments_made", IntegerType(), True),
    StructField("missed_payments", IntegerType(), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("is_delinquent", BooleanType(), True),
    StructField("is_default", BooleanType(), True),
    StructField("payment_frequency", StringType(), True),
    StructField("auto_pay_enabled", BooleanType(), True),
    StructField("underwriter_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.createDataFrame(all_loans, schema)

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
print(f"Total Loans: {df.count()}")
print(f"Unique Loan IDs: {df.select('loan_id').distinct().count()}")
print(f"Total Loan Amount: ${df.agg(sum('loan_amount')).collect()[0][0]:,.2f}")
print(f"Total Outstanding: ${df.agg(sum('remaining_balance')).collect()[0][0]:,.2f}")
print(f"Delinquent Loans: {df.filter(col('is_delinquent') == True).count()}")
print(f"Default Loans: {df.filter(col('is_default') == True).count()}")
print("\nLoan Type Distribution:")
df.groupBy("loan_type").agg(
    count("*").alias("count"),
    sum("loan_amount").alias("total_amount"),
    avg("interest_rate").alias("avg_rate")
).orderBy(desc("count")).show()
print("\nLoan Status:")
df.groupBy("loan_status").count().orderBy(desc("count")).show()

