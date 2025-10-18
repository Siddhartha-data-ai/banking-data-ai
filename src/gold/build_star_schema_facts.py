# Databricks notebook source
# MAGIC %md
# MAGIC # Build Star Schema Fact Tables
# MAGIC 
# MAGIC Populates fact tables with measures and foreign keys to dimensions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

CATALOG = "banking_catalog"
SILVER = "banking_silver"
GOLD = "banking_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact: Transactions

# COMMAND ----------

def build_fact_transactions():
    """Build transaction fact table with dimension keys"""
    
    # Load source tables
    transactions = spark.table(f"{CATALOG}.{SILVER}.transactions_clean")
    dim_date = spark.table(f"{CATALOG}.{GOLD}.dim_date")
    
    # Try to load dimensions, create empty if not exist
    try:
        dim_customer = spark.table(f"{CATALOG}.{GOLD}.dim_customer").filter(col("is_current") == True)
    except:
        dim_customer = None
        print("⚠ dim_customer not found, using customer_id as surrogate key")
    
    try:
        dim_account = spark.table(f"{CATALOG}.{GOLD}.dim_account").filter(col("is_current") == True)
    except:
        dim_account = None
        print("⚠ dim_account not found, using account_id as surrogate key")
    
    try:
        dim_merchant = spark.table(f"{CATALOG}.{GOLD}.dim_merchant").filter(col("is_current") == True)
    except:
        dim_merchant = None
        print("⚠ dim_merchant not found, using merchant_id as surrogate key")
    
    # Create date key from transaction date
    fact = transactions.withColumn(
        "transaction_date_key",
        date_format(col("transaction_date"), "yyyyMMdd").cast("int")
    )
    
    # Join with dimensions to get surrogate keys
    if dim_customer is not None:
        fact = fact.join(
            dim_customer.select(col("customer_sk"), col("customer_id").alias("dim_customer_id")),
            fact.customer_id == col("dim_customer_id"),
            "left"
        ).drop("dim_customer_id")
    else:
        fact = fact.withColumn("customer_sk", lit(None).cast("bigint"))
    
    if dim_account is not None:
        fact = fact.join(
            dim_account.select(col("account_sk"), col("account_id").alias("dim_account_id")),
            fact.account_id == col("dim_account_id"),
            "left"
        ).drop("dim_account_id")
    else:
        fact = fact.withColumn("account_sk", lit(None).cast("bigint"))
    
    if dim_merchant is not None:
        fact = fact.join(
            dim_merchant.select(col("merchant_sk"), col("merchant_id").alias("dim_merchant_id")),
            fact.merchant_id == col("dim_merchant_id"),
            "left"
        ).drop("dim_merchant_id")
    else:
        fact = fact.withColumn("merchant_sk", lit(None).cast("bigint"))
    
    # Select fact table columns
    fact_transactions = fact.select(
        col("transaction_date_key"),
        col("customer_sk"),
        col("account_sk"),
        col("merchant_sk"),
        col("transaction_id"),
        col("transaction_type"),
        col("channel"),
        col("status"),
        col("amount").alias("transaction_amount"),
        col("balance_after"),
        lit(0.0).alias("fee_amount"),  # Can be calculated if fee data available
        col("is_debit"),
        col("is_international"),
        col("is_fraud"),
        col("is_weekend"),
        col("is_night_transaction"),
        col("fraud_score"),
        when(col("transaction_risk_flag") == "High", 3)
            .when(col("transaction_risk_flag") == "Medium", 2)
            .otherwise(1).alias("risk_score"),
        col("transaction_date").alias("transaction_timestamp"),
        current_timestamp().alias("processed_timestamp"),
        current_timestamp().alias("created_at")
    )
    
    # Write to fact table
    fact_transactions.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.fact_transactions")
    
    print(f"✓ Created fact_transactions with {fact_transactions.count():,} records")
    return fact_transactions

fact_txn = build_fact_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact: Account Daily Snapshot

# COMMAND ----------

def build_fact_account_daily_snapshot():
    """Build daily account balance snapshot"""
    
    accounts = spark.table(f"{CATALOG}.{SILVER}.accounts_clean")
    transactions = spark.table(f"{CATALOG}.{SILVER}.transactions_clean")
    
    # Get dimension keys if available
    try:
        dim_account = spark.table(f"{CATALOG}.{GOLD}.dim_account").filter(col("is_current") == True)
        dim_customer = spark.table(f"{CATALOG}.{GOLD}.dim_customer").filter(col("is_current") == True)
    except:
        print("⚠ Using business keys instead of surrogate keys")
        dim_account = None
        dim_customer = None
    
    # Calculate daily statistics per account
    daily_stats = transactions.withColumn(
        "transaction_date_only",
        to_date(col("transaction_date"))
    ).groupBy(
        col("account_id"),
        col("customer_id"),
        col("transaction_date_only")
    ).agg(
        count("*").alias("total_transaction_count"),
        count(when(~col("is_debit"), 1)).alias("deposit_count"),
        count(when(col("is_debit"), 1)).alias("withdrawal_count"),
        sum(when(~col("is_debit"), col("amount")).otherwise(0)).alias("total_deposits"),
        sum(when(col("is_debit"), col("amount")).otherwise(0)).alias("total_withdrawals"),
        lit(0.0).alias("total_fees"),
        lit(0.0).alias("interest_earned"),
        max("balance_after").alias("ending_balance"),
        min("balance_after").alias("min_balance"),
        max("balance_after").alias("max_balance"),
        avg("balance_after").alias("average_balance")
    )
    
    # Add date key and other columns
    snapshot = daily_stats.withColumn(
        "snapshot_date_key",
        date_format(col("transaction_date_only"), "yyyyMMdd").cast("int")
    ).withColumn(
        "beginning_balance",
        col("ending_balance") - col("total_deposits") + col("total_withdrawals")
    ).withColumn(
        "is_overdrawn",
        col("min_balance") < 0
    ).withColumn(
        "is_dormant",
        col("total_transaction_count") == 0
    ).withColumn(
        "snapshot_timestamp",
        current_timestamp()
    ).withColumn(
        "created_at",
        current_timestamp()
    )
    
    # Add surrogate keys if dimensions exist
    if dim_account is not None:
        snapshot = snapshot.join(
            dim_account.select(col("account_sk"), col("account_id").alias("dim_account_id")),
            snapshot.account_id == col("dim_account_id"),
            "left"
        ).drop("dim_account_id", "account_id")
    else:
        snapshot = snapshot.withColumnRenamed("account_id", "account_sk")
    
    if dim_customer is not None:
        snapshot = snapshot.join(
            dim_customer.select(col("customer_sk"), col("customer_id").alias("dim_customer_id")),
            snapshot.customer_id == col("dim_customer_id"),
            "left"
        ).drop("dim_customer_id", "customer_id")
    else:
        snapshot = snapshot.withColumnRenamed("customer_id", "customer_sk")
    
    # Select final columns
    fact_snapshot = snapshot.select(
        "snapshot_date_key",
        "account_sk",
        "customer_sk",
        "beginning_balance",
        "ending_balance",
        "average_balance",
        "min_balance",
        "max_balance",
        "deposit_count",
        "withdrawal_count",
        "total_transaction_count",
        "total_deposits",
        "total_withdrawals",
        "total_fees",
        "interest_earned",
        "is_overdrawn",
        "is_dormant",
        "snapshot_timestamp",
        "created_at"
    )
    
    fact_snapshot.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.fact_account_daily_snapshot")
    
    print(f"✓ Created fact_account_daily_snapshot with {fact_snapshot.count():,} records")

build_fact_account_daily_snapshot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact: Loan Performance

# COMMAND ----------

def build_fact_loan_performance():
    """Build loan performance fact table"""
    
    loans = spark.table(f"{CATALOG}.{SILVER}.loans_clean")
    
    try:
        dim_customer = spark.table(f"{CATALOG}.{GOLD}.dim_customer").filter(col("is_current") == True)
    except:
        dim_customer = None
    
    # Add date key
    fact = loans.withColumn(
        "origination_date_key",
        date_format(col("origination_date"), "yyyyMMdd").cast("int")
    )
    
    # Join with customer dimension
    if dim_customer is not None:
        fact = fact.join(
            dim_customer.select(col("customer_sk"), col("customer_id").alias("dim_customer_id")),
            fact.customer_id == col("dim_customer_id"),
            "left"
        ).drop("dim_customer_id")
    else:
        fact = fact.withColumn("customer_sk", lit(None).cast("bigint"))
    
    # Select fact columns
    fact_loans = fact.select(
        col("origination_date_key"),
        col("customer_sk"),
        col("loan_id"),
        col("loan_type"),
        col("loan_status"),
        col("loan_amount").alias("original_loan_amount"),
        col("remaining_balance").alias("current_balance"),
        (col("loan_amount") - col("remaining_balance")).alias("principal_paid"),
        col("total_paid") - (col("loan_amount") - col("remaining_balance")).alias("interest_paid"),
        lit(0.0).alias("fees_paid"),
        col("monthly_payment").alias("monthly_payment_amount"),
        col("payments_made").alias("payment_count"),
        col("missed_payments").alias("missed_payment_count"),
        col("days_past_due"),
        lit(0.0).alias("default_probability"),  # Can be added from ML model
        col("interest_rate"),
        col("loan_to_value_ratio"),
        col("debt_to_income_ratio"),
        col("is_delinquent"),
        col("is_default"),
        col("auto_pay_enabled").alias("has_autopay"),
        to_date(col("origination_date")).alias("origination_date"),
        to_date(col("maturity_date")).alias("maturity_date"),
        to_date(col("next_payment_date")).alias("next_payment_date"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )
    
    fact_loans.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.fact_loan_performance")
    
    print(f"✓ Created fact_loan_performance with {fact_loans.count():,} records")

build_fact_loan_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Fact: Credit Card Usage

# COMMAND ----------

def build_fact_credit_card_usage():
    """Build credit card usage fact from transactions"""
    
    transactions = spark.table(f"{CATALOG}.{SILVER}.transactions_clean")
    credit_cards = spark.table(f"{CATALOG}.{SILVER}.credit_cards_clean")
    
    # Filter card transactions (transactions with card_last_4)
    card_txn = transactions.filter(col("card_last_4").isNotNull())
    
    # Join with credit card data to get card details
    card_usage = card_txn.join(
        credit_cards.select(
            col("card_id"),
            col("card_last_4"),
            col("card_type"),
            col("customer_id").alias("card_customer_id"),
            col("credit_limit"),
            col("current_balance"),
            col("available_credit"),
            col("utilization_percent")
        ),
        card_txn.card_last_4 == credit_cards.card_last_4,
        "left"
    )
    
    # Add date key
    fact = card_usage.withColumn(
        "transaction_date_key",
        date_format(col("transaction_date"), "yyyyMMdd").cast("int")
    )
    
    # Get dimension keys
    try:
        dim_customer = spark.table(f"{CATALOG}.{GOLD}.dim_customer").filter(col("is_current") == True)
        dim_merchant = spark.table(f"{CATALOG}.{GOLD}.dim_merchant").filter(col("is_current") == True)
    except:
        dim_customer = None
        dim_merchant = None
    
    if dim_customer is not None:
        fact = fact.join(
            dim_customer.select(col("customer_sk"), col("customer_id").alias("dim_customer_id")),
            coalesce(fact.card_customer_id, fact.customer_id) == col("dim_customer_id"),
            "left"
        ).drop("dim_customer_id")
    else:
        fact = fact.withColumn("customer_sk", lit(None).cast("bigint"))
    
    if dim_merchant is not None:
        fact = fact.join(
            dim_merchant.select(col("merchant_sk"), col("merchant_id").alias("dim_merchant_id")),
            fact.merchant_id == col("dim_merchant_id"),
            "left"
        ).drop("dim_merchant_id")
    else:
        fact = fact.withColumn("merchant_sk", lit(None).cast("bigint"))
    
    # Select fact columns
    fact_card = fact.select(
        col("transaction_date_key"),
        col("customer_sk"),
        col("merchant_sk"),
        coalesce(col("card_id"), lit("UNKNOWN")).alias("card_id"),
        col("transaction_id"),
        coalesce(col("card_type"), lit("Unknown")).alias("card_type"),
        col("amount").alias("transaction_amount"),
        lit(0.0).alias("fee_amount"),
        lit(0.0).alias("rewards_earned"),
        coalesce(col("credit_limit"), lit(0.0)).alias("credit_limit"),
        coalesce(col("available_credit"), lit(0.0)).alias("available_credit"),
        coalesce(col("utilization_percent"), lit(0.0)).alias("utilization_percent"),
        col("is_international"),
        lit(False).alias("is_cash_advance"),
        lit(False).alias("is_balance_transfer"),
        when(col("status") == "Failed", True).otherwise(False).alias("is_declined"),
        col("is_fraud").alias("is_fraud_suspected"),
        col("fraud_score"),
        col("transaction_date").alias("transaction_timestamp"),
        current_timestamp().alias("created_at")
    )
    
    fact_card.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD}.fact_credit_card_usage")
    
    print(f"✓ Created fact_credit_card_usage with {fact_card.count():,} records")

build_fact_credit_card_usage()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Fact Tables

# COMMAND ----------

print("\n📊 Fact Table Summary:")
print("=" * 70)

fact_tables = [
    'fact_transactions',
    'fact_account_daily_snapshot',
    'fact_loan_performance',
    'fact_credit_card_usage'
]

for table in fact_tables:
    try:
        count = spark.table(f"{CATALOG}.{GOLD}.{table}").count()
        print(f"✓ {table}: {count:,} records")
    except Exception as e:
        print(f"⚠ {table}: Error - {str(e)}")

print("=" * 70)
print("✓ Star schema facts built successfully!")

# COMMAND ----------

# Show sample from transaction fact
print("\n📋 Sample Transaction Fact Records:")
display(
    spark.table(f"{CATALOG}.{GOLD}.fact_transactions")
    .select(
        "transaction_date_key",
        "customer_sk",
        "account_sk",
        "transaction_id",
        "transaction_amount",
        "fraud_score",
        "is_fraud"
    )
    .limit(10)
)

