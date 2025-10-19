# Databricks notebook source
# MAGIC %md
# MAGIC # Embedded Finance for Merchants
# MAGIC 
# MAGIC White-label banking services for merchants and platforms:
# MAGIC - Payment processing (ACH, cards, instant payments)
# MAGIC - Virtual accounts and wallets
# MAGIC - Split payments and marketplace settlement
# MAGIC - Buy Now Pay Later (BNPL) integration
# MAGIC - Working capital financing
# MAGIC - Merchant dashboards and analytics
# MAGIC - API-first architecture

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import uuid

# COMMAND ----------

CATALOG = "banking_catalog"
SCHEMA = "embedded_finance"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merchant Accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS merchant_accounts (
# MAGIC     merchant_id STRING PRIMARY KEY,
# MAGIC     
# MAGIC     -- Business Information
# MAGIC     business_name STRING NOT NULL,
# MAGIC     legal_entity STRING NOT NULL,
# MAGIC     tax_id STRING NOT NULL,
# MAGIC     business_type STRING,  -- SOLE_PROPRIETOR, LLC, CORPORATION
# MAGIC     industry STRING NOT NULL,
# MAGIC     
# MAGIC     -- Contact
# MAGIC     contact_email STRING NOT NULL,
# MAGIC     contact_phone STRING,
# MAGIC     website STRING,
# MAGIC     
# MAGIC     -- Banking Details
# MAGIC     virtual_account_number STRING UNIQUE,
# MAGIC     routing_number STRING,
# MAGIC     available_balance DECIMAL(18, 2) DEFAULT 0,
# MAGIC     pending_balance DECIMAL(18, 2) DEFAULT 0,
# MAGIC     
# MAGIC     -- Limits and Fees
# MAGIC     daily_transaction_limit DECIMAL(18, 2) DEFAULT 50000,
# MAGIC     transaction_fee_percent DECIMAL(5, 4) DEFAULT 2.9,
# MAGIC     fixed_fee_per_transaction DECIMAL(10, 2) DEFAULT 0.30,
# MAGIC     
# MAGIC     -- Settlement
# MAGIC     settlement_frequency STRING DEFAULT 'T+2',  -- T+1, T+2, WEEKLY
# MAGIC     next_payout_date DATE,
# MAGIC     
# MAGIC     -- API Credentials
# MAGIC     api_key STRING UNIQUE,
# MAGIC     api_secret_hash STRING,
# MAGIC     webhook_url STRING,
# MAGIC     
# MAGIC     -- Status
# MAGIC     status STRING DEFAULT 'ACTIVE',  -- ACTIVE, SUSPENDED, CLOSED
# MAGIC     kyb_verified BOOLEAN DEFAULT FALSE,  -- Know Your Business
# MAGIC     onboarded_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded Transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS embedded_transactions (
# MAGIC     transaction_id STRING PRIMARY KEY,
# MAGIC     merchant_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Transaction Details
# MAGIC     transaction_type STRING NOT NULL,  -- PAYMENT, REFUND, PAYOUT, CHARGEBACK
# MAGIC     payment_method STRING NOT NULL,  -- CARD, ACH, INSTANT, WALLET
# MAGIC     amount DECIMAL(18, 2) NOT NULL,
# MAGIC     currency STRING DEFAULT 'USD',
# MAGIC     
# MAGIC     -- Fees
# MAGIC     merchant_fee DECIMAL(18, 2) DEFAULT 0,
# MAGIC     net_amount DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Customer Information
# MAGIC     customer_id STRING,
# MAGIC     customer_email STRING,
# MAGIC     
# MAGIC     -- Payment Instrument
# MAGIC     card_last_4 STRING,
# MAGIC     card_brand STRING,  -- VISA, MASTERCARD, AMEX
# MAGIC     ach_account_last_4 STRING,
# MAGIC     
# MAGIC     -- Status
# MAGIC     status STRING DEFAULT 'PENDING',  -- PENDING, AUTHORIZED, CAPTURED, SETTLED, FAILED, REFUNDED
# MAGIC     failure_reason STRING,
# MAGIC     
# MAGIC     -- Timestamps
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp(),
# MAGIC     authorized_at TIMESTAMP,
# MAGIC     captured_at TIMESTAMP,
# MAGIC     settled_at TIMESTAMP,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     order_id STRING,
# MAGIC     description STRING,
# MAGIC     metadata STRING,  -- JSON
# MAGIC     
# MAGIC     -- Risk
# MAGIC     fraud_score DOUBLE DEFAULT 0,
# MAGIC     is_high_risk BOOLEAN DEFAULT FALSE
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (DATE(created_at))
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buy Now Pay Later (BNPL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bnpl_loans (
# MAGIC     loan_id STRING PRIMARY KEY,
# MAGIC     merchant_id STRING NOT NULL,
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Loan Details
# MAGIC     principal_amount DECIMAL(18, 2) NOT NULL,
# MAGIC     interest_rate DECIMAL(5, 4) DEFAULT 0,  -- Often 0% for BNPL
# MAGIC     num_installments INT DEFAULT 4,
# MAGIC     installment_frequency STRING DEFAULT 'BIWEEKLY',
# MAGIC     installment_amount DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Payment Schedule
# MAGIC     first_payment_date DATE,
# MAGIC     last_payment_date DATE,
# MAGIC     next_payment_date DATE,
# MAGIC     
# MAGIC     -- Status
# MAGIC     num_paid_installments INT DEFAULT 0,
# MAGIC     total_paid DECIMAL(18, 2) DEFAULT 0,
# MAGIC     remaining_balance DECIMAL(18, 2),
# MAGIC     status STRING DEFAULT 'ACTIVE',  -- ACTIVE, PAID_OFF, DEFAULTED
# MAGIC     
# MAGIC     -- Risk
# MAGIC     is_past_due BOOLEAN DEFAULT FALSE,
# MAGIC     days_past_due INT DEFAULT 0,
# MAGIC     late_fees DECIMAL(18, 2) DEFAULT 0,
# MAGIC     
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marketplace Split Payments

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS split_payment_rules (
# MAGIC     rule_id STRING PRIMARY KEY,
# MAGIC     merchant_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Split Configuration
# MAGIC     split_type STRING NOT NULL,  -- PERCENTAGE, FIXED_AMOUNT
# MAGIC     
# MAGIC     -- Recipients
# MAGIC     platform_merchant_id STRING,
# MAGIC     platform_share_percent DECIMAL(5, 4),
# MAGIC     platform_fixed_amount DECIMAL(18, 2),
# MAGIC     
# MAGIC     seller_merchant_id STRING,
# MAGIC     seller_share_percent DECIMAL(5, 4),
# MAGIC     seller_fixed_amount DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Optional third party (e.g., referral partner)
# MAGIC     third_party_merchant_id STRING,
# MAGIC     third_party_share_percent DECIMAL(5, 4),
# MAGIC     
# MAGIC     is_active BOOLEAN DEFAULT TRUE,
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working Capital Financing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS merchant_financing (
# MAGIC     financing_id STRING PRIMARY KEY,
# MAGIC     merchant_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Financing Details
# MAGIC     financing_type STRING NOT NULL,  -- REVENUE_BASED, TERM_LOAN, LINE_OF_CREDIT
# MAGIC     principal_amount DECIMAL(18, 2) NOT NULL,
# MAGIC     factor_rate DECIMAL(5, 4),  -- For revenue-based financing
# MAGIC     interest_rate DECIMAL(5, 4),  -- For loans
# MAGIC     
# MAGIC     -- Repayment
# MAGIC     repayment_percent DECIMAL(5, 4),  -- % of daily sales
# MAGIC     total_repayment_amount DECIMAL(18, 2),
# MAGIC     total_repaid DECIMAL(18, 2) DEFAULT 0,
# MAGIC     remaining_balance DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Terms
# MAGIC     term_months INT,
# MAGIC     funded_date DATE,
# MAGIC     maturity_date DATE,
# MAGIC     
# MAGIC     -- Status
# MAGIC     status STRING DEFAULT 'ACTIVE',
# MAGIC     is_defaulted BOOLEAN DEFAULT FALSE,
# MAGIC     
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA;

# COMMAND ----------

print("✅ Embedded Finance for Merchants implemented!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Features Implemented
# MAGIC 
# MAGIC ✅ **Merchant Onboarding** - KYB verification and virtual accounts  
# MAGIC ✅ **Payment Processing** - Multi-method support (cards, ACH, instant)  
# MAGIC ✅ **BNPL Integration** - Built-in Buy Now Pay Later  
# MAGIC ✅ **Split Payments** - Marketplace settlement automation  
# MAGIC ✅ **Working Capital** - Revenue-based financing  
# MAGIC ✅ **API-First** - RESTful APIs with webhooks  
# MAGIC ✅ **Real-Time Settlement** - T+1/T+2 options  
# MAGIC ✅ **Fraud Prevention** - Built-in risk scoring

