-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Bronze Layer Tables
-- MAGIC 
-- MAGIC Creates table definitions for bronze layer (raw data)

-- COMMAND ----------

USE CATALOG banking_catalog;
USE SCHEMA banking_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customers Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers (
  customer_id STRING NOT NULL,
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
  account_open_date TIMESTAMP,
  kyc_verified BOOLEAN,
  kyc_verification_date TIMESTAMP,
  employment_status STRING,
  occupation STRING,
  annual_income INT,
  account_status STRING,
  preferred_contact STRING,
  marketing_opt_in BOOLEAN,
  online_banking_enabled BOOLEAN,
  mobile_app_user BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
COMMENT 'Customer master data - bronze layer';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accounts Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS accounts (
  account_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  account_type STRING,
  account_number STRING,
  routing_number STRING,
  account_status STRING,
  balance DOUBLE,
  available_balance DOUBLE,
  hold_amount DOUBLE,
  currency STRING,
  interest_rate DOUBLE,
  minimum_balance DOUBLE,
  overdraft_protection BOOLEAN,
  overdraft_limit DOUBLE,
  monthly_fee DOUBLE,
  branch_id STRING,
  account_open_date TIMESTAMP,
  last_transaction_date TIMESTAMP,
  transaction_count_30d INT,
  average_daily_balance DOUBLE,
  year_to_date_interest DOUBLE,
  is_joint_account BOOLEAN,
  online_access_enabled BOOLEAN,
  paper_statements BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
COMMENT 'Account information - bronze layer';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transactions Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id STRING NOT NULL,
  account_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  transaction_date TIMESTAMP,
  transaction_type STRING,
  amount DOUBLE,
  is_debit BOOLEAN,
  balance_after DOUBLE,
  merchant_name STRING,
  merchant_category STRING,
  merchant_id STRING,
  channel STRING,
  status STRING,
  description STRING,
  reference_number STRING,
  authorization_code STRING,
  card_last_4 STRING,
  location_city STRING,
  location_state STRING,
  location_country STRING,
  is_international BOOLEAN,
  is_fraud BOOLEAN,
  fraud_score DOUBLE,
  ip_address STRING,
  device_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (DATE(transaction_date))
COMMENT 'Transaction history - bronze layer';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loans Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS loans (
  loan_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  loan_type STRING,
  loan_purpose STRING,
  loan_amount DOUBLE,
  interest_rate DOUBLE,
  term_months INT,
  monthly_payment DOUBLE,
  remaining_balance DOUBLE,
  total_paid DOUBLE,
  application_date TIMESTAMP,
  approval_date TIMESTAMP,
  origination_date TIMESTAMP,
  maturity_date TIMESTAMP,
  first_payment_date TIMESTAMP,
  next_payment_date TIMESTAMP,
  last_payment_date TIMESTAMP,
  loan_status STRING,
  approval_status STRING,
  denial_reason STRING,
  risk_score INT,
  collateral_type STRING,
  collateral_value DOUBLE,
  loan_to_value_ratio DOUBLE,
  debt_to_income_ratio DOUBLE,
  payments_made INT,
  missed_payments INT,
  days_past_due INT,
  is_delinquent BOOLEAN,
  is_default BOOLEAN,
  payment_frequency STRING,
  auto_pay_enabled BOOLEAN,
  underwriter_id STRING,
  branch_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
COMMENT 'Loan accounts - bronze layer';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Credit Cards Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_cards (
  card_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  card_type STRING,
  card_network STRING,
  card_number STRING,
  card_last_4 STRING,
  card_status STRING,
  issue_date TIMESTAMP,
  expiry_date TIMESTAMP,
  cvv STRING,
  credit_limit DOUBLE,
  current_balance DOUBLE,
  available_credit DOUBLE,
  utilization_percent DOUBLE,
  apr DOUBLE,
  annual_fee DOUBLE,
  rewards_program STRING,
  rewards_balance DOUBLE,
  rewards_earned_ytd DOUBLE,
  cash_back_rate DOUBLE,
  statement_balance DOUBLE,
  minimum_payment DOUBLE,
  last_statement_date TIMESTAMP,
  payment_due_date TIMESTAMP,
  last_payment_date TIMESTAMP,
  last_payment_amount DOUBLE,
  payments_made INT,
  late_payments INT,
  missed_payments INT,
  days_past_due INT,
  is_delinquent BOOLEAN,
  autopay_enabled BOOLEAN,
  autopay_amount STRING,
  paperless_statements BOOLEAN,
  foreign_transaction_fee DOUBLE,
  balance_transfer_apr DOUBLE,
  balance_transfer_fee DOUBLE,
  cash_advance_limit DOUBLE,
  cash_advance_fee DOUBLE,
  cash_advance_apr DOUBLE,
  over_limit_fee DOUBLE,
  late_payment_fee DOUBLE,
  card_replacement_fee DOUBLE,
  fraud_alerts_enabled BOOLEAN,
  travel_notification BOOLEAN,
  contactless_enabled BOOLEAN,
  virtual_card_enabled BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
COMMENT 'Credit card accounts - bronze layer';

-- COMMAND ----------

-- Show all tables
SHOW TABLES IN banking_catalog.banking_bronze;

-- COMMAND ----------

SELECT 'Bronze tables created successfully!' as status;

