-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Star Schema for Banking Analytics
-- MAGIC 
-- MAGIC Creates a proper star schema with:
-- MAGIC - Dimension tables (slowly changing dimensions)
-- MAGIC - Fact tables (transaction and event data)
-- MAGIC - Conformed dimensions for consistency
-- MAGIC - Optimized for analytical queries

-- COMMAND ----------

USE CATALOG banking_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Gold Schema for Star Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS banking_gold
COMMENT 'Gold layer - Star schema for business analytics';

USE SCHEMA banking_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Date Dimension (Critical for time-series analysis)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT NOT NULL,
    date_value DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    week INT,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Date dimension for time-series analysis';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Merchant Dimension

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    merchant_id STRING NOT NULL,
    merchant_name STRING,
    merchant_category STRING,
    merchant_type STRING,
    risk_level STRING,
    is_high_risk BOOLEAN,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_merchant PRIMARY KEY (merchant_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Merchant dimension - SCD Type 2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Product Dimension (Banking Products)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id STRING NOT NULL,
    product_name STRING,
    product_type STRING,
    product_category STRING,
    interest_rate_min DOUBLE,
    interest_rate_max DOUBLE,
    minimum_balance DOUBLE,
    monthly_fee DOUBLE,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Banking product dimension - SCD Type 2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Branch Dimension

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    branch_id STRING NOT NULL,
    branch_name STRING,
    branch_type STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,
    manager_name STRING,
    phone STRING,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_branch PRIMARY KEY (branch_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Branch/location dimension - SCD Type 2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact: Transactions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_fact_id BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Foreign keys to dimensions
    transaction_date_key INT NOT NULL,
    customer_sk BIGINT,
    account_sk BIGINT,
    merchant_sk BIGINT,
    
    -- Degenerate dimensions (transaction-specific attributes)
    transaction_id STRING NOT NULL,
    transaction_type STRING,
    channel STRING,
    status STRING,
    
    -- Measures (facts)
    transaction_amount DOUBLE,
    balance_after DOUBLE,
    fee_amount DOUBLE,
    
    -- Flags
    is_debit BOOLEAN,
    is_international BOOLEAN,
    is_fraud BOOLEAN,
    is_weekend BOOLEAN,
    is_night_transaction BOOLEAN,
    
    -- Metrics
    fraud_score DOUBLE,
    risk_score DOUBLE,
    
    -- Timestamps
    transaction_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP,
    
    -- Audit
    created_at TIMESTAMP,
    
    CONSTRAINT pk_fact_transactions PRIMARY KEY (transaction_fact_id)
)
USING DELTA
PARTITIONED BY (transaction_date_key)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Transaction fact table - partitioned by date';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact: Account Daily Snapshot

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fact_account_daily_snapshot (
    snapshot_date_key INT NOT NULL,
    account_sk BIGINT NOT NULL,
    customer_sk BIGINT NOT NULL,
    
    -- Daily measures
    beginning_balance DOUBLE,
    ending_balance DOUBLE,
    average_balance DOUBLE,
    min_balance DOUBLE,
    max_balance DOUBLE,
    
    -- Activity counts
    deposit_count INT,
    withdrawal_count INT,
    total_transaction_count INT,
    
    -- Activity amounts
    total_deposits DOUBLE,
    total_withdrawals DOUBLE,
    total_fees DOUBLE,
    interest_earned DOUBLE,
    
    -- Flags
    is_overdrawn BOOLEAN,
    is_dormant BOOLEAN,
    
    -- Snapshot metadata
    snapshot_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    
    CONSTRAINT pk_fact_account_snapshot PRIMARY KEY (snapshot_date_key, account_sk)
)
USING DELTA
PARTITIONED BY (snapshot_date_key)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Daily account balance snapshot fact table';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact: Loan Performance

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fact_loan_performance (
    loan_fact_id BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Foreign keys
    origination_date_key INT NOT NULL,
    customer_sk BIGINT NOT NULL,
    
    -- Degenerate dimensions
    loan_id STRING NOT NULL,
    loan_type STRING,
    loan_status STRING,
    
    -- Measures
    original_loan_amount DOUBLE,
    current_balance DOUBLE,
    principal_paid DOUBLE,
    interest_paid DOUBLE,
    fees_paid DOUBLE,
    monthly_payment_amount DOUBLE,
    
    -- Metrics
    payment_count INT,
    missed_payment_count INT,
    days_past_due INT,
    default_probability DOUBLE,
    
    -- Rates and ratios
    interest_rate DOUBLE,
    loan_to_value_ratio DOUBLE,
    debt_to_income_ratio DOUBLE,
    
    -- Flags
    is_delinquent BOOLEAN,
    is_default BOOLEAN,
    has_autopay BOOLEAN,
    
    -- Dates
    origination_date DATE,
    maturity_date DATE,
    next_payment_date DATE,
    
    -- Audit
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    
    CONSTRAINT pk_fact_loan PRIMARY KEY (loan_fact_id)
)
USING DELTA
PARTITIONED BY (origination_date_key)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Loan performance fact table';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact: Credit Card Usage

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fact_credit_card_usage (
    card_usage_fact_id BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Foreign keys
    transaction_date_key INT NOT NULL,
    customer_sk BIGINT NOT NULL,
    merchant_sk BIGINT,
    
    -- Degenerate dimensions
    card_id STRING NOT NULL,
    transaction_id STRING NOT NULL,
    card_type STRING,
    
    -- Measures
    transaction_amount DOUBLE,
    fee_amount DOUBLE,
    rewards_earned DOUBLE,
    
    -- Card state at transaction time
    credit_limit DOUBLE,
    available_credit DOUBLE,
    utilization_percent DOUBLE,
    
    -- Flags
    is_international BOOLEAN,
    is_cash_advance BOOLEAN,
    is_balance_transfer BOOLEAN,
    is_declined BOOLEAN,
    is_fraud_suspected BOOLEAN,
    
    -- Metrics
    fraud_score DOUBLE,
    
    -- Timestamps
    transaction_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    
    CONSTRAINT pk_fact_card_usage PRIMARY KEY (card_usage_fact_id)
)
USING DELTA
PARTITIONED BY (transaction_date_key)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Credit card usage fact table';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Indexes for Performance

-- COMMAND ----------

-- Optimize fact tables with Z-ORDER
OPTIMIZE banking_catalog.banking_gold.fact_transactions 
ZORDER BY (customer_sk, account_sk, transaction_date_key);

OPTIMIZE banking_catalog.banking_gold.fact_account_daily_snapshot 
ZORDER BY (customer_sk, account_sk);

OPTIMIZE banking_catalog.banking_gold.fact_loan_performance 
ZORDER BY (customer_sk, loan_id);

OPTIMIZE banking_catalog.banking_gold.fact_credit_card_usage 
ZORDER BY (customer_sk, card_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Show All Tables

-- COMMAND ----------

SHOW TABLES IN banking_catalog.banking_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Star Schema Documentation
-- MAGIC 
-- MAGIC ### Schema Overview:
-- MAGIC ```
-- MAGIC                    dim_date
-- MAGIC                       |
-- MAGIC                       |
-- MAGIC     dim_customer ---- fact_transactions ---- dim_merchant
-- MAGIC            |              |
-- MAGIC            |              |
-- MAGIC     dim_account -------- dim_branch
-- MAGIC 
-- MAGIC 
-- MAGIC     dim_customer ---- fact_account_daily_snapshot ---- dim_date
-- MAGIC            |
-- MAGIC     dim_account
-- MAGIC 
-- MAGIC 
-- MAGIC     dim_customer ---- fact_loan_performance ---- dim_date
-- MAGIC            |
-- MAGIC      dim_product
-- MAGIC 
-- MAGIC 
-- MAGIC     dim_customer ---- fact_credit_card_usage ---- dim_merchant
-- MAGIC            |                   |
-- MAGIC      dim_product           dim_date
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Benefits:
-- MAGIC - **Optimized for BI tools** (Power BI, Tableau)
-- MAGIC - **Fast aggregations** on fact tables
-- MAGIC - **Conformed dimensions** for consistency
-- MAGIC - **Historical tracking** with SCD Type 2
-- MAGIC - **Partitioned facts** for performance

-- COMMAND ----------

SELECT '✓ Star Schema created successfully!' as status;

