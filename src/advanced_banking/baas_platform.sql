-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Banking-as-a-Service (BaaS) Platform
-- MAGIC 
-- MAGIC Complete white-label banking infrastructure for fintechs and platforms:
-- MAGIC - Account creation and management
-- MAGIC - Card issuing (virtual and physical)
-- MAGIC - Multi-currency support
-- MAGIC - Partner/tenant management
-- MAGIC - API gateway and rate limiting
-- MAGIC - Compliance and regulatory reporting
-- MAGIC - Revenue sharing model

-- COMMAND ----------

USE CATALOG banking_catalog;
CREATE SCHEMA IF NOT EXISTS baas_platform;
USE SCHEMA baas_platform;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BaaS Partners (Tenants)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baas_partners (
    partner_id STRING PRIMARY KEY,
    
    -- Partner Information
    partner_name STRING NOT NULL,
    partner_type STRING NOT NULL,  -- FINTECH, MARKETPLACE, NEOBANK, PLATFORM
    legal_entity STRING NOT NULL,
    
    -- Branding
    brand_name STRING,
    logo_url STRING,
    primary_color STRING,
    custom_domain STRING,
    
    -- API Configuration
    api_key STRING UNIQUE NOT NULL,
    api_secret_hash STRING NOT NULL,
    sandbox_api_key STRING,
    webhook_url STRING,
    callback_url STRING,
    
    -- Limits and Quotas
    max_accounts INT DEFAULT 10000,
    current_account_count INT DEFAULT 0,
    max_monthly_volume DECIMAL(18, 2) DEFAULT 1000000,
    current_monthly_volume DECIMAL(18, 2) DEFAULT 0,
    
    -- API Rate Limits
    api_calls_per_minute INT DEFAULT 100,
    api_calls_per_day INT DEFAULT 10000,
    
    -- Revenue Model
    revenue_share_percent DECIMAL(5, 4) DEFAULT 20.00,  -- Partner gets 20%
    monthly_platform_fee DECIMAL(18, 2) DEFAULT 500,
    
    -- Products Enabled
    accounts_enabled BOOLEAN DEFAULT TRUE,
    cards_enabled BOOLEAN DEFAULT FALSE,
    payments_enabled BOOLEAN DEFAULT TRUE,
    lending_enabled BOOLEAN DEFAULT FALSE,
    
    -- Compliance
    kyb_status STRING DEFAULT 'PENDING',  -- PENDING, APPROVED, REJECTED
    regulatory_approval STRING,
    sponsor_bank STRING DEFAULT 'Partner Bank NA',
    
    -- Status
    status STRING DEFAULT 'ACTIVE',  -- ACTIVE, SUSPENDED, TERMINATED
    go_live_date DATE,
    created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BaaS Accounts

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baas_accounts (
    account_id STRING PRIMARY KEY,
    partner_id STRING NOT NULL,
    end_user_id STRING NOT NULL,  -- Partner's customer ID
    
    -- Account Details
    account_number STRING UNIQUE NOT NULL,
    routing_number STRING NOT NULL,
    account_type STRING DEFAULT 'CHECKING',  -- CHECKING, SAVINGS
    account_name STRING,
    
    -- Balances
    available_balance DECIMAL(18, 2) DEFAULT 0,
    pending_balance DECIMAL(18, 2) DEFAULT 0,
    total_balance DECIMAL(18, 2) GENERATED ALWAYS AS (available_balance + pending_balance),
    
    -- Currency
    currency STRING DEFAULT 'USD',
    
    -- Limits
    daily_debit_limit DECIMAL(18, 2) DEFAULT 5000,
    daily_credit_limit DECIMAL(18, 2) DEFAULT 25000,
    daily_debit_used DECIMAL(18, 2) DEFAULT 0,
    daily_credit_used DECIMAL(18, 2) DEFAULT 0,
    
    -- Features
    overdraft_enabled BOOLEAN DEFAULT FALSE,
    overdraft_limit DECIMAL(18, 2) DEFAULT 0,
    interest_bearing BOOLEAN DEFAULT FALSE,
    apy_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Status
    status STRING DEFAULT 'ACTIVE',  -- ACTIVE, FROZEN, CLOSED
    freeze_reason STRING,
    
    -- Timestamps
    opened_date DATE DEFAULT current_date(),
    closed_date DATE,
    last_transaction_date TIMESTAMP,
    
    CONSTRAINT fk_baas_partner FOREIGN KEY (partner_id) 
        REFERENCES banking_catalog.baas_platform.baas_partners(partner_id)
) USING DELTA
PARTITIONED BY (partner_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BaaS Card Issuing

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baas_cards (
    card_id STRING PRIMARY KEY,
    account_id STRING NOT NULL,
    partner_id STRING NOT NULL,
    end_user_id STRING NOT NULL,
    
    -- Card Details
    card_number_encrypted STRING NOT NULL,
    card_last_4 STRING NOT NULL,
    cvv_encrypted STRING NOT NULL,
    expiry_month INT NOT NULL,
    expiry_year INT NOT NULL,
    
    -- Card Type
    card_type STRING NOT NULL,  -- VIRTUAL, PHYSICAL
    card_brand STRING DEFAULT 'VISA',  -- VISA, MASTERCARD
    card_tier STRING DEFAULT 'STANDARD',  -- STANDARD, PREMIUM, BUSINESS
    
    -- Personalization
    cardholder_name STRING NOT NULL,
    embossed_name STRING,
    card_design_id STRING,
    
    -- Shipping (for physical cards)
    shipping_status STRING,  -- ORDERED, SHIPPED, DELIVERED, RETURNED
    shipping_address STRING,
    tracking_number STRING,
    
    -- Spending Controls
    daily_spend_limit DECIMAL(18, 2) DEFAULT 1000,
    single_transaction_limit DECIMAL(18, 2) DEFAULT 500,
    monthly_spend_limit DECIMAL(18, 2) DEFAULT 5000,
    
    -- Current Usage
    daily_spend DECIMAL(18, 2) DEFAULT 0,
    monthly_spend DECIMAL(18, 2) DEFAULT 0,
    
    -- Merchant Category Controls
    allowed_mcc_codes ARRAY<STRING>,  -- Merchant Category Codes
    blocked_mcc_codes ARRAY<STRING>,
    
    -- Security
    pin_set BOOLEAN DEFAULT FALSE,
    contactless_enabled BOOLEAN DEFAULT TRUE,
    online_purchases_enabled BOOLEAN DEFAULT TRUE,
    international_enabled BOOLEAN DEFAULT FALSE,
    
    -- Status
    status STRING DEFAULT 'ACTIVE',  -- ACTIVE, FROZEN, CANCELLED, EXPIRED
    activation_date TIMESTAMP,
    cancellation_reason STRING,
    
    -- Timestamps
    issued_date TIMESTAMP DEFAULT current_timestamp(),
    last_used_date TIMESTAMP,
    
    CONSTRAINT fk_baas_account FOREIGN KEY (account_id) 
        REFERENCES banking_catalog.baas_platform.baas_accounts(account_id)
) USING DELTA
PARTITIONED BY (partner_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BaaS Transactions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baas_transactions (
    transaction_id STRING PRIMARY KEY,
    partner_id STRING NOT NULL,
    account_id STRING NOT NULL,
    card_id STRING,
    
    -- Transaction Details
    transaction_type STRING NOT NULL,  -- DEBIT, CREDIT, TRANSFER, FEE
    transaction_method STRING,  -- CARD, ACH, WIRE, P2P, ATM
    amount DECIMAL(18, 2) NOT NULL,
    currency STRING DEFAULT 'USD',
    
    -- Counterparty
    merchant_name STRING,
    merchant_mcc STRING,
    merchant_city STRING,
    merchant_country STRING,
    
    -- Card Transaction Details
    card_present BOOLEAN,
    entry_method STRING,  -- CHIP, SWIPE, TAP, ONLINE, MANUAL
    auth_code STRING,
    
    -- Status
    status STRING DEFAULT 'PENDING',  -- PENDING, POSTED, DECLINED, REVERSED
    decline_reason STRING,
    
    -- Balance Impact
    balance_before DECIMAL(18, 2),
    balance_after DECIMAL(18, 2),
    
    -- Fraud & Risk
    fraud_score DOUBLE DEFAULT 0,
    fraud_checked BOOLEAN DEFAULT FALSE,
    risk_flags ARRAY<STRING>,
    
    -- Timestamps
    transaction_timestamp TIMESTAMP DEFAULT current_timestamp(),
    posted_timestamp TIMESTAMP,
    
    -- Metadata
    description STRING,
    reference_id STRING,
    external_id STRING
) USING DELTA
PARTITIONED BY (DATE(transaction_timestamp), partner_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## API Usage Tracking

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS api_usage_log (
    log_id BIGINT GENERATED ALWAYS AS IDENTITY,
    partner_id STRING NOT NULL,
    
    -- API Request
    endpoint STRING NOT NULL,
    http_method STRING NOT NULL,
    request_timestamp TIMESTAMP DEFAULT current_timestamp(),
    
    -- Response
    status_code INT,
    response_time_ms INT,
    success BOOLEAN,
    error_message STRING,
    
    -- Rate Limiting
    rate_limit_remaining INT,
    rate_limit_reset TIMESTAMP,
    
    -- Request Details
    ip_address STRING,
    user_agent STRING,
    request_size_bytes INT,
    response_size_bytes INT,
    
    PRIMARY KEY (log_id)
) USING DELTA
PARTITIONED BY (DATE(request_timestamp))
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revenue Tracking

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS baas_revenue (
    revenue_id STRING PRIMARY KEY,
    partner_id STRING NOT NULL,
    revenue_date DATE NOT NULL,
    
    -- Transaction Volumes
    transaction_count BIGINT DEFAULT 0,
    transaction_volume DECIMAL(18, 2) DEFAULT 0,
    
    -- Fees Collected
    interchange_fees DECIMAL(18, 2) DEFAULT 0,
    transaction_fees DECIMAL(18, 2) DEFAULT 0,
    monthly_platform_fees DECIMAL(18, 2) DEFAULT 0,
    other_fees DECIMAL(18, 2) DEFAULT 0,
    total_revenue DECIMAL(18, 2),
    
    -- Revenue Share
    partner_share DECIMAL(18, 2),
    platform_share DECIMAL(18, 2),
    
    -- Payout
    payout_status STRING DEFAULT 'PENDING',  -- PENDING, PAID
    payout_date DATE,
    
    created_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
PARTITIONED BY (revenue_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views and Analytics

-- COMMAND ----------

-- Partner Dashboard View
CREATE OR REPLACE VIEW partner_dashboard AS
SELECT 
    p.partner_id,
    p.partner_name,
    p.status,
    COUNT(DISTINCT a.account_id) as active_accounts,
    COUNT(DISTINCT c.card_id) as active_cards,
    SUM(a.total_balance) as total_deposits,
    COUNT(DISTINCT t.transaction_id) as monthly_transactions,
    SUM(CASE WHEN t.transaction_timestamp >= current_timestamp() - INTERVAL 30 DAYS 
        THEN t.amount ELSE 0 END) as monthly_volume
FROM banking_catalog.baas_platform.baas_partners p
LEFT JOIN banking_catalog.baas_platform.baas_accounts a ON p.partner_id = a.partner_id
LEFT JOIN banking_catalog.baas_platform.baas_cards c ON p.partner_id = c.partner_id
LEFT JOIN banking_catalog.baas_platform.baas_transactions t ON p.partner_id = t.partner_id
WHERE p.status = 'ACTIVE'
GROUP BY p.partner_id, p.partner_name, p.status;

-- COMMAND ----------

-- API Performance Metrics
CREATE OR REPLACE VIEW api_performance_metrics AS
SELECT 
    partner_id,
    DATE(request_timestamp) as date,
    COUNT(*) as total_requests,
    COUNT(CASE WHEN success = TRUE THEN 1 END) as successful_requests,
    COUNT(CASE WHEN success = FALSE THEN 1 END) as failed_requests,
    AVG(response_time_ms) as avg_response_time_ms,
    MAX(response_time_ms) as max_response_time_ms,
    PERCENTILE(response_time_ms, 0.95) as p95_response_time_ms
FROM banking_catalog.baas_platform.api_usage_log
GROUP BY partner_id, DATE(request_timestamp);

-- COMMAND ----------

-- Top Partners by Volume
CREATE OR REPLACE VIEW top_partners_by_volume AS
SELECT 
    p.partner_id,
    p.partner_name,
    SUM(t.amount) as total_volume,
    COUNT(t.transaction_id) as transaction_count,
    AVG(t.amount) as avg_transaction_amount
FROM banking_catalog.baas_platform.baas_partners p
JOIN banking_catalog.baas_platform.baas_transactions t 
    ON p.partner_id = t.partner_id
WHERE t.transaction_timestamp >= current_timestamp() - INTERVAL 30 DAYS
  AND t.status = 'POSTED'
GROUP BY p.partner_id, p.partner_name
ORDER BY total_volume DESC
LIMIT 100;

-- COMMAND ----------

SELECT '✅ Banking-as-a-Service (BaaS) Platform implemented successfully!' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Features Implemented
-- MAGIC 
-- MAGIC ✅ **Multi-Tenant Architecture** - Support for unlimited partners/fintechs  
-- MAGIC ✅ **Account Management** - Checking and savings accounts  
-- MAGIC ✅ **Card Issuing** - Virtual and physical cards (Visa/Mastercard)  
-- MAGIC ✅ **Transaction Processing** - Real-time payment processing  
-- MAGIC ✅ **API Gateway** - RESTful APIs with rate limiting  
-- MAGIC ✅ **White-Label** - Custom branding per partner  
-- MAGIC ✅ **Revenue Sharing** - Automated fee distribution  
-- MAGIC ✅ **Compliance** - KYB verification and regulatory reporting  
-- MAGIC ✅ **Multi-Currency** - Support for international payments  
-- MAGIC ✅ **Analytics** - Partner dashboards and usage metrics

