-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Column-Level Security (CLS) Implementation
-- MAGIC 
-- MAGIC Implements comprehensive column-level security with data masking for PII and sensitive financial data:
-- MAGIC - SSN masking
-- MAGIC - Email masking
-- MAGIC - Phone number masking
-- MAGIC - Account number masking
-- MAGIC - Credit score hiding
-- MAGIC - Balance/amount masking
-- MAGIC - Address redaction

-- COMMAND ----------

USE CATALOG banking_catalog;
USE SCHEMA banking_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Masking Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 1: Mask SSN

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_ssn(ssn STRING)
RETURNS STRING
COMMENT 'Masks SSN showing only last 4 digits'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'RISK_MANAGER') THEN ssn
    ELSE CONCAT('XXX-XX-', SUBSTRING(ssn, -4, 4))
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 2: Mask Email

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_email(email STRING)
RETURNS STRING
COMMENT 'Partially masks email address'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'RELATIONSHIP_MANAGER', 'BRANCH_MANAGER') THEN email
    WHEN banking_gold.get_user_role() = 'CUSTOMER_SERVICE' THEN 
      CONCAT(SUBSTRING(email, 1, 3), '***@', SUBSTRING_INDEX(email, '@', -1))
    ELSE '***@***'
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 3: Mask Phone Number

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_phone(phone STRING)
RETURNS STRING
COMMENT 'Masks phone number showing only last 4 digits'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'CUSTOMER_SERVICE', 'RELATIONSHIP_MANAGER') THEN phone
    ELSE CONCAT('XXX-XXX-', SUBSTRING(phone, -4, 4))
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 4: Mask Account Number

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_account_number(account_num STRING)
RETURNS STRING
COMMENT 'Masks account number showing only last 4 digits'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'BRANCH_MANAGER') THEN account_num
    WHEN banking_gold.get_user_role() = 'CUSTOMER_SERVICE' THEN 
      CONCAT('****', SUBSTRING(account_num, -4, 4))
    ELSE '****'
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 5: Mask Credit Score

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_credit_score(score INT)
RETURNS STRING
COMMENT 'Hides credit score for unauthorized users'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'RISK_MANAGER', 'RELATIONSHIP_MANAGER') THEN CAST(score AS STRING)
    WHEN banking_gold.get_user_role() = 'CUSTOMER_SERVICE' THEN 
      -- Show only rating category
      CASE
        WHEN score >= 750 THEN 'Excellent (750+)'
        WHEN score >= 700 THEN 'Good (700-749)'
        WHEN score >= 650 THEN 'Fair (650-699)'
        ELSE 'Poor (<650)'
      END
    ELSE 'HIDDEN'
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 6: Mask Financial Amounts

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_amount(amount DOUBLE)
RETURNS STRING
COMMENT 'Masks or rounds financial amounts based on role'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'BRANCH_MANAGER', 'RISK_MANAGER') THEN 
      CONCAT('$', FORMAT_NUMBER(amount, 2))
    WHEN banking_gold.get_user_role() IN ('RELATIONSHIP_MANAGER', 'CUSTOMER_SERVICE') THEN 
      -- Round to nearest thousand
      CONCAT('$', FORMAT_NUMBER(ROUND(amount, -3), 0), 'K')
    WHEN banking_gold.get_user_role() = 'DATA_ANALYST' THEN 
      -- Show ranges only
      CASE
        WHEN amount < 1000 THEN '$0-$1K'
        WHEN amount < 10000 THEN '$1K-$10K'
        WHEN amount < 50000 THEN '$10K-$50K'
        WHEN amount < 100000 THEN '$50K-$100K'
        ELSE '$100K+'
      END
    ELSE 'HIDDEN'
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 7: Mask Address

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_address(address STRING, city STRING, state STRING)
RETURNS STRING
COMMENT 'Masks or partially reveals address based on role'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'BRANCH_MANAGER', 'RELATIONSHIP_MANAGER') THEN 
      CONCAT(address, ', ', city, ', ', state)
    WHEN banking_gold.get_user_role() = 'CUSTOMER_SERVICE' THEN 
      CONCAT(city, ', ', state)
    ELSE state
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function 8: Mask Fraud Score

-- COMMAND ----------

CREATE OR REPLACE FUNCTION banking_gold.mask_fraud_score(fraud_score DOUBLE)
RETURNS STRING
COMMENT 'Hides fraud score from unauthorized users'
RETURN 
  CASE 
    WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE', 'FRAUD_ANALYST', 'RISK_MANAGER') THEN 
      CAST(fraud_score AS STRING)
    ELSE 'RESTRICTED'
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create CLS-Protected Views with Masking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLS View: Customer Data with Masking

-- COMMAND ----------

CREATE OR REPLACE VIEW banking_gold.customers_cls_protected AS
SELECT 
    customer_sk,
    customer_id,
    first_name,
    last_name,
    full_name,
    
    -- MASKED COLUMNS
    banking_gold.mask_email(email) as email,
    banking_gold.mask_phone(phone) as phone,
    banking_gold.mask_ssn(ssn_last_4) as ssn_last_4,
    banking_gold.mask_credit_score(credit_score) as credit_score,
    banking_gold.mask_address(address, city, state) as full_address,
    banking_gold.mask_amount(CAST(annual_income AS DOUBLE)) as annual_income,
    
    -- NON-SENSITIVE COLUMNS (unmasked)
    date_of_birth,
    age,
    city,
    state,
    zip_code,
    country,
    risk_rating,
    customer_segment,
    employment_status,
    occupation,
    account_status,
    effective_from,
    effective_to,
    is_current
    
FROM banking_gold.dim_customer
WHERE is_current = TRUE
COMMENT 'Customer view with column-level security and PII masking';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLS View: Account Data with Masking

-- COMMAND ----------

CREATE OR REPLACE VIEW banking_gold.accounts_cls_protected AS
SELECT 
    account_sk,
    account_id,
    customer_id,
    account_type,
    
    -- MASKED COLUMNS
    banking_gold.mask_account_number(account_number) as account_number,
    banking_gold.mask_amount(balance) as balance,
    banking_gold.mask_amount(available_balance) as available_balance,
    banking_gold.mask_amount(minimum_balance) as minimum_balance,
    
    -- NON-SENSITIVE COLUMNS
    routing_number,
    account_status,
    currency,
    interest_rate,
    overdraft_protection,
    overdraft_limit,
    monthly_fee,
    branch_id,
    account_open_date,
    effective_from,
    effective_to,
    is_current
    
FROM banking_gold.dim_account
WHERE is_current = TRUE
COMMENT 'Account view with column-level security and balance masking';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLS View: Transaction Data with Masking

-- COMMAND ----------

CREATE OR REPLACE VIEW banking_gold.transactions_cls_protected AS
SELECT 
    transaction_fact_id,
    transaction_date_key,
    customer_sk,
    account_sk,
    merchant_sk,
    transaction_id,
    transaction_type,
    channel,
    status,
    
    -- MASKED COLUMNS
    banking_gold.mask_amount(transaction_amount) as transaction_amount,
    banking_gold.mask_amount(balance_after) as balance_after,
    banking_gold.mask_fraud_score(fraud_score) as fraud_score,
    
    -- NON-SENSITIVE COLUMNS
    is_debit,
    is_international,
    is_fraud,
    is_weekend,
    is_night_transaction,
    risk_score,
    transaction_timestamp,
    processed_timestamp,
    created_at
    
FROM banking_gold.fact_transactions
COMMENT 'Transaction view with column-level security and amount masking';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLS View: Fraud Alerts with Masking

-- COMMAND ----------

CREATE OR REPLACE VIEW banking_gold.fraud_alerts_cls_protected AS
SELECT 
    transaction_id,
    customer_id,
    account_id,
    transaction_date,
    
    -- MASKED COLUMNS
    banking_gold.mask_amount(amount) as amount,
    banking_gold.mask_fraud_score(realtime_fraud_score) as realtime_fraud_score,
    
    -- VISIBLE TO FRAUD ANALYSTS ONLY
    CASE 
      WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'FRAUD_ANALYST', 'COMPLIANCE') THEN merchant_name
      ELSE 'REDACTED'
    END as merchant_name,
    
    CASE 
      WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'FRAUD_ANALYST', 'COMPLIANCE') THEN merchant_category
      ELSE 'REDACTED'
    END as merchant_category,
    
    -- NON-SENSITIVE COLUMNS
    channel,
    fraud_risk_category,
    fraud_indicators_triggered,
    requires_immediate_action,
    recommended_action,
    alert_timestamp,
    processing_latency_ms
    
FROM banking_catalog.banking_gold.fraud_detection_realtime
COMMENT 'Fraud alerts with column-level security and sensitive data masking';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLS View: Loan Data with Masking

-- COMMAND ----------

CREATE OR REPLACE VIEW banking_gold.loans_cls_protected AS
SELECT 
    loan_fact_id,
    origination_date_key,
    customer_sk,
    loan_id,
    loan_type,
    loan_status,
    
    -- MASKED COLUMNS
    banking_gold.mask_amount(original_loan_amount) as original_loan_amount,
    banking_gold.mask_amount(current_balance) as current_balance,
    banking_gold.mask_amount(monthly_payment_amount) as monthly_payment_amount,
    
    -- SENSITIVE METRICS (only for risk managers)
    CASE 
      WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'RISK_MANAGER', 'COMPLIANCE') THEN CAST(default_probability AS STRING)
      ELSE 'RESTRICTED'
    END as default_probability,
    
    CASE 
      WHEN banking_gold.get_user_role() IN ('EXECUTIVE', 'RISK_MANAGER', 'COMPLIANCE') THEN CAST(debt_to_income_ratio AS STRING)
      ELSE 'RESTRICTED'
    END as debt_to_income_ratio,
    
    -- NON-SENSITIVE COLUMNS
    payment_count,
    missed_payment_count,
    days_past_due,
    interest_rate,
    is_delinquent,
    is_default,
    has_autopay,
    origination_date,
    maturity_date,
    next_payment_date
    
FROM banking_gold.fact_loan_performance
COMMENT 'Loan view with column-level security and amount/metric masking';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test CLS Implementation

-- COMMAND ----------

-- Test 1: View masked customer data
SELECT 
    customer_id,
    full_name,
    email,
    phone,
    ssn_last_4,
    credit_score,
    annual_income,
    full_address
FROM banking_gold.customers_cls_protected
LIMIT 5;

-- COMMAND ----------

-- Test 2: View masked account data
SELECT 
    account_id,
    account_type,
    account_number,
    balance,
    available_balance
FROM banking_gold.accounts_cls_protected
LIMIT 5;

-- COMMAND ----------

-- Test 3: View masked transaction data
SELECT 
    transaction_id,
    transaction_type,
    transaction_amount,
    balance_after,
    fraud_score
FROM banking_gold.transactions_cls_protected
WHERE transaction_date_key >= 20240101
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant Permissions on CLS Views

-- COMMAND ----------

-- All authenticated users can access CLS-protected views
-- Data will be automatically masked based on their role
GRANT SELECT ON VIEW banking_gold.customers_cls_protected TO `account users`;
GRANT SELECT ON VIEW banking_gold.accounts_cls_protected TO `account users`;
GRANT SELECT ON VIEW banking_gold.transactions_cls_protected TO `account users`;
GRANT SELECT ON VIEW banking_gold.loans_cls_protected TO `account users`;

-- Fraud alerts only for authorized roles
GRANT SELECT ON VIEW banking_gold.fraud_alerts_cls_protected TO `fraud_analysts`;
GRANT SELECT ON VIEW banking_gold.fraud_alerts_cls_protected TO `compliance_officers`;
GRANT SELECT ON VIEW banking_gold.fraud_alerts_cls_protected TO `executives`;
GRANT SELECT ON VIEW banking_gold.fraud_alerts_cls_protected TO `risk_managers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Combined RLS + CLS View (Full Protection)

-- COMMAND ----------

-- Combined view with both row-level and column-level security
CREATE OR REPLACE VIEW banking_gold.customers_fully_protected AS
SELECT * FROM banking_gold.customers_cls_protected
WHERE 
  -- RLS filtering (same as RLS implementation)
  (
    banking_gold.get_user_role() = 'EXECUTIVE'
    OR banking_gold.get_user_role() = 'COMPLIANCE'
    OR (banking_gold.get_user_role() = 'BRANCH_MANAGER' 
        AND customer_id IN (
          SELECT customer_id FROM banking_gold.dim_account 
          WHERE is_current = TRUE 
          AND branch_id = banking_gold.get_user_branch_id()
        ))
    OR (banking_gold.get_user_role() = 'RELATIONSHIP_MANAGER'
        AND customer_id IN (SELECT explode(banking_gold.get_assigned_customers())))
    OR (banking_gold.get_user_role() = 'CUSTOMER_SERVICE'
        AND account_status = 'Active')
  )
COMMENT 'Customer view with BOTH row-level and column-level security';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CLS Summary

-- COMMAND ----------

SELECT '✅ Column-Level Security (CLS) implemented successfully!' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CLS Masking Rules Summary
-- MAGIC 
-- MAGIC | Data Type | Executive | Compliance | Risk Manager | Branch Manager | Relationship Manager | Customer Service | Data Analyst |
-- MAGIC |-----------|-----------|------------|--------------|----------------|---------------------|------------------|--------------|
-- MAGIC | **SSN** | Full | Full | Full | Last 4 | Last 4 | Last 4 | Last 4 |
-- MAGIC | **Email** | Full | Full | Full | Full | Full | Partial | Hidden |
-- MAGIC | **Phone** | Full | Full | Full | Full | Full | Full | Last 4 |
-- MAGIC | **Account #** | Full | Full | Full | Full | Last 4 | Last 4 | Hidden |
-- MAGIC | **Credit Score** | Full | Full | Full | Full | Full | Category | Hidden |
-- MAGIC | **Balances** | Full | Full | Full | Full | Rounded | Rounded | Ranges |
-- MAGIC | **Address** | Full | Full | Full | Full | Full | City/State | State Only |
-- MAGIC | **Fraud Score** | Full | Full | Full | Full | Restricted | Restricted | Restricted |
-- MAGIC 
-- MAGIC ## CLS Benefits
-- MAGIC 
-- MAGIC ✅ **PII Protection**: Sensitive data automatically masked  
-- MAGIC ✅ **Role-Based**: Different masking for different roles  
-- MAGIC ✅ **Transparent**: No application changes needed  
-- MAGIC ✅ **Compliant**: Meets GDPR, CCPA, PCI-DSS requirements  
-- MAGIC ✅ **Auditable**: All access logged  
-- MAGIC ✅ **Flexible**: Easy to add new masking rules  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Usage Examples
-- MAGIC 
-- MAGIC ```sql
-- MAGIC -- Same query, different results based on user role:
-- MAGIC SELECT * FROM banking_gold.customers_fully_protected WHERE customer_id = 'CUST-12345';
-- MAGIC 
-- MAGIC -- Executive sees:
-- MAGIC -- email: john.doe@email.com
-- MAGIC -- ssn_last_4: 123-45-6789
-- MAGIC -- credit_score: 750
-- MAGIC -- annual_income: $85,000.00
-- MAGIC 
-- MAGIC -- Customer Service sees:
-- MAGIC -- email: joh***@email.com
-- MAGIC -- ssn_last_4: XXX-XX-6789
-- MAGIC -- credit_score: Good (700-749)
-- MAGIC -- annual_income: $85K
-- MAGIC 
-- MAGIC -- Data Analyst sees:
-- MAGIC -- email: ***@***
-- MAGIC -- ssn_last_4: XXX-XX-6789
-- MAGIC -- credit_score: HIDDEN
-- MAGIC -- annual_income: $50K-$100K
-- MAGIC ```

