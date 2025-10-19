-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PII Field Tagging System
-- MAGIC 
-- MAGIC Implements comprehensive PII classification and tagging across all tables:
-- MAGIC - Automatic PII detection and tagging
-- MAGIC - Classification levels (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, PII)
-- MAGIC - Column-level metadata with tags
-- MAGIC - GDPR category mapping
-- MAGIC - Automated PII discovery

-- COMMAND ----------

USE CATALOG banking_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create PII Metadata Tables

-- COMMAND ----------

-- PII Classification Catalog
CREATE TABLE IF NOT EXISTS banking_catalog.security.pii_classification_catalog (
    classification_id INT PRIMARY KEY,
    classification_name STRING NOT NULL,
    classification_level INT NOT NULL,  -- 1=PUBLIC, 2=INTERNAL, 3=CONFIDENTIAL, 4=RESTRICTED, 5=PII
    description STRING,
    requires_encryption BOOLEAN DEFAULT FALSE,
    requires_masking BOOLEAN DEFAULT FALSE,
    requires_audit_log BOOLEAN DEFAULT FALSE,
    gdpr_category STRING,  -- PERSONAL_DATA, SPECIAL_CATEGORY, FINANCIAL, HEALTH, etc.
    retention_days INT,
    deletion_method STRING  -- ANONYMIZE, HARD_DELETE, ARCHIVE
) USING DELTA;

INSERT INTO banking_catalog.security.pii_classification_catalog VALUES
(1, 'PUBLIC', 1, 'Publicly available information', FALSE, FALSE, FALSE, NULL, NULL, NULL),
(2, 'INTERNAL', 2, 'Internal business data', FALSE, FALSE, TRUE, NULL, 2555, 'ARCHIVE'),
(3, 'CONFIDENTIAL', 3, 'Confidential business data', TRUE, FALSE, TRUE, NULL, 2555, 'ARCHIVE'),
(4, 'RESTRICTED', 4, 'Highly restricted data', TRUE, TRUE, TRUE, NULL, 2555, 'ANONYMIZE'),
(5, 'PII_SSN', 5, 'Social Security Number', TRUE, TRUE, TRUE, 'SPECIAL_CATEGORY', 2555, 'ANONYMIZE'),
(6, 'PII_EMAIL', 5, 'Email Address', TRUE, TRUE, TRUE, 'PERSONAL_DATA', 2555, 'ANONYMIZE'),
(7, 'PII_PHONE', 5, 'Phone Number', TRUE, TRUE, TRUE, 'PERSONAL_DATA', 2555, 'ANONYMIZE'),
(8, 'PII_ADDRESS', 5, 'Physical Address', TRUE, TRUE, TRUE, 'PERSONAL_DATA', 2555, 'ANONYMIZE'),
(9, 'PII_NAME', 5, 'Full Name', TRUE, TRUE, TRUE, 'PERSONAL_DATA', 2555, 'ANONYMIZE'),
(10, 'FINANCIAL_ACCOUNT', 5, 'Account Number', TRUE, TRUE, TRUE, 'FINANCIAL', 2555, 'ANONYMIZE'),
(11, 'FINANCIAL_CARD', 5, 'Credit Card Number', TRUE, TRUE, TRUE, 'FINANCIAL', 2555, 'ANONYMIZE'),
(12, 'FINANCIAL_BALANCE', 4, 'Account Balance', TRUE, TRUE, TRUE, 'FINANCIAL', 2555, 'ARCHIVE'),
(13, 'BIOMETRIC', 5, 'Biometric Data', TRUE, TRUE, TRUE, 'SPECIAL_CATEGORY', 2555, 'ANONYMIZE'),
(14, 'HEALTH', 5, 'Health Information', TRUE, TRUE, TRUE, 'SPECIAL_CATEGORY', 2555, 'ANONYMIZE');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PII Column Registry

-- COMMAND ----------

-- Master registry of all PII columns
CREATE TABLE IF NOT EXISTS banking_catalog.security.pii_column_registry (
    registry_id BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Column identification
    catalog_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    
    -- PII Classification
    classification_id INT NOT NULL,
    classification_name STRING NOT NULL,
    pii_type STRING NOT NULL,  -- SSN, EMAIL, PHONE, ADDRESS, NAME, ACCOUNT_NUMBER, etc.
    sensitivity_level STRING NOT NULL,  -- LOW, MEDIUM, HIGH, CRITICAL
    
    -- GDPR Information
    gdpr_category STRING,
    gdpr_lawful_basis STRING,  -- CONSENT, CONTRACT, LEGAL_OBLIGATION, etc.
    gdpr_special_category BOOLEAN DEFAULT FALSE,
    
    -- Data characteristics
    data_type STRING,
    is_encrypted BOOLEAN DEFAULT FALSE,
    is_masked BOOLEAN DEFAULT FALSE,
    masking_function STRING,
    
    -- Discovery information
    discovered_date TIMESTAMP DEFAULT current_timestamp(),
    discovered_by STRING,
    discovery_method STRING,  -- MANUAL, AUTOMATED, PATTERN_MATCH
    confidence_score DOUBLE,  -- 0.0 to 1.0
    
    -- Compliance
    requires_consent BOOLEAN DEFAULT FALSE,
    retention_days INT,
    last_accessed TIMESTAMP,
    access_count BIGINT DEFAULT 0,
    
    -- Metadata
    description STRING,
    business_owner STRING,
    data_steward STRING,
    is_active BOOLEAN DEFAULT TRUE,
    tags ARRAY<STRING>,
    
    CONSTRAINT pk_pii_registry PRIMARY KEY (registry_id),
    CONSTRAINT fk_classification FOREIGN KEY (classification_id) 
        REFERENCES banking_catalog.security.pii_classification_catalog(classification_id)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tag All PII Fields Across Banking Tables

-- COMMAND ----------

-- Customer Table PII Tags
INSERT INTO banking_catalog.security.pii_column_registry 
(catalog_name, schema_name, table_name, column_name, classification_id, classification_name, 
 pii_type, sensitivity_level, gdpr_category, gdpr_special_category, data_type, 
 requires_consent, retention_days, discovery_method, confidence_score, tags) VALUES

-- Customer Dimension
('banking_catalog', 'banking_gold', 'dim_customer', 'ssn_last_4', 5, 'PII_SSN', 'SSN', 'CRITICAL', 'SPECIAL_CATEGORY', TRUE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR', 'SENSITIVE')),
('banking_catalog', 'banking_gold', 'dim_customer', 'email', 6, 'PII_EMAIL', 'EMAIL', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR', 'CONTACT')),
('banking_catalog', 'banking_gold', 'dim_customer', 'phone', 7, 'PII_PHONE', 'PHONE', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR', 'CONTACT')),
('banking_catalog', 'banking_gold', 'dim_customer', 'address', 8, 'PII_ADDRESS', 'ADDRESS', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR', 'LOCATION')),
('banking_catalog', 'banking_gold', 'dim_customer', 'first_name', 9, 'PII_NAME', 'NAME', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR')),
('banking_catalog', 'banking_gold', 'dim_customer', 'last_name', 9, 'PII_NAME', 'NAME', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR')),
('banking_catalog', 'banking_gold', 'dim_customer', 'full_name', 9, 'PII_NAME', 'NAME', 'HIGH', 'PERSONAL_DATA', FALSE, 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR')),
('banking_catalog', 'banking_gold', 'dim_customer', 'date_of_birth', 5, 'PII_SSN', 'DOB', 'HIGH', 'PERSONAL_DATA', FALSE, 'TIMESTAMP', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'GDPR', 'DEMOGRAPHIC')),
('banking_catalog', 'banking_gold', 'dim_customer', 'credit_score', 12, 'FINANCIAL_BALANCE', 'CREDIT_SCORE', 'HIGH', 'FINANCIAL', FALSE, 'INT', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'SENSITIVE')),
('banking_catalog', 'banking_gold', 'dim_customer', 'annual_income', 12, 'FINANCIAL_BALANCE', 'INCOME', 'HIGH', 'FINANCIAL', FALSE, 'INT', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'SENSITIVE')),

-- Account Dimension
('banking_catalog', 'banking_gold', 'dim_account', 'account_number', 10, 'FINANCIAL_ACCOUNT', 'ACCOUNT_NUMBER', 'CRITICAL', 'FINANCIAL', FALSE, 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'FINANCIAL', 'ACCOUNT')),
('banking_catalog', 'banking_gold', 'dim_account', 'routing_number', 10, 'FINANCIAL_ACCOUNT', 'ROUTING_NUMBER', 'HIGH', 'FINANCIAL', FALSE, 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BANK')),
('banking_catalog', 'banking_gold', 'dim_account', 'balance', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BALANCE')),
('banking_catalog', 'banking_gold', 'dim_account', 'available_balance', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BALANCE')),

-- Transaction Fact
('banking_catalog', 'banking_gold', 'fact_transactions', 'transaction_amount', 12, 'FINANCIAL_BALANCE', 'AMOUNT', 'MEDIUM', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'TRANSACTION')),
('banking_catalog', 'banking_gold', 'fact_transactions', 'balance_after', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BALANCE')),

-- Loan Performance Fact
('banking_catalog', 'banking_gold', 'fact_loan_performance', 'original_loan_amount', 12, 'FINANCIAL_BALANCE', 'LOAN_AMOUNT', 'MEDIUM', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'LOAN')),
('banking_catalog', 'banking_gold', 'fact_loan_performance', 'current_balance', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'LOAN')),
('banking_catalog', 'banking_gold', 'fact_loan_performance', 'monthly_payment_amount', 12, 'FINANCIAL_BALANCE', 'PAYMENT', 'MEDIUM', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'LOAN')),

-- Credit Card Usage Fact
('banking_catalog', 'banking_gold', 'fact_credit_card_usage', 'card_id', 11, 'FINANCIAL_CARD', 'CARD_ID', 'CRITICAL', 'FINANCIAL', FALSE, 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'FINANCIAL', 'CARD')),
('banking_catalog', 'banking_gold', 'fact_credit_card_usage', 'credit_limit', 12, 'FINANCIAL_BALANCE', 'CREDIT_LIMIT', 'HIGH', 'FINANCIAL', FALSE, 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'CREDIT'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer PII Tags

-- COMMAND ----------

INSERT INTO banking_catalog.security.pii_column_registry 
(catalog_name, schema_name, table_name, column_name, classification_id, classification_name, 
 pii_type, sensitivity_level, gdpr_category, data_type, requires_consent, retention_days, 
 discovery_method, confidence_score, tags) VALUES

-- Bronze Customers
('banking_catalog', 'banking_bronze', 'customers', 'ssn_last_4', 5, 'PII_SSN', 'SSN', 'CRITICAL', 'SPECIAL_CATEGORY', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'customers', 'email', 6, 'PII_EMAIL', 'EMAIL', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'customers', 'phone', 7, 'PII_PHONE', 'PHONE', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'customers', 'address', 8, 'PII_ADDRESS', 'ADDRESS', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'customers', 'first_name', 9, 'PII_NAME', 'NAME', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'customers', 'last_name', 9, 'PII_NAME', 'NAME', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),

-- Bronze Accounts
('banking_catalog', 'banking_bronze', 'accounts', 'account_number', 10, 'FINANCIAL_ACCOUNT', 'ACCOUNT_NUMBER', 'CRITICAL', 'FINANCIAL', 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'accounts', 'routing_number', 10, 'FINANCIAL_ACCOUNT', 'ROUTING_NUMBER', 'HIGH', 'FINANCIAL', 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BRONZE', 'RAW')),
('banking_catalog', 'banking_bronze', 'accounts', 'balance', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'BRONZE', 'RAW'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer PII Tags

-- COMMAND ----------

INSERT INTO banking_catalog.security.pii_column_registry 
(catalog_name, schema_name, table_name, column_name, classification_id, classification_name, 
 pii_type, sensitivity_level, gdpr_category, data_type, requires_consent, retention_days, 
 discovery_method, confidence_score, tags) VALUES

-- Silver Customers Clean
('banking_catalog', 'banking_silver', 'customers_clean', 'ssn_last_4_masked', 5, 'PII_SSN', 'SSN', 'CRITICAL', 'SPECIAL_CATEGORY', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'SILVER', 'MASKED')),
('banking_catalog', 'banking_silver', 'customers_clean', 'email', 6, 'PII_EMAIL', 'EMAIL', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'SILVER', 'CLEANED')),
('banking_catalog', 'banking_silver', 'customers_clean', 'phone', 7, 'PII_PHONE', 'PHONE', 'HIGH', 'PERSONAL_DATA', 'STRING', TRUE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'SILVER', 'CLEANED')),

-- Silver Accounts Clean
('banking_catalog', 'banking_silver', 'accounts_clean', 'account_number', 10, 'FINANCIAL_ACCOUNT', 'ACCOUNT_NUMBER', 'CRITICAL', 'FINANCIAL', 'STRING', FALSE, 2555, 'MANUAL', 1.0, ARRAY('PII', 'SILVER', 'CLEANED')),
('banking_catalog', 'banking_silver', 'accounts_clean', 'balance', 12, 'FINANCIAL_BALANCE', 'BALANCE', 'HIGH', 'FINANCIAL', 'DOUBLE', FALSE, 2555, 'MANUAL', 1.0, ARRAY('FINANCIAL', 'SILVER', 'CLEANED'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create PII Discovery Functions

-- COMMAND ----------

-- Function to check if column contains PII
CREATE OR REPLACE FUNCTION is_pii_column(
    p_catalog STRING,
    p_schema STRING,
    p_table STRING,
    p_column STRING
)
RETURNS BOOLEAN
RETURN (
    SELECT COUNT(*) > 0
    FROM banking_catalog.security.pii_column_registry
    WHERE catalog_name = p_catalog
      AND schema_name = p_schema
      AND table_name = p_table
      AND column_name = p_column
      AND is_active = TRUE
);

-- COMMAND ----------

-- Function to get PII classification for a column
CREATE OR REPLACE FUNCTION get_pii_classification(
    p_catalog STRING,
    p_schema STRING,
    p_table STRING,
    p_column STRING
)
RETURNS STRING
RETURN (
    SELECT classification_name
    FROM banking_catalog.security.pii_column_registry
    WHERE catalog_name = p_catalog
      AND schema_name = p_schema
      AND table_name = p_table
      AND column_name = p_column
      AND is_active = TRUE
    LIMIT 1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PII Column Tags (Unity Catalog Tags)

-- COMMAND ----------

-- Apply Unity Catalog tags to PII columns
-- Tag: PII Classification
ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN ssn_last_4 SET TAGS ('PII_TYPE' = 'SSN', 'SENSITIVITY' = 'CRITICAL', 'GDPR' = 'SPECIAL_CATEGORY');

ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN email SET TAGS ('PII_TYPE' = 'EMAIL', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'PERSONAL_DATA');

ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN phone SET TAGS ('PII_TYPE' = 'PHONE', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'PERSONAL_DATA');

ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN address SET TAGS ('PII_TYPE' = 'ADDRESS', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'PERSONAL_DATA');

ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN first_name SET TAGS ('PII_TYPE' = 'NAME', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'PERSONAL_DATA');

ALTER TABLE banking_catalog.banking_gold.dim_customer 
ALTER COLUMN last_name SET TAGS ('PII_TYPE' = 'NAME', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'PERSONAL_DATA');

ALTER TABLE banking_catalog.banking_gold.dim_account 
ALTER COLUMN account_number SET TAGS ('PII_TYPE' = 'ACCOUNT_NUMBER', 'SENSITIVITY' = 'CRITICAL', 'GDPR' = 'FINANCIAL');

ALTER TABLE banking_catalog.banking_gold.dim_account 
ALTER COLUMN balance SET TAGS ('PII_TYPE' = 'BALANCE', 'SENSITIVITY' = 'HIGH', 'GDPR' = 'FINANCIAL');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PII Summary Views

-- COMMAND ----------

-- View: All PII columns by table
CREATE OR REPLACE VIEW banking_catalog.security.pii_columns_by_table AS
SELECT 
    catalog_name,
    schema_name,
    table_name,
    COUNT(*) as pii_column_count,
    collect_list(column_name) as pii_columns,
    collect_list(pii_type) as pii_types,
    MAX(sensitivity_level) as highest_sensitivity,
    CASE WHEN MAX(gdpr_special_category) THEN 'YES' ELSE 'NO' END as contains_special_category
FROM banking_catalog.security.pii_column_registry
WHERE is_active = TRUE
GROUP BY catalog_name, schema_name, table_name
ORDER BY pii_column_count DESC;

-- COMMAND ----------

-- View: PII by classification
CREATE OR REPLACE VIEW banking_catalog.security.pii_by_classification AS
SELECT 
    classification_name,
    pii_type,
    sensitivity_level,
    gdpr_category,
    COUNT(*) as column_count,
    collect_list(CONCAT(schema_name, '.', table_name, '.', column_name)) as columns
FROM banking_catalog.security.pii_column_registry
WHERE is_active = TRUE
GROUP BY classification_name, pii_type, sensitivity_level, gdpr_category
ORDER BY sensitivity_level DESC, column_count DESC;

-- COMMAND ----------

-- View: GDPR Special Category Data
CREATE OR REPLACE VIEW banking_catalog.security.gdpr_special_category_data AS
SELECT 
    catalog_name,
    schema_name,
    table_name,
    column_name,
    pii_type,
    gdpr_category,
    requires_consent,
    retention_days
FROM banking_catalog.security.pii_column_registry
WHERE gdpr_special_category = TRUE
  AND is_active = TRUE
ORDER BY sensitivity_level DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test PII Tagging System

-- COMMAND ----------

-- View all PII columns
SELECT * FROM banking_catalog.security.pii_columns_by_table;

-- COMMAND ----------

-- View PII by classification
SELECT * FROM banking_catalog.security.pii_by_classification;

-- COMMAND ----------

-- Test PII detection function
SELECT 
    banking_catalog.security.is_pii_column(
        'banking_catalog', 
        'banking_gold', 
        'dim_customer', 
        'ssn_last_4'
    ) as is_pii,
    banking_catalog.security.get_pii_classification(
        'banking_catalog', 
        'banking_gold', 
        'dim_customer', 
        'ssn_last_4'
    ) as classification;

-- COMMAND ----------

SELECT '✅ PII Tagging System implemented successfully!' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PII Statistics

-- COMMAND ----------

SELECT 
    COUNT(DISTINCT CONCAT(catalog_name, schema_name, table_name)) as tables_with_pii,
    COUNT(*) as total_pii_columns,
    COUNT(CASE WHEN sensitivity_level = 'CRITICAL' THEN 1 END) as critical_columns,
    COUNT(CASE WHEN gdpr_special_category THEN 1 END) as special_category_columns,
    COUNT(CASE WHEN requires_consent THEN 1 END) as consent_required_columns
FROM banking_catalog.security.pii_column_registry
WHERE is_active = TRUE;

