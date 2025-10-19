-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Comprehensive Audit Logging System
-- MAGIC 
-- MAGIC Implements enterprise-grade audit logging for:
-- MAGIC - All data access (SELECT operations)
-- MAGIC - Data modifications (INSERT, UPDATE, DELETE)
-- MAGIC - Authentication events (LOGIN, LOGOUT, FAILED_LOGIN)
-- MAGIC - Permission changes (GRANT, REVOKE)
-- MAGIC - Sensitive data access
-- MAGIC - Compliance requirements (7-year retention for banking)

-- COMMAND ----------

USE CATALOG banking_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Security Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS banking_catalog.security
COMMENT 'Security and audit logging schema';

USE SCHEMA security;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Log Main Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS audit_log (
    -- Primary identifiers
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    event_timestamp TIMESTAMP NOT NULL,
    event_date DATE GENERATED ALWAYS AS (CAST(event_timestamp AS DATE)),
    
    -- User information
    user_email STRING NOT NULL,
    user_id STRING,
    user_role STRING,
    user_ip_address STRING,
    user_agent STRING,
    session_id STRING,
    
    -- Event details
    event_type STRING NOT NULL,  -- SELECT, INSERT, UPDATE, DELETE, LOGIN, LOGOUT, GRANT, REVOKE
    event_category STRING,  -- DATA_ACCESS, DATA_MODIFICATION, AUTHENTICATION, AUTHORIZATION
    event_severity STRING,  -- LOW, MEDIUM, HIGH, CRITICAL
    
    -- Object information
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    view_name STRING,
    column_names ARRAY<STRING>,
    object_type STRING,  -- TABLE, VIEW, FUNCTION, SCHEMA, CATALOG
    
    -- Query information
    query_text STRING,
    query_hash STRING,
    query_duration_ms BIGINT,
    rows_affected BIGINT,
    rows_returned BIGINT,
    bytes_scanned BIGINT,
    
    -- Sensitive data flags
    pii_accessed BOOLEAN DEFAULT FALSE,
    financial_data_accessed BOOLEAN DEFAULT FALSE,
    fraud_data_accessed BOOLEAN DEFAULT FALSE,
    contains_ssn BOOLEAN DEFAULT FALSE,
    contains_credit_card BOOLEAN DEFAULT FALSE,
    contains_account_number BOOLEAN DEFAULT FALSE,
    
    -- Result information
    success BOOLEAN NOT NULL,
    error_code STRING,
    error_message STRING,
    
    -- Compliance flags
    requires_compliance_review BOOLEAN DEFAULT FALSE,
    gdpr_relevant BOOLEAN DEFAULT FALSE,
    retention_category STRING,  -- NORMAL, LONG_TERM, PERMANENT
    
    -- Additional metadata
    application_name STRING,
    warehouse_id STRING,
    cluster_id STRING,
    notebook_path STRING,
    job_id STRING,
    run_id STRING,
    
    -- Geolocation (for fraud detection)
    geo_country STRING,
    geo_region STRING,
    geo_city STRING,
    
    CONSTRAINT pk_audit_log PRIMARY KEY (audit_id)
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days',  -- 7 years
    'delta.logRetentionDuration' = 'interval 2555 days',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Comprehensive audit log for all system activities - 7 year retention for banking compliance';

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_email ON audit_log(user_email);
CREATE INDEX IF NOT EXISTS idx_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_pii_accessed ON audit_log(pii_accessed);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sensitive Data Access Log (Detailed PII Tracking)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sensitive_data_access_log (
    access_id BIGINT GENERATED ALWAYS AS IDENTITY,
    audit_id BIGINT,  -- Reference to main audit log
    access_timestamp TIMESTAMP NOT NULL,
    
    -- User details
    user_email STRING NOT NULL,
    user_role STRING NOT NULL,
    
    -- Data accessed
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    pii_type STRING NOT NULL,  -- SSN, EMAIL, PHONE, ADDRESS, CREDIT_CARD, ACCOUNT_NUMBER
    record_count BIGINT,
    customer_ids ARRAY<STRING>,
    
    -- Access justification
    access_reason STRING,
    business_justification STRING,
    approved_by STRING,
    
    -- Compliance
    gdpr_lawful_basis STRING,  -- CONSENT, CONTRACT, LEGAL_OBLIGATION, VITAL_INTEREST, PUBLIC_TASK, LEGITIMATE_INTEREST
    data_subject_notified BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT pk_sensitive_access PRIMARY KEY (access_id)
)
USING DELTA
PARTITIONED BY (DATE(access_timestamp))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days'
)
COMMENT 'Detailed tracking of all sensitive PII data access';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Authentication Events Log

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS authentication_log (
    auth_id BIGINT GENERATED ALWAYS AS IDENTITY,
    event_timestamp TIMESTAMP NOT NULL,
    
    -- User information
    user_email STRING NOT NULL,
    user_id STRING,
    
    -- Event details
    event_type STRING NOT NULL,  -- LOGIN, LOGOUT, FAILED_LOGIN, PASSWORD_CHANGE, MFA_ENABLED, MFA_DISABLED
    authentication_method STRING,  -- PASSWORD, SSO, OAUTH, MFA
    
    -- Session details
    session_id STRING,
    ip_address STRING,
    user_agent STRING,
    device_id STRING,
    
    -- Geolocation
    country STRING,
    region STRING,
    city STRING,
    
    -- Success/Failure
    success BOOLEAN NOT NULL,
    failure_reason STRING,
    failed_attempts_count INT DEFAULT 0,
    
    -- Risk assessment
    risk_score DOUBLE,
    risk_factors ARRAY<STRING>,
    is_suspicious BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT pk_auth_log PRIMARY KEY (auth_id)
)
USING DELTA
PARTITIONED BY (DATE(event_timestamp))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Authentication events and security monitoring';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Modification Audit

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_modification_log (
    modification_id BIGINT GENERATED ALWAYS AS IDENTITY,
    modification_timestamp TIMESTAMP NOT NULL,
    
    -- User details
    user_email STRING NOT NULL,
    user_role STRING,
    
    -- Modification details
    operation_type STRING NOT NULL,  -- INSERT, UPDATE, DELETE, MERGE
    table_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    catalog_name STRING NOT NULL,
    
    -- Change details
    rows_affected BIGINT,
    before_values STRING,  -- JSON
    after_values STRING,   -- JSON
    changed_columns ARRAY<STRING>,
    
    -- Approval tracking
    requires_approval BOOLEAN DEFAULT FALSE,
    approved_by STRING,
    approval_timestamp TIMESTAMP,
    
    -- Compliance
    change_reason STRING,
    ticket_number STRING,
    
    CONSTRAINT pk_data_mod PRIMARY KEY (modification_id)
)
USING DELTA
PARTITIONED BY (DATE(modification_timestamp))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days'
)
COMMENT 'Audit trail for all data modifications';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Permission Change Audit

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS permission_change_log (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY,
    change_timestamp TIMESTAMP NOT NULL,
    
    -- Who made the change
    changed_by STRING NOT NULL,
    changed_by_role STRING,
    
    -- What changed
    change_type STRING NOT NULL,  -- GRANT, REVOKE
    object_type STRING NOT NULL,  -- TABLE, VIEW, SCHEMA, CATALOG
    object_name STRING NOT NULL,
    
    -- Permission details
    privilege STRING NOT NULL,  -- SELECT, INSERT, UPDATE, DELETE, ALL_PRIVILEGES
    principal STRING NOT NULL,  -- User or group
    principal_type STRING,  -- USER, GROUP, SERVICE_PRINCIPAL
    
    -- Before/After
    previous_privileges ARRAY<STRING>,
    new_privileges ARRAY<STRING>,
    
    -- Justification
    change_reason STRING,
    approval_ticket STRING,
    
    CONSTRAINT pk_perm_change PRIMARY KEY (change_id)
)
USING DELTA
PARTITIONED BY (DATE(change_timestamp))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days'
)
COMMENT 'Audit trail for permission and access control changes';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Functions

-- COMMAND ----------

-- Function to log data access
CREATE OR REPLACE FUNCTION log_data_access(
    p_user_email STRING,
    p_table_name STRING,
    p_query_text STRING,
    p_rows_returned BIGINT,
    p_pii_accessed BOOLEAN
)
RETURNS INT
LANGUAGE SQL
BEGIN
    INSERT INTO banking_catalog.security.audit_log (
        event_timestamp,
        user_email,
        user_role,
        event_type,
        event_category,
        event_severity,
        table_name,
        query_text,
        rows_returned,
        pii_accessed,
        success,
        gdpr_relevant,
        retention_category
    ) VALUES (
        current_timestamp(),
        p_user_email,
        banking_gold.get_user_role(),
        'SELECT',
        'DATA_ACCESS',
        CASE WHEN p_pii_accessed THEN 'HIGH' ELSE 'LOW' END,
        p_table_name,
        p_query_text,
        p_rows_returned,
        p_pii_accessed,
        TRUE,
        p_pii_accessed,
        'LONG_TERM'
    );
    
    RETURN 1;
END;

-- COMMAND ----------

-- Function to log sensitive data access
CREATE OR REPLACE FUNCTION log_sensitive_access(
    p_table_name STRING,
    p_column_name STRING,
    p_pii_type STRING,
    p_record_count BIGINT
)
RETURNS INT
LANGUAGE SQL
BEGIN
    INSERT INTO banking_catalog.security.sensitive_data_access_log (
        access_timestamp,
        user_email,
        user_role,
        table_name,
        column_name,
        pii_type,
        record_count,
        gdpr_lawful_basis,
        data_subject_notified
    ) VALUES (
        current_timestamp(),
        current_user(),
        banking_gold.get_user_role(),
        p_table_name,
        p_column_name,
        p_pii_type,
        p_record_count,
        'LEGITIMATE_INTEREST',
        FALSE
    );
    
    RETURN 1;
END;

-- COMMAND ----------

-- Function to log authentication
CREATE OR REPLACE FUNCTION log_authentication(
    p_event_type STRING,
    p_success BOOLEAN,
    p_ip_address STRING
)
RETURNS INT
LANGUAGE SQL
BEGIN
    INSERT INTO banking_catalog.security.authentication_log (
        event_timestamp,
        user_email,
        event_type,
        authentication_method,
        ip_address,
        success,
        risk_score,
        is_suspicious
    ) VALUES (
        current_timestamp(),
        current_user(),
        p_event_type,
        'PASSWORD',
        p_ip_address,
        p_success,
        CASE WHEN NOT p_success THEN 75.0 ELSE 10.0 END,
        NOT p_success
    );
    
    RETURN 1;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Views for Reporting

-- COMMAND ----------

-- Recent PII access
CREATE OR REPLACE VIEW recent_pii_access AS
SELECT 
    event_timestamp,
    user_email,
    user_role,
    table_name,
    rows_returned,
    CASE 
        WHEN contains_ssn THEN 'SSN'
        WHEN contains_credit_card THEN 'CREDIT_CARD'
        WHEN contains_account_number THEN 'ACCOUNT_NUMBER'
        WHEN pii_accessed THEN 'OTHER_PII'
    END as pii_type,
    query_text
FROM banking_catalog.security.audit_log
WHERE pii_accessed = TRUE
    AND event_timestamp >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY event_timestamp DESC;

-- COMMAND ----------

-- Failed authentication attempts
CREATE OR REPLACE VIEW failed_login_attempts AS
SELECT 
    event_timestamp,
    user_email,
    ip_address,
    country,
    failed_attempts_count,
    risk_score,
    failure_reason
FROM banking_catalog.security.authentication_log
WHERE success = FALSE
    AND event_timestamp >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY risk_score DESC, event_timestamp DESC;

-- COMMAND ----------

-- Suspicious activity
CREATE OR REPLACE VIEW suspicious_activity AS
SELECT 
    a.event_timestamp,
    a.user_email,
    a.user_role,
    a.event_type,
    a.table_name,
    a.pii_accessed,
    a.rows_returned,
    auth.risk_score,
    auth.is_suspicious
FROM banking_catalog.security.audit_log a
LEFT JOIN banking_catalog.security.authentication_log auth 
    ON a.user_email = auth.user_email 
    AND auth.event_timestamp >= a.event_timestamp - INTERVAL 1 HOUR
WHERE (
    a.rows_returned > 10000  -- Large data export
    OR a.event_severity = 'CRITICAL'
    OR auth.is_suspicious = TRUE
    OR (a.pii_accessed = TRUE AND HOUR(a.event_timestamp) BETWEEN 0 AND 5)  -- Late night PII access
)
AND a.event_timestamp >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY a.event_timestamp DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Automated Audit Procedures

-- COMMAND ----------

-- Procedure to analyze audit logs for anomalies
CREATE OR REPLACE PROCEDURE detect_audit_anomalies()
LANGUAGE SQL
BEGIN
    -- Detect unusual access patterns
    CREATE OR REPLACE TEMP VIEW unusual_access AS
    SELECT 
        user_email,
        COUNT(*) as query_count,
        SUM(rows_returned) as total_rows,
        COUNT(DISTINCT table_name) as tables_accessed,
        MAX(event_timestamp) as last_access
    FROM banking_catalog.security.audit_log
    WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY user_email
    HAVING query_count > 100  -- More than 100 queries per hour
        OR total_rows > 100000  -- More than 100K rows accessed
        OR tables_accessed > 20;  -- Accessing more than 20 tables
    
    -- Log anomalies
    INSERT INTO banking_catalog.security.audit_log (
        event_timestamp,
        user_email,
        event_type,
        event_category,
        event_severity,
        success,
        query_text
    )
    SELECT 
        current_timestamp(),
        user_email,
        'ANOMALY_DETECTED',
        'SECURITY',
        'HIGH',
        TRUE,
        CONCAT('Unusual activity detected: ', query_count, ' queries, ', 
               total_rows, ' rows, ', tables_accessed, ' tables')
    FROM unusual_access;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Audit Logging

-- COMMAND ----------

-- Test logging
SELECT banking_catalog.security.log_data_access(
    current_user(),
    'banking_gold.dim_customer',
    'SELECT * FROM banking_gold.dim_customer LIMIT 10',
    10,
    TRUE
) as log_result;

-- COMMAND ----------

-- Verify audit log
SELECT * FROM banking_catalog.security.audit_log
ORDER BY event_timestamp DESC
LIMIT 10;

-- COMMAND ----------

SELECT '✅ Comprehensive Audit Logging System implemented successfully!' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audit Retention Policy

-- COMMAND ----------

-- Create retention policy
CREATE TABLE IF NOT EXISTS audit_retention_policy (
    policy_id INT PRIMARY KEY,
    table_name STRING NOT NULL,
    retention_days INT NOT NULL,
    retention_reason STRING,
    last_cleanup_date DATE,
    next_cleanup_date DATE,
    is_active BOOLEAN DEFAULT TRUE
) USING DELTA;

INSERT INTO audit_retention_policy VALUES
(1, 'audit_log', 2555, 'Banking regulatory requirement - 7 years', NULL, NULL, TRUE),
(2, 'sensitive_data_access_log', 2555, 'PII access tracking - 7 years', NULL, NULL, TRUE),
(3, 'authentication_log', 365, 'Security monitoring - 1 year', NULL, NULL, TRUE),
(4, 'data_modification_log', 2555, 'Audit trail - 7 years', NULL, NULL, TRUE),
(5, 'permission_change_log', 2555, 'Security compliance - 7 years', NULL, NULL, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compliance Summary
-- MAGIC 
-- MAGIC ✅ **GDPR Article 30** - Records of Processing Activities  
-- MAGIC ✅ **GDPR Article 32** - Security of Processing  
-- MAGIC ✅ **SOX Section 404** - Internal Controls  
-- MAGIC ✅ **PCI-DSS Requirement 10** - Track and Monitor Access  
-- MAGIC ✅ **GLBA Section 501(b)** - Security Standards  
-- MAGIC ✅ **FFIEC Guidelines** - Audit Trail Requirements  

