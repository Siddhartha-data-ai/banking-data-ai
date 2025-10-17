-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Layer Tables
-- MAGIC 
-- MAGIC Creates table definitions for silver layer (cleaned and validated data)
-- MAGIC Note: Most silver tables are created via DLT pipelines, but we can create placeholders here

-- COMMAND ----------

USE CATALOG banking_catalog;
USE SCHEMA banking_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality Results Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_quality_results (
  check STRING,
  table STRING,
  column STRING,
  status STRING,
  actual STRING,
  expected STRING,
  message STRING,
  check_timestamp TIMESTAMP
) USING DELTA
COMMENT 'Data quality monitoring results';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline Monitoring Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pipeline_monitoring (
  timestamp TIMESTAMP,
  bronze_row_count BIGINT,
  silver_row_count BIGINT,
  gold_row_count BIGINT,
  total_tables INT,
  healthy_tables INT,
  error_tables INT,
  overall_status STRING,
  total_account_balance DOUBLE,
  transactions_24h BIGINT,
  fraud_alerts INT
) USING DELTA
COMMENT 'Pipeline health monitoring metrics';

-- COMMAND ----------

-- Show all tables
SHOW TABLES IN banking_catalog.banking_silver;

-- COMMAND ----------

SELECT 'Silver tables created successfully!' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: The main silver tables (customers_clean, accounts_clean, transactions_clean, etc.) 
-- MAGIC are created automatically by the DLT pipelines defined in the src/pipelines directory.

